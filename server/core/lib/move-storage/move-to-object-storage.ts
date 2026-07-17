import { FileStorage, VideoResolution, VideoState, VideoStateType } from '@peertube/peertube-models'
import { logger, LoggerTags, loggerTagsFactory } from '@server/helpers/logger.js'
import { CONFIG } from '@server/initializers/config.js'
import { P2P_MEDIA_LOADER_PEER_VERSION } from '@server/initializers/constants.js'
import { buildCaptionM3U8Content, updateM3U8AndShaPlaylist } from '@server/lib/hls.js'
import {
  BucketInfo,
  checkObjectStorageReadiness,
  generateCaptionObjectStorageKey,
  generateHLSObjectStorageKey,
  generateOriginalVideoObjectStorageKey,
  generateStoryboardObjectStorageKey,
  generateThumbnailObjectStorageKey,
  generateTorrentObjectStorageKey,
  generateWebVideoObjectStorageKey,
  storeHLSFileFromContent,
  storeHLSFileFromFilename,
  storeOriginalVideoFile,
  storeStoryboard,
  storeThumbnail,
  storeTorrentFile,
  storeVideoCaption,
  storeWebVideoFile
} from '@server/lib/object-storage/index.js'
import { getFSTorrentFilePath, getHLSDirectory, getHLSResolutionPlaylistFilename } from '@server/lib/paths.js'
import { updateHLSMasterOnCaptionChange } from '@server/lib/video-captions.js'
import { VideoPathManager } from '@server/lib/video-path-manager.js'
import { moveToFailedMoveToObjectStorageState, moveToNextState } from '@server/lib/video-state.js'
import { updateTorrentMetadata } from '@server/lib/webtorrent.js'
import { StoryboardModel } from '@server/models/video/storyboard.js'
import { ThumbnailModel } from '@server/models/video/thumbnail.js'
import { VideoCaptionModel } from '@server/models/video/video-caption.js'
import { VideoFileModel } from '@server/models/video/video-file.js'
import { VideoModel } from '@server/models/video/video.js'
import { VideoJobInfoModel } from '@server/models/video/video-job-info.js'
import { VideoSourceModel } from '@server/models/video/video-source.js'
import { VideoStreamingPlaylistModel } from '@server/models/video/video-streaming-playlist.js'
import { JobQueue } from '@server/lib/job-queue/index.js'
import { buildMoveVideoJob } from '@server/lib/video-jobs.js'
import {
  MStreamingPlaylistFiles,
  MStreamingPlaylistVideo,
  MVideo,
  MVideoCaption,
  MVideoFile,
  MVideoWithAllFiles,
  MThumbnail,
  MStoryboard
} from '@server/types/models/index.js'
import { MVideoSource } from '@server/types/models/video/video-source.js'
import { pathExists, remove } from 'fs-extra/esm'
import { rmdir, stat } from 'fs/promises'
import { dirname, join, resolve } from 'path'
import PQueue from 'p-queue'
import { Op } from 'sequelize'
import { federateVideoIfNeeded } from '../activitypub/videos/federate.js'
import { moveCaptionToStorage } from './shared/move-caption.js'
import { hasVideoResourcesToBeMoved, moveVideoToStorage, onMoveVideoToStorageFailure } from './shared/move-video.js'

const lTagsBase = loggerTagsFactory('object-storage', 'move-object-storage')
const LOCAL_CLEANUP_RETRY_DELAY_MS = 30_000
const RETAINED_LOCAL_FILES_CLEANUP_START_DELAY_MS = 10 * 60 * 1000
const RETAINED_LOCAL_FILES_CLEANUP_INTERVAL_MS = 30 * 60 * 1000
const RETAINED_LOCAL_FILES_CLEANUP_CONCURRENCY = 2
const RETAINED_LOCAL_FILES_CLEANUP_BATCH_SIZE = 100
const RETAINED_VIDEO_ATTRIBUTES = [ 'id', 'uuid', 'privacy' ]
const RETAINED_VIDEO_FILE_ATTRIBUTES = [ 'id', 'filename', 'torrentFilename', 'videoId', 'videoStreamingPlaylistId' ]
const scheduledLocalFileRemovals = new Set<string>()
let retainedLocalFilesCleanupScheduled = false
let retainedLocalFilesCleanupRunPromise: Promise<{ scheduled: number; skippedMissing: number }> | undefined
const retainedLocalFilesCleanupProgressListeners = new Set<(progress: RetainedLocalFilesCleanupProgress) => void | Promise<void>>()

type RetainedLocalFileCleanupCandidate = {
  path: string
  videoUUID: string
  objectStorageKey: string
  bucketInfo: BucketInfo
}

type RetainedLocalFilesCleanupProgress = {
  currentPhase: string
  processedBatches: number
  discoveredCandidates: number
  uniqueCandidates: number
  scheduled: number
  skippedMissing: number
}

export async function maybeTransitionAfterObjectStorageMove (options: {
  videoUUID: string
  moveVideoState: {
    isNewVideo: boolean
    previousVideoState: VideoStateType
  }
  reason: string
  allowFailedState?: boolean
}) {
  const { videoUUID, moveVideoState, reason, allowFailedState = true } = options

  const videoForStateCheck = await VideoModel.loadFull(videoUUID)
  if (!videoForStateCheck) {
    logger.warn('[MOVE_STORAGE] Video %s not found during state transition (%s)', videoUUID, reason)
    return
  }

  if (videoForStateCheck.state === VideoState.PUBLISHED) {
    await federateVideoIfNeeded(videoForStateCheck, false, undefined)
    logger.info('[MOVE_STORAGE] Video %s is already published; federated without re-queuing object-storage moves', videoUUID)
    return
  }

  const canTransitionFromState = videoForStateCheck.state === VideoState.TO_MOVE_TO_EXTERNAL_STORAGE ||
    (allowFailedState && videoForStateCheck.state === VideoState.TO_MOVE_TO_EXTERNAL_STORAGE_FAILED)

  if (!canTransitionFromState) {
    logger.warn(
      '[MOVE_STORAGE] Skipping state transition after object storage move for video %s in state %s (%s)',
      videoUUID,
      videoForStateCheck.state,
      reason
    )
    return
  }

  if (videoForStateCheck.state === VideoState.TO_MOVE_TO_EXTERNAL_STORAGE_FAILED) {
    const hasRemainingResources = await hasVideoResourcesToBeMoved(videoForStateCheck, FileStorage.OBJECT_STORAGE)
    if (hasRemainingResources) {
      logger.warn(
        '[MOVE_STORAGE] Skipping state transition from failed object-storage move for video %s because resources still need moving (%s)',
        videoUUID,
        reason
      )
      return
    }
  }

  await moveToNextState({ video: { uuid: videoUUID }, ...moveVideoState })
}

export async function moveVideoToObjectStorage (options: {
  videoUUID: string

  moveVideoState?: {
    isNewVideo: boolean
    previousVideoState: VideoStateType
  }

  hlsCutover?: {
    playlistId: number
    fileIds: number[]
  }

  loggerTags: LoggerTags['tags']

  onProgress?: (percent: number) => void
}) {
  const { videoUUID, moveVideoState, hlsCutover, loggerTags, onProgress } = options

  const emitProgress = (() => {
    if (!onProgress) return undefined

    let lastProgress = -1

    return (percent: number) => {
      if (!Number.isFinite(percent)) return

      let normalized = Math.round(percent)
      if (normalized < 0) normalized = 0
      if (normalized > 100) normalized = 100

      if (normalized <= lastProgress) return

      lastProgress = normalized
      onProgress(normalized)
    }
  })()

  // This is the deferred HLS cutover finalization job
  // Just finalize the cutover and return - no state transitions here
  if (hlsCutover) {
    await finalizeInitialHLSCutover({ videoUUID, moveVideoState, hlsCutover, onProgress: emitProgress })
    return
  }

  // Normal move: move files and handle state transitions
  const hlsCutoverDeferred = await moveVideoToStorage({
    videoUUID,
    loggerTags: [ ...lTagsBase().tags, ...loggerTags ],

    targetStorage: FileStorage.OBJECT_STORAGE,

    moveWebVideoFiles,
    moveHLSFiles,
    moveVideoSourceFile,
    moveCaptionFiles,
    moveThumbnailFiles,
    moveStoryboardFiles,
    moveTorrentFiles,
    onInitialHLSCutoverReady: async cutover => {
      emitProgress?.(90) // HLS cutover starting
      const job = await buildMoveVideoJob({
        type: 'move-to-object-storage',
        video: { uuid: videoUUID },
        moveVideoState,
        hlsCutover: cutover,
        isFollowUp: true
      })
      if (job) {
        await JobQueue.Instance.createJob(job)
      }
    },

    onProgress: emitProgress
  })

  if (hlsCutoverDeferred) {
    logger.info(
      '[MOVE_STORAGE] Deferring state transition/federation for video %s until HLS cutover finalization completes.',
      videoUUID,
      lTagsBase(videoUUID)
    )
    return
  }

  // Handle state transitions after normal move completion
  if (moveVideoState) {
    await maybeTransitionAfterObjectStorageMove({ videoUUID, moveVideoState, reason: 'normal move completion' })
  } else {
    const videoFull = await VideoModel.loadFull(videoUUID)
    if (!videoFull) {
      logger.warn('[MOVE_STORAGE] Video %s not found during federation', videoUUID)
      return
    }
    await federateVideoIfNeeded(videoFull, false, undefined)
  }
}

export function moveCaptionToObjectStorage (options: {
  captionId: number
  videoUUID?: string
  includeAllVideoCaptions?: boolean
  loggerTags: LoggerTags['tags']
}) {
  const { captionId, videoUUID, includeAllVideoCaptions, loggerTags } = options

  return moveCaptionToStorage({
    captionId,
    videoUUID,
    includeAllVideoCaptions,
    loggerTags: [ ...lTagsBase().tags, ...loggerTags ],
    moveCaptionFiles
  })
}

// ---------------------------------------------------------------------------
// Granular move functions for independent file/playlist moves
// ---------------------------------------------------------------------------

export async function moveVideoFileToObjectStorage (videoUUID: string, fileId: number) {
  const video = await VideoModel.loadWithFiles(videoUUID)
  if (!video) {
    throw new Error(`Video ${videoUUID} not found`)
  }

  const videoFile = video.VideoFiles.find(f => f.id === fileId)
  if (!videoFile) {
    throw new Error(`Video file ${fileId} not found`)
  }

  if (videoFile.storage === FileStorage.OBJECT_STORAGE) {
    logger.info('[GRANULAR_MOVE] File %s already on object storage, skipping', fileId)
    return
  }

  const sourcePath = VideoPathManager.Instance.getFSVideoFileOutputPath(video, videoFile)
  const destinationKey = `${CONFIG.OBJECT_STORAGE.WEB_VIDEOS.PREFIX || ''}${videoFile.filename}`

  logger.info('[GRANULAR_MOVE] Moving video file %s to object storage', fileId, {
    sourcePath,
    destinationBucket: CONFIG.OBJECT_STORAGE.WEB_VIDEOS.BUCKET_NAME,
    destinationKey,
    ...lTagsBase()
  })

  await storeWebVideoFile(video, videoFile)

  await removeLocalFileAfterMove({
    path: sourcePath,
    videoUUID: video.uuid,
    objectStorageKey: videoFile.filename,
    bucketInfo: CONFIG.OBJECT_STORAGE.WEB_VIDEOS
  })

  videoFile.storage = FileStorage.OBJECT_STORAGE
  // Keep torrent metadata/storage aligned with the new file storage.
  // This uploads the torrent file to object storage (and removes local copy)
  // when object storage is enabled.
  await updateTorrentMetadata(video, videoFile)
  await videoFile.save()

  logger.info('[GRANULAR_MOVE] Video file %s moved successfully', fileId, { ...lTagsBase() })
}

export async function getHLSSegmentFilesToMoveCount (videoUUID: string, playlistId: number, fileIds: number[]) {
  const video = await VideoModel.loadWithFiles(videoUUID)
  if (!video) return 0

  const playlist = video.VideoStreamingPlaylists?.find(p => p.id === playlistId)
  if (!playlist) return 0

  return playlist.VideoFiles.filter(f => fileIds.includes(f.id) && f.storage !== FileStorage.OBJECT_STORAGE).length
}

export async function moveHLSSegmentFilesToObjectStorage (
  videoUUID: string,
  playlistId: number,
  fileIds: number[],
  options: {
    deleteLocalFiles?: boolean
  } = {}
) {
  const { deleteLocalFiles = true } = options

  const video = await VideoModel.loadWithFiles(videoUUID)
  if (!video) {
    throw new Error(`Video ${videoUUID} not found`)
  }

  const playlist = video.VideoStreamingPlaylists?.find(p => p.id === playlistId)
  if (!playlist) {
    throw new Error(`Playlist ${playlistId} not found`)
  }

  const filesToMove = playlist.VideoFiles.filter(f => fileIds.includes(f.id))

  for (const videoFile of filesToMove) {
    if (videoFile.storage === FileStorage.OBJECT_STORAGE) {
      logger.info('[GRANULAR_MOVE] HLS file %s already on object storage, skipping', videoFile.id)
      continue
    }

    const fragmentFilename = videoFile.filename
    const fragmentPath = VideoPathManager.Instance.getFSHLSOutputPath(video, fragmentFilename)
    const playlistFilename = getHLSResolutionPlaylistFilename(fragmentFilename)
    const playlistPath = VideoPathManager.Instance.getFSHLSOutputPath(video, playlistFilename)

    const fragmentExists = await pathExists(fragmentPath)
    const resolutionPlaylistExists = await pathExists(playlistPath)

    if (!fragmentExists) {
      const fragmentReady = await checkObjectStorageReadiness({
        key: generateHLSObjectStorageKey(video, fragmentFilename),
        bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS,
        maxRetries: 1,
        retryIntervalMs: 0,
        logNotReadyAsDebug: true
      })

      if (!fragmentReady) {
        throw new Error(`HLS fragment ${fragmentFilename} is missing locally and not ready in object storage`)
      }
    }

    if (!resolutionPlaylistExists) {
      const resolutionPlaylistReady = await checkObjectStorageReadiness({
        key: generateHLSObjectStorageKey(video, playlistFilename),
        bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS,
        maxRetries: 1,
        retryIntervalMs: 0,
        logNotReadyAsDebug: true
      })

      if (!resolutionPlaylistReady) {
        throw new Error(`HLS resolution playlist ${playlistFilename} is missing locally and not ready in object storage`)
      }
    }

    if (!fragmentExists && !resolutionPlaylistExists) {
      logger.info(
        '[GRANULAR_MOVE] Both fragment and resolution playlist missing for file %s but object storage copies are ready, ' +
          'marking as OBJECT_STORAGE',
        videoFile.id
      )
      videoFile.storage = FileStorage.OBJECT_STORAGE
      await videoFile.save()
      continue
    }

    // Step 1: Upload fragment if it exists locally
    if (fragmentExists) {
      const fragmentDestKey = `${CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS.PREFIX || ''}hls/${videoUUID}/${fragmentFilename}`
      logger.info('[GRANULAR_MOVE] Uploading HLS fragment %s', fragmentFilename, {
        sourcePath: fragmentPath,
        destinationBucket: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS.BUCKET_NAME,
        destinationKey: fragmentDestKey,
        ...lTagsBase()
      })
      await storeHLSFileFromFilename(video, fragmentFilename)
      logger.info('[GRANULAR_MOVE] HLS fragment %s uploaded to object storage', fragmentFilename, { ...lTagsBase() })
    }

    // Step 2: Upload resolution playlist if it exists locally
    if (resolutionPlaylistExists) {
      const playlistDestKey = `${CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS.PREFIX || ''}hls/${videoUUID}/${playlistFilename}`
      logger.info('[GRANULAR_MOVE] Uploading HLS resolution playlist %s', playlistFilename, {
        sourcePath: playlistPath,
        destinationBucket: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS.BUCKET_NAME,
        destinationKey: playlistDestKey,
        ...lTagsBase()
      })
      await storeHLSFileFromFilename(video, playlistFilename)
      logger.info('[GRANULAR_MOVE] HLS resolution playlist %s uploaded to object storage', playlistFilename, { ...lTagsBase() })
    }

    // Step 3: Delete local files only when requested by the caller.
    // Granular workflow can keep local files for deferred cleanup.
    if (deleteLocalFiles) {
      if (fragmentExists) {
        await removeLocalFileAfterMove({
          path: fragmentPath,
          videoUUID: video.uuid,
          objectStorageKey: generateHLSObjectStorageKey(video, fragmentFilename),
          bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS,
          skipReadinessCheck: true
        })
        logger.info('[GRANULAR_MOVE] Scheduled local fragment cleanup %s', fragmentPath, { ...lTagsBase() })
      }
      if (resolutionPlaylistExists) {
        await removeLocalFileAfterMove({
          path: playlistPath,
          videoUUID: video.uuid,
          objectStorageKey: generateHLSObjectStorageKey(video, playlistFilename),
          bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS,
          skipReadinessCheck: true
        })
        logger.info('[GRANULAR_MOVE] Scheduled local resolution playlist cleanup %s', playlistPath, { ...lTagsBase() })
      }
    }

    videoFile.storage = FileStorage.OBJECT_STORAGE
    // Keep torrent metadata/storage aligned with the new file storage.
    await updateTorrentMetadata(playlist.withVideo(video), videoFile)
    await videoFile.save()
    logger.info('[GRANULAR_MOVE] HLS segment file %s marked as OBJECT_STORAGE', videoFile.id, { ...lTagsBase() })
  }

  // Note: Do NOT set playlist.storage = OBJECT_STORAGE here yet.
  // The playlist.storage will be set to OBJECT_STORAGE only after the master playlist
  // is successfully uploaded to object storage. This ensures video players can find
  // the master playlist when they request it from object storage.
}

// Pure upload: uploads pre-generated master playlist + sha from LOCAL filesystem.
// Regeneration must be done by the caller BEFORE calling this function.
export async function moveMasterPlaylistToObjectStorage (videoUUID: string, playlistId: number) {
  const video = await VideoModel.loadWithFiles(videoUUID)
  if (!video) {
    throw new Error(`Video ${videoUUID} not found`)
  }

  const playlist = video.VideoStreamingPlaylists?.find(p => p.id === playlistId)
  if (!playlist) {
    throw new Error(`Playlist ${playlistId} not found`)
  }

  const masterPlaylistFilename = playlist.playlistFilename
  const masterPath = VideoPathManager.Instance.getFSHLSOutputPath(video, masterPlaylistFilename)
  const segmentsSha256Filename = getSegmentsSha256FilenameToMove(playlist)
  const shaPath = segmentsSha256Filename
    ? VideoPathManager.Instance.getFSHLSOutputPath(video, segmentsSha256Filename)
    : undefined

  if (!(await pathExists(masterPath))) {
    throw new Error(`Master playlist not found at ${masterPath}`)
  }
  if (shaPath && !(await pathExists(shaPath))) {
    throw new Error(`SHA file not found at ${shaPath}`)
  }

  logger.info('[GRANULAR_MOVE] Uploading master playlist to object storage', {
    videoUUID,
    playlistFilename: masterPlaylistFilename,
    segmentsSha256Filename,
    destinationBucket: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS.BUCKET_NAME,
    playlistStorageAlreadySet: playlist.storage === FileStorage.OBJECT_STORAGE,
    ...lTagsBase()
  })

  await storeHLSFileFromFilename(video, masterPlaylistFilename)
  if (segmentsSha256Filename) {
    await storeHLSFileFromFilename(video, segmentsSha256Filename)
  }

  // Note: playlist.storage is already set to OBJECT_STORAGE by the caller before regenerating
  // We don't need to set it again here
  logger.info('[GRANULAR_MOVE] HLS playlist %s master playlist uploaded to object storage', playlistId, { ...lTagsBase() })
}

export async function moveThumbnailToObjectStorage (videoUUID: string, thumbnailId: number) {
  const video = await VideoModel.loadWithFiles(videoUUID)
  if (!video) {
    throw new Error(`Video ${videoUUID} not found`)
  }

  const thumbnail = video.Thumbnails?.find(t => t.id === thumbnailId)
  if (!thumbnail) {
    throw new Error(`Thumbnail ${thumbnailId} not found`)
  }

  if (thumbnail.storage === FileStorage.OBJECT_STORAGE) {
    logger.info('[GRANULAR_MOVE] Thumbnail %s already on object storage, skipping', thumbnailId)
    return
  }

  const sourcePath = thumbnail.getFSPath()
  const destinationKey = `${CONFIG.OBJECT_STORAGE.THUMBNAILS.PREFIX || ''}${thumbnail.filename}`

  logger.info('[GRANULAR_MOVE] Moving thumbnail %s to object storage', thumbnailId, {
    sourcePath,
    destinationBucket: CONFIG.OBJECT_STORAGE.THUMBNAILS.BUCKET_NAME,
    destinationKey,
    ...lTagsBase()
  })

  await storeThumbnail(sourcePath, thumbnail.filename)

  await removeLocalFileAfterMove({
    path: sourcePath,
    videoUUID: video.uuid,
    objectStorageKey: thumbnail.filename,
    bucketInfo: CONFIG.OBJECT_STORAGE.THUMBNAILS
  })

  thumbnail.storage = FileStorage.OBJECT_STORAGE
  await thumbnail.save()

  logger.info('[GRANULAR_MOVE] Thumbnail %s moved successfully', thumbnailId, { ...lTagsBase() })
}

export async function isThumbnailMoveNeeded (videoUUID: string, thumbnailId: number) {
  const video = await VideoModel.loadWithFiles(videoUUID)
  if (!video) return false

  const thumbnail = video.Thumbnails?.find(t => t.id === thumbnailId)
  if (!thumbnail) return false

  return thumbnail.storage !== FileStorage.OBJECT_STORAGE
}

export async function onMoveVideoToObjectStorageFailure (options: {
  videoUUID: string
  loggerTags: LoggerTags['tags']
  err: Error
}) {
  const { videoUUID, err, loggerTags } = options

  await onMoveVideoToStorageFailure({
    videoUUID,
    err,
    loggerTags: [ ...lTagsBase().tags, ...loggerTags ],
    moveToFailedState: moveToFailedMoveToObjectStorageState
  })
}

// ---------------------------------------------------------------------------

async function moveVideoSourceFile (source: MVideoSource, video: MVideoWithAllFiles) {
  if (source.storage !== FileStorage.FILE_SYSTEM) return

  const sourcePath = VideoPathManager.Instance.getFSOriginalVideoFilePath(source.keptOriginalFilename)
  const destinationKey = `${CONFIG.OBJECT_STORAGE.ORIGINAL_VIDEO_FILES.PREFIX || ''}${source.keptOriginalFilename}`

  logger.info('[MOVE_STORAGE] Moving original video file to object storage', {
    sourcePath,
    destinationBucket: CONFIG.OBJECT_STORAGE.ORIGINAL_VIDEO_FILES.BUCKET_NAME,
    destinationKey,
    filename: source.keptOriginalFilename,
    ...lTagsBase()
  })

  await storeOriginalVideoFile(sourcePath, source.keptOriginalFilename)

  logger.debug('Checking readiness before marking original video file as moved to object storage', lTagsBase())
  await removeLocalFileAfterMove({
    path: sourcePath,
    videoUUID: video.uuid,
    objectStorageKey: source.keptOriginalFilename,
    bucketInfo: CONFIG.OBJECT_STORAGE.ORIGINAL_VIDEO_FILES
  })

  source.storage = FileStorage.OBJECT_STORAGE
  await source.save()

  logger.info('[MOVE_STORAGE] Original video file moved successfully', {
    sourcePath,
    destinationKey,
    filename: source.keptOriginalFilename,
    ...lTagsBase()
  })
}

// ---------------------------------------------------------------------------

async function moveCaptionFiles (captions: MVideoCaption[], hls: MStreamingPlaylistVideo, video?: MVideoWithAllFiles) {
  const queue = new PQueue({ concurrency: CONFIG.OBJECT_STORAGE.UPLOAD_CONCURRENCY })
  const pipelineVideoUUID = video?.uuid ?? hls?.Video?.uuid

  const results = await queue.addAll(captions.map(caption => async () => {
    let captionUpdated = false

    if (caption.storage === FileStorage.FILE_SYSTEM) {
      const captionPath = caption.getFSFilePath()
      const destinationKey = `${CONFIG.OBJECT_STORAGE.CAPTIONS.PREFIX || ''}${caption.filename}`
      const fileExists = await pathExists(captionPath)

      if (fileExists) {
        logger.info('[MOVE_STORAGE] Moving caption file to object storage', {
          captionId: caption.id,
          sourcePath: captionPath,
          destinationBucket: CONFIG.OBJECT_STORAGE.CAPTIONS.BUCKET_NAME,
          destinationKey,
          filename: caption.filename,
          language: caption.language,
          ...lTagsBase()
        })

        await storeVideoCaption(captionPath, caption.filename)

        logger.debug(`Checking readiness before marking caption file as moved to object storage`, lTagsBase())
        await removeLocalFileAfterMove({
          path: captionPath,
          videoUUID: pipelineVideoUUID ?? (caption as MVideoCaption & { Video?: MVideo }).Video?.uuid,
          objectStorageKey: caption.filename,
          bucketInfo: CONFIG.OBJECT_STORAGE.CAPTIONS
        })

        logger.info('[MOVE_STORAGE] Caption file moved successfully', {
          captionId: caption.id,
          sourcePath: captionPath,
          destinationKey,
          filename: caption.filename,
          ...lTagsBase()
        })
      } else {
        const objectStorageReady = await checkObjectStorageReadiness({
          key: caption.filename,
          bucketInfo: CONFIG.OBJECT_STORAGE.CAPTIONS,
          maxRetries: 1,
          retryIntervalMs: 0,
          logNotReadyAsDebug: true
        })

        if (!objectStorageReady) {
          throw new Error(
            `Caption file ${caption.filename} does not exist at ${captionPath} and object storage copy is not ready`
          )
        }

        logger.warn(
          '[MOVE_STORAGE] Caption file %s is missing locally but object storage copy is ready; marking it as moved',
          caption.filename,
          {
            captionId: caption.id,
            sourcePath: captionPath,
            destinationKey,
            ...lTagsBase()
          }
        )
      }

      // Assign new values before building the m3u8 file
      caption.storage = FileStorage.OBJECT_STORAGE
      await caption.save()
    }

    if (hls) {
      captionUpdated = true

      const m3u8PathToRemove = caption.getFSM3U8Path(hls.Video)

      // Caption file URL has been updated, so we must also update the HLS caption playlist
      const content = await buildCaptionM3U8Content({ video: hls.Video, caption })

      caption.m3u8Filename = VideoCaptionModel.generateM3U8Filename(caption.filename)

      await storeHLSFileFromContent({
        video: hls.Video,
        pathOrFilename: caption.m3u8Filename,
        content
      })

      await caption.save()

      if (m3u8PathToRemove) {
        logger.debug(`Removing video caption playlist file ${m3u8PathToRemove} because it's now on object storage`, lTagsBase())
        const captionM3u8ObjectStorageKey = generateHLSObjectStorageKey(hls.Video, caption.m3u8Filename)
        await removeLocalFileAfterMove({
          path: m3u8PathToRemove,
          videoUUID: hls.Video.uuid,
          objectStorageKey: captionM3u8ObjectStorageKey,
          bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS
        })
      }
    }

    return captionUpdated
  }))

  const hlsUpdated = results.some(r => r === true)

  if (hlsUpdated) {
    await updateHLSMasterOnCaptionChange(hls.Video, hls)
  }
}

// ---------------------------------------------------------------------------

async function moveWebVideoFiles (video: MVideoWithAllFiles) {
  const queue = new PQueue({ concurrency: CONFIG.OBJECT_STORAGE.UPLOAD_CONCURRENCY })

  const filesToMove = video.VideoFiles.filter(f => f.storage === FileStorage.FILE_SYSTEM)

  if (filesToMove.length > 0) {
    logger.info('[MOVE_STORAGE] Starting web video files move to object storage', {
      videoUUID: video.uuid,
      fileCount: filesToMove.length,
      files: filesToMove.map(f => ({
        filename: f.filename,
        resolution: f.resolution,
        size: f.size,
        sourcePath: VideoPathManager.Instance.getFSVideoFileOutputPath(video, f),
        destinationKey: `${CONFIG.OBJECT_STORAGE.WEB_VIDEOS.PREFIX || ''}${f.filename}`,
        destinationBucket: CONFIG.OBJECT_STORAGE.WEB_VIDEOS.BUCKET_NAME
      })),
      ...lTagsBase()
    })
  }

  await queue.addAll(video.VideoFiles.map(file => async () => {
    if (file.storage !== FileStorage.FILE_SYSTEM) return

    const sourcePath = VideoPathManager.Instance.getFSVideoFileOutputPath(video, file)
    const destinationKey = `${CONFIG.OBJECT_STORAGE.WEB_VIDEOS.PREFIX || ''}${file.filename}`

    logger.debug('[MOVE_STORAGE] Moving web video file to object storage', {
      filename: file.filename,
      resolution: file.resolution,
      sourcePath,
      destinationBucket: CONFIG.OBJECT_STORAGE.WEB_VIDEOS.BUCKET_NAME,
      destinationKey,
      ...lTagsBase()
    })

    await storeWebVideoFile(video, file)

    const oldPath = VideoPathManager.Instance.getFSVideoFileOutputPath(video, file)
    await onVideoFileMoved({
      videoOrPlaylist: video,
      file,
      oldPath,
      videoUUID: video.uuid,
      objectStorageKey: file.filename,
      bucketInfo: CONFIG.OBJECT_STORAGE.WEB_VIDEOS
    })

    logger.info('[MOVE_STORAGE] Web video file moved successfully', {
      filename: file.filename,
      resolution: file.resolution,
      sourcePath,
      destinationKey,
      ...lTagsBase()
    })
  }))
}

async function moveHLSFiles (video: MVideoWithAllFiles, options?: {
  onInitialCutoverReady?: (options: { playlistId: number, fileIds: number[] }) => Promise<void>
}): Promise<boolean> {
  const queue = new PQueue({ concurrency: CONFIG.OBJECT_STORAGE.UPLOAD_CONCURRENCY })
  let initialCutoverDeferred = false

  for (const playlist of video.VideoStreamingPlaylists) {

    const filesToMove = playlist.VideoFiles.filter(f => f.storage === FileStorage.FILE_SYSTEM)
    if (filesToMove.length > 0) {
      logger.info('[MOVE_STORAGE] Starting HLS files move to object storage', {
        videoUUID: video.uuid,
        playlistId: playlist.id,
        playlistFilename: playlist.playlistFilename,
        fileCount: filesToMove.length,
        files: filesToMove.map(f => ({
          filename: f.filename,
          resolution: f.resolution,
          size: f.size,
          sourcePath: join(getHLSDirectory(video), f.filename),
          destinationKey: `${CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS.PREFIX || ''}hls/${video.uuid}/${f.filename}`,
          destinationBucket: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS.BUCKET_NAME
        })),
        ...lTagsBase()
      })
    }

    const results = await queue.addAll(playlist.VideoFiles.map(file => async () => {
      if (file.storage !== FileStorage.FILE_SYSTEM) return undefined

      // Resolution playlist
      const playlistFilename = getHLSResolutionPlaylistFilename(file.filename)
      const sourcePlaylistPath = join(getHLSDirectory(video), playlistFilename)
      const destPlaylistKey = `${CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS.PREFIX || ''}hls/${video.uuid}/${playlistFilename}`

      logger.debug('[MOVE_STORAGE] Moving HLS resolution playlist to object storage', {
        filename: playlistFilename,
        sourcePath: sourcePlaylistPath,
        destinationBucket: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS.BUCKET_NAME,
        destinationKey: destPlaylistKey,
        ...lTagsBase()
      })

      await storeHLSFileFromFilename(video, playlistFilename)

      // Resolution fragmented file
      const sourceFragmentPath = join(getHLSDirectory(video), file.filename)
      const destFragmentKey = `${CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS.PREFIX || ''}hls/${video.uuid}/${file.filename}`

      logger.debug('[MOVE_STORAGE] Moving HLS fragment file to object storage', {
        filename: file.filename,
        resolution: file.resolution,
        sourcePath: sourceFragmentPath,
        destinationBucket: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS.BUCKET_NAME,
        destinationKey: destFragmentKey,
        ...lTagsBase()
      })

      await storeHLSFileFromFilename(video, file.filename)

      const oldPath = join(getHLSDirectory(video), file.filename)
      const fragmentObjectStorageKey = generateHLSObjectStorageKey(video, file.filename)
      const resolutionPlaylistObjectStorageKey = generateHLSObjectStorageKey(video, playlistFilename)

      if (playlist.storage === FileStorage.FILE_SYSTEM) {
        return {
          file,
          oldPath,
          sourcePlaylistPath,
          playlistId: playlist.id
        }
      }

      await onVideoFileMoved({
        videoOrPlaylist: Object.assign(playlist, { Video: video }),
        file,
        oldPath,
        videoUUID: video.uuid,
        objectStorageKey: fragmentObjectStorageKey,
        bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS
      })
      await removeLocalFileAfterMove({
        path: sourcePlaylistPath,
        videoUUID: video.uuid,
        objectStorageKey: resolutionPlaylistObjectStorageKey,
        bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS
      })

      logger.info('[MOVE_STORAGE] HLS file moved successfully', {
        filename: file.filename,
        resolution: file.resolution,
        sourcePath: sourceFragmentPath,
        destinationKey: destFragmentKey,
        ...lTagsBase()
      })

      return {
        file,
        oldPath,
        sourcePlaylistPath
      }
    }))

    const movedFiles = results.filter(r => !!r)
    const updatedFile = movedFiles.length !== 0

    if (playlist.storage === FileStorage.FILE_SYSTEM) {
      const segmentsSha256Filename = getSegmentsSha256FilenameToMove(playlist)
      const masterPlaylistSourcePath = join(getHLSDirectory(video), playlist.playlistFilename)
      const masterPlaylistDestKey =
        `${CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS.PREFIX || ''}hls/${video.uuid}/${playlist.playlistFilename}`

      logger.info('[MOVE_STORAGE] Moving HLS master playlist files to object storage', {
        videoUUID: video.uuid,
        playlistFilename: playlist.playlistFilename,
        segmentsSha256Filename,
        sourcePath: masterPlaylistSourcePath,
        destinationBucket: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS.BUCKET_NAME,
        destinationKey: masterPlaylistDestKey,
        ...lTagsBase()
      })

      await storeHLSFileFromFilename(video, playlist.playlistFilename)
      if (segmentsSha256Filename) {
        await storeHLSFileFromFilename(video, segmentsSha256Filename)
      }
      const cutoverFileIds = movedFiles.map(moved => moved.file.id)

      logger.info('[MOVE_STORAGE] Initial HLS cutover is ready for finalization', {
        videoUUID: video.uuid,
        playlistId: playlist.id,
        fileIds: cutoverFileIds,
        playlistFilename: playlist.playlistFilename,
        ...lTagsBase()
      })

      logger.info('[MOVE_STORAGE] HLS master playlist files moved successfully', {
        videoUUID: video.uuid,
        playlistFilename: playlist.playlistFilename,
        destinationKey: masterPlaylistDestKey,
        ...lTagsBase()
      })

      if (cutoverFileIds.length !== 0 && options?.onInitialCutoverReady) {
        await options.onInitialCutoverReady({
          playlistId: playlist.id,
          fileIds: cutoverFileIds
        })
        initialCutoverDeferred = true
      } else {
        await finalizeHLSPlaylistObjectStorageState({
          video,
          playlist,
          movedFiles: movedFiles.map(moved => moved.file)
        })
      }
    }

    if (updatedFile === true && playlist.storage === FileStorage.OBJECT_STORAGE) {
      const playlistUpdated = await updateM3U8AndShaPlaylist(video, playlist, { throwOnError: true })
      if (!playlistUpdated) {
        throw new Error(`HLS playlist ${playlist.id} was not updated after object-storage move`)
      }
    }

    if (updatedFile === true && playlist.storage !== FileStorage.OBJECT_STORAGE) {
      await playlist.assignP2PMediaLoaderInfoHashes(video, playlist.VideoFiles)
      playlist.p2pMediaLoaderPeerVersion = P2P_MEDIA_LOADER_PEER_VERSION

      await playlist.save()
    }
  }

  try {
    await rmdir(getHLSDirectory(video))
  } catch {
    // Nothing to do, directory may be not empty if there is a transcoding in progress
  }

  return initialCutoverDeferred
}

async function finalizeInitialHLSCutover (options: {
  videoUUID: string
  moveVideoState?: {
    isNewVideo: boolean
    previousVideoState: VideoStateType
  }
  hlsCutover: {
    playlistId: number
    fileIds: number[]
  }
  onProgress?: (percent: number) => void
}) {
  const fileMutexReleaser = await VideoPathManager.Instance.lockFiles(options.videoUUID)

  try {
    return await finalizeInitialHLSCutoverLocked(options)
  } finally {
    fileMutexReleaser()
  }
}

async function finalizeInitialHLSCutoverLocked (options: {
  videoUUID: string
  moveVideoState?: {
    isNewVideo: boolean
    previousVideoState: VideoStateType
  }
  hlsCutover: {
    playlistId: number
    fileIds: number[]
  }
  onProgress?: (percent: number) => void
}) {
  const { videoUUID, moveVideoState, hlsCutover, onProgress } = options

  if (onProgress) onProgress(5)

  if (onProgress) onProgress(20)

  const video = await VideoModel.loadWithFiles(videoUUID)
  if (!video) {
    logger.warn('[MOVE_STORAGE] Video %s not found during HLS cutover finalization', videoUUID)
    await completeSkippedInitialHLSCutover({
      videoUUID,
      moveVideoState,
      onProgress,
      reason: 'HLS cutover finalization skipped because video is missing'
    })
    return
  }

  const playlist = video.VideoStreamingPlaylists.find(p => p.id === hlsCutover.playlistId)
  if (!playlist) {
    logger.warn('[MOVE_STORAGE] Playlist %s not found during HLS cutover finalization', hlsCutover.playlistId)
    await completeSkippedInitialHLSCutover({
      videoUUID,
      moveVideoState,
      onProgress,
      reason: `HLS cutover finalization skipped because playlist ${hlsCutover.playlistId} is missing`
    })
    return
  }

  const movedFiles = playlist.VideoFiles.filter(file => hlsCutover.fileIds.includes(file.id))

  await finalizeHLSPlaylistObjectStorageState({ video, playlist, movedFiles, onProgress })

  logger.info('[MOVE_STORAGE] Initial HLS cutover finalized for video %s', videoUUID)

  // Decrement pendingMove and check if all move work is done
  const pendingMove = await VideoJobInfoModel.decrease(videoUUID, 'pendingMove')
  logger.info('[MOVE_STORAGE] pendingMove after HLS cutover finalization for %s: %d', videoUUID, pendingMove)

  // Only transition state when ALL move work is complete (pendingMove reaches 0)
  if (pendingMove === 0) {
    logger.info('[MOVE_STORAGE] All move work complete for %s, transitioning state', videoUUID)

    if (onProgress) onProgress(95)

    if (moveVideoState) {
      await maybeTransitionAfterObjectStorageMove({ videoUUID, moveVideoState, reason: 'HLS cutover finalization' })
    } else {
      const videoFull = await VideoModel.loadFull(videoUUID)
      if (videoFull) await federateVideoIfNeeded(videoFull, false, undefined)
    }
  } else {
    logger.info('[MOVE_STORAGE] More move work pending for %s (pendingMove: %d), deferring state transition', videoUUID, pendingMove)
  }

  if (onProgress) onProgress(100)
}

async function completeSkippedInitialHLSCutover (options: {
  videoUUID: string
  moveVideoState?: {
    isNewVideo: boolean
    previousVideoState: VideoStateType
  }
  onProgress?: (percent: number) => void
  reason: string
}) {
  const { videoUUID, moveVideoState, onProgress, reason } = options

  const pendingMove = await VideoJobInfoModel.decrease(videoUUID, 'pendingMove')
  logger.info('[MOVE_STORAGE] pendingMove after skipped HLS cutover finalization for %s: %d', videoUUID, pendingMove)

  if (pendingMove === 0) {
    if (onProgress) onProgress(95)

    if (moveVideoState) {
      await maybeTransitionAfterObjectStorageMove({ videoUUID, moveVideoState, reason })
    } else {
      const videoFull = await VideoModel.loadFull(videoUUID)
      if (videoFull) await federateVideoIfNeeded(videoFull, false, undefined)
    }
  }

  if (onProgress) onProgress(100)
}

async function finalizeHLSPlaylistObjectStorageState (options: {
  video: MVideoWithAllFiles
  playlist: MStreamingPlaylistFiles
  movedFiles: MVideoFile[]
  onProgress?: (percent: number) => void
}) {
  const { video, playlist, movedFiles, onProgress } = options

  playlist.storage = FileStorage.OBJECT_STORAGE

  const totalFiles = movedFiles.length
  for (let i = 0; i < movedFiles.length; i++) {
    const file = movedFiles[i]
    file.storage = FileStorage.OBJECT_STORAGE
    await updateTorrentMetadata(Object.assign(playlist, { Video: video }), file)
    await file.save()

    // Update progress (20-80% for file processing)
    if (onProgress && totalFiles !== 0) {
      const fileProgress = 20 + Math.floor(((i + 1) / totalFiles) * 60)
      onProgress(fileProgress)
    }
  }

  await playlist.assignP2PMediaLoaderInfoHashes(video, playlist.VideoFiles)
  playlist.p2pMediaLoaderPeerVersion = P2P_MEDIA_LOADER_PEER_VERSION
  await playlist.save()
  await scheduleMovedHLSLocalCleanup({ video, playlist, movedFiles })

  if (onProgress) onProgress(90)
}

async function onVideoFileMoved (options: {
  videoOrPlaylist: MVideo | MStreamingPlaylistVideo
  file: MVideoFile
  oldPath: string
  videoUUID: string
  objectStorageKey: string
  bucketInfo: BucketInfo
}) {
  const { videoOrPlaylist, file, oldPath, videoUUID, objectStorageKey, bucketInfo } = options

  logger.debug('Checking readiness before marking file as moved to object storage', {
    filename: file.filename,
    sourcePath: oldPath,
    destinationBucket: bucketInfo.BUCKET_NAME,
    destinationKey: objectStorageKey,
    ...lTagsBase()
  })

  await removeLocalFileAfterMove({
    path: oldPath,
    videoUUID,
    objectStorageKey,
    bucketInfo
  })

  file.storage = FileStorage.OBJECT_STORAGE

  await updateTorrentMetadata(videoOrPlaylist, file)
  await file.save()

  logger.debug('Removed %s after confirming object storage readiness', oldPath, {
    filename: file.filename,
    sourcePath: oldPath,
    destinationKey: objectStorageKey,
    ...lTagsBase()
  })
}

// ---------------------------------------------------------------------------

async function moveThumbnailFiles (thumbnails: MThumbnail[], video: MVideoWithAllFiles) {
  const queue = new PQueue({ concurrency: CONFIG.OBJECT_STORAGE.UPLOAD_CONCURRENCY })

  const filesToMove = thumbnails.filter(t => t.storage === FileStorage.FILE_SYSTEM)
  if (filesToMove.length > 0) {
    logger.info('[MOVE_STORAGE] Starting thumbnails move to object storage', {
      thumbnailCount: filesToMove.length,
      thumbnails: filesToMove.map(t => ({
        filename: t.filename,
        width: t.width,
        height: t.height,
        sourcePath: t.getFSPath(),
        destinationKey: `${CONFIG.OBJECT_STORAGE.THUMBNAILS.PREFIX || ''}${t.filename}`,
        destinationBucket: CONFIG.OBJECT_STORAGE.THUMBNAILS.BUCKET_NAME
      })),
      ...lTagsBase()
    })
  }

  await queue.addAll(thumbnails.map(thumbnail => async () => {
    if (thumbnail.storage !== FileStorage.FILE_SYSTEM) return

    const thumbnailPath = thumbnail.getFSPath()
    const destinationKey = `${CONFIG.OBJECT_STORAGE.THUMBNAILS.PREFIX || ''}${thumbnail.filename}`

    logger.debug('[MOVE_STORAGE] Moving thumbnail to object storage', {
      filename: thumbnail.filename,
      width: thumbnail.width,
      height: thumbnail.height,
      sourcePath: thumbnailPath,
      destinationBucket: CONFIG.OBJECT_STORAGE.THUMBNAILS.BUCKET_NAME,
      destinationKey,
      ...lTagsBase()
    })

    await storeThumbnail(thumbnailPath, thumbnail.filename)

    logger.debug(`Checking readiness before marking thumbnail file as moved to object storage`, lTagsBase())
    await removeLocalFileAfterMove({
      path: thumbnailPath,
      videoUUID: video.uuid,
      objectStorageKey: thumbnail.filename,
      bucketInfo: CONFIG.OBJECT_STORAGE.THUMBNAILS
    })

    thumbnail.storage = FileStorage.OBJECT_STORAGE
    await thumbnail.save()

    logger.info('[MOVE_STORAGE] Thumbnail moved successfully', {
      filename: thumbnail.filename,
      width: thumbnail.width,
      height: thumbnail.height,
      sourcePath: thumbnailPath,
      destinationKey,
      ...lTagsBase()
    })
  }))
}

// ---------------------------------------------------------------------------

async function moveStoryboardFiles (storyboards: MStoryboard[], video: MVideoWithAllFiles) {
  const queue = new PQueue({ concurrency: CONFIG.OBJECT_STORAGE.UPLOAD_CONCURRENCY })

  const filesToMove = storyboards.filter(s => s.storage === FileStorage.FILE_SYSTEM)
  if (filesToMove.length > 0) {
    logger.info('[MOVE_STORAGE] Starting storyboards move to object storage', {
      storyboardCount: filesToMove.length,
      storyboards: filesToMove.map(s => ({
        filename: s.filename,
        sourcePath: s.getFSPath(),
        destinationKey: `${CONFIG.OBJECT_STORAGE.STORYBOARDS.PREFIX || ''}${s.filename}`,
        destinationBucket: CONFIG.OBJECT_STORAGE.STORYBOARDS.BUCKET_NAME
      })),
      ...lTagsBase()
    })
  }

  await queue.addAll(storyboards.map(storyboard => async () => {
    if (storyboard.storage !== FileStorage.FILE_SYSTEM) return

    const storyboardPath = storyboard.getFSPath()
    const destinationKey = `${CONFIG.OBJECT_STORAGE.STORYBOARDS.PREFIX || ''}${storyboard.filename}`

    logger.debug('[MOVE_STORAGE] Moving storyboard to object storage', {
      filename: storyboard.filename,
      sourcePath: storyboardPath,
      destinationBucket: CONFIG.OBJECT_STORAGE.STORYBOARDS.BUCKET_NAME,
      destinationKey,
      ...lTagsBase()
    })

    await storeStoryboard(storyboardPath, storyboard.filename)

    logger.debug(`Checking readiness before marking storyboard file as moved to object storage`, lTagsBase())
    await removeLocalFileAfterMove({
      path: storyboardPath,
      videoUUID: video.uuid,
      objectStorageKey: storyboard.filename,
      bucketInfo: CONFIG.OBJECT_STORAGE.STORYBOARDS
    })

    storyboard.storage = FileStorage.OBJECT_STORAGE
    await storyboard.save()

    logger.info('[MOVE_STORAGE] Storyboard moved successfully', {
      filename: storyboard.filename,
      sourcePath: storyboardPath,
      destinationKey,
      ...lTagsBase()
    })
  }))
}

// ---------------------------------------------------------------------------

async function moveTorrentFiles (video: MVideoWithAllFiles) {
  const allFiles = [
    ...video.VideoFiles,
    ...(video.VideoStreamingPlaylists || []).flatMap(p => p.VideoFiles)
  ]

  const queue = new PQueue({ concurrency: CONFIG.OBJECT_STORAGE.UPLOAD_CONCURRENCY })

  const filesToMove = allFiles.filter(f => f.torrentFilename)
  if (filesToMove.length > 0) {
    logger.info('[MOVE_STORAGE] Starting torrent files move to object storage', {
      videoUUID: video.uuid,
      fileCount: filesToMove.length,
      torrents: filesToMove.map(f => ({
        filename: f.torrentFilename,
        sourcePath: join(CONFIG.STORAGE.TORRENTS_DIR, f.torrentFilename),
        destinationKey: `${CONFIG.OBJECT_STORAGE.TORRENTS.PREFIX || ''}${f.torrentFilename}`,
        destinationBucket: CONFIG.OBJECT_STORAGE.TORRENTS.BUCKET_NAME
      })),
      ...lTagsBase()
    })
  }

  await queue.addAll(allFiles.map(file => async () => {
    if (!file.torrentFilename) return

    const torrentPath = join(CONFIG.STORAGE.TORRENTS_DIR, file.torrentFilename)

    // Skip if local torrent was already moved to object storage by updateTorrentMetadata in onVideoFileMoved
    if (!await pathExists(torrentPath)) {
      logger.debug(`Torrent file ${torrentPath} not found locally, already on object storage`, lTagsBase())
      return
    }

    const destinationKey = `${CONFIG.OBJECT_STORAGE.TORRENTS.PREFIX || ''}${file.torrentFilename}`

    try {
      logger.debug('[MOVE_STORAGE] Moving torrent file to object storage', {
        filename: file.torrentFilename,
        sourcePath: torrentPath,
        destinationBucket: CONFIG.OBJECT_STORAGE.TORRENTS.BUCKET_NAME,
        destinationKey,
        ...lTagsBase()
      })

      await storeTorrentFile(torrentPath, file.torrentFilename)

      logger.debug(`Removing torrent file ${torrentPath} because it's now on object storage`, lTagsBase())
      await removeLocalFileAfterMove({
        path: torrentPath,
        videoUUID: video.uuid,
        objectStorageKey: file.torrentFilename,
        bucketInfo: CONFIG.OBJECT_STORAGE.TORRENTS
      })

      logger.info('[MOVE_STORAGE] Torrent file moved successfully', {
        filename: file.torrentFilename,
        sourcePath: torrentPath,
        destinationKey,
        ...lTagsBase()
      })
    } catch (err) {
      logger.warn(`Cannot move torrent file ${torrentPath} to object storage`, { err, ...lTagsBase() })
    }
  }))
}

function getSegmentsSha256FilenameToMove (playlist: { segmentsSha256Filename?: string }) {
  if (CONFIG.OBJECT_STORAGE.GENERATE_SHA256_SEGMENTS === false) return undefined

  return playlist.segmentsSha256Filename
}

export function scheduleRetainedLocalFilesCleanup () {
  if (retainedLocalFilesCleanupScheduled || !CONFIG.OBJECT_STORAGE.ENABLED) return

  retainedLocalFilesCleanupScheduled = true
  scheduleRetainedLocalFilesCleanupTimer(RETAINED_LOCAL_FILES_CLEANUP_START_DELAY_MS)
}

function scheduleRetainedLocalFilesCleanupTimer (delayMs: number) {
  const timer = setTimeout(() => {
    cleanupRetainedLocalFilesAfterRestart()
      .catch(err => logger.warn('Cannot cleanup retained local object-storage files after restart.', { err, ...lTagsBase() }))
      .finally(() => {
        if (!CONFIG.OBJECT_STORAGE.ENABLED) {
          retainedLocalFilesCleanupScheduled = false
          return
        }

        scheduleRetainedLocalFilesCleanupTimer(RETAINED_LOCAL_FILES_CLEANUP_INTERVAL_MS)
      })
  }, delayMs)

  timer.unref?.()
}

export async function cleanupRetainedLocalFilesAfterRestart (options: {
  onProgress?: (progress: RetainedLocalFilesCleanupProgress) => void | Promise<void>
} = {}) {
  if (!CONFIG.OBJECT_STORAGE.ENABLED) return { scheduled: 0, skippedMissing: 0 }

  if (options.onProgress) {
    retainedLocalFilesCleanupProgressListeners.add(options.onProgress)
  }

  try {
    if (retainedLocalFilesCleanupRunPromise === undefined) {
      retainedLocalFilesCleanupRunPromise = performRetainedLocalFilesCleanupAfterRestart()
        .finally(() => {
          retainedLocalFilesCleanupRunPromise = undefined
        })
    }

    return await retainedLocalFilesCleanupRunPromise
  } finally {
    if (options.onProgress) {
      retainedLocalFilesCleanupProgressListeners.delete(options.onProgress)
    }
  }
}

async function performRetainedLocalFilesCleanupAfterRestart () {
  const counts: RetainedLocalFilesCleanupProgress = {
    currentPhase: 'starting',
    processedBatches: 0,
    discoveredCandidates: 0,
    uniqueCandidates: 0,
    scheduled: 0,
    skippedMissing: 0
  }
  const seenCleanupKeys = new Set<string>()

  await emitRetainedLocalFilesCleanupProgress(counts)

  await addRetainedWebVideoFileCandidates(counts, seenCleanupKeys)
  await addRetainedHLSFileCandidates(counts, seenCleanupKeys)
  await addRetainedPlaylistFileCandidates(counts, seenCleanupKeys)
  await addRetainedOriginalFileCandidates(counts, seenCleanupKeys)
  await addRetainedCaptionFileCandidates(counts, seenCleanupKeys)
  await addRetainedThumbnailFileCandidates(counts, seenCleanupKeys)
  await addRetainedStoryboardFileCandidates(counts, seenCleanupKeys)

  counts.currentPhase = 'completed'
  await emitRetainedLocalFilesCleanupProgress(counts)

  if (counts.scheduled !== 0 || counts.skippedMissing !== 0) {
    logger.info(
      'Scheduled cleanup for %d retained local object-storage file(s) after restart ' +
      '(%d missing already, %d unique candidates discovered across %d batch(es)).',
      counts.scheduled,
      counts.skippedMissing,
      counts.uniqueCandidates,
      counts.processedBatches,
      lTagsBase()
    )
  }

  return {
    scheduled: counts.scheduled,
    skippedMissing: counts.skippedMissing
  }
}

async function addRetainedWebVideoFileCandidates (
  counts: RetainedLocalFilesCleanupProgress,
  seenCleanupKeys: Set<string>
) {
  await processRetainedLocalCandidatesInBatches({
    currentPhase: 'web-videos',
    model: VideoFileModel,
    attributes: RETAINED_VIDEO_FILE_ATTRIBUTES,
    where: {
      storage: FileStorage.OBJECT_STORAGE,
      videoId: { [Op.ne]: null },
      filename: { [Op.ne]: null }
    },
    include: [
      {
        attributes: RETAINED_VIDEO_ATTRIBUTES,
        model: VideoModel.unscoped(),
        required: true,
        where: { remote: false }
      }
    ],
    counts,
    seenCleanupKeys,
    buildCandidates: (file: any) => {
      const video = file.Video
      if (!video?.uuid) return []

      const candidates: RetainedLocalFileCleanupCandidate[] = [
        {
          path: VideoPathManager.Instance.getFSVideoFileOutputPath(video, file),
          videoUUID: video.uuid,
          objectStorageKey: generateWebVideoObjectStorageKey(file.filename),
          bucketInfo: CONFIG.OBJECT_STORAGE.WEB_VIDEOS
        }
      ]

      addRetainedTorrentFileCandidate(candidates, file, video.uuid)
      return candidates
    }
  })
}

async function addRetainedHLSFileCandidates (
  counts: RetainedLocalFilesCleanupProgress,
  seenCleanupKeys: Set<string>
) {
  await processRetainedLocalCandidatesInBatches({
    currentPhase: 'hls-files',
    model: VideoFileModel,
    attributes: RETAINED_VIDEO_FILE_ATTRIBUTES,
    where: {
      storage: FileStorage.OBJECT_STORAGE,
      videoStreamingPlaylistId: { [Op.ne]: null },
      filename: { [Op.ne]: null }
    },
    include: [
      {
        attributes: [ 'id', 'videoId' ],
        model: VideoStreamingPlaylistModel.unscoped(),
        required: true,
        include: [
          {
            attributes: RETAINED_VIDEO_ATTRIBUTES,
            model: VideoModel.unscoped(),
            required: true,
            where: { remote: false }
          }
        ]
      }
    ],
    counts,
    seenCleanupKeys,
    buildCandidates: (file: any) => {
      const playlist = file.VideoStreamingPlaylist
      const video = playlist?.Video
      if (!video?.uuid) return []

      const resolutionPlaylistFilename = getHLSResolutionPlaylistFilename(file.filename)
      const candidates: RetainedLocalFileCleanupCandidate[] = [
        {
          path: join(getHLSDirectory(video), file.filename),
          videoUUID: video.uuid,
          objectStorageKey: generateHLSObjectStorageKey(video, file.filename),
          bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS
        },
        {
          path: join(getHLSDirectory(video), resolutionPlaylistFilename),
          videoUUID: video.uuid,
          objectStorageKey: generateHLSObjectStorageKey(video, resolutionPlaylistFilename),
          bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS
        }
      ]

      addRetainedTorrentFileCandidate(candidates, file, video.uuid)
      return candidates
    }
  })
}

async function addRetainedPlaylistFileCandidates (
  counts: RetainedLocalFilesCleanupProgress,
  seenCleanupKeys: Set<string>
) {
  await processRetainedLocalCandidatesInBatches({
    currentPhase: 'playlist-files',
    model: VideoStreamingPlaylistModel,
    attributes: [ 'id', 'playlistFilename', 'segmentsSha256Filename', 'videoId' ],
    where: {
      storage: FileStorage.OBJECT_STORAGE,
      playlistFilename: { [Op.ne]: null }
    },
    include: [
      {
        attributes: RETAINED_VIDEO_ATTRIBUTES,
        model: VideoModel.unscoped(),
        required: true,
        where: { remote: false }
      }
    ],
    counts,
    seenCleanupKeys,
    buildCandidates: (playlist: any) => {
      const video = playlist.Video
      if (!video?.uuid) return []

      const candidates: RetainedLocalFileCleanupCandidate[] = [
        {
          path: VideoPathManager.Instance.getFSHLSOutputPath(video, playlist.playlistFilename),
          videoUUID: video.uuid,
          objectStorageKey: generateHLSObjectStorageKey(video, playlist.playlistFilename),
          bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS
        }
      ]

      const segmentsSha256Filename = getSegmentsSha256FilenameToMove(playlist)
      if (segmentsSha256Filename) {
        candidates.push({
          path: VideoPathManager.Instance.getFSHLSOutputPath(video, segmentsSha256Filename),
          videoUUID: video.uuid,
          objectStorageKey: generateHLSObjectStorageKey(video, segmentsSha256Filename),
          bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS
        })
      }

      return candidates
    }
  })
}

async function addRetainedOriginalFileCandidates (
  counts: RetainedLocalFilesCleanupProgress,
  seenCleanupKeys: Set<string>
) {
  await processRetainedLocalCandidatesInBatches({
    currentPhase: 'original-files',
    model: VideoSourceModel,
    attributes: [ 'id', 'keptOriginalFilename', 'videoId' ],
    where: {
      storage: FileStorage.OBJECT_STORAGE,
      keptOriginalFilename: { [Op.ne]: null }
    },
    include: [
      {
        attributes: RETAINED_VIDEO_ATTRIBUTES,
        model: VideoModel.unscoped(),
        required: true,
        where: { remote: false }
      }
    ],
    counts,
    seenCleanupKeys,
    buildCandidates: (source: any) => {
      const video = source.Video
      if (!video?.uuid) return []

      return [
        {
          path: VideoPathManager.Instance.getFSOriginalVideoFilePath(source.keptOriginalFilename),
          videoUUID: video.uuid,
          objectStorageKey: generateOriginalVideoObjectStorageKey(source.keptOriginalFilename),
          bucketInfo: CONFIG.OBJECT_STORAGE.ORIGINAL_VIDEO_FILES
        }
      ]
    }
  })
}

async function addRetainedCaptionFileCandidates (
  counts: RetainedLocalFilesCleanupProgress,
  seenCleanupKeys: Set<string>
) {
  await processRetainedLocalCandidatesInBatches({
    currentPhase: 'captions',
    model: VideoCaptionModel,
    attributes: [ 'id', 'filename', 'm3u8Filename', 'videoId' ],
    where: {
      storage: FileStorage.OBJECT_STORAGE,
      cached: false,
      filename: { [Op.ne]: null }
    },
    include: [
      {
        attributes: RETAINED_VIDEO_ATTRIBUTES,
        model: VideoModel.unscoped(),
        required: true,
        where: { remote: false }
      }
    ],
    counts,
    seenCleanupKeys,
    buildCandidates: (caption: any) => {
      const video = caption.Video
      if (!video?.uuid) return []

      const candidates: RetainedLocalFileCleanupCandidate[] = [
        {
          path: caption.getFSFilePath(),
          videoUUID: video.uuid,
          objectStorageKey: generateCaptionObjectStorageKey(caption.filename),
          bucketInfo: CONFIG.OBJECT_STORAGE.CAPTIONS
        }
      ]

      if (caption.m3u8Filename) {
        candidates.push({
          path: VideoPathManager.Instance.getFSHLSOutputPath(video, caption.m3u8Filename),
          videoUUID: video.uuid,
          objectStorageKey: generateHLSObjectStorageKey(video, caption.m3u8Filename),
          bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS
        })
      }

      return candidates
    }
  })
}

async function addRetainedThumbnailFileCandidates (
  counts: RetainedLocalFilesCleanupProgress,
  seenCleanupKeys: Set<string>
) {
  await processRetainedLocalCandidatesInBatches({
    currentPhase: 'thumbnails',
    model: ThumbnailModel,
    attributes: [ 'id', 'filename', 'videoId' ],
    where: {
      storage: FileStorage.OBJECT_STORAGE,
      cached: false,
      videoId: { [Op.ne]: null },
      filename: { [Op.ne]: null }
    },
    include: [
      {
        attributes: RETAINED_VIDEO_ATTRIBUTES,
        model: VideoModel.unscoped(),
        required: true,
        where: { remote: false }
      }
    ],
    counts,
    seenCleanupKeys,
    buildCandidates: (thumbnail: any) => {
      const video = thumbnail.Video
      if (!video?.uuid) return []

      return [
        {
          path: thumbnail.getFSPath(),
          videoUUID: video.uuid,
          objectStorageKey: generateThumbnailObjectStorageKey(thumbnail.filename),
          bucketInfo: CONFIG.OBJECT_STORAGE.THUMBNAILS
        }
      ]
    }
  })
}

async function addRetainedStoryboardFileCandidates (
  counts: RetainedLocalFilesCleanupProgress,
  seenCleanupKeys: Set<string>
) {
  await processRetainedLocalCandidatesInBatches({
    currentPhase: 'storyboards',
    model: StoryboardModel,
    attributes: [ 'id', 'filename', 'videoId' ],
    where: {
      storage: FileStorage.OBJECT_STORAGE,
      cached: false,
      filename: { [Op.ne]: null }
    },
    include: [
      {
        attributes: RETAINED_VIDEO_ATTRIBUTES,
        model: VideoModel.unscoped(),
        required: true,
        where: { remote: false }
      }
    ],
    counts,
    seenCleanupKeys,
    buildCandidates: (storyboard: any) => {
      const video = storyboard.Video
      if (!video?.uuid) return []

      return [
        {
          path: storyboard.getFSPath(),
          videoUUID: video.uuid,
          objectStorageKey: generateStoryboardObjectStorageKey(storyboard.filename),
          bucketInfo: CONFIG.OBJECT_STORAGE.STORYBOARDS
        }
      ]
    }
  })
}

function addRetainedTorrentFileCandidate (
  candidates: RetainedLocalFileCleanupCandidate[],
  file: { torrentFilename?: string },
  videoUUID: string
) {
  if (!file.torrentFilename) return

  candidates.push({
    path: getFSTorrentFilePath(file as MVideoFile),
    videoUUID,
    objectStorageKey: generateTorrentObjectStorageKey(file.torrentFilename),
    bucketInfo: CONFIG.OBJECT_STORAGE.TORRENTS
  })
}

async function processRetainedLocalCandidatesInBatches (options: {
  currentPhase: string
  model: any
  attributes: string[]
  where: Record<string, any>
  include: any[]
  counts: RetainedLocalFilesCleanupProgress
  seenCleanupKeys: Set<string>
  buildCandidates: (row: any) => RetainedLocalFileCleanupCandidate[]
}) {
  let lastId = 0

  while (true) {
    const rows = await options.model.unscoped().findAll({
      attributes: options.attributes,
      where: {
        ...options.where,
        id: { [Op.gt]: lastId }
      },
      include: options.include,
      order: [ [ 'id', 'ASC' ] ],
      limit: RETAINED_LOCAL_FILES_CLEANUP_BATCH_SIZE
    })

    if (rows.length === 0) return

    const candidates: RetainedLocalFileCleanupCandidate[] = []
    for (const row of rows as any[]) {
      candidates.push(...options.buildCandidates(row))
    }

    options.counts.currentPhase = options.currentPhase
    await processRetainedLocalFileCleanupCandidateBatch({
      candidates,
      counts: options.counts,
      seenCleanupKeys: options.seenCleanupKeys
    })

    options.counts.processedBatches++
    await emitRetainedLocalFilesCleanupProgress(options.counts)

    lastId = rows[rows.length - 1].id
  }
}

async function processRetainedLocalFileCleanupCandidateBatch (options: {
  candidates: RetainedLocalFileCleanupCandidate[]
  counts: RetainedLocalFilesCleanupProgress
  seenCleanupKeys: Set<string>
}) {
  const { candidates, counts, seenCleanupKeys } = options
  const uniqueCandidates: RetainedLocalFileCleanupCandidate[] = []

  for (const candidate of candidates) {
    counts.discoveredCandidates++

    if (!candidate.path || !candidate.videoUUID || !candidate.objectStorageKey) continue

    const cleanupKey = `${candidate.videoUUID}:${resolve(candidate.path)}`
    if (seenCleanupKeys.has(cleanupKey)) continue

    seenCleanupKeys.add(cleanupKey)
    counts.uniqueCandidates++
    uniqueCandidates.push(candidate)
  }

  if (uniqueCandidates.length === 0) return

  const queue = new PQueue({ concurrency: RETAINED_LOCAL_FILES_CLEANUP_CONCURRENCY })
  await queue.addAll(uniqueCandidates.map(candidate => async () => {
    const delayMs = await getRemainingRetainedLocalFileCleanupDelay(candidate.path)
    if (delayMs === undefined) {
      counts.skippedMissing++
      return
    }

    const scheduled = scheduleLocalFileRemovalAfterActiveFileWork({
      path: candidate.path,
      videoUUID: candidate.videoUUID,
      delayMs
    })

    if (scheduled) counts.scheduled++
  }))
}

async function emitRetainedLocalFilesCleanupProgress (progress: RetainedLocalFilesCleanupProgress) {
  if (retainedLocalFilesCleanupProgressListeners.size === 0) return

  const snapshot = { ...progress }
  for (const listener of retainedLocalFilesCleanupProgressListeners) {
    try {
      await listener(snapshot)
    } catch (err) {
      logger.warn('Cannot report retained local file cleanup progress.', { err, ...lTagsBase() })
    }
  }
}

async function getRemainingRetainedLocalFileCleanupDelay (path: string) {
  try {
    const stats = await stat(path)
    if (!stats.isFile()) return undefined

    return buildRetainedLocalFileCleanupDelay({
      keepLocalFileAfterMoveMs: CONFIG.OBJECT_STORAGE.KEEP_LOCAL_FILE_AFTER_MOVE,
      mtimeMs: stats.mtimeMs,
      nowMs: Date.now()
    })
  } catch (err) {
    if ((err as { code?: string })?.code === 'ENOENT') return undefined

    throw err
  }
}

export function buildRetainedLocalFileCleanupDelay (options: {
  keepLocalFileAfterMoveMs: number
  mtimeMs: number
  nowMs: number
}) {
  const keepLocalFileAfterMoveMs = Math.max(0, options.keepLocalFileAfterMoveMs || 0)
  const ageMs = Math.max(0, options.nowMs - options.mtimeMs)

  return Math.max(0, keepLocalFileAfterMoveMs - ageMs)
}

async function removeLocalPathNow (path: string) {
  if (!await pathExists(path)) return

  await remove(path)
  await removeParentDirIfEmpty(path)
}

export async function removeLocalFileAfterMove (options: {
  path: string
  videoUUID?: string
  objectStorageKey?: string
  bucketInfo?: BucketInfo
  skipReadinessCheck?: boolean
  waitForPipelineCompletion?: boolean
}) {
  const {
    path,
    videoUUID,
    waitForPipelineCompletion = true
  } = options
  const delayMs = CONFIG.OBJECT_STORAGE.KEEP_LOCAL_FILE_AFTER_MOVE

  if (videoUUID) {
    scheduleLocalFileRemovalAfterActiveFileWork({
      path,
      videoUUID,
      delayMs,
      waitForPipelineCompletion
    })
    return
  }

  if (!delayMs) {
    await removeLocalPathNow(path)
    return
  }

  logger.info(
    'Keeping local file %s for %d ms after moving to object storage before deletion.',
    path,
    delayMs,
    lTagsBase()
  )

  const timer = setTimeout(() => {
    removeLocalPathNow(path)
      .then(() => logger.debug('Removed delayed local file %s after object storage move.', path, lTagsBase()))
      .catch(err => logger.warn('Cannot remove delayed local file %s.', path, { err, ...lTagsBase() }))
  }, delayMs)

  timer.unref?.()
}

function scheduleLocalFileRemovalAfterActiveFileWork (options: {
  path: string
  videoUUID: string
  delayMs: number
  waitForPipelineCompletion?: boolean
}): boolean {
  const { path, videoUUID, delayMs, waitForPipelineCompletion = true } = options
  const cleanupKey = `${videoUUID}:${resolve(path)}`

  if (scheduledLocalFileRemovals.has(cleanupKey)) {
    logger.debug(
      'Local file %s for video %s is already scheduled for cleanup after active local video work.',
      path,
      videoUUID,
      lTagsBase(videoUUID)
    )
    return false
  }

  scheduledLocalFileRemovals.add(cleanupKey)

  void (async () => {
    try {
      while (true) {
        if (delayMs > 0) {
          logger.info(
            'Local file %s for video %s is ready for retained cleanup. Keeping it for %d ms before deletion.',
            path,
            videoUUID,
            delayMs,
            lTagsBase(videoUUID)
          )

          await wait(delayMs)
        }

        const jobInfo = waitForPipelineCompletion
          ? await VideoJobInfoModel.loadByUUID(videoUUID)
          : null
        const pendingMove = jobInfo?.pendingMove ?? 0
        const pendingTranscode = jobInfo?.pendingTranscode ?? 0
        const pendingTranscription = jobInfo?.pendingTranscription ?? 0

        if (waitForPipelineCompletion && (pendingMove > 0 || pendingTranscode > 0 || pendingTranscription > 0)) {
          logger.info(
            'Keeping local file %s for video %s because pipeline counters are still pending ' +
            '(pendingMove=%d, pendingTranscode=%d, pendingTranscription=%d).',
            path,
            videoUUID,
            pendingMove,
            pendingTranscode,
            pendingTranscription,
            lTagsBase(videoUUID)
          )

          await wait(CONFIG.OBJECT_STORAGE.MOVE_FILE_DELAY || LOCAL_CLEANUP_RETRY_DELAY_MS)
          continue
        }

        if (waitForPipelineCompletion && await JobQueue.Instance.hasPendingOrActiveLocalFileConsumerJob(videoUUID)) {
          logger.info(
            'Keeping local file %s for video %s because queued local file consumer jobs are still pending.',
            path,
            videoUUID,
            lTagsBase(videoUUID)
          )

          await wait(CONFIG.OBJECT_STORAGE.MOVE_FILE_DELAY || LOCAL_CLEANUP_RETRY_DELAY_MS)
          continue
        }

        if (VideoPathManager.Instance.hasLockedFiles(videoUUID)) {
          logger.info(
            'Keeping local file %s for video %s because local video file work is still active.',
            path,
            videoUUID,
            lTagsBase(videoUUID)
          )
        }

        const releaser = await VideoPathManager.Instance.lockFiles(videoUUID)
        let retryRemovalAfterLock = false
        try {
          try {
            await removeLocalPathNow(path)
          } catch (err) {
            if (isRetryableLocalRemovalError(err)) {
              logger.warn(
                'Cannot remove retained local file %s yet (%s). Will retry after delay.',
                path,
                (err as { code?: string }).code,
                { err, ...lTagsBase(videoUUID) }
              )

              retryRemovalAfterLock = true
            } else {
              throw err
            }
          }

          if (!retryRemovalAfterLock) {
            logger.debug('Removed local file %s after video pipeline completed object-storage move.', path, lTagsBase(videoUUID))
            return
          }
        } finally {
          releaser()
        }

        if (retryRemovalAfterLock) {
          await wait(CONFIG.OBJECT_STORAGE.MOVE_FILE_DELAY || LOCAL_CLEANUP_RETRY_DELAY_MS)
          continue
        }
      }
    } catch (err) {
      logger.warn('Cannot remove retained local file %s after video pipeline completion.', path, { err, ...lTagsBase(videoUUID) })
    } finally {
      scheduledLocalFileRemovals.delete(cleanupKey)
    }
  })()

  return true
}

export function shouldWaitForPipelineCompletionBeforeDeletingMovedHLS (options: {
  file: Pick<MVideoFile, 'resolution' | 'hasVideo'>
  maxVideoResolution: number
}) {
  const { file, maxVideoResolution } = options

  if (!file.hasVideo()) return true
  if (maxVideoResolution === VideoResolution.H_NOVIDEO) return true

  return file.resolution >= maxVideoResolution
}

function getMaxHLSVideoResolution (files: MVideoFile[]) {
  const videoResolutions = files
    .filter(file => file.hasVideo())
    .map(file => file.resolution)

  if (videoResolutions.length === 0) return VideoResolution.H_NOVIDEO

  return Math.max(...videoResolutions)
}

async function scheduleMovedHLSLocalCleanup (options: {
  video: MVideoWithAllFiles
  playlist: MStreamingPlaylistFiles
  movedFiles: MVideoFile[]
}) {
  const { video, playlist, movedFiles } = options
  const maxVideoResolution = getMaxHLSVideoResolution(playlist.VideoFiles)

  for (const file of movedFiles) {
    const waitForPipelineCompletion = shouldWaitForPipelineCompletionBeforeDeletingMovedHLS({
      file,
      maxVideoResolution
    })

    await removeLocalFileAfterMove({
      path: join(getHLSDirectory(video), file.filename),
      videoUUID: video.uuid,
      objectStorageKey: generateHLSObjectStorageKey(video, file.filename),
      bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS,
      skipReadinessCheck: true,
      waitForPipelineCompletion
    })

    await removeLocalFileAfterMove({
      path: join(getHLSDirectory(video), getHLSResolutionPlaylistFilename(file.filename)),
      videoUUID: video.uuid,
      objectStorageKey: generateHLSObjectStorageKey(video, getHLSResolutionPlaylistFilename(file.filename)),
      bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS,
      skipReadinessCheck: true,
      waitForPipelineCompletion: false
    })
  }

  await removeLocalFileAfterMove({
    path: join(getHLSDirectory(video), playlist.playlistFilename),
    videoUUID: video.uuid,
    objectStorageKey: generateHLSObjectStorageKey(video, playlist.playlistFilename),
    bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS,
    skipReadinessCheck: true,
    waitForPipelineCompletion: false
  })

  if (playlist.segmentsSha256Filename) {
    await removeLocalFileAfterMove({
      path: join(getHLSDirectory(video), playlist.segmentsSha256Filename),
      videoUUID: video.uuid,
      objectStorageKey: generateHLSObjectStorageKey(video, playlist.segmentsSha256Filename),
      bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS,
      skipReadinessCheck: true,
      waitForPipelineCompletion: false
    })
  }
}

function isRetryableLocalRemovalError (err: unknown) {
  if (!err || typeof err !== 'object') return false

  const code = (err as { code?: string }).code

  return code === 'EBUSY' || code === 'EPERM' || code === 'ENOTEMPTY'
}

function wait (ms: number) {
  return new Promise<void>(resolve => {
    const timer = setTimeout(resolve, ms)
    timer.unref?.()
  })
}

async function removeParentDirIfEmpty (path: string) {
  const parent = resolve(dirname(path))

  // Keep top-level storage directories in place even if they are empty.
  const preservedRoots = new Set([
    CONFIG.STORAGE.WEB_VIDEOS_DIR,
    CONFIG.STORAGE.STREAMING_PLAYLISTS_DIR,
    CONFIG.STORAGE.ORIGINAL_VIDEO_FILES_DIR,
    CONFIG.STORAGE.THUMBNAILS_DIR,
    CONFIG.STORAGE.STORYBOARDS_DIR,
    CONFIG.STORAGE.CAPTIONS_DIR,
    CONFIG.STORAGE.TORRENTS_DIR
  ].map(p => resolve(p)))

  if (preservedRoots.has(parent)) return

  try {
    await rmdir(parent)
    logger.debug('Removed empty local directory %s after object storage move.', parent, lTagsBase())
  } catch (err) {
    if (err?.code === 'ENOTEMPTY' || err?.code === 'ENOENT') return

    logger.warn('Cannot remove local directory %s after object storage move.', parent, { err, ...lTagsBase() })
  }
}
