import { FileStorage, VideoStateType } from '@peertube/peertube-models'
import { logger, LoggerTags, loggerTagsFactory } from '@server/helpers/logger.js'
import { CONFIG } from '@server/initializers/config.js'
import { P2P_MEDIA_LOADER_PEER_VERSION } from '@server/initializers/constants.js'
import { buildCaptionM3U8Content } from '@server/lib/hls.js'
import {
  BucketInfo,
  checkObjectStorageReadiness,
  generateHLSObjectStorageKey,
  storeHLSFileFromContent,
  storeHLSFileFromFilename,
  storeOriginalVideoFile,
  storeStoryboard,
  storeThumbnail,
  storeTorrentFile,
  storeVideoCaption,
  storeWebVideoFile
} from '@server/lib/object-storage/index.js'
import { getHLSDirectory, getHLSResolutionPlaylistFilename } from '@server/lib/paths.js'
import { updateHLSMasterOnCaptionChange } from '@server/lib/video-captions.js'
import { VideoPathManager } from '@server/lib/video-path-manager.js'
import { moveToFailedMoveToObjectStorageState, moveToNextState } from '@server/lib/video-state.js'
import { updateTorrentMetadata } from '@server/lib/webtorrent.js'
import { VideoCaptionModel } from '@server/models/video/video-caption.js'
import { VideoModel } from '@server/models/video/video.js'
import { MStreamingPlaylistVideo, MVideo, MVideoCaption, MVideoFile, MVideoWithAllFiles, MThumbnail, MStoryboard, isStreamingPlaylist } from '@server/types/models/index.js'
import { MVideoSource } from '@server/types/models/video/video-source.js'
import { pathExists, remove } from 'fs-extra/esm'
import { rmdir } from 'fs/promises'
import { dirname, join, resolve } from 'path'
import PQueue from 'p-queue'
import { federateVideoIfNeeded } from '../activitypub/videos/federate.js'
import { moveCaptionToStorage } from './shared/move-caption.js'
import { moveVideoToStorage, onMoveVideoToStorageFailure } from './shared/move-video.js'

const lTagsBase = loggerTagsFactory('object-storage', 'move-object-storage')

export async function moveVideoToObjectStorage (options: {
  videoUUID: string

  moveVideoState?: {
    isNewVideo: boolean
    previousVideoState: VideoStateType
  }

  loggerTags: LoggerTags['tags']
}) {
  const { videoUUID, moveVideoState, loggerTags } = options

  await moveVideoToStorage({
    videoUUID,
    loggerTags: [ ...lTagsBase().tags, ...loggerTags ],

    targetStorage: FileStorage.OBJECT_STORAGE,

    moveWebVideoFiles,
    moveHLSFiles,
    moveVideoSourceFile,
    moveCaptionFiles,
    moveThumbnailFiles,
    moveStoryboardFiles,
    moveTorrentFiles
  })

  if (options.moveVideoState) {
    await moveToNextState({ video: { uuid: videoUUID }, ...moveVideoState })
  } else {
    const videoFull = await VideoModel.loadFull(videoUUID)
    await federateVideoIfNeeded(videoFull, false, undefined)
  }
}

export function moveCaptionToObjectStorage (options: {
  captionId: number
  loggerTags: LoggerTags['tags']
}) {
  const { captionId, loggerTags } = options

  return moveCaptionToStorage({
    captionId,
    loggerTags: [ ...lTagsBase().tags, ...loggerTags ],
    moveCaptionFiles
  })
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

async function moveVideoSourceFile (source: MVideoSource) {
  if (source.storage !== FileStorage.FILE_SYSTEM) return

  const sourcePath = VideoPathManager.Instance.getFSOriginalVideoFilePath(source.keptOriginalFilename)
  await storeOriginalVideoFile(sourcePath, source.keptOriginalFilename)

  logger.debug('Checking readiness before marking original video file as moved to object storage', lTagsBase())
  await removeLocalFileAfterMove({
    path: sourcePath,
    objectStorageKey: source.keptOriginalFilename,
    bucketInfo: CONFIG.OBJECT_STORAGE.ORIGINAL_VIDEO_FILES
  })

  source.storage = FileStorage.OBJECT_STORAGE
  await source.save()
}

// ---------------------------------------------------------------------------

async function moveCaptionFiles (captions: MVideoCaption[], hls: MStreamingPlaylistVideo) {
  const queue = new PQueue({ concurrency: CONFIG.OBJECT_STORAGE.UPLOAD_CONCURRENCY })

  const results = await queue.addAll(captions.map(caption => async () => {
    let captionUpdated = false

    if (caption.storage === FileStorage.FILE_SYSTEM) {
      const captionPath = caption.getFSFilePath()

      await storeVideoCaption(captionPath, caption.filename)

      logger.debug(`Checking readiness before marking caption file as moved to object storage`, lTagsBase())
      await removeLocalFileAfterMove({
        path: captionPath,
        objectStorageKey: caption.filename,
        bucketInfo: CONFIG.OBJECT_STORAGE.CAPTIONS
      })

      // Assign new values before building the m3u8 file
      caption.storage = FileStorage.OBJECT_STORAGE
      await caption.save()
    }

    if (hls) {
      captionUpdated = true

      const m3u8PathToRemove = caption.getFSM3U8Path(hls.Video)

      // Caption file URL has been updated, so we must also update the HLS caption playlist
      const content = buildCaptionM3U8Content({ video: hls.Video, caption })

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

  await queue.addAll(video.VideoFiles.map(file => async () => {
    if (file.storage !== FileStorage.FILE_SYSTEM) return

    await storeWebVideoFile(video, file)

    const oldPath = VideoPathManager.Instance.getFSVideoFileOutputPath(video, file)
    await onVideoFileMoved({
      videoOrPlaylist: video,
      file,
      oldPath,
      objectStorageKey: file.filename,
      bucketInfo: CONFIG.OBJECT_STORAGE.WEB_VIDEOS
    })
  }))
}

async function moveHLSFiles (video: MVideoWithAllFiles) {
  const queue = new PQueue({ concurrency: CONFIG.OBJECT_STORAGE.UPLOAD_CONCURRENCY })

  for (const playlist of video.VideoStreamingPlaylists) {

    const results = await queue.addAll(playlist.VideoFiles.map(file => async () => {
      if (file.storage !== FileStorage.FILE_SYSTEM) return false

      // Resolution playlist
      const playlistFilename = getHLSResolutionPlaylistFilename(file.filename)
      await storeHLSFileFromFilename(video, playlistFilename)

      // Resolution fragmented file
      await storeHLSFileFromFilename(video, file.filename)

      const oldPath = join(getHLSDirectory(video), file.filename)

      await onVideoFileMoved({
        videoOrPlaylist: Object.assign(playlist, { Video: video }),
        file,
        oldPath,
        objectStorageKey: file.filename,
        bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS
      })

      // Resolution playlist file is in the same HLS directory
      const resolutionPlaylistObjectStorageKey = generateHLSObjectStorageKey(video, playlistFilename)
      await removeLocalFileAfterMove({
        path: join(getHLSDirectory(video), playlistFilename),
        objectStorageKey: resolutionPlaylistObjectStorageKey,
        bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS
      })

      return true
    }))

    const updatedFile = results.some(r => r === true)

    if (playlist.storage === FileStorage.FILE_SYSTEM) {
      await storeHLSFileFromFilename(video, playlist.playlistFilename)
      await storeHLSFileFromFilename(video, playlist.segmentsSha256Filename)

      const playlistObjectStorageKey = generateHLSObjectStorageKey(video, playlist.playlistFilename)
      const isPlaylistReady = await checkObjectStorageReadiness({
        key: playlistObjectStorageKey,
        bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS,
        maxRetries: 30,
        retryIntervalMs: 10000
      })

      if (!isPlaylistReady) {
        logger.warn(`HLS playlist ${playlist.playlistFilename} not ready in object storage, keeping local files`, lTagsBase())
      } else {
        playlist.storage = FileStorage.OBJECT_STORAGE
        await playlist.save()

        const segmentsSha256ObjectStorageKey = generateHLSObjectStorageKey(video, playlist.segmentsSha256Filename)
        await removeLocalFileAfterMove({
          path: join(getHLSDirectory(video), playlist.playlistFilename),
          objectStorageKey: playlistObjectStorageKey,
          bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS
        })
        await removeLocalFileAfterMove({
          path: join(getHLSDirectory(video), playlist.segmentsSha256Filename),
          objectStorageKey: segmentsSha256ObjectStorageKey,
          bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS
        })
      }
    }

    if (updatedFile === true) {
      playlist.assignP2PMediaLoaderInfoHashes(video, playlist.VideoFiles)
      playlist.p2pMediaLoaderPeerVersion = P2P_MEDIA_LOADER_PEER_VERSION

      await playlist.save()
    }
  }

  try {
    await rmdir(getHLSDirectory(video))
  } catch {
    // Nothing to do, directory may be not empty if there is a transcoding in progress
  }
}

async function onVideoFileMoved (options: {
  videoOrPlaylist: MVideo | MStreamingPlaylistVideo
  file: MVideoFile
  oldPath: string
  objectStorageKey?: string
  bucketInfo?: BucketInfo
}) {
  const { videoOrPlaylist, file, oldPath, objectStorageKey, bucketInfo } = options

  let actualObjectStorageKey = objectStorageKey

  if (isStreamingPlaylist(videoOrPlaylist) && bucketInfo === CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS) {
    actualObjectStorageKey = generateHLSObjectStorageKey(videoOrPlaylist.Video, objectStorageKey)
  }

  // Check readiness BEFORE updating database
  logger.debug('Checking readiness before marking file as moved to object storage', lTagsBase())
  await removeLocalFileAfterMove({
    path: oldPath,
    objectStorageKey: actualObjectStorageKey,
    bucketInfo
  })

  // Only update database after readiness is confirmed
  file.storage = FileStorage.OBJECT_STORAGE

  await updateTorrentMetadata(videoOrPlaylist, file)
  await file.save()

  logger.debug('Removed %s after confirming object storage readiness', oldPath, lTagsBase())
}

// ---------------------------------------------------------------------------

async function moveThumbnailFiles (thumbnails: MThumbnail[]) {
  const queue = new PQueue({ concurrency: CONFIG.OBJECT_STORAGE.UPLOAD_CONCURRENCY })

  await queue.addAll(thumbnails.map(thumbnail => async () => {
    if (thumbnail.storage !== FileStorage.FILE_SYSTEM) return

    const thumbnailPath = thumbnail.getFSPath()
    await storeThumbnail(thumbnailPath, thumbnail.filename)

    logger.debug(`Checking readiness before marking thumbnail file as moved to object storage`, lTagsBase())
    await removeLocalFileAfterMove({
      path: thumbnailPath,
      objectStorageKey: thumbnail.filename,
      bucketInfo: CONFIG.OBJECT_STORAGE.THUMBNAILS
    })

    thumbnail.storage = FileStorage.OBJECT_STORAGE
    await thumbnail.save()
  }))
}

// ---------------------------------------------------------------------------

async function moveStoryboardFiles (storyboards: MStoryboard[]) {
  const queue = new PQueue({ concurrency: CONFIG.OBJECT_STORAGE.UPLOAD_CONCURRENCY })

  await queue.addAll(storyboards.map(storyboard => async () => {
    if (storyboard.storage !== FileStorage.FILE_SYSTEM) return

    const storyboardPath = storyboard.getFSPath()
    await storeStoryboard(storyboardPath, storyboard.filename)

    logger.debug(`Checking readiness before marking storyboard file as moved to object storage`, lTagsBase())
    await removeLocalFileAfterMove({
      path: storyboardPath,
      objectStorageKey: storyboard.filename,
      bucketInfo: CONFIG.OBJECT_STORAGE.STORYBOARDS
    })

    storyboard.storage = FileStorage.OBJECT_STORAGE
    await storyboard.save()
  }))
}

// ---------------------------------------------------------------------------

async function moveTorrentFiles (video: MVideoWithAllFiles) {
  const allFiles = [
    ...video.VideoFiles,
    ...(video.VideoStreamingPlaylists || []).flatMap(p => p.VideoFiles)
  ]

  const queue = new PQueue({ concurrency: CONFIG.OBJECT_STORAGE.UPLOAD_CONCURRENCY })

  await queue.addAll(allFiles.map(file => async () => {
    if (!file.torrentFilename) return

    const torrentPath = join(CONFIG.STORAGE.TORRENTS_DIR, file.torrentFilename)

    // Skip if local torrent was already moved to object storage by updateTorrentMetadata in onVideoFileMoved
    if (!await pathExists(torrentPath)) {
      logger.debug(`Torrent file ${torrentPath} not found locally, already on object storage`, lTagsBase())
      return
    }

    try {
      await storeTorrentFile(torrentPath, file.torrentFilename)

      logger.debug(`Removing torrent file ${torrentPath} because it's now on object storage`, lTagsBase())
      await removeLocalFileAfterMove({
        path: torrentPath,
        objectStorageKey: file.torrentFilename,
        bucketInfo: CONFIG.OBJECT_STORAGE.TORRENTS
      })
    } catch (err) {
      logger.warn(`Cannot move torrent file ${torrentPath} to object storage`, { err, ...lTagsBase() })
    }
  }))
}

async function removeLocalFileAfterMove (options: {
  path: string
  objectStorageKey?: string
  bucketInfo?: BucketInfo
}) {
  const { path, objectStorageKey, bucketInfo } = options
  const delayMs = CONFIG.OBJECT_STORAGE.KEEP_LOCAL_FILE_AFTER_MOVE

  // Check readiness before removal if object storage key is provided
  if (objectStorageKey && bucketInfo) {
    const isReady = await checkObjectStorageReadiness({
      key: objectStorageKey,
      bucketInfo,
      maxRetries: 30,
      retryIntervalMs: 10000
    })

    if (!isReady) {
      logger.error(
        'Object storage file %s is not ready after max retries, keeping local file and failing job',
        objectStorageKey,
        lTagsBase()
      )
      throw new Error(`Object storage file ${objectStorageKey} is not ready after max retries`)
    }
  }

  if (!delayMs) {
    await remove(path)
    await removeParentDirIfEmpty(path)
    return
  }

  logger.info(
    'Keeping local file %s for %d ms after moving to object storage before deletion.',
    path,
    delayMs,
    lTagsBase()
  )

  const timer = setTimeout(() => {
    remove(path)
      .then(async () => {
        logger.debug('Removed delayed local file %s after object storage move.', path, lTagsBase())
        await removeParentDirIfEmpty(path)
      })
      .catch(err => logger.warn('Cannot remove delayed local file %s.', path, { err, ...lTagsBase() }))
  }, delayMs)

  timer.unref?.()
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
