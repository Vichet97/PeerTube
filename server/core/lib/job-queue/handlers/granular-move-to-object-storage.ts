import {
  FileStorage,
  MoveHLSPlaylistPayload,
  VideoState,
  isMoveVideoFilePayload,
  isMoveHLSPlaylistPayload,
  isMoveThumbnailPayload
} from '@peertube/peertube-models'
import { logger } from '@server/helpers/logger.js'
import { CONFIG } from '@server/initializers/config.js'
import { JOB_TTL } from '@server/initializers/constants.js'
import { getHLSResolutionPlaylistFilename } from '@server/lib/paths.js'
import { moveToFailedMoveToObjectStorageState } from '@server/lib/video-state.js'
import { VideoJobInfoModel } from '@server/models/video/video-job-info.js'
import { VideoModel } from '@server/models/video/video.js'
import { VideoPathManager } from '@server/lib/video-path-manager.js'
import { move, pathExists } from 'fs-extra/esm'
import { Job } from 'bullmq'
import {
  getHLSSegmentFilesToMoveCount,
  isThumbnailMoveNeeded,
  maybeTransitionAfterObjectStorageMove,
  moveHLSSegmentFilesToObjectStorage,
  moveMasterPlaylistToObjectStorage,
  moveThumbnailToObjectStorage,
  removeLocalFileAfterMove,
  moveVideoFileToObjectStorage
} from '@server/lib/move-storage/move-to-object-storage.js'
import { checkObjectStorageReadiness, generateHLSObjectStorageKey } from '@server/lib/object-storage/index.js'
import { MStreamingPlaylistFiles, MVideoWithAllFiles } from '@server/types/models/index.js'
import { makeHLSFileAvailable } from '@server/lib/object-storage/videos.js'
import { createAllCaptionPlaylistsOnFSIfNeeded } from '@server/lib/video-captions.js'
import { createHash } from 'node:crypto'

type MoveHLSPlaylistPayloadWithCleanup = MoveHLSPlaylistPayload & {
  isFollowUp?: boolean
  cleanupMode?: 'move' | 'cleanup'
  cleanupPaths?: string[]
  retryAttempt?: number
  retryOfFailedJob?: boolean
}

type RetryableGranularMovePayload = {
  retryOfFailedJob?: boolean
}

export async function processGranularMoveToObjectStorage (job: Job) {
  const payload = job.data

  const updateProgress = (percent: number) => {
    const normalized = normalizeProgressPercent(percent)
    const currentProgress = typeof job.progress === 'number' ? job.progress : 0

    if (normalized <= currentProgress) return

    job.updateProgress(normalized).catch(err => logger.error('Cannot update granular move progress', { err }))
  }

  if (CONFIG.OBJECT_STORAGE.ENABLED !== true) {
    logger.info('[GRANULAR_MOVE] Skipping %s job %s because object storage is disabled', job.name, job.id)
    await releaseSkippedGranularMoveJob(payload)
    return
  }

  try {
    if (isMoveVideoFilePayload(payload)) {
      return await runWithVideoFileLock(payload.videoUUID, () => processMoveVideoFile(job, payload, updateProgress))
    }

    if (isMoveHLSPlaylistPayload(payload)) {
      if ((payload as MoveHLSPlaylistPayloadWithCleanup).cleanupMode === 'cleanup') {
        return await processMoveHLSPlaylist(job, payload, updateProgress)
      }

      return await runWithVideoFileLock(payload.videoUUID, () => processMoveHLSPlaylist(job, payload, updateProgress))
    }

    if (isMoveThumbnailPayload(payload)) {
      return await runWithVideoFileLock(payload.videoUUID, () => processMoveThumbnail(job, payload, updateProgress))
    }

    throw new Error('Unknown granular move payload type: ' + JSON.stringify(payload))
  } catch (err) {
    logger.error('[GRANULAR_MOVE] Job %s failed: %s', job.id, err.message || err)
    throw err
  }
}

async function releaseSkippedGranularMoveJob (payload: unknown) {
  if (isMoveVideoFilePayload(payload) || isMoveThumbnailPayload(payload)) {
    await checkAndTransitionVideoState(
      payload.videoUUID,
      payload.isNewVideo,
      payload.previousVideoState,
      false,
      isRetryOfFailedJob(payload)
    )
    return
  }

  if (isMoveHLSPlaylistPayload(payload)) {
    const cleanupMode = (payload as MoveHLSPlaylistPayloadWithCleanup).cleanupMode
    if (cleanupMode === 'cleanup') return

    await checkAndTransitionVideoState(
      payload.videoUUID,
      payload.isNewVideo,
      payload.previousVideoState,
      false,
      isRetryOfFailedJob(payload)
    )
  }
}

async function runWithVideoFileLock <T> (videoUUID: string, run: () => Promise<T>) {
  const releaser = await VideoPathManager.Instance.lockFiles(videoUUID)

  try {
    return await run()
  } finally {
    releaser()
  }
}

// ---------------------------------------------------------------------------
// Individual file move handlers
// ---------------------------------------------------------------------------

async function processMoveVideoFile (
  job: Job,
  payload: { videoUUID: string; fileId: number; isNewVideo: boolean; previousVideoState?: any },
  updateProgress: (percent: number) => void
) {
  const { videoUUID, fileId, isNewVideo, previousVideoState } = payload

  logger.info('[GRANULAR_MOVE] Processing video file move job %s for file %s of video %s', job.id, fileId, videoUUID)

  const video = await VideoModel.loadWithFiles(videoUUID)
  if (!video) {
    logger.warn('[GRANULAR_MOVE] Video %s not found, skipping file move', videoUUID)
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState, false, isRetryOfFailedJob(payload))
    return
  }

  const videoFile = video.VideoFiles.find(f => f.id === fileId)
  if (!videoFile) {
    logger.warn('[GRANULAR_MOVE] Video file %s not found for video %s', fileId, videoUUID)
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState, false, isRetryOfFailedJob(payload))
    return
  }

  // Skip if already on object storage
  if (videoFile.storage === FileStorage.OBJECT_STORAGE) {
    logger.info('[GRANULAR_MOVE] File %s already on object storage, skipping', fileId)
    updateProgress(100)
    // Decrement pendingMove for this job even though we skip processing
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState, false, isRetryOfFailedJob(payload))
    return
  }

  // Check source file exists
  const sourcePath = VideoPathManager.Instance.getFSVideoFileOutputPath(video, videoFile)
  const fileExists = await pathExists(sourcePath)

  if (!fileExists) {
    const objectStorageReady = await checkObjectStorageReadiness({
      key: videoFile.filename,
      bucketInfo: CONFIG.OBJECT_STORAGE.WEB_VIDEOS,
      maxRetries: 1,
      retryIntervalMs: 0,
      logNotReadyAsDebug: true
    })

    if (!objectStorageReady) {
      throw new Error(
        `Source file ${sourcePath} does not exist and object storage copy ${videoFile.filename} is not ready`
      )
    }

    logger.warn('[GRANULAR_MOVE] Source file %s does not exist but object storage copy is ready, marking as moved', sourcePath)
    videoFile.storage = FileStorage.OBJECT_STORAGE
    await videoFile.save()
    updateProgress(100)

    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState, false, isRetryOfFailedJob(payload))
    return
  }

  logger.info('[GRANULAR_MOVE] Moving video file %s from %s', fileId, sourcePath)
  updateProgress(5)

  try {
    updateProgress(60)
    await moveVideoFileToObjectStorage(videoUUID, fileId)

    updateProgress(95)
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState, false, isRetryOfFailedJob(payload))
    updateProgress(100)
    logger.info('[GRANULAR_MOVE] Video file %s move completed', fileId)

  } catch (err) {
    logger.error('[GRANULAR_MOVE] Failed to move video file %s: %s', fileId, err.message || err)
    throw err
  }
}

async function processMoveHLSPlaylist (
  job: Job,
  payload: MoveHLSPlaylistPayloadWithCleanup,
  updateProgress: (percent: number) => void
) {
  const { videoUUID, playlistId, fileIds, isNewVideo, previousVideoState, cleanupMode } = payload

  if (cleanupMode === 'cleanup') {
    updateProgress(5)
    await processCleanupHLSPaths(job, payload)
    updateProgress(100)
    return
  }

  logger.info('[GRANULAR_MOVE] Processing HLS playlist move job %s for playlist %s, files %s', job.id, playlistId, fileIds.join(', '))

  const video = await VideoModel.loadWithFiles(videoUUID)
  if (!video) {
    logger.warn('[GRANULAR_MOVE] Video %s not found, skipping HLS playlist move', videoUUID)
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState, false, isRetryOfFailedJob(payload))
    return
  }

  const playlist = video.VideoStreamingPlaylists?.find(p => p.id === playlistId)
  if (!playlist) {
    logger.warn('[GRANULAR_MOVE] HLS playlist %s not found for video %s', playlistId, videoUUID)
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState, false, isRetryOfFailedJob(payload))
    return
  }

  // Check if playlist is already in object storage
  if (playlist.storage === FileStorage.OBJECT_STORAGE) {
    logger.info('[GRANULAR_MOVE] Playlist %s is already in object storage, will still process files', playlistId, job.id)
    // Continue processing - we still need to move individual files that may not be on OBJECT_STORAGE yet
    // The isFirstResolutionMove will be false, so we'll only move files and regenerate master playlist
  }

  // Check if files exist before attempting to move
  const missingFiles: string[] = []
  const skippedFileIds: number[] = [] // Files already in object storage (don't need new job)
  const filesToMove: number[] = [] // Files that need to be moved

  for (const fileId of fileIds) {
    const videoFile = playlist.VideoFiles.find(f => f.id === fileId)
    if (!videoFile) {
      missingFiles.push(`file-${fileId}-db`)
      continue
    }

    // Skip files that are already in object storage - they don't need a new job
    if (videoFile.storage === FileStorage.OBJECT_STORAGE) {
      logger.info('[GRANULAR_MOVE] File %s (id: %d) is already in object storage, skipping', videoFile.filename, fileId)
      skippedFileIds.push(fileId)
      continue
    }

    const playlistFilename = getHLSResolutionPlaylistFilename(videoFile.filename)
    const fragmentPath = VideoPathManager.Instance.getFSHLSOutputPath(video, videoFile.filename)
    const playlistPath = VideoPathManager.Instance.getFSHLSOutputPath(video, playlistFilename)

    const fragmentExists = await pathExists(fragmentPath)
    const playlistExists = await pathExists(playlistPath)

    if (!fragmentExists && !await isHLSFileAlreadyReadyOnObjectStorage(video, videoFile.filename)) {
      missingFiles.push(fragmentPath)
    }
    if (!playlistExists && !await isHLSFileAlreadyReadyOnObjectStorage(video, playlistFilename)) {
      missingFiles.push(playlistPath)
    }

    filesToMove.push(fileId)
  }

  // If ALL files were already in object storage, skip the job entirely
  if (filesToMove.length === 0 && skippedFileIds.length === fileIds.length) {
    logger.info('[GRANULAR_MOVE] All files for playlist %s are already in object storage, skipping job %s', playlistId, job.id)
    // Decrement pendingMove for this job even though we skip processing
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState, false, isRetryOfFailedJob(payload))
    return
  }

  // If some files are missing and some still need to be moved, create a delayed retry.
  // Use a deterministic job ID to dedupe the retry branch.
  if (missingFiles.length > 0 && filesToMove.length > 0) {
    const delayMs = CONFIG.OBJECT_STORAGE.MOVE_FILE_DELAY || 30000

    logger.info('[GRANULAR_MOVE] HLS files not ready for playlist %s. Creating delayed job in %dms. Missing: %s, Files to move: %s',
      playlistId, delayMs, missingFiles.join(', '), filesToMove.join(', '))

    // Create a new delayed job with ONLY the files that still need to be moved.
    // Guard against duplicate delayed retries for the same video/playlist/fileIds tuple.
    const { JobQueue } = await import('@server/lib/job-queue/index.js')

    const existingJobs = await JobQueue.Instance.getExistingHLSPlaylistMoveJobs(videoUUID, playlistId, filesToMove)
    const hasOtherPendingRetry = existingJobs.some(existing => {
      const isCurrentJob = String(existing.id) === String(job.id)
      const isFailed = existing.failedReason !== undefined && existing.failedReason !== null
      return !isCurrentJob && !isFailed
    })

    const retryAttempt = Math.max(
      payload.retryAttempt ?? 0,
      ...existingJobs.map(existing => (existing.data as MoveHLSPlaylistPayloadWithCleanup)?.retryAttempt ?? 0)
    ) + 1
    const maxRetryAttempts = Math.max(
      1,
      Math.ceil(JOB_TTL['move-hls-playlist-to-object-storage'] / delayMs)
    )

    if (retryAttempt > maxRetryAttempts) {
      throw new Error(
        `HLS files for playlist ${playlistId} of video ${videoUUID} were still missing after ` +
          `${maxRetryAttempts} delayed retries: ${missingFiles.join(', ')}`
      )
    }

    if (hasOtherPendingRetry) {
      logger.info(
        '[GRANULAR_MOVE] Delayed retry already queued for playlist %s files %s. Skipping duplicate schedule from job %s.',
        playlistId,
        filesToMove.join(', '),
        job.id
      )
      return
    }

    const retryJobId = buildHLSMoveRetryJobId({
      videoUUID,
      playlistId,
      fileIds: filesToMove,
      mode: 'move',
      retryAttempt
    })

    try {
      await JobQueue.Instance.createJob({
        type: 'move-hls-playlist-to-object-storage',
        payload: {
          ...payload,
          fileIds: filesToMove,
          cleanupMode: 'move',
          retryAttempt
        } as MoveHLSPlaylistPayload,
        delay: delayMs,
        customJobId: retryJobId
      })
    } catch (err) {
      if (isDuplicateRetryJobError(err)) {
        logger.info(
          '[GRANULAR_MOVE] Delayed retry %s already exists, skipping duplicate creation for playlist %s files %s.',
          retryJobId,
          playlistId,
          filesToMove.join(', ')
        )
        return
      }

      throw err
    }

    // Keep pendingMove unchanged: this is the same move work rescheduled because files are not ready yet.
    return
  }

  // If some files need to be moved (no missing files), continue with the move
  // Update fileIds to only include files that need to be moved
  const effectiveFileIds = filesToMove.length > 0 ? filesToMove : fileIds

  updateProgress(5)

  try {
    let workingVideo = video
    let workingPlaylist = playlist

    // Step 0: Ensure local master playlist exists
    // If local master playlist is missing (e.g., deleted after first resolution), download from OS
    ;({ video: workingVideo, playlist: workingPlaylist } = await ensureLocalMasterPlaylistExists(video, playlist))

    // Determine if this is the first resolution being moved to object storage
    const isFirstResolutionMove = workingPlaylist.storage !== FileStorage.OBJECT_STORAGE

    if (isFirstResolutionMove) {
      logger.info('[GRANULAR_MOVE] First resolution move for %s, will upload master playlist and update storage', videoUUID)
    } else {
      logger.info('[GRANULAR_MOVE] Subsequent resolution move for %s, will update local master playlist only', videoUUID)
    }

    // Step 1: Upload fragment + resolution playlist files to object storage
    // This must be done BEFORE setting file.storage = OBJECT_STORAGE
    // because regenerating master playlist needs to hash these files (via makeAvailable)
    updateProgress(20)

    const filesCountBeforeMove = await getHLSSegmentFilesToMoveCount(videoUUID, playlistId, effectiveFileIds)

    await moveHLSSegmentFilesToObjectStorage(videoUUID, playlistId, effectiveFileIds, { deleteLocalFiles: false })
    // Note: moveHLSSegmentFilesToObjectStorage sets videoFile.storage = OBJECT_STORAGE

    if (filesCountBeforeMove > 0) {
      updateProgress(55)
    }

    // Step 2: If this is the first resolution, set playlist.storage to OBJECT_STORAGE
    // BEFORE regenerating master playlist so it generates OS segment URLs
    if (isFirstResolutionMove) {
      workingPlaylist.storage = FileStorage.OBJECT_STORAGE
      await workingPlaylist.save()
      logger.info('[GRANULAR_MOVE] Set playlist.storage = OBJECT_STORAGE before master regeneration', playlistId)
    }

    await includeCaptionsInMasterPlaylistIfNeeded(workingVideo)

    // Step 3: Regenerate master playlist + SHA
    // When playlist.storage = OBJECT_STORAGE and file.storage = OBJECT_STORAGE,
    // this generates OS segment URLs while preferring retained local files for probing/hashing.
    logger.info('[GRANULAR_MOVE] Regenerating master playlist and SHA for playlist %s', playlistId)
    const { updateM3U8AndShaPlaylist } = await import('@server/lib/hls.js')
    const playlistUpdated = await updateM3U8AndShaPlaylist(workingVideo, workingPlaylist, { throwOnError: true })
    if (!playlistUpdated) {
      throw new Error(`HLS playlist ${playlistId} was not regenerated before object-storage upload`)
    }

    // Step 4: Reload video and playlist to get the updated filenames from DB
    // updateM3U8AndShaPlaylist saves new playlistFilename to the database
    const reloadedVideo = await VideoModel.loadWithFiles(videoUUID)
    if (!reloadedVideo) {
      throw new Error(`Video ${videoUUID} not found after playlist regeneration`)
    }
    const reloadedPlaylist = reloadedVideo.VideoStreamingPlaylists?.find(p => p.id === playlistId)
    if (!reloadedPlaylist) {
      throw new Error(`Playlist ${playlistId} not found after playlist regeneration`)
    }
    logger.info('[GRANULAR_MOVE] Reloaded playlist with new filename: %s', reloadedPlaylist.playlistFilename)

    // Ensure local master playlist exists before uploading
    // This is critical for subsequent resolution moves where the local file may have been deleted
    ;({ video: workingVideo, playlist: workingPlaylist } = await ensureLocalMasterPlaylistExists(reloadedVideo, reloadedPlaylist))

    // Step 5: Upload master playlist to object storage
    // Master playlist now has correct OS segment URLs
    updateProgress(80)

    await moveMasterPlaylistToObjectStorage(videoUUID, playlistId)
    logger.info('[GRANULAR_MOVE] Master playlist uploaded with OS URLs', playlistId)

    updateProgress(90)

    // Step 6: Transition video state
    // This is done immediately after first resolution move to publish the video
    // Subsequent resolution moves will not trigger state transition
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState, isFirstResolutionMove, isRetryOfFailedJob(payload))

    updateProgress(95)

    const localCleanupPaths = buildHLSCleanupPaths(workingVideo, workingPlaylist, effectiveFileIds)

    for (const path of localCleanupPaths) {
      await removeLocalFileAfterMove({
        path,
        videoUUID,
        skipReadinessCheck: true
      })
    }

    updateProgress(100)

  } catch (err) {
    logger.error('[GRANULAR_MOVE] Failed to move HLS playlist %s: %s', playlistId, err.message || err)
    throw err
  }
}

async function processMoveThumbnail (
  job: Job,
  payload: { videoUUID: string; thumbnailId: number; isNewVideo: boolean; previousVideoState?: any },
  updateProgress: (percent: number) => void
) {
  const { videoUUID, thumbnailId, isNewVideo, previousVideoState } = payload

  logger.info('[GRANULAR_MOVE] Processing thumbnail move job %s for thumbnail %s', job.id, thumbnailId)

  const video = await VideoModel.loadWithFiles(videoUUID)
  if (!video) {
    logger.warn('[GRANULAR_MOVE] Video %s not found, skipping thumbnail move', videoUUID)
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState, false, isRetryOfFailedJob(payload))
    return
  }

  const thumbnail = video.Thumbnails?.find(t => t.id === thumbnailId)
  if (!thumbnail) {
    logger.warn('[GRANULAR_MOVE] Thumbnail %s not found for video %s', thumbnailId, videoUUID)
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState, false, isRetryOfFailedJob(payload))
    return
  }

  // Skip if already on object storage
  if (thumbnail.storage === FileStorage.OBJECT_STORAGE) {
    logger.info('[GRANULAR_MOVE] Thumbnail %s already on object storage, skipping', thumbnailId)
    updateProgress(100)
    // Decrement pendingMove for this job even though we skip processing
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState, false, isRetryOfFailedJob(payload))
    return
  }

  // Check source file exists
  const sourcePath = thumbnail.getFSPath()
  const fileExists = await pathExists(sourcePath)

  if (!fileExists) {
    const objectStorageReady = await checkObjectStorageReadiness({
      key: thumbnail.filename,
      bucketInfo: CONFIG.OBJECT_STORAGE.THUMBNAILS,
      maxRetries: 1,
      retryIntervalMs: 0,
      logNotReadyAsDebug: true
    })

    if (!objectStorageReady) {
      throw new Error(
        `Source thumbnail ${sourcePath} does not exist and object storage copy ${thumbnail.filename} is not ready`
      )
    }

    logger.warn('[GRANULAR_MOVE] Source thumbnail %s does not exist but object storage copy is ready, marking as moved', sourcePath)
    thumbnail.storage = FileStorage.OBJECT_STORAGE
    await thumbnail.save()
    updateProgress(100)
    // Thumbnails don't affect video state, but still decrement pendingMove
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState, false, isRetryOfFailedJob(payload))
    return
  }

  updateProgress(5)

  try {
    const shouldMoveThumbnail = await isThumbnailMoveNeeded(videoUUID, thumbnailId)
    if (shouldMoveThumbnail) {
      updateProgress(60)
    }

    await moveThumbnailToObjectStorage(videoUUID, thumbnailId)

    updateProgress(95)
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState, false, isRetryOfFailedJob(payload))
    updateProgress(100)
    logger.info('[GRANULAR_MOVE] Thumbnail %s move completed', thumbnailId)

  } catch (err) {
    logger.error('[GRANULAR_MOVE] Failed to move thumbnail %s: %s', thumbnailId, err.message || err)
    throw err
  }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

async function processCleanupHLSPaths (job: Job, payload: MoveHLSPlaylistPayload) {
  const payloadWithCleanup = payload as MoveHLSPlaylistPayloadWithCleanup
  const cleanupPaths = payloadWithCleanup.cleanupPaths ?? []
  if (cleanupPaths.length === 0) {
    logger.info('[GRANULAR_MOVE] Cleanup-only HLS job %s has no paths, skipping', job.id)
    return
  }

  logger.info('[GRANULAR_MOVE] Cleanup-only HLS job %s scheduling cleanup of %d local path(s)', job.id, cleanupPaths.length, {
    videoUUID: payload.videoUUID
  })

  for (const path of cleanupPaths) {
    await removeLocalFileAfterMove({
      path,
      videoUUID: payload.videoUUID,
      skipReadinessCheck: true
    })
  }
}

function buildHLSCleanupPaths (
  video: MVideoWithAllFiles,
  playlist: MStreamingPlaylistFiles,
  fileIds: number[]
) {
  const paths: string[] = []

  const filesToCleanup = playlist.VideoFiles.filter(file => fileIds.includes(file.id))
  for (const file of filesToCleanup) {
    paths.push(VideoPathManager.Instance.getFSHLSOutputPath(video, file.filename))
    paths.push(
      VideoPathManager.Instance.getFSHLSOutputPath(video, getHLSResolutionPlaylistFilename(file.filename))
    )
  }

  paths.push(VideoPathManager.Instance.getFSHLSOutputPath(video, playlist.playlistFilename))
  if (playlist.segmentsSha256Filename) {
    paths.push(VideoPathManager.Instance.getFSHLSOutputPath(video, playlist.segmentsSha256Filename))
  }

  return [ ...new Set(paths) ]
}

async function isHLSFileAlreadyReadyOnObjectStorage (video: MVideoWithAllFiles, filename: string) {
  const objectStorageKey = generateHLSObjectStorageKey(video, filename)

  const ready = await checkObjectStorageReadiness({
    key: objectStorageKey,
    bucketInfo: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS,
    maxRetries: 1,
    retryIntervalMs: 0,
    logNotReadyAsDebug: true
  })

  if (ready) {
    logger.info('[GRANULAR_MOVE] Local HLS file %s is missing but already ready on object storage.', filename, {
      videoUUID: video.uuid,
      objectStorageKey
    })
  }

  return ready
}

function buildHLSMoveRetryJobId (options: {
  videoUUID: string
  playlistId: number
  fileIds: number[]
  mode: 'move' | 'cleanup'
  retryAttempt?: number
  cleanupPaths?: string[]
}) {
  const { videoUUID, playlistId, fileIds, mode, retryAttempt = 0, cleanupPaths = [] } = options

  const descriptor = JSON.stringify({
    scope: 'move-hls-playlist-to-object-storage',
    mode,
    videoUUID,
    playlistId,
    fileIds: [ ...fileIds ].sort((a, b) => a - b),
    retryAttempt,
    cleanupKey: mode === 'cleanup' ? buildCleanupPathsKey(cleanupPaths) : ''
  })

  return buildStableUUID(descriptor)
}

function buildCleanupPathsKey (cleanupPaths: string[]) {
  if (cleanupPaths.length === 0) return 'no-paths'

  const normalized = [ ...cleanupPaths ].sort().join('|')
  const digest = createHash('sha1').update(normalized).digest('hex').slice(0, 20)

  return `paths-${cleanupPaths.length}-${digest}`
}

function buildStableUUID (input: string) {
  const hex = createHash('sha1').update(input).digest('hex').slice(0, 32).split('')

  // UUID v5-like shape for readability (deterministic by descriptor hash)
  hex[12] = '5'
  hex[16] = (parseInt(hex[16], 16) & 0x3 | 0x8).toString(16)

  return [
    hex.slice(0, 8).join(''),
    hex.slice(8, 12).join(''),
    hex.slice(12, 16).join(''),
    hex.slice(16, 20).join(''),
    hex.slice(20, 32).join('')
  ].join('-')
}

function isDuplicateRetryJobError (err: unknown) {
  const message = err instanceof Error ? err.message : String(err)
  return message.includes('Job is already waiting') ||
         message.includes('JobId') && message.includes('already exists')
}

async function includeCaptionsInMasterPlaylistIfNeeded (video: MVideoWithAllFiles) {
  const { VideoCaptionModel } = await import('@server/models/video/video-caption.js')
  const captions = await VideoCaptionModel.listVideoCaptions(video.id)
  if (captions.length === 0) return

  await createAllCaptionPlaylistsOnFSIfNeeded(video)
}

async function ensureLocalMasterPlaylistExists (
  video: MVideoWithAllFiles,
  playlist: MStreamingPlaylistFiles
) {
  const masterPath = VideoPathManager.Instance.getFSHLSOutputPath(video, playlist.playlistFilename)
  const requiresSha = CONFIG.OBJECT_STORAGE.GENERATE_SHA256_SEGMENTS !== false && !!playlist.segmentsSha256Filename
  const shaPath = requiresSha
    ? VideoPathManager.Instance.getFSHLSOutputPath(video, playlist.segmentsSha256Filename)
    : null

  const hasMaster = await pathExists(masterPath)
  const hasSha = requiresSha ? await pathExists(shaPath) : true

  if (hasMaster && hasSha) {
    logger.debug(
      '[GRANULAR_MOVE] Local master playlist%s exist at %s%s',
      requiresSha ? ' + SHA' : '',
      masterPath,
      requiresSha ? ` and ${shaPath}` : ''
    )
    return { video, playlist }
  }

  if (!hasMaster) {
    logger.info('[GRANULAR_MOVE] Local master playlist not found at %s, attempting to restore', masterPath)
  }
  if (requiresSha && !hasSha) {
    logger.info('[GRANULAR_MOVE] Local SHA file not found at %s, attempting to restore', shaPath)
  }

  // Try to download missing files from object storage first.
  if (playlist.storage === FileStorage.OBJECT_STORAGE) {
    try {
      if (!hasMaster) {
        const tmpMasterPath = await makeHLSFileAvailable(
          video,
          playlist.playlistFilename,
          VideoPathManager.Instance.buildTMPDestination(playlist.playlistFilename)
        )

        await move(tmpMasterPath, masterPath)
        logger.info('[GRANULAR_MOVE] Restored master playlist from object storage to %s', masterPath)
      }

      if (requiresSha && !hasSha) {
        const tmpShaPath = await makeHLSFileAvailable(
          video,
          playlist.segmentsSha256Filename,
          VideoPathManager.Instance.buildTMPDestination(playlist.segmentsSha256Filename)
        )

        await move(tmpShaPath, shaPath)
        logger.info('[GRANULAR_MOVE] Restored SHA file from object storage to %s', shaPath)
      }
    } catch (err) {
      logger.warn(
        '[GRANULAR_MOVE] Failed to restore missing master/SHA files from object storage: %s',
        err.message || err
      )
    }
  }

  const hasMasterAfterRestore = await pathExists(masterPath)
  const hasShaAfterRestore = requiresSha ? await pathExists(shaPath) : true
  if (hasMasterAfterRestore && hasShaAfterRestore) {
    return { video, playlist }
  }

  // If download failed or not on object storage, regenerate
  logger.info('[GRANULAR_MOVE] Regenerating master playlist and SHA locally')
  const { updateM3U8AndShaPlaylist } = await import('@server/lib/hls.js')
  const playlistUpdated = await updateM3U8AndShaPlaylist(video, playlist, { throwOnError: true })
  if (!playlistUpdated) {
    throw new Error(`HLS playlist ${playlist.id} was not regenerated locally`)
  }

  const reloadedVideo = await VideoModel.loadWithFiles(video.uuid)
  if (!reloadedVideo) {
    throw new Error(`Video ${video.uuid} not found after master playlist regeneration`)
  }

  const reloadedPlaylist = reloadedVideo.VideoStreamingPlaylists?.find(p => p.id === playlist.id)
  if (!reloadedPlaylist) {
    throw new Error(`Playlist ${playlist.id} not found after master playlist regeneration`)
  }

  const reloadedMasterPath = VideoPathManager.Instance.getFSHLSOutputPath(reloadedVideo, reloadedPlaylist.playlistFilename)
  const reloadedRequiresSha = CONFIG.OBJECT_STORAGE.GENERATE_SHA256_SEGMENTS !== false && !!reloadedPlaylist.segmentsSha256Filename
  const reloadedShaPath = reloadedRequiresSha
    ? VideoPathManager.Instance.getFSHLSOutputPath(reloadedVideo, reloadedPlaylist.segmentsSha256Filename)
    : null

  const hasReloadedMasterNow = await pathExists(reloadedMasterPath)
  const hasReloadedShaNow = reloadedRequiresSha ? await pathExists(reloadedShaPath) : true
  if (hasReloadedMasterNow && hasReloadedShaNow) {
    logger.info(
      '[GRANULAR_MOVE] Regenerated master playlist%s at %s%s',
      reloadedRequiresSha ? ' + SHA' : '',
      reloadedMasterPath,
      reloadedRequiresSha ? ` and ${reloadedShaPath}` : ''
    )
    return { video: reloadedVideo, playlist: reloadedPlaylist }
  }

  if (reloadedPlaylist.storage === FileStorage.OBJECT_STORAGE) {
    try {
      const hasReloadedMaster = await pathExists(reloadedMasterPath)
      const hasReloadedSha = reloadedRequiresSha ? await pathExists(reloadedShaPath) : true

      if (!hasReloadedMaster) {
        const tmpMasterPath = await makeHLSFileAvailable(
          reloadedVideo,
          reloadedPlaylist.playlistFilename,
          VideoPathManager.Instance.buildTMPDestination(reloadedPlaylist.playlistFilename)
        )

        await move(tmpMasterPath, reloadedMasterPath)
        logger.info('[GRANULAR_MOVE] Restored regenerated master playlist to %s', reloadedMasterPath)
      }

      if (reloadedRequiresSha && !hasReloadedSha) {
        const tmpShaPath = await makeHLSFileAvailable(
          reloadedVideo,
          reloadedPlaylist.segmentsSha256Filename,
          VideoPathManager.Instance.buildTMPDestination(reloadedPlaylist.segmentsSha256Filename)
        )

        await move(tmpShaPath, reloadedShaPath)
        logger.info('[GRANULAR_MOVE] Restored regenerated SHA file to %s', reloadedShaPath)
      }

      if (await pathExists(reloadedMasterPath) && (!reloadedRequiresSha || await pathExists(reloadedShaPath))) {
        return { video: reloadedVideo, playlist: reloadedPlaylist }
      }
    } catch (err) {
      logger.warn(
        '[GRANULAR_MOVE] Failed to restore regenerated master/SHA files from object storage: %s',
        err.message || err
      )
    }
  }

  throw new Error(
    `Master playlist or SHA file not found at ${reloadedMasterPath}` + (reloadedRequiresSha ? ` / ${reloadedShaPath}` : '')
  )
}

async function checkAndTransitionVideoState (
  videoUUID: string,
  isNewVideo: boolean,
  previousVideoState: any,
  isFirstResolutionMove: boolean,
  allowFailedStateTransition: boolean
) {
  // For first resolution move, publish immediately so playback can start while
  // the next resolutions are still transcoding/moving.
  if (isFirstResolutionMove) {
    logger.info(
      '[GRANULAR_MOVE] First resolution moved, publishing video now for %s',
      videoUUID
    )

    await publishVideoAfterFirstResolutionMove({ videoUUID, isNewVideo })
  }

  // Decrement pendingMove counter after critical state transition work, so
  // final failure handling can safely decrement if the transition throws.
  const pendingMove = await VideoJobInfoModel.decrease(videoUUID, 'pendingMove')
  logger.info('[GRANULAR_MOVE] Decremented pendingMove for %s, remaining: %d', videoUUID, pendingMove)

  if (isFirstResolutionMove) {
    // If pendingMove is still > 0, there are more moves pending so don't delete local files yet
    if (pendingMove > 0) {
      logger.info(
        '[GRANULAR_MOVE] More move jobs pending for %s (pendingMove: %d), keeping local master playlist',
        videoUUID,
        pendingMove
      )
      return
    }
  }

  if (pendingMove === 0) {
    logger.info('[GRANULAR_MOVE] All granular move work complete for %s, advancing video state if needed.', videoUUID)
    await maybeTransitionAfterObjectStorageMove({
      videoUUID,
      moveVideoState: { isNewVideo, previousVideoState },
      reason: 'granular move completion',
      allowFailedState: allowFailedStateTransition
    })
  }

  logger.info('[GRANULAR_MOVE] Move state check complete for %s. pendingMove=%d', videoUUID, pendingMove)
}

async function publishVideoAfterFirstResolutionMove (options: {
  videoUUID: string
  isNewVideo: boolean
}) {
  const { videoUUID, isNewVideo } = options

  const { sequelizeTypescript } = await import('@server/initializers/database.js')
  const video = await VideoModel.load(videoUUID)
  if (!video) return

  if (video.state === VideoState.PUBLISHED) return

  await sequelizeTypescript.transaction(async transaction => {
    await video.setNewState(VideoState.PUBLISHED, isNewVideo, transaction)
  })

  logger.info('[GRANULAR_MOVE] Published video %s after first HLS batch move', videoUUID)
}

function normalizeProgressPercent (percent: number) {
  if (!Number.isFinite(percent)) return 0

  const rounded = Math.round(percent)
  if (rounded < 0) return 0
  if (rounded > 100) return 100

  return rounded
}

export async function onGranularMoveToObjectStorageFailure (job: Job, err: any) {
  logger.error('[GRANULAR_MOVE] Granular move job %s failed: %s', job.id, err.message || err)

  const maxAttempts = job.opts?.attempts ?? 1
  if (job.attemptsMade < maxAttempts) return

  const payload = job.data as MoveHLSPlaylistPayloadWithCleanup & { videoUUID?: string }
  if (!payload.videoUUID || payload.cleanupMode === 'cleanup') return

  const pendingMove = await VideoJobInfoModel.decrease(payload.videoUUID, 'pendingMove')
  logger.info(
    '[GRANULAR_MOVE] Final failure for job %s decremented pendingMove for %s, remaining: %d',
    job.id,
    payload.videoUUID,
    pendingMove
  )

  const video = await VideoModel.loadWithFiles(payload.videoUUID)
  if (!video) return

  if (video.state === VideoState.PUBLISHED) {
    logger.warn(
      '[GRANULAR_MOVE] Final failure for published video %s leaves the video published; failed object-storage job can be retried.',
      payload.videoUUID,
      { err }
    )
    return
  }

  await moveToFailedMoveToObjectStorageState(video)
}

function isRetryOfFailedJob (payload: unknown) {
  return (payload as RetryableGranularMovePayload)?.retryOfFailedJob === true
}
