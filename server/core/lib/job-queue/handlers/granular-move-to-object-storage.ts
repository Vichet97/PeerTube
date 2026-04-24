import {
  FileStorage,
  isMoveVideoFilePayload,
  isMoveHLSPlaylistPayload,
  isMoveThumbnailPayload
} from '@peertube/peertube-models'
import { logger } from '@server/helpers/logger.js'
import { CONFIG } from '@server/initializers/config.js'
import { VideoJobInfoModel } from '@server/models/video/video-job-info.js'
import { VideoModel } from '@server/models/video/video.js'
import { VideoPathManager } from '@server/lib/video-path-manager.js'
import { pathExists, remove, move } from 'fs-extra/esm'
import { Job } from 'bullmq'
import { moveVideoFileToObjectStorage } from '@server/lib/move-storage/move-to-object-storage.js'
import { moveThumbnailToObjectStorage } from '@server/lib/move-storage/move-to-object-storage.js'
import { moveMasterPlaylistToObjectStorage } from '@server/lib/move-storage/move-to-object-storage.js'
import { moveHLSSegmentFilesToObjectStorage } from '@server/lib/move-storage/move-to-object-storage.js'
import { MStreamingPlaylist, MVideo } from '@server/types/models/index.js'
import { makeHLSFileAvailable } from '@server/lib/object-storage/videos.js'

export async function processGranularMoveToObjectStorage (job: Job) {
  const payload = job.data

  if (CONFIG.OBJECT_STORAGE.ENABLED !== true) {
    logger.info('[GRANULAR_MOVE] Skipping %s job %s because object storage is disabled', job.name, job.id)
    return
  }

  const updateProgress = (percent: number) => {
    job.updateProgress(percent).catch(err => logger.error('Cannot update granular move progress', { err }))
  }

  try {
    if (isMoveVideoFilePayload(payload)) {
      return await processMoveVideoFile(job, payload, updateProgress)
    }

    if (isMoveHLSPlaylistPayload(payload)) {
      return await processMoveHLSPlaylist(job, payload, updateProgress)
    }

    if (isMoveThumbnailPayload(payload)) {
      return await processMoveThumbnail(job, payload, updateProgress)
    }

    throw new Error('Unknown granular move payload type: ' + JSON.stringify(payload))
  } catch (err) {
    logger.error('[GRANULAR_MOVE] Job %s failed: %s', job.id, err.message || err)
    throw err
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
    return
  }

  const videoFile = video.VideoFiles.find(f => f.id === fileId)
  if (!videoFile) {
    logger.warn('[GRANULAR_MOVE] Video file %s not found for video %s', fileId, videoUUID)
    return
  }

  // Skip if already on object storage
  if (videoFile.storage === FileStorage.OBJECT_STORAGE) {
    logger.info('[GRANULAR_MOVE] File %s already on object storage, skipping', fileId)
    updateProgress(100)
    return
  }

  // Check source file exists
  const sourcePath = VideoPathManager.Instance.getFSVideoFileOutputPath(video, videoFile)
  const fileExists = await pathExists(sourcePath)

  if (!fileExists) {
    logger.warn('[GRANULAR_MOVE] Source file %s does not exist, marking as moved', sourcePath)
    videoFile.storage = FileStorage.OBJECT_STORAGE
    await videoFile.save()
    updateProgress(100)
    return
  }

  logger.info('[GRANULAR_MOVE] Moving video file %s from %s', fileId, sourcePath)
  updateProgress(0)

  try {
    await moveVideoFileToObjectStorage(videoUUID, fileId)

    updateProgress(100)
    logger.info('[GRANULAR_MOVE] Video file %s move completed', fileId)

    // Check if all files are moved to determine if we should transition video state
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState, false)

  } catch (err) {
    logger.error('[GRANULAR_MOVE] Failed to move video file %s: %s', fileId, err.message || err)
    throw err
  }
}

async function processMoveHLSPlaylist (
  job: Job,
  payload: { videoUUID: string; playlistId: number; fileIds: number[]; isNewVideo: boolean; previousVideoState?: any },
  updateProgress: (percent: number) => void
) {
  const { videoUUID, playlistId, fileIds, isNewVideo, previousVideoState } = payload

  logger.info('[GRANULAR_MOVE] Processing HLS playlist move job %s for playlist %s, files %s', job.id, playlistId, fileIds.join(', '))

  const video = await VideoModel.loadWithFiles(videoUUID)
  if (!video) {
    logger.warn('[GRANULAR_MOVE] Video %s not found, skipping HLS playlist move', videoUUID)
    return
  }

  const playlist = video.VideoStreamingPlaylists?.find(p => p.id === playlistId)
  if (!playlist) {
    logger.warn('[GRANULAR_MOVE] HLS playlist %s not found for video %s', playlistId, videoUUID)
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

    const fragmentPath = VideoPathManager.Instance.getFSHLSOutputPath(video, videoFile.filename)
    const playlistPath = VideoPathManager.Instance.getFSHLSOutputPath(
      video,
      videoFile.filename.replace(/\.m4s$/, '.m3u8')
    )

    const fragmentExists = await pathExists(fragmentPath)
    const playlistExists = await pathExists(playlistPath)

    if (!fragmentExists) missingFiles.push(fragmentPath)
    if (!playlistExists) missingFiles.push(playlistPath)

    filesToMove.push(fileId)
  }

  // If ALL files were already in object storage, skip the job entirely
  if (filesToMove.length === 0 && skippedFileIds.length === fileIds.length) {
    logger.info('[GRANULAR_MOVE] All files for playlist %s are already in object storage, skipping job %s', playlistId, job.id)
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState, false)
    return
  }

  // If some files are missing and some still need to be moved, create a new job with only filesToMove
  if (missingFiles.length > 0 && filesToMove.length > 0) {
    const delayMs = CONFIG.OBJECT_STORAGE.MOVE_FILE_DELAY || 30000

    logger.info('[GRANULAR_MOVE] HLS files not ready for playlist %s. Creating delayed job in %dms. Missing: %s, Files to move: %s',
      playlistId, delayMs, missingFiles.join(', '), filesToMove.join(', '))

    // Create a new delayed job with ONLY the files that still need to be moved
    const { JobQueue } = await import('@server/lib/job-queue/index.js')
    await JobQueue.Instance.createJob({
      type: 'move-hls-playlist-to-object-storage',
      payload: {
        ...payload,
        fileIds: filesToMove
      },
      delay: delayMs
    })

    // Return successfully - the new delayed job will handle the retry
    return
  }

  // If some files need to be moved (no missing files), continue with the move
  // Update fileIds to only include files that need to be moved
  const effectiveFileIds = filesToMove.length > 0 ? filesToMove : fileIds

  updateProgress(0)

  // Determine if this is the first resolution being moved to object storage
  const isFirstResolutionMove = playlist.storage !== FileStorage.OBJECT_STORAGE

  if (isFirstResolutionMove) {
    logger.info('[GRANULAR_MOVE] First resolution move for %s, will upload master playlist and update storage', videoUUID)
  } else {
    logger.info('[GRANULAR_MOVE] Subsequent resolution move for %s, will update local master playlist only', videoUUID)
  }

  try {
    // Step 0: Ensure local master playlist exists
    // If local master playlist is missing (e.g., deleted after first resolution), download from OS
    await ensureLocalMasterPlaylistExists(video, playlist)

    // Step 1: Upload fragment + resolution playlist files to object storage
    // This must be done BEFORE setting file.storage = OBJECT_STORAGE
    // because regenerating master playlist needs to hash these files (via makeAvailable)
    updateProgress(20)
    await moveHLSSegmentFilesToObjectStorage(videoUUID, playlistId, effectiveFileIds)
    // Note: moveHLSSegmentFilesToObjectStorage sets videoFile.storage = OBJECT_STORAGE

    // Step 2: If this is the first resolution, set playlist.storage to OBJECT_STORAGE
    // BEFORE regenerating master playlist so it generates OS segment URLs
    if (isFirstResolutionMove) {
      playlist.storage = FileStorage.OBJECT_STORAGE
      await playlist.save()
      logger.info('[GRANULAR_MOVE] Set playlist.storage = OBJECT_STORAGE before master regeneration', playlistId)
    }

    // Step 3: Regenerate master playlist + SHA
    // When playlist.storage = OBJECT_STORAGE and file.storage = OBJECT_STORAGE,
    // this generates OS segment URLs and SHA256 by downloading from object storage
    logger.info('[GRANULAR_MOVE] Regenerating master playlist and SHA for playlist %s', playlistId)
    const { updateM3U8AndShaPlaylist } = await import('@server/lib/hls.js')
    await updateM3U8AndShaPlaylist(video, playlist)

    // Step 4: Upload master playlist to object storage
    // Master playlist now has correct OS segment URLs
    updateProgress(80)
    await moveMasterPlaylistToObjectStorage(videoUUID, playlistId)
    logger.info('[GRANULAR_MOVE] Master playlist uploaded with OS URLs', playlistId)

    updateProgress(100)

    // Step 5: Transition video state
    // This is done immediately after first resolution move to publish the video
    // Subsequent resolution moves will not trigger state transition
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState, isFirstResolutionMove)

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
    return
  }

  const thumbnail = video.Thumbnails?.find(t => t.id === thumbnailId)
  if (!thumbnail) {
    logger.warn('[GRANULAR_MOVE] Thumbnail %s not found for video %s', thumbnailId, videoUUID)
    return
  }

  // Skip if already on object storage
  if (thumbnail.storage === FileStorage.OBJECT_STORAGE) {
    logger.info('[GRANULAR_MOVE] Thumbnail %s already on object storage, skipping', thumbnailId)
    updateProgress(100)
    return
  }

  // Check source file exists
  const sourcePath = thumbnail.getFSPath()
  const fileExists = await pathExists(sourcePath)

  if (!fileExists) {
    logger.warn('[GRANULAR_MOVE] Source thumbnail %s does not exist, marking as moved', sourcePath)
    thumbnail.storage = FileStorage.OBJECT_STORAGE
    await thumbnail.save()
    updateProgress(100)
    return
  }

  updateProgress(0)

  try {
    await moveThumbnailToObjectStorage(videoUUID, thumbnailId)

    updateProgress(100)
    logger.info('[GRANULAR_MOVE] Thumbnail %s move completed', thumbnailId)

    // Check if all files are moved to determine if we should transition video state
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState, false)

  } catch (err) {
    logger.error('[GRANULAR_MOVE] Failed to move thumbnail %s: %s', thumbnailId, err.message || err)
    throw err
  }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

async function ensureLocalMasterPlaylistExists (video: MVideo, playlist: MStreamingPlaylist) {
  const masterPath = VideoPathManager.Instance.getFSHLSOutputPath(video, playlist.playlistFilename)
  const masterExists = await pathExists(masterPath)

  if (masterExists) {
    logger.debug('[GRANULAR_MOVE] Local master playlist exists at %s', masterPath)
    return
  }

  logger.info('[GRANULAR_MOVE] Local master playlist not found at %s, attempting to restore', masterPath)

  // Try to download from object storage
  if (playlist.storage === FileStorage.OBJECT_STORAGE) {
    try {
      const tmpPath = await makeHLSFileAvailable(video, playlist.playlistFilename, VideoPathManager.Instance.buildTMPDestination(playlist.playlistFilename))

      await move(tmpPath, masterPath)
      logger.info('[GRANULAR_MOVE] Restored master playlist from object storage to %s', masterPath)
      return
    } catch (err) {
      logger.warn('[GRANULAR_MOVE] Failed to download master playlist from object storage: %s', err.message || err)
    }
  }

  // If download failed or not on object storage, regenerate
  logger.info('[GRANULAR_MOVE] Regenerating master playlist locally')
  const { updateM3U8AndShaPlaylist } = await import('@server/lib/hls.js')
  await updateM3U8AndShaPlaylist(video, playlist)
  logger.info('[GRANULAR_MOVE] Regenerated master playlist at %s', masterPath)
}

async function checkAndTransitionVideoState (videoUUID: string, isNewVideo: boolean, previousVideoState: any, isFirstResolutionMove: boolean) {
  // For first resolution move, transition video state immediately
  // This publishes the video so users can start watching from object storage
  if (isFirstResolutionMove) {
    logger.info('[GRANULAR_MOVE] First resolution moved, transitioning video state now', videoUUID)

    const { moveToNextState } = await import('@server/lib/video-state.js')
    await moveToNextState({ video: { uuid: videoUUID }, isNewVideo, previousVideoState })
    return
  }

  // For subsequent moves, just wait for pendingMove to reach 0
  // Local master playlist is kept for SHA256 regeneration during subsequent moves
  // It will be deleted when the video is fully processed (by the video transcoding job)
  const pendingMove = await VideoJobInfoModel.decrease(videoUUID, 'pendingMove')
  logger.info('[GRANULAR_MOVE] Decremented pendingMove for %s, remaining: %d', videoUUID, pendingMove)

  if (pendingMove > 0) {
    logger.info('[GRANULAR_MOVE] More move jobs pending for %s (pendingMove: %d), skipping', videoUUID, pendingMove)
    return
  }

  // All moves complete - delete local master playlist
  logger.info('[GRANULAR_MOVE] All move jobs complete for %s, deleting local master playlist', videoUUID)
  await deleteLocalMasterPlaylist(videoUUID)
}

async function deleteLocalMasterPlaylist (videoUUID: string) {
  try {
    const video = await VideoModel.loadWithFiles(videoUUID)
    if (!video) return

    const playlist = video.VideoStreamingPlaylists?.[0]
    if (!playlist) return

    // Delete local master playlist and SHA if they exist
    const masterPath = VideoPathManager.Instance.getFSHLSOutputPath(video, playlist.playlistFilename)
    const shaPath = VideoPathManager.Instance.getFSHLSOutputPath(video, playlist.segmentsSha256Filename)

    await removeLocalPathNow(masterPath)
    await removeLocalPathNow(shaPath)
    logger.info('[GRANULAR_MOVE] Local master playlist and SHA deleted for %s', videoUUID)
  } catch (err) {
    logger.warn('[GRANULAR_MOVE] Error deleting local master playlist for %s: %s', videoUUID, err)
    // Don't throw - this is cleanup, shouldn't block state transition
  }
}

async function removeLocalPathNow (path: string) {
  try {
    await remove(path)
    logger.debug('[GRANULAR_MOVE] Removed local file %s', path)
  } catch (err: any) {
    if (err.code !== 'ENOENT') {
      logger.warn('[GRANULAR_MOVE] Failed to remove local file %s: %s', path, err.message)
    }
  }
}

export async function onGranularMoveToObjectStorageFailure (job: Job, err: any) {
  logger.error('[GRANULAR_MOVE] Granular move job %s failed: %s', job.id, err.message || err)
}
