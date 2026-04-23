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
import { pathExists, remove } from 'fs-extra/esm'
import { Job } from 'bullmq'
import { moveVideoFileToObjectStorage } from '@server/lib/move-storage/move-to-object-storage.js'
import { moveThumbnailToObjectStorage } from '@server/lib/move-storage/move-to-object-storage.js'
import { moveMasterPlaylistToObjectStorage } from '@server/lib/move-storage/move-to-object-storage.js'
import { moveHLSSegmentFilesToObjectStorage } from '@server/lib/move-storage/move-to-object-storage.js'

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
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState)

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
    logger.info('[GRANULAR_MOVE] Playlist %s is already in object storage, skipping job %s', playlistId, job.id)
    return
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
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState)
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

  // Check if there are pending transcoding jobs for this video
  // If yes, we should NOT upload/delete the master playlist yet because other resolutions are still transcoding
  // Also check if master playlist is already in object storage
  const hasPendingTranscodingJobs = await checkHasPendingTranscodingJobs(videoUUID)
  const playlistAlreadyOnOS = playlist.storage === FileStorage.OBJECT_STORAGE as any
  const shouldMoveMasterPlaylist = !hasPendingTranscodingJobs && !playlistAlreadyOnOS

  if (hasPendingTranscodingJobs) {
    logger.info('[GRANULAR_MOVE] Pending transcoding jobs found for %s, will move segments but skip master playlist upload', videoUUID)
  } else if (playlistAlreadyOnOS) {
    logger.info('[GRANULAR_MOVE] Playlist already on object storage for %s, skipping master playlist upload', videoUUID)
  } else {
    logger.info('[GRANULAR_MOVE] No pending transcoding for %s, will upload master playlist', videoUUID)
  }

  try {
    // Step 1: Regenerate master playlist + SHA while LOCAL segment files still exist
    // This ensures the SHA can read from local files for fast hashing
    logger.info('[GRANULAR_MOVE] Regenerating master playlist and SHA before segment moves for playlist %s', playlistId)
    const { updateM3U8AndShaPlaylist } = await import('@server/lib/hls.js')
    await updateM3U8AndShaPlaylist(video, playlist)

    // Step 2: Upload fragment + resolution playlist files, then delete locally
    // This is done by moveHLSSegmentFilesToObjectStorage
    updateProgress(20)
    await moveHLSSegmentFilesToObjectStorage(videoUUID, playlistId, effectiveFileIds)

    // Step 3: Upload master playlist + SHA, then delete locally
    // Do this AFTER segment files are uploaded so master references valid URLs
    // BUT only if there are no pending transcoding jobs
    if (shouldMoveMasterPlaylist) {
      updateProgress(80)
      await moveMasterPlaylistToObjectStorage(videoUUID, playlistId)

      // Step 4: Delete local master + SHA files
      const masterPath = VideoPathManager.Instance.getFSHLSOutputPath(video, playlist.playlistFilename)
      const shaPath = VideoPathManager.Instance.getFSHLSOutputPath(video, playlist.segmentsSha256Filename)
      await removeLocalPathNow(masterPath)
      await removeLocalPathNow(shaPath)
      logger.info('[GRANULAR_MOVE] Deleted local master playlist and SHA for playlist %s', playlistId)
    } else {
      logger.info('[GRANULAR_MOVE] Skipped master playlist upload/delete for %s due to pending transcoding', playlistId)
      updateProgress(100)
      logger.info('[GRANULAR_MOVE] HLS playlist %s bundle move completed (segments only)', playlistId)
      // Still check state transition, but don't complete the full bundle move
      await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState)
      return
    }

    updateProgress(100)
    logger.info('[GRANULAR_MOVE] HLS playlist %s bundle move completed', playlistId)

    // Check if all files are moved to determine if we should transition video state
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState)

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
    await checkAndTransitionVideoState(videoUUID, isNewVideo, previousVideoState)

  } catch (err) {
    logger.error('[GRANULAR_MOVE] Failed to move thumbnail %s: %s', thumbnailId, err.message || err)
    throw err
  }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

// Check if there are pending transcoding jobs for this video
// Used to determine if we should upload/delete the master playlist
async function checkHasPendingTranscodingJobs (videoUUID: string): Promise<boolean> {
  const { JobQueue } = await import('@server/lib/job-queue/index.js')
  const pendingJobs = await JobQueue.Instance.listVideoUUIDsWithPendingTranscodingJobs()
  return pendingJobs.has(videoUUID)
}

async function checkAndTransitionVideoState (videoUUID: string, isNewVideo: boolean, previousVideoState: any) {
  const pendingMove = await VideoJobInfoModel.decrease(videoUUID, 'pendingMove')
  logger.info('[GRANULAR_MOVE] Decremented pendingMove for %s, remaining: %d', videoUUID, pendingMove)

  // Only transition video state when ALL move jobs have completed (pendingMove reaches 0)
  if (pendingMove > 0) {
    logger.info('[GRANULAR_MOVE] More move jobs pending for %s (pendingMove: %d), skipping state transition', videoUUID, pendingMove)
    return
  }

  // Check if there are still pending transcoding jobs
  // If yes, we should NOT publish yet because the master playlist hasn't been uploaded
  const hasPendingTranscoding = await checkHasPendingTranscodingJobs(videoUUID)
  if (hasPendingTranscoding) {
    logger.info('[GRANULAR_MOVE] Pending transcoding jobs found for %s, skipping state transition until master playlist is uploaded', videoUUID)
    return
  }

  // Check if master playlist is on object storage
  const masterOnObjectStorage = await checkMasterPlaylistOnObjectStorage(videoUUID)
  if (!masterOnObjectStorage) {
    logger.info('[GRANULAR_MOVE] Master playlist not yet on object storage for %s, uploading now before state transition', videoUUID)
    // Trigger master playlist upload
    await uploadMasterPlaylistNow(videoUUID)
  }

  // All move jobs complete, no pending transcoding, master playlist on object storage - safe to transition
  logger.info('[GRANULAR_MOVE] All conditions met for %s, calling moveToNextState', videoUUID)
  const { moveToNextState } = await import('@server/lib/video-state.js')
  await moveToNextState({ video: { uuid: videoUUID }, isNewVideo, previousVideoState })
}

async function checkMasterPlaylistOnObjectStorage (videoUUID: string): Promise<boolean> {
  try {
    const video = await VideoModel.loadWithFiles(videoUUID)
    if (!video) return false

    const playlist = video.VideoStreamingPlaylists?.[0]
    if (!playlist) return false

    // Check if playlist storage is OBJECT_STORAGE
    return playlist.storage === FileStorage.OBJECT_STORAGE as any
  } catch (err) {
    logger.warn('[GRANULAR_MOVE] Error checking master playlist storage for %s: %s', videoUUID, err)
    return false
  }
}

async function uploadMasterPlaylistNow (videoUUID: string) {
  try {
    const video = await VideoModel.loadWithFiles(videoUUID)
    if (!video) return

    const playlist = video.VideoStreamingPlaylists?.[0]
    if (!playlist) return

    // Check if playlist storage is already OBJECT_STORAGE
    if (playlist.storage === FileStorage.OBJECT_STORAGE as any) {
      logger.info('[GRANULAR_MOVE] Master playlist already on object storage for %s', videoUUID)
      return
    }

    // Regenerate master playlist with object storage URLs
    const { updateM3U8AndShaPlaylist } = await import('@server/lib/hls.js')
    await updateM3U8AndShaPlaylist(video, playlist)

    // Upload master playlist to object storage
    const { moveMasterPlaylistToObjectStorage } = await import('@server/lib/move-storage/move-to-object-storage.js')
    await moveMasterPlaylistToObjectStorage(videoUUID, playlist.id)

    // Delete local master playlist
    const masterPath = VideoPathManager.Instance.getFSHLSOutputPath(video, playlist.playlistFilename)
    const shaPath = VideoPathManager.Instance.getFSHLSOutputPath(video, playlist.segmentsSha256Filename)
    await removeLocalPathNow(masterPath)
    await removeLocalPathNow(shaPath)

    logger.info('[GRANULAR_MOVE] Master playlist uploaded to object storage for %s', videoUUID)
  } catch (err) {
    logger.error('[GRANULAR_MOVE] Failed to upload master playlist for %s: %s', videoUUID, err)
    throw err
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
