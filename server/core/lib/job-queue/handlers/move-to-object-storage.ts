import {
  FileStorage,
  VideoStateType,
  isMoveCaptionPayload,
  isMoveVideoStoragePayload,
  MoveStoragePayload
} from '@peertube/peertube-models'
import { logger, loggerTagsFactory } from '@server/helpers/logger.js'
import { CONFIG } from '@server/initializers/config.js'
import {
  moveCaptionToObjectStorage,
  moveVideoToObjectStorage,
  onMoveVideoToObjectStorageFailure
} from '@server/lib/move-storage/move-to-object-storage.js'
import { storeHLSFileFromFilename } from '@server/lib/object-storage/index.js'
import { VideoPathManager } from '@server/lib/video-path-manager.js'
import { VideoModel } from '@server/models/video/video.js'
import { pathExists } from 'fs-extra/esm'
import { Job } from 'bullmq'
import { join } from 'path'

const lTagsBase = loggerTagsFactory('move-object-storage')

type LegacyMoveToObjectStoragePayload = {
  videoUUID: string
  moveVideoState: {
    isNewVideo: boolean
    previousVideoState?: VideoStateType
  }
  hlsCutover?: {
    playlistId: number
    fileIds: number[]
  }
  isFollowUp?: boolean
}

export async function processMoveToObjectStorage (job: Job) {
  const rawPayload = job.data

  let payload: MoveStoragePayload

  if (isMoveCaptionPayload(rawPayload)) {
    payload = rawPayload
  } else if (isMoveVideoStoragePayload(rawPayload)) {
    // Modern format with hlsCutover
    payload = rawPayload
  } else if ('videoUUID' in rawPayload && 'moveVideoState' in rawPayload) {
    // Legacy format: moveVideoState contains isNewVideo/previousVideoState directly
    // Convert to modern format
    logger.info('[MOVE_JOB] Detected legacy move-to-object-storage job %s, converting to modern format', job.id)
    const legacyPayload = rawPayload as LegacyMoveToObjectStoragePayload
    const { isNewVideo, previousVideoState } = legacyPayload.moveVideoState
    payload = {
      videoUUID: legacyPayload.videoUUID,
      isNewVideo,
      previousVideoState,
      hlsCutover: legacyPayload.hlsCutover,
      isFollowUp: legacyPayload.isFollowUp
    }
  } else if ('videoUUID' in rawPayload && Object.keys(rawPayload).length === 1) {
    // Minimal format: only videoUUID provided
    // Load video to determine state
    logger.info('[MOVE_JOB] Detected minimal move-to-object-storage job %s, inferring state from video', job.id)
    const video = await VideoModel.load(rawPayload.videoUUID)
    if (!video) {
      logger.info('[MOVE_JOB] Video %s not found for minimal job %s, skipping stale job', rawPayload.videoUUID, job.id)
      return
    }
    payload = {
      videoUUID: rawPayload.videoUUID,
      isNewVideo: false,
      previousVideoState: video.state
    }
  } else {
    throw new Error('Unknown payload type: ' + JSON.stringify(rawPayload))
  }

  const updateProgress = (percent: number) => {
    const normalized = normalizeProgressPercent(percent)
    const currentProgress = typeof job.progress === 'number' ? job.progress : 0
    if (normalized <= currentProgress) return

    job.updateProgress(normalized).catch(err => logger.error('Cannot update move job progress', { err }))
  }

  if (CONFIG.OBJECT_STORAGE.ENABLED !== true) {
    logger.info('[MOVE_JOB] Skipping move-to-object-storage job %s because object storage is disabled', job.id)
    return
  }

  if (isMoveVideoStoragePayload(payload)) {
    // [LOGGER] Job started (video)
    logger.info('[MOVE_JOB] Move-to-object-storage job %s started for video %s', job.id, payload.videoUUID)

    const video = await VideoModel.loadWithFiles(payload.videoUUID)
    if (!video) {
      // [LOGGER] Video not found
      logger.warn('[MOVE_JOB] Video %s not found, cancelling job %s', payload.videoUUID, job.id)
      throw new Error('Video was deleted - transcoding job cancelled')
    }

    // Load captions separately (not included in loadWithFiles)
    const { VideoCaptionModel } = await import('@server/models/video/video-caption.js')
    const captions = await VideoCaptionModel.listVideoCaptions(video.id)
    const captionsOnFileSystem = captions.filter(c => c.storage === FileStorage.FILE_SYSTEM)

    // Collect file information for logging
    const webVideoFiles = video.VideoFiles?.filter(f => f.storage === FileStorage.FILE_SYSTEM) || []
    const hlsFiles = video.VideoStreamingPlaylists?.flatMap(p => p.VideoFiles.filter(f => f.storage === FileStorage.FILE_SYSTEM)) || []
    const thumbnails = video.Thumbnails?.filter(t => t.storage === FileStorage.FILE_SYSTEM) || []
    const torrentFilenames = new Set<string>()
    for (const file of [ ...video.VideoFiles, ...(video.VideoStreamingPlaylists?.flatMap(p => p.VideoFiles) || []) ]) {
      if (file.torrentFilename) torrentFilenames.add(file.torrentFilename)
    }

    const localTorrentFiles: { filename: string, sourcePath: string }[] = []
    for (const torrentFilename of torrentFilenames) {
      const sourcePath = join(CONFIG.STORAGE.TORRENTS_DIR, torrentFilename)
      if (await pathExists(sourcePath)) {
        localTorrentFiles.push({ filename: torrentFilename, sourcePath })
      }
    }

    // [LOGGER] Video loaded with files - detailed move plan
    logger.info('[MOVE_JOB] Video %s move plan for job %s', payload.videoUUID, job.id, {
      totalFileCount: {
        webVideos: webVideoFiles.length,
        hlsFiles: hlsFiles.length,
        thumbnails: thumbnails.length,
        captions: captionsOnFileSystem.length,
        torrents: localTorrentFiles.length
      },
      webVideoFiles: webVideoFiles.map(f => ({
        filename: f.filename,
        resolution: f.resolution,
        size: f.size,
        sourcePath: VideoPathManager.Instance.getFSVideoFileOutputPath(video, f),
        destinationBucket: CONFIG.OBJECT_STORAGE.WEB_VIDEOS.BUCKET_NAME,
        destinationKey: `${CONFIG.OBJECT_STORAGE.WEB_VIDEOS.PREFIX || ''}${f.filename}`
      })),
      hlsFiles: hlsFiles.map(f => ({
        filename: f.filename,
        resolution: f.resolution,
        size: f.size,
        destinationBucket: CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS.BUCKET_NAME,
        destinationKey: `${CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS.PREFIX || ''}hls/${video.uuid}/${f.filename}`
      })),
      thumbnails: thumbnails.map(t => ({
        filename: t.filename,
        width: t.width,
        height: t.height,
        sourcePath: t.getFSPath(),
        destinationBucket: CONFIG.OBJECT_STORAGE.THUMBNAILS.BUCKET_NAME,
        destinationKey: `${CONFIG.OBJECT_STORAGE.THUMBNAILS.PREFIX || ''}${t.filename}`
      })),
      captions: captionsOnFileSystem.map(c => ({
        filename: c.filename,
        language: c.language,
        sourcePath: c.getFSFilePath(),
        destinationBucket: CONFIG.OBJECT_STORAGE.CAPTIONS.BUCKET_NAME,
        destinationKey: `${CONFIG.OBJECT_STORAGE.CAPTIONS.PREFIX || ''}${c.filename}`
      })),
      torrents: localTorrentFiles.map(t => ({
        filename: t.filename,
        sourcePath: t.sourcePath,
        destinationBucket: CONFIG.OBJECT_STORAGE.TORRENTS.BUCKET_NAME,
        destinationKey: `${CONFIG.OBJECT_STORAGE.TORRENTS.PREFIX || ''}${t.filename}`
      })),
      objectStorageEndpoint: CONFIG.OBJECT_STORAGE.ENDPOINT
    })

    // Check if there are files that actually need to be moved
    const filesToMove = [
      ...webVideoFiles,
      ...hlsFiles,
      ...thumbnails,
      ...captionsOnFileSystem,
      ...localTorrentFiles
    ]

    if (filesToMove.length === 0) {
      logger.info('[MOVE_JOB] All files for video %s are already on object storage, skipping job %s', payload.videoUUID, job.id)
      return
    }

    // Verify source files exist before starting the move
    const missingFiles: string[] = []
    for (const f of webVideoFiles) {
      const path = VideoPathManager.Instance.getFSVideoFileOutputPath(video, f)
      if (!(await pathExists(path))) {
        missingFiles.push(`web-video: ${f.filename} at ${path}`)
      }
    }
    for (const f of hlsFiles) {
      const path = VideoPathManager.Instance.getFSHLSOutputPath(video, f.filename)
      if (!(await pathExists(path))) {
        missingFiles.push(`hls: ${f.filename} at ${path}`)
      }
    }
    for (const t of thumbnails) {
      const path = t.getFSPath()
      if (!(await pathExists(path))) {
        missingFiles.push(`thumbnail: ${t.filename} at ${path}`)
      }
    }
    for (const c of captionsOnFileSystem) {
      const path = c.getFSFilePath()
      if (!(await pathExists(path))) {
        missingFiles.push(`caption: ${c.filename} at ${path}`)
      }
    }

    if (missingFiles.length > 0) {
      logger.error('[MOVE_JOB] Source files missing for video %s, job %s: %s', payload.videoUUID, job.id, missingFiles.join(', '))
      // Update the database to mark files as on object storage since source is gone
      for (const f of webVideoFiles) {
        if (missingFiles.some(m => m.includes(f.filename))) {
          f.storage = FileStorage.OBJECT_STORAGE
          await f.save()
        }
      }
      // For HLS files, we need to be more careful - just fail the job
      if (hlsFiles.some(f => missingFiles.some(m => m.includes(f.filename)))) {
        throw new Error('HLS source files are missing, cannot complete move operation')
      }
    }

    // Update initial progress
    updateProgress(5)

    // Determine moveVideoState
    // When retrying a failed job, use the video's CURRENT state as previousVideoState
    // so that buildNextVideoState can correctly transition to PUBLISHED
    let moveVideoState = payload.moveVideoState

    if (moveVideoState && payload.isNewVideo === undefined) {
      // This is a retry of a failed job (not a new job creation)
      // Use the current video state as previousVideoState to ensure proper state transition
      moveVideoState = {
        ...moveVideoState,
        previousVideoState: video.state
      }
      // [LOGGER] Retry detected, using current video state
      logger.info('[MOVE_JOB] Retry detected for %s, current state: %s', payload.videoUUID, video.state)
    } else if (payload.isNewVideo !== undefined) {
      moveVideoState = {
        isNewVideo: payload.isNewVideo,
        previousVideoState: payload.previousVideoState
      }
      // [LOGGER] New job, using provided state
      logger.info('[MOVE_JOB] New job for %s, isNewVideo=%s', payload.videoUUID, payload.isNewVideo)
    }

    // [LOGGER] Starting move operation
    logger.info('[MOVE_JOB] Starting move operation for %s', payload.videoUUID)

    const startTime = Date.now()

    try {
      await moveVideoToObjectStorage({
        videoUUID: payload.videoUUID,
        moveVideoState,
        hlsCutover: payload.hlsCutover,
        loggerTags: lTagsBase().tags,
        onProgress: (percent: number) => updateProgress(percent)
      })

      const duration = Date.now() - startTime
      // [LOGGER] Move operation completed
      logger.info('[MOVE_JOB] Move operation completed for %s (duration: %dms)', payload.videoUUID, duration)
      updateProgress(100)
    } catch (err) {
      logger.error('[MOVE_JOB] Move operation failed for %s: %s', payload.videoUUID, err.message || err)
      throw err
    }
  } else if (isMoveCaptionPayload(payload)) {
    // [LOGGER] Job started (caption)
    logger.info('[MOVE_JOB] Move-to-object-storage job %s started for caption %s', job.id, payload.captionId)

    // Import models inside the function
    const { VideoCaptionModel } = await import('@server/models/video/video-caption.js')

    // Check if caption exists and get its current state
    const caption = await VideoCaptionModel.loadWithVideo(payload.captionId)

    if (!caption) {
      // Caption was deleted, skip this job
      logger.info('[MOVE_JOB] Caption %s not found, skipping job %s', payload.captionId, job.id)
      return
    }

    // Skip if caption is already on object storage
    if (caption.storage === FileStorage.OBJECT_STORAGE) {
      logger.info('[MOVE_JOB] Caption %s is already on object storage, skipping job %s', payload.captionId, job.id)
      return
    }

    // Check if the source file exists
    const captionPath = caption.getFSFilePath()
    const fileExists = await pathExists(captionPath)

    if (!fileExists) {
      // File doesn't exist on filesystem, mark as already moved or skip
      logger.warn('[MOVE_JOB] Caption file %s does not exist at %s, skipping job %s', caption.filename, captionPath, job.id)
      // Update storage status since file is gone
      caption.storage = FileStorage.OBJECT_STORAGE
      await caption.save()
      return
    }

    logger.info('[MOVE_JOB] Moving caption %s from %s', caption.filename, captionPath)

    updateProgress(50)

    await moveCaptionToObjectStorage({
      captionId: payload.captionId,
      loggerTags: lTagsBase().tags
    })

    // After moving the caption, regenerate and upload the master playlist
    // to include the caption reference (so players can load caption tracks)
    if (CONFIG.OBJECT_STORAGE.ENABLED) {
      try {
        const { VideoStreamingPlaylistModel } = await import('@server/models/video/video-streaming-playlist.js')
        const hls = await VideoStreamingPlaylistModel.loadHLSByVideo(caption.videoId)

        if (hls?.storage === FileStorage.OBJECT_STORAGE) {
          const video = await VideoModel.loadFull(caption.videoId)
          if (video) {
            logger.info('[MOVE_JOB] Regenerating master playlist after caption move for video %s', video.uuid)
            const { updateM3U8AndShaPlaylist } = await import('@server/lib/hls.js')
            await updateM3U8AndShaPlaylist(video, hls)

            // Upload the updated master playlist
            await storeHLSFileFromFilename(video, hls.playlistFilename)
            await storeHLSFileFromFilename(video, hls.segmentsSha256Filename)
            logger.info('[MOVE_JOB] Master playlist updated and uploaded after caption move')
          }
        }
      } catch (err) {
        logger.warn('[MOVE_JOB] Failed to update master playlist after caption move: %s', err.message || err)
        // Don't fail the job if master playlist update fails
      }
    }

    updateProgress(100)
    logger.info('[MOVE_JOB] Caption move completed for caption %s', payload.captionId)
  } else {
    throw new Error('Unknown payload type')
  }
}

function normalizeProgressPercent (percent: number) {
  if (!Number.isFinite(percent)) return 0

  const rounded = Math.round(percent)
  if (rounded < 0) return 0
  if (rounded > 100) return 100

  return rounded
}

export async function onMoveToObjectStorageFailure (job: Job, err: any) {
  const payload = job.data as MoveStoragePayload

  if (!isMoveVideoStoragePayload(payload)) return

  // [LOGGER] Move job failed
  logger.error('[MOVE_JOB] Move-to-object-storage job %s FAILED for video %s: %s', 
    job.id, payload.videoUUID, err.message || err)

  await onMoveVideoToObjectStorageFailure({
    videoUUID: payload.videoUUID,
    err,
    loggerTags: lTagsBase().tags
  })

  // [LOGGER] Failure handler complete
  logger.info('[MOVE_JOB] Failure handler complete for %s', payload.videoUUID)
}
