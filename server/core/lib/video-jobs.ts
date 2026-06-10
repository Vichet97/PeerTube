import {
  FileStorage,
  ManageVideoTorrentPayload,
  VideoFileStream,
  VideoPrivacy,
  VideoPrivacyType,
  VideoState,
  VideoStateType
} from '@peertube/peertube-models'
import { logger, loggerTagsFactory } from '@server/helpers/logger.js'
import { CONFIG } from '@server/initializers/config.js'
import { VideoJobInfoModel } from '@server/models/video/video-job-info.js'
import { VideoModel } from '@server/models/video/video.js'
import { MVideo, MVideoFile, MVideoFullLight, MVideoUUID } from '@server/types/models/index.js'
import { CreateJobArgument, CreateJobOptions, JobQueue } from './job-queue/job-queue.js'
import { VideoStoryboardJobHandler } from './runners/index.js'
import { createTranscriptionTaskIfNeeded } from './video-captions.js'
import { moveFilesIfPrivacyChanged } from './video-privacy.js'

const lTags = loggerTagsFactory('video-jobs')

export async function buildMoveVideoJob (options: {
  video: MVideoUUID
  type: 'move-to-object-storage' | 'move-to-file-system'

  moveVideoState?: {
    isNewVideo: boolean
    previousVideoState: VideoStateType
  }

  hlsCutover?: {
    playlistId: number
    fileIds: number[]
  }

  // When true, doesn't increment pendingMove (for follow-up jobs)
  isFollowUp?: boolean
}) {
  const { video, moveVideoState, type, hlsCutover, isFollowUp } = options

  // Check if there's already a pending/active move job for this video
  // Only create new job if no existing job or if existing job failed
  const existingJob = await JobQueue.Instance.getExistingMoveJob(
    type,
    video.uuid,
    { isFollowUp: isFollowUp === true }
  )
  if (existingJob) {
    // Check if job failed by looking at failedReason
    const hasFailed = existingJob.failedReason !== undefined && existingJob.failedReason !== null
    if (hasFailed) {
      logger.info(`[MOVE_JOB] Previous job %s failed for video %s, will create new job`, existingJob.id, video.uuid, lTags(video.uuid))
    } else {
      logger.info(
        `[MOVE_JOB] Skipping duplicate move job for video %s - job %s already pending/active`,
        video.uuid,
        existingJob.id,
        lTags(video.uuid)
      )
      return undefined
    }
  }

  // Follow-up jobs (like HLS cutover) shouldn't increment pendingMove
  // because the parent job already established the count
  if (!isFollowUp) {
    await VideoJobInfoModel.increaseOrCreate(video.uuid, 'pendingMove')
  }

  return {
    type,
    payload: {
      videoUUID: video.uuid,
      moveVideoState,
      hlsCutover,
      isFollowUp
    }
  }
}

export async function buildCaptionMoveJob (captionId: number, videoUUID?: string) {
  // Check if there's already a pending/active move job for this caption/video batch
  const existingJob = videoUUID
    ? await JobQueue.Instance.getExistingCaptionMoveJobByVideoUUID(videoUUID)
    : await JobQueue.Instance.getExistingCaptionMoveJob(captionId)
  if (existingJob) {
    // Check if job failed by looking at failedReason
    const hasFailed = existingJob.failedReason !== undefined && existingJob.failedReason !== null
    if (hasFailed) {
      logger.info(
        `[MOVE_JOB] Previous caption job %s failed, will create new job for caption %s%s`,
        existingJob.id,
        captionId,
        videoUUID ? ` (video ${videoUUID})` : ''
      )
    } else {
      logger.info(
        `[MOVE_JOB] Skipping duplicate move job for caption %s%s - job %s already pending/active`,
        captionId,
        videoUUID ? ` (video ${videoUUID})` : '',
        existingJob.id
      )
      return undefined
    }
  }

  return {
    type: 'move-caption-to-object-storage' as const,
    payload: videoUUID
      ? { captionId, videoUUID }
      : { captionId }
  }
}

export async function createMoveJobWithPendingMoveRollback (job: (CreateJobArgument & CreateJobOptions) | undefined) {
  if (!job) return undefined

  try {
    const createdJob = await JobQueue.Instance.createJob(job)
    if (!createdJob) throw new Error(`Cannot create ${job.type} job for video ${(job.payload as { videoUUID?: string })?.videoUUID}`)

    return createdJob
  } catch (err) {
    await rollbackPendingMoveForMoveJob(job)
    throw err
  }
}

export async function createPendingMoveJobs (options: {
  videoUUID: string
  jobs: (CreateJobArgument & CreateJobOptions)[]
}) {
  const { videoUUID, jobs } = options
  if (jobs.length === 0) return 0

  await VideoJobInfoModel.increaseOrCreate(videoUUID, 'pendingMove', jobs.length)

  let createdJobs = 0
  try {
    for (const job of jobs) {
      const createdJob = await JobQueue.Instance.createJob(job)
      if (!createdJob) throw new Error(`Cannot create ${job.type} job for video ${videoUUID}`)

      createdJobs++
    }

    return createdJobs
  } catch (err) {
    const unqueuedJobs = jobs.length - createdJobs
    if (unqueuedJobs > 0) {
      await VideoJobInfoModel.decrease(videoUUID, 'pendingMove', unqueuedJobs)
    }

    throw err
  }
}

export async function rollbackPendingMoveForMoveJob (job: (CreateJobArgument & CreateJobOptions) | undefined) {
  if (!job) return
  if (!hasPendingMoveIncrement(job)) return

  await VideoJobInfoModel.decrease((job.payload as { videoUUID: string }).videoUUID, 'pendingMove')
}

function hasPendingMoveIncrement (job: CreateJobArgument & CreateJobOptions) {
  if (job.type !== 'move-to-object-storage' && job.type !== 'move-to-file-system') return false

  const payload = job.payload as { videoUUID?: string, isFollowUp?: boolean }

  return !!payload.videoUUID && payload.isFollowUp !== true
}

// ---------------------------------------------------------------------------
// Granular move job builders
// ---------------------------------------------------------------------------

export async function buildGranularMoveJobs (options: {
  videoUUID: string
  isNewVideo: boolean
  previousVideoState?: VideoStateType
}) {
  const { videoUUID, isNewVideo, previousVideoState } = options
  const jobs: any[] = []

  const video = await VideoModel.loadWithFiles(videoUUID)
  if (!video) {
    logger.warn(`[GRANULAR_MOVE] Video ${videoUUID} not found, cannot create move jobs`)
    return jobs
  }

  // 1. Individual web video files
  const webVideoFiles = video.VideoFiles?.filter(f => f.storage === FileStorage.FILE_SYSTEM) || []
  for (const file of webVideoFiles) {
    const job = await buildGranularVideoFileMoveJob({
      videoUUID,
      fileId: file.id,
      isNewVideo,
      previousVideoState
    })
    if (job) jobs.push(job)
  }

  // 2. HLS playlists with their segment files
  const hlsPlaylists = video.VideoStreamingPlaylists || []
  for (const playlist of hlsPlaylists) {
    const hlsFilesOnFS = playlist.VideoFiles.filter(f => f.storage === FileStorage.FILE_SYSTEM)

    if (hlsFilesOnFS.length > 0) {
      // Use a single HLS move job per playlist to avoid overlapping jobs
      // concurrently regenerating/deleting master playlist and SHA files.
      const playlistJob = await buildGranularHLSPlaylistMoveJob({
        videoUUID,
        playlistId: playlist.id,
        fileIds: hlsFilesOnFS.map(f => f.id),
        isNewVideo,
        previousVideoState
      })

      if (playlistJob) jobs.push(playlistJob)
      // Master playlist and SHA are handled INSIDE the HLS playlist job — no separate job needed.
    }
  }

  // 3. Thumbnails
  const thumbnails = video.Thumbnails?.filter(t => t.storage === FileStorage.FILE_SYSTEM) || []
  for (const thumbnail of thumbnails) {
    const job = await buildGranularThumbnailMoveJob({
      videoUUID,
      thumbnailId: thumbnail.id,
      isNewVideo,
      previousVideoState
    })
    if (job) jobs.push(job)
  }

  // 4. Captions (handled separately by their own flow)
  // We don't add caption move jobs here - they're created when captions are created

  logger.info(`[GRANULAR_MOVE] Created ${jobs.length} granular move jobs for video ${videoUUID}`, {
    webVideos: webVideoFiles.length,
    hlsPlaylists: hlsPlaylists.length,
    thumbnails: thumbnails.length
  })

  return jobs
}

export async function buildGranularVideoFileMoveJob (options: {
  videoUUID: string
  fileId: number
  isNewVideo: boolean
  previousVideoState?: VideoStateType
}) {
  const { videoUUID, fileId, isNewVideo, previousVideoState } = options

  const existingJob = await JobQueue.Instance.getExistingMoveJob(
    'move-video-file-to-object-storage',
    videoUUID,
    { fileId }
  )
  if (existingJob) {
    const hasFailed = existingJob.failedReason !== undefined && existingJob.failedReason !== null
    if (!hasFailed) {
      logger.info(`[GRANULAR_MOVE] Skipping duplicate video file move job for ${fileId} - job ${existingJob.id} already pending/active`)
      return undefined
    }
    logger.info(`[GRANULAR_MOVE] Previous video file move job ${existingJob.id} failed, will create new job`)
  }

  return {
    type: 'move-video-file-to-object-storage' as const,
    payload: {
      videoUUID,
      fileId,
      isNewVideo,
      previousVideoState
    }
  }
}

export async function buildGranularHLSPlaylistMoveJob (options: {
  videoUUID: string
  playlistId: number
  fileIds: number[]
  isNewVideo: boolean
  previousVideoState?: VideoStateType
}) {
  const { videoUUID, playlistId, fileIds, isNewVideo, previousVideoState } = options
  let fileIdsToSchedule = fileIds

  // Dedupe by exact video+playlist+fileIds payload.
  // Regular flow now queues one playlist-scoped job containing all fileIds;
  // subset fileIds are mainly used by delayed retry jobs spawned by the handler.
  const existingJobs = await JobQueue.Instance.getExistingHLSPlaylistMoveJobs(videoUUID, playlistId, fileIds, { match: 'overlap' })
  if (existingJobs.length > 0) {
    const pendingOrActiveJobs = existingJobs.filter((j: any) => j.failedReason === undefined || j.failedReason === null)
    const hasFailedJobs = existingJobs.some((j: any) => j.failedReason !== undefined && j.failedReason !== null)
    const pendingFileIds = new Set<number>(
      pendingOrActiveJobs.flatMap((j: any) => Array.isArray(j.data?.fileIds) ? j.data.fileIds : [])
    )
    const uncoveredFileIds = fileIds.filter(id => !pendingFileIds.has(id))

    if (pendingOrActiveJobs.length !== 0 && uncoveredFileIds.length === 0) {
      logger.info(
        `[GRANULAR_MOVE] Skipping duplicate HLS playlist move job for playlist ${playlistId} files ${fileIds.join(',')} - ` +
          `job(s) ${pendingOrActiveJobs.map((j: any) => j.id).join(', ')} already pending/active`
      )
      return undefined
    }

    if (pendingOrActiveJobs.length !== 0) {
      logger.info(
        `[GRANULAR_MOVE] Existing HLS playlist move job(s) ${pendingOrActiveJobs.map((j: any) => j.id).join(', ')} ` +
          `already cover file(s) ${Array.from(pendingFileIds).join(',')}; scheduling uncovered file(s) ${uncoveredFileIds.join(',')}`
      )
      fileIdsToSchedule = uncoveredFileIds
    }

    if (hasFailedJobs) {
      logger.info(`[GRANULAR_MOVE] Previous HLS playlist move job(s) failed, will create new job for playlist ${playlistId}`)
    }
  }

  return {
    type: 'move-hls-playlist-to-object-storage' as const,
    payload: {
      videoUUID,
      playlistId,
      fileIds: fileIdsToSchedule,
      isNewVideo,
      previousVideoState
    }
  }
}

export async function buildGranularThumbnailMoveJob (options: {
  videoUUID: string
  thumbnailId: number
  isNewVideo: boolean
  previousVideoState?: VideoStateType
}) {
  const { videoUUID, thumbnailId, isNewVideo, previousVideoState } = options

  const existingJob = await JobQueue.Instance.getExistingMoveJob(
    'move-thumbnail-to-object-storage',
    videoUUID,
    { thumbnailId }
  )
  if (existingJob) {
    const hasFailed = existingJob.failedReason !== undefined && existingJob.failedReason !== null
    if (!hasFailed) {
      logger.info(`[GRANULAR_MOVE] Skipping duplicate thumbnail move job for ${thumbnailId} - job ${existingJob.id} already pending/active`)
      return undefined
    }
    logger.info(`[GRANULAR_MOVE] Previous thumbnail move job ${existingJob.id} failed, will create new job`)
  }

  return {
    type: 'move-thumbnail-to-object-storage' as const,
    payload: {
      videoUUID,
      thumbnailId,
      isNewVideo,
      previousVideoState
    }
  }
}

// ---------------------------------------------------------------------------
// Storyboard
// ---------------------------------------------------------------------------

export async function buildLocalStoryboardJobIfNeeded (options: {
  video: MVideo
  federate: boolean
}) {
  const { video, federate } = options

  const hasVideo = await VideoModel.loadHasStream(video.id, VideoFileStream.VIDEO)

  if (hasVideo && CONFIG.STORYBOARDS.ENABLED && !CONFIG.STORYBOARDS.REMOTE_RUNNERS.ENABLED) {
    return {
      type: 'generate-video-storyboard' as 'generate-video-storyboard',
      payload: {
        videoUUID: video.uuid,
        federate
      }
    }
  }

  if (federate === true) {
    return {
      type: 'federate-video' as 'federate-video',
      payload: {
        videoUUID: video.uuid,
        isNewVideoForFederation: false
      }
    }
  }

  return undefined
}

export async function addRemoteStoryboardJobIfNeeded (video: MVideo) {
  if (CONFIG.STORYBOARDS.ENABLED !== true) return
  if (CONFIG.STORYBOARDS.REMOTE_RUNNERS.ENABLED !== true) return
  if (!await VideoModel.loadHasStream(video.id, VideoFileStream.VIDEO)) return

  return new VideoStoryboardJobHandler().create({ videoUUID: video.uuid })
}

export async function addLocalOrRemoteStoryboardJobIfNeeded (options: {
  video: MVideo
  federate: boolean
}) {
  const { video, federate } = options

  if (CONFIG.STORYBOARDS.ENABLED !== true) return

  if (CONFIG.STORYBOARDS.REMOTE_RUNNERS.ENABLED === true) {
    await addRemoteStoryboardJobIfNeeded(video)
  } else {
    await JobQueue.Instance.createJob(await buildLocalStoryboardJobIfNeeded({ video, federate }))
  }
}

// ---------------------------------------------------------------------------
// Multiple jobs creation
// ---------------------------------------------------------------------------

export async function addVideoJobsAfterCreation (options: {
  video: MVideo
  videoFile: MVideoFile
  generateTranscription: boolean
}) {
  const { video, videoFile, generateTranscription } = options

  const jobs: ((CreateJobArgument & CreateJobOptions) | undefined)[] = [
    {
      type: 'manage-video-torrent' as 'manage-video-torrent',
      payload: {
        videoId: video.id,
        videoFileId: videoFile.id,
        action: 'create'
      }
    },

    await buildLocalStoryboardJobIfNeeded({ video, federate: false }),

    {
      type: 'notify',
      payload: {
        action: 'new-video',
        videoUUID: video.uuid
      }
    },

    {
      type: 'federate-video' as 'federate-video',
      payload: {
        videoUUID: video.uuid,
        isNewVideoForFederation: true
      }
    }
  ]

  const criticalJobs: Promise<unknown>[] = []

  // No transcoding, move the files directly on object storage using granular jobs.
  // Granular jobs move each file type independently, avoiding the HLS cutover
  // follow-up job that was causing double-decrement of pendingMove.
  if (video.state === VideoState.TO_MOVE_TO_EXTERNAL_STORAGE) {
    const moveJobs = await buildGranularMoveJobs({
      videoUUID: video.uuid,
      isNewVideo: true,
      previousVideoState: undefined
    })

    if (moveJobs.length > 0) {
      logger.info('[VIDEO_JOBS] Creating %d granular move jobs for %s (no transcoding)', moveJobs.length, video.uuid)
      criticalJobs.push(createPendingMoveJobs({ videoUUID: video.uuid, jobs: moveJobs }))
    } else {
      logger.info('[VIDEO_JOBS] No files to move for %s, skipping move jobs', video.uuid)
    }
  }

  if (video.state === VideoState.TO_TRANSCODE) {
    criticalJobs.push(
      JobQueue.Instance.createJob({
        type: 'transcoding-job-builder' as 'transcoding-job-builder',
        payload: {
          videoUUID: video.uuid,
          optimizeJob: {
            isNewVideo: true
          }
        }
      })
    )
  }

  if (generateTranscription === true) {
    criticalJobs.push(createTranscriptionTaskIfNeeded(video))
  }

  await Promise.all([
    ...jobs.map(job => JobQueue.Instance.createJob(job)),
    ...criticalJobs,
    addRemoteStoryboardJobIfNeeded(video)
  ])
}

export async function addVideoJobsAfterUpdate (options: {
  video: MVideoFullLight
  isNewVideoForFederation: boolean

  nameChanged: boolean
  oldPrivacy: VideoPrivacyType
}) {
  const { video, nameChanged, oldPrivacy, isNewVideoForFederation } = options
  const jobs: CreateJobArgument[] = []

  const filePathChanged = await moveFilesIfPrivacyChanged(video, oldPrivacy)
  const hls = video.getHLSPlaylist()

  if (filePathChanged && hls) {
    await hls.assignP2PMediaLoaderInfoHashes(video, hls.VideoFiles)
    await hls.save()
  }

  if (!video.isLive && (nameChanged || filePathChanged)) {
    for (const file of (video.VideoFiles || [])) {
      const payload: ManageVideoTorrentPayload = { action: 'update-metadata', videoId: video.id, videoFileId: file.id }

      jobs.push({ type: 'manage-video-torrent', payload })
    }

    const hls = video.getHLSPlaylist()

    for (const file of (hls?.VideoFiles || [])) {
      const payload: ManageVideoTorrentPayload = { action: 'update-metadata', streamingPlaylistId: hls.id, videoFileId: file.id }

      jobs.push({ type: 'manage-video-torrent', payload })
    }
  }

  jobs.push({
    type: 'federate-video',
    payload: {
      videoUUID: video.uuid,
      isNewVideoForFederation
    }
  })

  const wasConfidentialVideoForNotification = new Set<VideoPrivacyType>([
    VideoPrivacy.PRIVATE,
    VideoPrivacy.UNLISTED
  ]).has(oldPrivacy)

  if (wasConfidentialVideoForNotification) {
    jobs.push({
      type: 'notify',
      payload: {
        action: 'new-video',
        videoUUID: video.uuid
      }
    })
  }

  return JobQueue.Instance.createSequentialJobFlow(...jobs)
}
