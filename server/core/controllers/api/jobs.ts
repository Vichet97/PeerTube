import {
  FileStorage,
  HttpStatusCode,
  Job,
  JobState,
  JobType,
  ResultList,
  RunnerJobState,
  RunnerJobType,
  UserRight,
  VideoImportState,
  VideoFileStream,
  VideoState,
  VideoStateType
} from '@peertube/peertube-models'
import { Job as BullJob } from 'bullmq'
import express from 'express'
import { readdir } from 'fs/promises'
import { pathExists, remove } from 'fs-extra/esm'
import { Op, QueryTypes } from 'sequelize'
import { isArray } from '../../helpers/custom-validators/misc.js'
import { logger } from '../../helpers/logger.js'
import { CONFIG } from '../../initializers/config.js'
import { DIRECTORIES } from '../../initializers/constants.js'
import { sequelizeTypescript } from '../../initializers/database.js'
import { JobQueue } from '../../lib/job-queue/index.js'
import { hasVideoResourcesToBeMoved } from '../../lib/move-storage/shared/move-video.js'
import { cleanupRetainedLocalFilesAfterRestart } from '../../lib/move-storage/move-to-object-storage.js'
import { getFSTorrentFilePath, getHLSResolutionPlaylistFilename } from '../../lib/paths.js'
import { Redis } from '../../lib/redis.js'
import { getUnprocessedOrphanedVideoRepairJobRefs } from './video-repair-job-orphans.js'
import {
  buildMoveVideoJob,
  buildLocalStoryboardJobIfNeeded,
  createMoveJobWithPendingMoveRollback
} from '../../lib/video-jobs.js'
import { VideoPathManager } from '../../lib/video-path-manager.js'
import { VideoCaptionModel } from '../../models/video/video-caption.js'
import { VideoImportModel } from '../../models/video/video-import.js'
import { VideoJobInfoModel } from '../../models/video/video-job-info.js'
import { VideoSourceModel } from '../../models/video/video-source.js'
import { VideoStreamingPlaylistModel } from '../../models/video/video-streaming-playlist.js'
import { StoryboardModel } from '../../models/video/storyboard.js'
import { VideoModel } from '../../models/video/video.js'
import { RunnerJobModel } from '../../models/runner/runner-job.js'
import { MVideoWithAllFiles } from '../../types/models/index.js'
import {
  apiRateLimiter,
  asyncMiddleware,
  authenticate,
  ensureUserHasRight,
  jobsSortValidator,
  openapiOperationDoc,
  paginationValidatorBuilder,
  setDefaultPagination,
  setDefaultSort
} from '../../middlewares/index.js'
import {
  createMoveStorageJobsValidator,
  createRetryTranscodingJobsValidator,
  listJobsValidator
} from '../../middlewares/validators/jobs.js'

const jobsRouter = express.Router()

jobsRouter.use(apiRateLimiter)

type VideoRepairJobRef = {
  job: BullJob
  state: JobState
  jobType: JobType
}

type VideoRepairJobIndex = {
  byVideoUUID: Map<string, VideoRepairJobRef[]>
  byVideoId: Map<number, VideoRepairJobRef[]>
  byVideoImportId: Map<number, VideoRepairJobRef[]>
}

type VideoMediaIntegrity = {
  hasMediaRecords: boolean
  hasPlayableMediaRecords: boolean
  hasFileSystemMediaRecords: boolean
  hasObjectStorageMediaRecords: boolean
  missingRequiredFiles: string[]
  missingRepairableFiles: string[]
  missingOriginalSourceIds: number[]
  missingShaPlaylistIds: number[]
}

type SystemVideoResetResult = {
  videosChecked: number
  videosUpdated: number
  videosDeleted: number
  jobsRemoved: number
  jobsRemoveFailed: number
  countersReset: number
  queuesPaused: number
  resetHoldEnabled: boolean
  queueJobsDrained: number
  queueJobsCleaned: number
  orphanDbRecordsDeleted: number
  localFilesDeleted: number
}

type SystemVideoResetStatus = {
  state: 'idle' | 'running' | 'completed' | 'failed'
  startedAt?: string
  finishedAt?: string
  error?: string
  result?: SystemVideoResetResult
}

type SystemResetPausedQueue = {
  jobType: JobType
  wasPaused: boolean
}

type VideoQueueCleanupResult = {
  drained: number
  cleaned: number
}

type SystemResetDbCleanupResult = {
  deleted: number
}

type SystemResetLocalFilesResult = {
  deleted: number
}

type ReferencedLocalFiles = {
  paths: Set<string>
  hlsDirectories: Set<string>
}

type GlobalQueueCleanupResult = {
  queuesPaused: number
  queueJobsDrained: number
  queueJobsCleaned: number
}

type GlobalQueueCleanupStatus = {
  state: 'idle' | 'running' | 'completed' | 'failed'
  startedAt?: string
  finishedAt?: string
  error?: string
  result?: GlobalQueueCleanupResult
}

const VIDEO_REPAIR_JOB_STATES = [ 'waiting', 'delayed', 'prioritized', 'waiting-children', 'active', 'failed' ] as const
const VIDEO_REPAIR_JOB_TYPES: JobType[] = [
  'video-import',
  'video-file-import',
  'transcoding-job-builder',
  'video-transcoding',
  'move-to-object-storage',
  'move-to-file-system',
  'move-video-file-to-object-storage',
  'move-hls-playlist-to-object-storage',
  'move-thumbnail-to-object-storage',
  'move-caption-to-object-storage',
  'video-transcription',
  'generate-video-storyboard',
  'video-studio-edition',
  'manage-video-torrent'
]
const INCOMPLETE_VIDEO_STATES = new Set<VideoStateType>([
  VideoState.TO_IMPORT,
  VideoState.TO_TRANSCODE,
  VideoState.TO_MOVE_TO_EXTERNAL_STORAGE,
  VideoState.TO_MOVE_TO_FILE_SYSTEM,
  VideoState.TO_EDIT
])
const FAILED_VIDEO_STATES = new Set<VideoStateType>([
  VideoState.TO_IMPORT_FAILED,
  VideoState.TRANSCODING_FAILED,
  VideoState.TO_MOVE_TO_EXTERNAL_STORAGE_FAILED,
  VideoState.TO_MOVE_TO_FILE_SYSTEM_FAILED
])
const ORPHAN_INCOMPLETE_VIDEO_IMPORT_STATES = [
  VideoImportState.PENDING,
  VideoImportState.PROCESSING
] as const
const VIDEO_QUEUE_CLEAN_STATES = [
  'completed',
  'failed',
  'active',
  'paused',
  'prioritized',
  'delayed',
  'waiting',
  'wait'
] as const
const VIDEO_QUEUE_CLEAN_LIMIT = 100_000
const LOCAL_VIDEO_STORAGE_DIRECTORIES = [
  DIRECTORIES.WEB_VIDEOS.PUBLIC,
  DIRECTORIES.WEB_VIDEOS.PRIVATE,
  DIRECTORIES.HLS_STREAMING_PLAYLIST.PUBLIC,
  DIRECTORIES.HLS_STREAMING_PLAYLIST.PRIVATE,
  DIRECTORIES.ORIGINAL_VIDEOS,
  CONFIG.STORAGE.THUMBNAILS_DIR,
  CONFIG.STORAGE.STORYBOARDS_DIR,
  CONFIG.STORAGE.CAPTIONS_DIR,
  CONFIG.STORAGE.TORRENTS_DIR
] as const
const VIDEO_PIPELINE_RUNNER_JOB_TYPES: RunnerJobType[] = [
  'vod-web-video-transcoding',
  'vod-hls-transcoding',
  'vod-audio-merge-transcoding',
  'video-studio-transcoding',
  'video-transcription',
  'generate-video-storyboard'
]

jobsRouter.post('/pause',
  authenticate,
  ensureUserHasRight(UserRight.MANAGE_JOBS),
  asyncMiddleware(pauseJobQueue)
)

jobsRouter.post('/resume',
  authenticate,
  ensureUserHasRight(UserRight.MANAGE_JOBS),
  resumeJobQueue
)

jobsRouter.post('/create-move-storage-jobs',
  authenticate,
  ensureUserHasRight(UserRight.MANAGE_JOBS),
  createMoveStorageJobsValidator,
  asyncMiddleware(createMoveStorageJobs)
)

jobsRouter.post('/create-retry-transcoding-jobs',
  authenticate,
  ensureUserHasRight(UserRight.MANAGE_JOBS),
  createRetryTranscodingJobsValidator,
  asyncMiddleware(createRetryTranscodingJobs)
)

jobsRouter.post('/create-transcription-jobs',
  authenticate,
  ensureUserHasRight(UserRight.MANAGE_JOBS),
  asyncMiddleware(createTranscriptionJobs)
)

jobsRouter.post('/create-storyboard-jobs',
  authenticate,
  ensureUserHasRight(UserRight.MANAGE_JOBS),
  asyncMiddleware(createStoryboardJobs)
)

jobsRouter.post('/cancel-jobs',
  authenticate,
  ensureUserHasRight(UserRight.MANAGE_JOBS),
  asyncMiddleware(cancelJobs)
)

jobsRouter.post('/clear-global-queue-backlog',
  authenticate,
  ensureUserHasRight(UserRight.MANAGE_JOBS),
  asyncMiddleware(clearGlobalQueueBacklog)
)

jobsRouter.post('/cleanup-retained-local-files',
  authenticate,
  ensureUserHasRight(UserRight.MANAGE_JOBS),
  asyncMiddleware(cleanupRetainedLocalFiles)
)

jobsRouter.get('/clear-global-queue-backlog',
  authenticate,
  ensureUserHasRight(UserRight.MANAGE_JOBS),
  asyncMiddleware(getClearGlobalQueueBacklogStatus)
)

jobsRouter.post('/recheck-videos-status',
  authenticate,
  ensureUserHasRight(UserRight.MANAGE_JOBS),
  asyncMiddleware(recheckVideosStatus)
)

jobsRouter.get('/recheck-videos-status',
  authenticate,
  ensureUserHasRight(UserRight.MANAGE_JOBS),
  asyncMiddleware(getRecheckVideosStatus)
)

jobsRouter.post('/retry-job',
  authenticate,
  ensureUserHasRight(UserRight.MANAGE_JOBS),
  asyncMiddleware(retryJob)
)

jobsRouter.delete('/:jobType/:jobId',
  authenticate,
  ensureUserHasRight(UserRight.MANAGE_JOBS),
  asyncMiddleware(removeJob)
)

jobsRouter.get('/video-maintenance-counts',
  authenticate,
  ensureUserHasRight(UserRight.MANAGE_JOBS),
  asyncMiddleware(getVideoMaintenanceCounts)
)

jobsRouter.get('/:state?',
  openapiOperationDoc({ operationId: 'getJobs' }),
  authenticate,
  ensureUserHasRight(UserRight.MANAGE_JOBS),
  paginationValidatorBuilder([ 'jobs' ]),
  jobsSortValidator,
  setDefaultSort,
  setDefaultPagination,
  listJobsValidator,
  asyncMiddleware(listJobs)
)

// ---------------------------------------------------------------------------

export {
  jobsRouter
}

// ---------------------------------------------------------------------------

async function createMoveStorageJobs (req: express.Request, res: express.Response) {
  const storage = req.body.storage as 'object-storage' | 'file-system'
  const scope = (req.body.scope as 'all' | 'disk-relief' | undefined) ?? 'all'
  const targetStorage = storage === 'object-storage' ? FileStorage.OBJECT_STORAGE : FileStorage.FILE_SYSTEM

  if (storage === 'object-storage' && !CONFIG.OBJECT_STORAGE.ENABLED) {
    return res.fail({
      status: HttpStatusCode.BAD_REQUEST_400,
      message: 'Object storage is not enabled on this instance'
    })
  }

  const ids = scope === 'disk-relief' && storage === 'object-storage'
    ? await listLocalIdsWithFileSystemMedia()
    : await VideoModel.listLocalIds()
  let jobsCreated = 0

  for (const id of ids) {
    const videoFull = await VideoModel.loadFull(id)
    if (videoFull.isLive) continue

    // Skip videos already in a move state (they have jobs)
    if (
      videoFull.state === VideoState.TO_MOVE_TO_EXTERNAL_STORAGE ||
      videoFull.state === VideoState.TO_MOVE_TO_FILE_SYSTEM
    ) {
      continue
    }

    if (await hasVideoResourcesToBeMoved(videoFull, targetStorage)) {
      try {
        const job = await buildMoveVideoJob({
          type: storage === 'object-storage' ? 'move-to-object-storage' : 'move-to-file-system',
          video: videoFull
        })
        if (job) {
          await createMoveJobWithPendingMoveRollback(job)
          jobsCreated++
        }
      } catch (err) {
        // Continue with other videos if one fails
      }
    }
  }

  return res.json({ jobsCreated })
}

async function listLocalIdsWithFileSystemMedia () {
  const rows = await sequelizeTypescript.query<{ id: number }>(`
    SELECT v."id"
    FROM "video" v
    WHERE v."remote" IS FALSE
      AND (
        EXISTS (
          SELECT 1
          FROM "videoFile" vf
          WHERE vf."videoId" = v."id" AND vf."storage" = ${FileStorage.FILE_SYSTEM}
        )
        OR EXISTS (
          SELECT 1
          FROM "videoStreamingPlaylist" vsp
          JOIN "videoFile" vfhls ON vfhls."videoStreamingPlaylistId" = vsp."id"
          WHERE vsp."videoId" = v."id" AND vfhls."storage" = ${FileStorage.FILE_SYSTEM}
        )
        OR EXISTS (
          SELECT 1
          FROM "videoSource" vs
          WHERE vs."videoId" = v."id" AND vs."storage" = ${FileStorage.FILE_SYSTEM}
        )
      )
    ORDER BY (
      COALESCE((
        SELECT SUM(vf."size")
        FROM "videoFile" vf
        WHERE vf."videoId" = v."id" AND vf."storage" = ${FileStorage.FILE_SYSTEM}
      ), 0)
      + COALESCE((
        SELECT SUM(vfhls."size")
        FROM "videoStreamingPlaylist" vsp
        JOIN "videoFile" vfhls ON vfhls."videoStreamingPlaylistId" = vsp."id"
        WHERE vsp."videoId" = v."id" AND vfhls."storage" = ${FileStorage.FILE_SYSTEM}
      ), 0)
      + COALESCE((
        SELECT SUM(vs."size")
        FROM "videoSource" vs
        WHERE vs."videoId" = v."id" AND vs."storage" = ${FileStorage.FILE_SYSTEM}
      ), 0)
    ) DESC,
    v."createdAt" ASC
  `, {
    type: QueryTypes.SELECT
  })

  return rows.map(row => row.id)
}

async function createRetryTranscodingJobs (req: express.Request, res: express.Response) {
  if (CONFIG.TRANSCODING.ENABLED !== true) {
    return res.fail({
      status: HttpStatusCode.BAD_REQUEST_400,
      message: 'Transcoding is not enabled on this instance'
    })
  }

  const ids = await VideoModel.listLocalIds()
  const videoUUIDsWithPendingJobs = await JobQueue.Instance.listVideoUUIDsWithPendingTranscodingJobs()
  const candidateStates = new Set<VideoStateType>([ VideoState.TRANSCODING_FAILED, VideoState.TO_TRANSCODE ])

  let jobsCreated = 0

  for (const id of ids) {
    const video = await VideoModel.loadFull(id)
    if (!video || video.isLive) continue
    if (!candidateStates.has(video.state)) continue

    if (videoUUIDsWithPendingJobs.has(video.uuid)) continue

    const info = await VideoJobInfoModel.load(video.id)
    if (info?.pendingTranscode > 0) continue

    const file = video.getMaxQualityFile(VideoFileStream.VIDEO) || video.getMaxQualityFile(VideoFileStream.AUDIO)
    if (!file) continue

    try {
      if (video.state !== VideoState.TO_TRANSCODE) {
        video.state = VideoState.TO_TRANSCODE
        await video.save()
      }

      await JobQueue.Instance.createJob({
        type: 'transcoding-job-builder',
        payload: {
          videoUUID: video.uuid,
          optimizeJob: { isNewVideo: false }
        }
      })

      jobsCreated++
      videoUUIDsWithPendingJobs.add(video.uuid)
    } catch {
      // Continue with other videos if one fails
    }
  }

  return res.json({ jobsCreated })
}

async function createTranscriptionJobs (req: express.Request, res: express.Response) {
  if (CONFIG.VIDEO_TRANSCRIPTION.ENABLED !== true) {
    return res.fail({
      status: HttpStatusCode.BAD_REQUEST_400,
      message: 'Video transcription is not enabled on this instance'
    })
  }

  const ids = await VideoModel.listLocalIds()
  let jobsCreated = 0

  for (const id of ids) {
    const videoFull = await VideoModel.loadFull(id)
    if (!videoFull || videoFull.isLive) continue

    // Skip videos that are in states where transcription doesn't make sense
    if (videoFull.state === VideoState.WAITING_FOR_LIVE || videoFull.state === VideoState.LIVE_ENDED) continue

    // Check if video has audio stream for transcription
    const hasAudio = await VideoModel.loadHasStream(videoFull.id, VideoFileStream.AUDIO)
    if (!hasAudio) continue

    // Check if there's already a pending transcription job
    const info = await VideoJobInfoModel.load(videoFull.id)
    if (info?.pendingTranscription > 0) continue

    // Check if video already has a successful auto-generated caption
    const existingCaptions = await VideoCaptionModel.findAll({
      where: {
        videoId: videoFull.id,
        automaticallyGenerated: true
      }
    })

    // If there's already a successful transcription, skip
    if (existingCaptions.length > 0) continue

    try {
      await JobQueue.Instance.createJob({
        type: 'video-transcription',
        payload: { videoUUID: videoFull.uuid }
      })

      await VideoJobInfoModel.increaseOrCreate(videoFull.uuid, 'pendingTranscription')
      jobsCreated++
    } catch {
      // Continue with other videos if one fails
    }
  }

  return res.json({ jobsCreated })
}

async function createStoryboardJobs (req: express.Request, res: express.Response) {
  if (CONFIG.STORYBOARDS.ENABLED !== true) {
    return res.fail({
      status: HttpStatusCode.BAD_REQUEST_400,
      message: 'Storyboards are not enabled on this instance'
    })
  }

  const ids = await VideoModel.listLocalIds()
  let jobsCreated = 0

  for (const id of ids) {
    const videoFull = await VideoModel.loadFull(id)
    if (!videoFull || videoFull.isLive) continue

    // Skip videos that are in states where storyboard generation doesn't make sense
    if (videoFull.state === VideoState.WAITING_FOR_LIVE || videoFull.state === VideoState.LIVE_ENDED) continue

    // Check if video has video stream
    const hasVideo = await VideoModel.loadHasStream(videoFull.id, VideoFileStream.VIDEO)
    if (!hasVideo) continue

    // Check if storyboard already exists
    const existingStoryboard = await StoryboardModel.findOne({
      where: { videoId: videoFull.id }
    })

    // If storyboard already exists, skip
    if (existingStoryboard) continue

    try {
      const job = await buildLocalStoryboardJobIfNeeded({
        video: videoFull,
        federate: false
      })

      if (job) {
        await JobQueue.Instance.createJob(job)
        jobsCreated++
      }
    } catch {
      // Continue with other videos if one fails
    }
  }

  return res.json({ jobsCreated })
}

async function cancelJobs (req: express.Request, res: express.Response) {
  const jobTypes = req.body.jobTypes as string[]
  const jobIds = req.body.jobIds as number[] | undefined

  if (!jobTypes || !Array.isArray(jobTypes) || jobTypes.length === 0) {
    return res.fail({
      status: HttpStatusCode.BAD_REQUEST_400,
      message: 'jobTypes must be a non-empty array'
    })
  }

  let cancelledCount = 0

  try {
    const queues = JobQueue.Instance.getQueues()

    if (jobIds && jobIds.length > 0) {
      // Cancel specific jobs by ID
      for (const jobId of jobIds) {
        for (const jobType of jobTypes) {
          const queue = queues[jobType as JobType]
          if (!queue) continue

          const job = await queue.getJob(String(jobId))
          if (!job) continue

          const state = await job.getState()
          if (state === 'waiting' || state === 'delayed') {
            await job.remove()
            cancelledCount++
          }
        }
      }
    } else {
      // Cancel all waiting/delayed jobs of the specified types
      const states: ('waiting' | 'delayed')[] = [ 'waiting', 'delayed' ]

      let jobTypesToCancel: string[] = jobTypes
      // If 'all' is specified, get all available queue names
      if (jobTypes.includes('all')) {
        jobTypesToCancel = Object.keys(queues)
      }

      for (const jobType of jobTypesToCancel) {
        const queue = queues[jobType as JobType]
        if (!queue) continue

        for (const state of states) {
          const jobs = await queue.getJobs([ state ], 0, 10000, true)

          for (const job of jobs) {
            cancelledCount += await removeQueuedJob(job)
          }
        }
      }
    }
  } catch (err) {
    logger.error('Error cancelling jobs', { err })
    return res.fail({
      status: HttpStatusCode.INTERNAL_SERVER_ERROR_500,
      message: 'Error cancelling jobs'
    })
  }

  return res.json({ cancelledCount })
}

async function removeQueuedJob (job: BullJob) {
  try {
    await job.remove()
    return 1
  } catch {
    // Job might have been processed already, continue
    return 0
  }
}

async function recheckVideosStatus (req: express.Request, res: express.Response) {
  const currentStatus = await getStoredVideoSystemResetStatus()
  if (currentStatus.state === 'running') {
    return res.json(currentStatus)
  }

  const jobType = req.body.jobType as string | undefined

  const startedAt = new Date().toISOString()
  const runningStatus: SystemVideoResetStatus = {
    state: 'running',
    startedAt
  }

  await Redis.Instance.setVideoPipelineSystemResetStatus(runningStatus)

  void startVideoSystemResetInBackground(jobType)

  return res.json(runningStatus)
}

async function cleanupRetainedLocalFiles (_req: express.Request, res: express.Response) {
  const result = await cleanupRetainedLocalFilesAfterRestart()

  return res.json(result)
}

async function getRecheckVideosStatus (_req: express.Request, res: express.Response) {
  return res.json(await getStoredVideoSystemResetStatus())
}

async function getStoredVideoSystemResetStatus (): Promise<SystemVideoResetStatus> {
  const status = await Redis.Instance.getVideoPipelineSystemResetStatus() as SystemVideoResetStatus | null

  return status || { state: 'idle' }
}

async function startVideoSystemResetInBackground (jobType: string | undefined) {
  try {
    const pausedQueues = await pauseVideoRepairQueuesForSystemReset()
    await Redis.Instance.setVideoPipelineSystemResetHold()

    const result = await runVideoSystemReset(jobType, pausedQueues)
    await Redis.Instance.setVideoPipelineSystemResetStatus({
      state: 'completed',
      startedAt: (await getStoredVideoSystemResetStatus()).startedAt,
      finishedAt: new Date().toISOString(),
      result
    })
  } catch (err) {
    logger.error('[SYSTEM_RESETTER] Video system reset failed.', { err })

    await Redis.Instance.setVideoPipelineSystemResetStatus({
      state: 'failed',
      startedAt: (await getStoredVideoSystemResetStatus()).startedAt,
      finishedAt: new Date().toISOString(),
      error: err instanceof Error
        ? err.message
        : String(err)
    })
  }
}

async function clearGlobalQueueBacklog (req: express.Request, res: express.Response) {
  const currentStatus = await getStoredGlobalQueueCleanupStatus()
  if (currentStatus.state === 'running') {
    return res.json(currentStatus)
  }

  const startedAt = new Date().toISOString()
  const runningStatus: GlobalQueueCleanupStatus = {
    state: 'running',
    startedAt
  }

  await Redis.Instance.setGlobalQueueCleanupStatus(runningStatus)

  void startGlobalQueueCleanupInBackground()

  return res.json(runningStatus)
}

async function getClearGlobalQueueBacklogStatus (_req: express.Request, res: express.Response) {
  return res.json(await getStoredGlobalQueueCleanupStatus())
}

async function getStoredGlobalQueueCleanupStatus (): Promise<GlobalQueueCleanupStatus> {
  const status = await Redis.Instance.getGlobalQueueCleanupStatus() as GlobalQueueCleanupStatus | null

  return status || { state: 'idle' }
}

async function startGlobalQueueCleanupInBackground () {
  try {
    const result = await clearAllQueuesWaitingAndDelayedBacklog()
    const currentStatus = await getStoredGlobalQueueCleanupStatus()

    await Redis.Instance.setGlobalQueueCleanupStatus({
      state: 'completed',
      startedAt: currentStatus.startedAt,
      finishedAt: new Date().toISOString(),
      result
    })
  } catch (err) {
    logger.error('[SYSTEM_RESETTER] Global queue cleanup failed.', { err })

    const currentStatus = await getStoredGlobalQueueCleanupStatus()
    await Redis.Instance.setGlobalQueueCleanupStatus({
      state: 'failed',
      startedAt: currentStatus.startedAt,
      finishedAt: new Date().toISOString(),
      error: err instanceof Error
        ? err.message
        : String(err)
    })
  }
}

async function runVideoSystemReset (
  _jobType: string | undefined,
  pausedQueues: SystemResetPausedQueue[]
): Promise<SystemVideoResetResult> {
  const ids = await VideoModel.listLocalIds()
  const existingVideoIds = new Set(ids)
  const existingVideoUUIDs = new Set<string>()
  const existingVideoImportIds = new Set<number>()
  const processedJobKeys = new Set<string>()
  const jobIndex = await buildVideoRepairJobIndex()

  const result: SystemVideoResetResult = {
    videosChecked: 0,
    videosUpdated: 0,
    videosDeleted: 0,
    jobsRemoved: 0,
    jobsRemoveFailed: 0,
    countersReset: 0,
    queuesPaused: pausedQueues.filter(q => !q.wasPaused).length,
    resetHoldEnabled: true,
    queueJobsDrained: 0,
    queueJobsCleaned: 0,
    orphanDbRecordsDeleted: 0,
    localFilesDeleted: 0
  }

  const queueCleanup = await clearVideoRepairQueuesForSystemReset()
  result.queueJobsDrained += queueCleanup.drained
  result.queueJobsCleaned += queueCleanup.cleaned

  for (const id of ids) {
    const video = await VideoModel.loadWithFiles(id)
    if (!video) continue

    existingVideoUUIDs.add(video.uuid)
    if (video.isLive) continue

    const info = await VideoJobInfoModel.load(video.id)
    const videoImport = await VideoImportModel.unscoped().findOne({ where: { videoId: video.id } })
    if (videoImport?.id) existingVideoImportIds.add(videoImport.id)
    const jobRefs = getVideoRepairJobRefs(jobIndex, video.uuid, video.id, videoImport?.id)
    const hasStaleCounters = !!info && (info.pendingMove > 0 || info.pendingTranscode > 0 || info.pendingTranscription > 0)

    result.videosChecked++

    const mediaIntegrity = await getVideoMediaIntegrity(video)
    const shouldDeleteVideo = shouldDeleteVideoDuringSystemReset({ video, videoImport, mediaIntegrity })

    const cleanupResult = await removeVideoRepairJobs(jobRefs)
    for (const ref of jobRefs) processedJobKeys.add(getVideoRepairJobKey(ref))
    result.jobsRemoved += cleanupResult.removed
    result.jobsRemoveFailed += cleanupResult.failed

    const resetCounters = await resetVideoJobInfoCounters(video.uuid, info)
    result.countersReset += resetCounters

    if (shouldDeleteVideo) {
      await markVideoImportAsFailedIfNeeded(videoImport, shouldDeleteVideo.reason)
      await Redis.Instance.setVideoDeletionFlag(video.uuid)
      try {
        await video.destroy()
      } catch (err) {
        await Redis.Instance.clearVideoDeletionFlag(video.uuid)
        throw err
      }

      logger.warn(
        '[SYSTEM_RESETTER] Deleted corrupted/incomplete video %s: %s',
        video.uuid,
        shouldDeleteVideo.reason,
        {
          missingRequiredFiles: mediaIntegrity.missingRequiredFiles,
          failedJobRemovals: cleanupResult.failed
        }
      )

      result.videosDeleted++
      result.videosUpdated++
      existingVideoIds.delete(video.id)
      existingVideoUUIDs.delete(video.uuid)
      continue
    }

    const metadataRepairs = await repairMissingMediaMetadataIfNeeded(mediaIntegrity)
    const importRepaired = await repairImportStateIfNeeded(video, videoImport)
    const transcodingRepaired = await repairTranscodingStateIfNeeded(video)
    const moveRepaired = await repairObjectStorageMoveStateIfNeeded(video)

    if (
      hasStaleCounters ||
      jobRefs.length !== 0 ||
      cleanupResult.removed !== 0 ||
      cleanupResult.failed !== 0 ||
      metadataRepairs !== 0 ||
      importRepaired ||
      transcodingRepaired ||
      moveRepaired
    ) {
      logger.info('[SYSTEM_RESETTER] Reset stale jobs/counters for video %s.', video.uuid, {
        removedJobs: cleanupResult.removed,
        failedJobRemovals: cleanupResult.failed,
        resetCounters,
        metadataRepairs,
        importRepaired,
        transcodingRepaired,
        moveRepaired
      })
      result.videosUpdated++
    }
  }

  const orphanCleanupResult = await removeOrphanedVideoRepairJobs({
    jobIndex,
    existingVideoIds,
    existingVideoUUIDs,
    existingVideoImportIds,
    processedJobKeys
  })

  result.jobsRemoved += orphanCleanupResult.removed
  result.jobsRemoveFailed += orphanCleanupResult.failed

  const dbCleanupResult = await cleanupOrphanSystemResetDbRecords()
  result.orphanDbRecordsDeleted += dbCleanupResult.deleted

  const localFilesCleanup = await cleanupOrphanLocalVideoFiles(ids)
  result.localFilesDeleted += localFilesCleanup.deleted

  logger.info('[SYSTEM_RESETTER] Video system reset completed.', result)

  return result
}

async function pauseVideoRepairQueuesForSystemReset (): Promise<SystemResetPausedQueue[]> {
  const queues = JobQueue.Instance.getQueues()
  const pausedQueues: SystemResetPausedQueue[] = []

  for (const jobType of VIDEO_REPAIR_JOB_TYPES) {
    const queue = queues[jobType]
    if (!queue) continue

    try {
      const wasPaused = await queue.isPaused()
      pausedQueues.push({ jobType, wasPaused })

      if (!wasPaused) {
        await queue.pause()
      }
    } catch (err) {
      logger.warn('[SYSTEM_RESETTER] Cannot pause %s queue before reset.', jobType, { err })
    }
  }

  await JobQueue.Instance.pause({ doNotWaitActive: true, jobTypes: VIDEO_REPAIR_JOB_TYPES })

  logger.info('[SYSTEM_RESETTER] Paused %d video pipeline queue(s) before reset.', pausedQueues.filter(q => !q.wasPaused).length)

  return pausedQueues
}


async function clearVideoRepairQueuesForSystemReset (): Promise<VideoQueueCleanupResult> {
  const queues = JobQueue.Instance.getQueues()
  let drained = 0
  let cleaned = 0

  for (const jobType of VIDEO_REPAIR_JOB_TYPES) {
    const queue = queues[jobType]
    if (!queue) continue

    try {
      const countsBefore = await queue.getJobCounts('waiting', 'wait', 'delayed', 'prioritized', 'waiting-children')
      const drainCandidates = Object.values(countsBefore).reduce((acc, value) => acc + (value || 0), 0)
      await queue.drain(true)
      drained += drainCandidates
    } catch (err) {
      logger.warn('[SYSTEM_RESETTER] Cannot drain %s queue before cleanup.', jobType, { err })
    }

    for (const state of VIDEO_QUEUE_CLEAN_STATES) {
      try {
        const removedIds = await queue.clean(0, VIDEO_QUEUE_CLEAN_LIMIT, state)
        cleaned += removedIds.length
      } catch (err) {
        logger.warn('[SYSTEM_RESETTER] Cannot clean %s jobs in state %s during system reset.', jobType, state, { err })
      }
    }
  }

  logger.info('[SYSTEM_RESETTER] Cleared queued video pipeline jobs before database/storage scrub.', { drained, cleaned })

  return { drained, cleaned }
}

async function clearAllQueuesWaitingAndDelayedBacklog (): Promise<GlobalQueueCleanupResult> {
  const queues = JobQueue.Instance.getQueues()
  const queueNames = Object.keys(queues)

  await JobQueue.Instance.pause({ doNotWaitActive: true, jobTypes: queueNames })

  let queuesPaused = 0
  let queueJobsDrained = 0
  let queueJobsCleaned = 0

  for (const jobType of queueNames) {
    const queue = queues[jobType]
    if (!queue) continue

    queuesPaused++

    try {
      const countsBefore = await queue.getJobCounts('waiting', 'wait', 'delayed', 'prioritized', 'waiting-children')
      const drainCandidates = Object.values(countsBefore).reduce((acc, value) => acc + (value || 0), 0)

      await queue.drain(true)
      queueJobsDrained += drainCandidates
    } catch (err) {
      logger.warn('[GLOBAL_QUEUE_SCRUB] Cannot drain %s queue.', jobType, { err })
    }

    for (const state of [ 'delayed', 'waiting', 'wait', 'prioritized' ] as const) {
      try {
        const removedIds = await queue.clean(0, VIDEO_QUEUE_CLEAN_LIMIT, state)
        queueJobsCleaned += removedIds.length
      } catch (err) {
        logger.warn('[GLOBAL_QUEUE_SCRUB] Cannot clean %s jobs in state %s.', jobType, state, { err })
      }
    }
  }

  logger.info('[GLOBAL_QUEUE_SCRUB] Cleared global BullMQ waiting/delayed backlog.', {
    queuesPaused,
    queueJobsDrained,
    queueJobsCleaned
  })

  return {
    queuesPaused,
    queueJobsDrained,
    queueJobsCleaned
  }
}

async function cleanupOrphanSystemResetDbRecords (): Promise<SystemResetDbCleanupResult> {
  const [ runnerJobsCancelled ] = await RunnerJobModel.update(
    {
      state: RunnerJobState.CANCELLED,
      processingJobToken: null,
      progress: null,
      runnerId: null,
      finishedAt: new Date(),
      error: 'Cleared by system resetter.'
    },
    {
      where: {
        type: VIDEO_PIPELINE_RUNNER_JOB_TYPES,
        state: {
          [Op.in]: [
            RunnerJobState.PENDING,
            RunnerJobState.PROCESSING,
            RunnerJobState.WAITING_FOR_PARENT_JOB,
            RunnerJobState.COMPLETING
          ]
        }
      }
    }
  )

  const [ orphanVideoImportsDeleted ] = await sequelizeTypescript.query(
    'DELETE FROM "videoImport" ' +
    'WHERE ("videoId" IS NULL ' +
    'OR NOT EXISTS (SELECT 1 FROM "video" WHERE "video"."id" = "videoImport"."videoId")) ' +
    `AND "state" IN (${ORPHAN_INCOMPLETE_VIDEO_IMPORT_STATES.join(', ')})`,
    { raw: true }
  )
  const [ orphanVideoJobInfoDeleted ] = await sequelizeTypescript.query(
    'DELETE FROM "videoJobInfo" WHERE NOT EXISTS (SELECT 1 FROM "video" WHERE "video"."id" = "videoJobInfo"."videoId") ' +
    'OR (COALESCE("pendingMove", 0) = 0 AND COALESCE("pendingTranscode", 0) = 0 AND COALESCE("pendingTranscription", 0) = 0)',
    { raw: true }
  )

  const deleted =
    runnerJobsCancelled +
    extractDeletedRowCount(orphanVideoImportsDeleted) +
    extractDeletedRowCount(orphanVideoJobInfoDeleted)

  logger.info('[SYSTEM_RESETTER] Deleted %d orphan/stale DB pipeline record(s).', deleted)

  return { deleted }
}

async function cleanupOrphanLocalVideoFiles (localVideoIds: number[]): Promise<SystemResetLocalFilesResult> {
  const referenced = await buildReferencedLocalFiles(localVideoIds)
  let deleted = 0

  for (const directory of LOCAL_VIDEO_STORAGE_DIRECTORIES) {
    deleted += await cleanupUnreferencedFilesInDirectory(directory, referenced)
  }

  deleted += await cleanupUnreferencedHLSDirectories(referenced)

  logger.info('[SYSTEM_RESETTER] Deleted %d orphan local video pipeline file(s).', deleted)

  return { deleted }
}

async function buildReferencedLocalFiles (localVideoIds: number[]): Promise<ReferencedLocalFiles> {
  const paths = new Set<string>()
  const hlsDirectories = new Set<string>()

  for (const id of localVideoIds) {
    const video = await VideoModel.loadWithFiles(id)
    if (!video || video.isLive) continue

    for (const file of video.VideoFiles || []) {
      if (file.filename) {
        paths.add(normalizeSystemResetPath(VideoPathManager.Instance.getFSVideoFileOutputPath(video, file)))
      }

      if (file.torrentFilename) {
        paths.add(normalizeSystemResetPath(getFSTorrentFilePath(file)))
      }
    }

    for (const playlist of video.VideoStreamingPlaylists || []) {
      const playlistDirectory = normalizeSystemResetPath(VideoPathManager.Instance.getFSHLSOutputPath(video))
      hlsDirectories.add(playlistDirectory)

      if (playlist.playlistFilename) {
        paths.add(normalizeSystemResetPath(VideoPathManager.Instance.getFSHLSOutputPath(video, playlist.playlistFilename)))
      }

      if (playlist.segmentsSha256Filename) {
        paths.add(normalizeSystemResetPath(VideoPathManager.Instance.getFSHLSOutputPath(video, playlist.segmentsSha256Filename)))
      }

      for (const file of playlist.VideoFiles || []) {
        if (file.filename) {
          paths.add(normalizeSystemResetPath(VideoPathManager.Instance.getFSHLSOutputPath(video, file.filename)))
          paths.add(normalizeSystemResetPath(
            VideoPathManager.Instance.getFSHLSOutputPath(video, getHLSResolutionPlaylistFilename(file.filename))
          ))
        }

        if (file.torrentFilename) {
          paths.add(normalizeSystemResetPath(getFSTorrentFilePath(file)))
        }
      }
    }

    const videoSources = await VideoSourceModel.listAll(video.id)
    for (const source of videoSources) {
      if (!source.keptOriginalFilename) continue
      paths.add(normalizeSystemResetPath(VideoPathManager.Instance.getFSOriginalVideoFilePath(source.keptOriginalFilename)))
    }

    const captions = await VideoCaptionModel.listVideoCaptions(video.id)
    for (const caption of captions) {
      if (caption.filename) {
        paths.add(normalizeSystemResetPath(caption.getFSFilePath()))
      }

      if (caption.m3u8Filename) {
        paths.add(normalizeSystemResetPath(VideoPathManager.Instance.getFSHLSOutputPath(video, caption.m3u8Filename)))
      }
    }

    for (const thumbnail of video.Thumbnails || []) {
      if (!thumbnail.isLocal() || thumbnail.cached) continue
      paths.add(normalizeSystemResetPath(thumbnail.getFSPath()))
    }

    const storyboard = await StoryboardModel.loadByVideo(video.id)
    if (storyboard?.isLocal()) {
      paths.add(normalizeSystemResetPath(storyboard.getFSPath()))
    }
  }

  return { paths, hlsDirectories }
}

async function cleanupUnreferencedFilesInDirectory (directory: string, referenced: ReferencedLocalFiles): Promise<number> {
  if (!await pathExists(directory)) return 0

  let deleted = 0

  for (const filePath of await listFilesRecursively(directory)) {
    const normalizedPath = normalizeSystemResetPath(filePath)

    if (referenced.paths.has(normalizedPath)) continue
    if (referenced.hlsDirectories.has(normalizedPath)) continue

    try {
      await remove(filePath)
      deleted++
    } catch (err) {
      logger.warn('[SYSTEM_RESETTER] Cannot remove orphan local file %s.', filePath, { err })
    }
  }

  return deleted
}

async function cleanupUnreferencedHLSDirectories (referenced: ReferencedLocalFiles): Promise<number> {
  let deleted = 0

  for (const directory of [ DIRECTORIES.HLS_STREAMING_PLAYLIST.PUBLIC, DIRECTORIES.HLS_STREAMING_PLAYLIST.PRIVATE ]) {
    if (!await pathExists(directory)) continue

    const entries = await readdir(directory, { withFileTypes: true })
    for (const entry of entries) {
      if (!entry.isDirectory()) continue

      const entryPath = normalizeSystemResetPath(joinSystemResetPath(directory, entry.name))
      if (referenced.hlsDirectories.has(entryPath)) continue

      try {
        await remove(entryPath)
        deleted++
      } catch (err) {
        logger.warn('[SYSTEM_RESETTER] Cannot remove orphan HLS directory %s.', entryPath, { err })
      }
    }
  }

  return deleted
}

async function listFilesRecursively (directory: string): Promise<string[]> {
  const result: string[] = []
  const entries = await readdir(directory, { withFileTypes: true })

  for (const entry of entries) {
    const entryPath = joinSystemResetPath(directory, entry.name)

    if (entry.isDirectory()) {
      result.push(...await listFilesRecursively(entryPath))
      continue
    }

    if (entry.isFile()) result.push(entryPath)
  }

  return result
}

function normalizeSystemResetPath (path: string) {
  return path.replace(/\\/g, '/').toLowerCase()
}

function extractDeletedRowCount (value: unknown) {
  if (typeof value === 'number') return value
  if (typeof value === 'bigint') return Number(value)
  if (value && typeof value === 'object') {
    const obj = value as Record<string, unknown>
    for (const key of [ 'rowCount', 'affectedRows', 'count' ]) {
      const current = obj[key]
      if (typeof current === 'number') return current
      if (typeof current === 'bigint') return Number(current)
    }
  }

  return 0
}

function joinSystemResetPath (...parts: string[]) {
  return parts.join('/').replace(/\\/g, '/')
}

async function buildVideoRepairJobIndex (): Promise<VideoRepairJobIndex> {
  const index: VideoRepairJobIndex = {
    byVideoUUID: new Map(),
    byVideoId: new Map(),
    byVideoImportId: new Map()
  }
  const queues = JobQueue.Instance.getQueues()

  for (const jobType of VIDEO_REPAIR_JOB_TYPES) {
    const queue = queues[jobType]
    if (!queue) continue

    for (const state of VIDEO_REPAIR_JOB_STATES) {
      let jobs: BullJob[]

      try {
        jobs = await queue.getJobs([ state ], 0, 10000, true)
      } catch (err) {
        logger.warn('[SYSTEM_RESETTER] Cannot list %s jobs in state %s.', jobType, state, { err })
        continue
      }

      for (const job of jobs) {
        const ref = { job, state, jobType }
        const data = job.data as {
          videoUUID?: unknown
          videoId?: unknown
          videoImportId?: unknown
        }

        const videoUUID = typeof data?.videoUUID === 'string'
          ? data.videoUUID
          : undefined
        const videoId = toPositiveInteger(data?.videoId)
        const videoImportId = toPositiveInteger(data?.videoImportId)

        if (videoUUID) pushMapValue(index.byVideoUUID, videoUUID, ref)
        if (videoId) pushMapValue(index.byVideoId, videoId, ref)
        if (videoImportId) pushMapValue(index.byVideoImportId, videoImportId, ref)
      }
    }
  }

  return index
}

function getVideoRepairJobRefs (
  index: VideoRepairJobIndex,
  videoUUID: string,
  videoId: number,
  videoImportId?: number
) {
  const refs = [
    ...(index.byVideoUUID.get(videoUUID) || []),
    ...(index.byVideoId.get(videoId) || []),
    ...(videoImportId ? index.byVideoImportId.get(videoImportId) || [] : [])
  ]

  return dedupeVideoRepairJobRefs(refs)
}

async function getVideoMediaIntegrity (video: MVideoWithAllFiles): Promise<VideoMediaIntegrity> {
  const missingRequiredFiles: string[] = []
  const missingRepairableFiles: string[] = []
  const missingOriginalSourceIds: number[] = []
  const missingShaPlaylistIds: number[] = []
  const webFiles = video.VideoFiles || []
  const playlists = video.VideoStreamingPlaylists || []
  const hlsFiles = playlists.flatMap(playlist => playlist.VideoFiles || [])
  const sources = await VideoSourceModel.listAll(video.id)
  const sourceFiles = sources.filter(source => !!source.keptOriginalFilename)

  let hasFileSystemMediaRecords = false
  let hasObjectStorageMediaRecords = false

  for (const file of webFiles) {
    if (file.storage === FileStorage.FILE_SYSTEM) {
      hasFileSystemMediaRecords = true
      await addMissingFileIfNeeded({
        missingRequiredFiles,
        label: `web video file ${file.id}`,
        path: VideoPathManager.Instance.getFSVideoFileOutputPath(video, file)
      })
    } else if (file.storage === FileStorage.OBJECT_STORAGE) {
      hasObjectStorageMediaRecords = true
    }
  }

  for (const playlist of playlists) {
    if (playlist.storage === FileStorage.FILE_SYSTEM) {
      hasFileSystemMediaRecords = true

      await addMissingFileIfNeeded({
        missingRequiredFiles,
        label: `HLS master playlist ${playlist.id}`,
        path: VideoPathManager.Instance.getFSHLSOutputPath(video, playlist.playlistFilename)
      })

      if (playlist.segmentsSha256Filename) {
        await addMissingFileIfNeeded({
          missingRequiredFiles: missingRepairableFiles,
          label: `HLS SHA playlist ${playlist.id}`,
          path: VideoPathManager.Instance.getFSHLSOutputPath(video, playlist.segmentsSha256Filename),
          onMissing: () => missingShaPlaylistIds.push(playlist.id)
        })
      }
    } else if (playlist.storage === FileStorage.OBJECT_STORAGE) {
      hasObjectStorageMediaRecords = true
    }

    for (const file of playlist.VideoFiles || []) {
      if (file.storage === FileStorage.FILE_SYSTEM) {
        hasFileSystemMediaRecords = true

        await addMissingFileIfNeeded({
          missingRequiredFiles,
          label: `HLS media file ${file.id}`,
          path: VideoPathManager.Instance.getFSHLSOutputPath(video, file.filename)
        })
        await addMissingFileIfNeeded({
          missingRequiredFiles,
          label: `HLS resolution playlist ${file.id}`,
          path: VideoPathManager.Instance.getFSHLSOutputPath(video, getHLSResolutionPlaylistFilename(file.filename))
        })
      } else if (file.storage === FileStorage.OBJECT_STORAGE) {
        hasObjectStorageMediaRecords = true
      }
    }
  }

  for (const source of sourceFiles) {
    if (source.storage === FileStorage.FILE_SYSTEM) {
      hasFileSystemMediaRecords = true

      await addMissingFileIfNeeded({
        missingRequiredFiles: missingRepairableFiles,
        label: `original video source ${source.id}`,
        path: VideoPathManager.Instance.getFSOriginalVideoFilePath(source.keptOriginalFilename),
        onMissing: () => missingOriginalSourceIds.push(source.id)
      })
    } else if (source.storage === FileStorage.OBJECT_STORAGE) {
      hasObjectStorageMediaRecords = true
    }
  }

  return {
    hasMediaRecords: webFiles.length !== 0 || hlsFiles.length !== 0 || sourceFiles.length !== 0,
    hasPlayableMediaRecords: webFiles.length !== 0 || hlsFiles.length !== 0,
    hasFileSystemMediaRecords,
    hasObjectStorageMediaRecords,
    missingRequiredFiles,
    missingRepairableFiles,
    missingOriginalSourceIds,
    missingShaPlaylistIds
  }
}

function shouldDeleteVideoDuringSystemReset (options: {
  video: MVideoWithAllFiles
  videoImport: VideoImportModel
  mediaIntegrity: VideoMediaIntegrity
}) {
  const { video, videoImport, mediaIntegrity } = options

  if (mediaIntegrity.missingRequiredFiles.length !== 0) {
    return {
      reason: `missing required local media file(s): ${mediaIntegrity.missingRequiredFiles.slice(0, 5).join(', ')}`
    }
  }

  if (!mediaIntegrity.hasPlayableMediaRecords && !canRetryVideoImport(video, videoImport)) {
    return { reason: 'video has no playable media records and cannot be retried as an import' }
  }

  if (!mediaIntegrity.hasMediaRecords && (FAILED_VIDEO_STATES.has(video.state) || INCOMPLETE_VIDEO_STATES.has(video.state))) {
    return { reason: `video is in state ${video.state} without media records` }
  }

  return undefined
}

async function removeVideoRepairJobs (jobRefs: VideoRepairJobRef[]) {
  let removed = 0
  let failed = 0

  for (const ref of dedupeVideoRepairJobRefs(jobRefs)) {
    try {
      await ref.job.remove()
      removed++
    } catch (err) {
      failed++
      logger.warn('[SYSTEM_RESETTER] Cannot remove %s job %s in state %s.', ref.jobType, ref.job.id, ref.state, { err })
    }
  }

  return { removed, failed }
}

async function resetVideoJobInfoCounters (videoUUID: string, info: VideoJobInfoModel) {
  if (!info) return 0

  let reset = 0

  if (info.pendingMove > 0) {
    await VideoJobInfoModel.decrease(videoUUID, 'pendingMove', info.pendingMove)
    reset += info.pendingMove
  }

  if (info.pendingTranscode > 0) {
    await VideoJobInfoModel.decrease(videoUUID, 'pendingTranscode', info.pendingTranscode)
    reset += info.pendingTranscode
  }

  if (info.pendingTranscription > 0) {
    await VideoJobInfoModel.decrease(videoUUID, 'pendingTranscription', info.pendingTranscription)
    reset += info.pendingTranscription
  }

  return reset
}

async function markVideoImportAsFailedIfNeeded (videoImport: VideoImportModel, reason: string) {
  if (!videoImport) return
  if (videoImport.state === VideoImportState.FAILED && videoImport.error === reason) return

  videoImport.state = VideoImportState.FAILED
  videoImport.error = reason
  videoImport.progress = null
  await videoImport.save()
}

async function repairMissingMediaMetadataIfNeeded (mediaIntegrity: VideoMediaIntegrity) {
  let repaired = 0

  for (const sourceId of mediaIntegrity.missingOriginalSourceIds) {
    const source = await VideoSourceModel.findByPk(sourceId)
    if (!source?.keptOriginalFilename) continue

    source.keptOriginalFilename = null
    source.storage = null
    await source.save()
    repaired++
  }

  for (const playlistId of mediaIntegrity.missingShaPlaylistIds) {
    const playlist = await VideoStreamingPlaylistModel.findByPk(playlistId)
    if (!playlist?.segmentsSha256Filename) continue

    playlist.segmentsSha256Filename = null
    playlist.segmentsSha256Url = null
    await playlist.save()
    repaired++
  }

  if (repaired !== 0) {
    logger.info('[SYSTEM_RESETTER] Repaired %d missing optional media metadata record(s).', repaired, {
      missingRepairableFiles: mediaIntegrity.missingRepairableFiles
    })
  }

  return repaired
}

async function repairImportStateIfNeeded (
  video: MVideoWithAllFiles,
  videoImport: VideoImportModel
) {
  if (!videoImport) return false
  if (!isImportRepairCandidate(video)) return false

  const reason = !canRetryVideoImport(video, videoImport)
    ? (videoImport.attempts >= CONFIG.IMPORT.VIDEOS.MAX_ATTEMPTS
        ? 'video import reached the maximum number of attempts'
        : 'video import is not retryable')
    : 'Cleared by system resetter. Import will not resume automatically.'

  await markVideoImportAsFailedIfNeeded(videoImport, reason)

  if (video.state !== VideoState.TO_IMPORT_FAILED || video.waitTranscoding !== false) {
    video.state = VideoState.TO_IMPORT_FAILED
    video.waitTranscoding = false
    await video.save()
  }

  videoImport.progress = null
  videoImport.error = reason
  await videoImport.save()

  logger.info('[SYSTEM_RESETTER] Marked import as failed for video %s/import %d without requeueing.', video.uuid, videoImport.id, {
    reason
  })

  return true
}

async function repairTranscodingStateIfNeeded (video: MVideoWithAllFiles) {
  if (video.state !== VideoState.TO_TRANSCODE && video.state !== VideoState.TRANSCODING_FAILED) return false

  const file = video.getMaxQualityFile(VideoFileStream.VIDEO) || video.getMaxQualityFile(VideoFileStream.AUDIO)
  if (!file) return false

  return publishVideoAfterStorageReset(video, 'system reset cleared stuck transcoding state')
}

async function repairObjectStorageMoveStateIfNeeded (video: MVideoWithAllFiles) {
  if (video.state === VideoState.TO_MOVE_TO_EXTERNAL_STORAGE || video.state === VideoState.TO_MOVE_TO_EXTERNAL_STORAGE_FAILED) {
    return repairMoveToObjectStorageStateIfNeeded(video)
  }

  if (video.state === VideoState.TO_MOVE_TO_FILE_SYSTEM || video.state === VideoState.TO_MOVE_TO_FILE_SYSTEM_FAILED) {
    return repairMoveToFileSystemStateIfNeeded(video)
  }

  return false
}

async function repairMoveToObjectStorageStateIfNeeded (video: MVideoWithAllFiles) {
  if (CONFIG.OBJECT_STORAGE.ENABLED !== true) {
    return publishVideoAfterStorageReset(video, 'object storage is disabled')
  }

  const hasResourcesToMove = await hasVideoResourcesToBeMoved(video, FileStorage.OBJECT_STORAGE)

  if (!hasResourcesToMove) {
    return publishVideoAfterStorageReset(video, 'all resources are already out of local file storage')
  }

  return publishVideoAfterStorageReset(video, 'system reset cleared pending object-storage move state')
}

async function repairMoveToFileSystemStateIfNeeded (video: MVideoWithAllFiles) {
  const hasResourcesToMove = await hasVideoResourcesToBeMoved(video, FileStorage.FILE_SYSTEM)

  if (!hasResourcesToMove) {
    return publishVideoAfterStorageReset(video, 'all resources are already on the file system')
  }

  return publishVideoAfterStorageReset(video, 'system reset cleared pending file-system move state')
}

async function publishVideoAfterStorageReset (video: MVideoWithAllFiles, reason: string) {
  if (video.state === VideoState.PUBLISHED && video.waitTranscoding === false) return false

  video.state = VideoState.PUBLISHED
  video.waitTranscoding = false
  await video.save()

  logger.info('[SYSTEM_RESETTER] Published video %s after storage reset: %s.', video.uuid, reason)

  return true
}

async function removeOrphanedVideoRepairJobs (options: {
  jobIndex: VideoRepairJobIndex
  existingVideoIds: Set<number>
  existingVideoUUIDs: Set<string>
  existingVideoImportIds: Set<number>
  processedJobKeys: Set<string>
}) {
  const { jobIndex, existingVideoIds, existingVideoUUIDs, existingVideoImportIds, processedJobKeys } = options
  const unprocessedOrphans = getUnprocessedOrphanedVideoRepairJobRefs({
    index: jobIndex,
    existingVideoIds,
    existingVideoUUIDs,
    existingVideoImportIds,
    processedJobKeys,
    dedupeRefs: dedupeVideoRepairJobRefs,
    getRefKey: getVideoRepairJobKey
  })

  if (unprocessedOrphans.length === 0) return { removed: 0, failed: 0 }

  const result = await removeVideoRepairJobs(unprocessedOrphans)

  logger.warn('[SYSTEM_RESETTER] Removed %d orphaned video job(s), failed to remove %d.', result.removed, result.failed)

  return result
}

async function addMissingFileIfNeeded (options: {
  missingRequiredFiles: string[]
  label: string
  path: string
  onMissing?: () => void
}) {
  if (await pathExists(options.path)) return

  options.missingRequiredFiles.push(`${options.label} (${options.path})`)
  options.onMissing?.()
}

function canRetryVideoImport (video: MVideoWithAllFiles, videoImport: VideoImportModel) {
  if (!videoImport) return false
  if (video.state !== VideoState.TO_IMPORT && video.state !== VideoState.TO_IMPORT_FAILED) return false
  if (videoImport.state === VideoImportState.CANCELLED || videoImport.state === VideoImportState.REJECTED) return false
  if (videoImport.attempts >= CONFIG.IMPORT.VIDEOS.MAX_ATTEMPTS) return false

  return !!videoImport.payload || !!videoImport.targetUrl || !!videoImport.magnetUri || !!videoImport.torrentName
}

function isImportRepairCandidate (video: MVideoWithAllFiles) {
  return video.state === VideoState.TO_IMPORT || video.state === VideoState.TO_IMPORT_FAILED
}

function pushMapValue<K, V> (map: Map<K, V[]>, key: K, value: V) {
  const values = map.get(key)
  if (values) values.push(value)
  else map.set(key, [ value ])
}

function toPositiveInteger (value: unknown) {
  if (typeof value === 'number' && Number.isInteger(value) && value > 0) return value
  if (typeof value !== 'string' || value.length === 0) return undefined

  const parsed = Number(value)
  return Number.isInteger(parsed) && parsed > 0
    ? parsed
    : undefined
}

function dedupeVideoRepairJobRefs (refs: VideoRepairJobRef[]) {
  const seen = new Set<string>()
  const result: VideoRepairJobRef[] = []

  for (const ref of refs) {
    const key = getVideoRepairJobKey(ref)
    if (seen.has(key)) continue

    seen.add(key)
    result.push(ref)
  }

  return result
}

function getVideoRepairJobKey (ref: VideoRepairJobRef) {
  return `${ref.jobType}:${ref.job.id}`
}

async function retryJob (req: express.Request, res: express.Response) {
  const jobType = req.body.jobType as string
  const jobId = req.body.jobId as string

  if (!jobType || !jobId) {
    return res.fail({
      status: HttpStatusCode.BAD_REQUEST_400,
      message: 'jobType and jobId are required'
    })
  }

  const result = await JobQueue.Instance.retryFailedJob({
    jobType: jobType as JobType,
    jobId
  })

  if (result.status === 'not_found') {
    return res.fail({
      status: HttpStatusCode.NOT_FOUND_404,
      message: 'Job was not found'
    })
  }

  if (result.status === 'not_failed') {
    return res.fail({
      status: HttpStatusCode.BAD_REQUEST_400,
      message: 'Only failed jobs can be retried'
    })
  }

  if (result.status === 'retried') {
    return res.json({ jobId: result.newJobId })
  }

  return res.fail({
    status: HttpStatusCode.INTERNAL_SERVER_ERROR_500,
    message: 'Unknown error occurred'
  })
}

async function removeJob (req: express.Request, res: express.Response) {
  const jobType = req.params.jobType
  const jobId = req.params.jobId

  if (!jobType || !jobId) {
    return res.fail({
      status: HttpStatusCode.BAD_REQUEST_400,
      message: 'jobType and jobId are required'
    })
  }

  const queues = JobQueue.Instance.getQueues()
  const queue = queues[jobType as JobType]

  if (!queue) {
    return res.fail({
      status: HttpStatusCode.NOT_FOUND_404,
      message: 'Queue not found'
    })
  }

  const job = await queue.getJob(jobId)
  if (!job) {
    return res.fail({
      status: HttpStatusCode.NOT_FOUND_404,
      message: 'Job not found'
    })
  }

  try {
    await job.remove()
    return res.sendStatus(HttpStatusCode.NO_CONTENT_204)
  } catch (err) {
    logger.error('Error removing job', { err, jobType, jobId })
    return res.fail({
      status: HttpStatusCode.INTERNAL_SERVER_ERROR_500,
      message: 'Error removing job'
    })
  }
}

async function getVideoMaintenanceCounts (req: express.Request, res: express.Response) {
  const result = await sequelizeTypescript.query(`
    SELECT
      COUNT(*) FILTER (
        WHERE EXISTS (SELECT 1 FROM "videoFile" vf WHERE vf."videoId" = v."id" AND vf."storage" = 0)
          OR EXISTS (
            SELECT 1 FROM "videoStreamingPlaylist" vsp
            JOIN "videoFile" vfhls ON vfhls."videoStreamingPlaylistId" = vsp."id"
            WHERE vsp."videoId" = v."id" AND vfhls."storage" = 0
          )
          OR EXISTS (SELECT 1 FROM "videoSource" vs WHERE vs."videoId" = v."id" AND vs."storage" = 0)
      )::int AS "localStorageVideos",
      COUNT(*) FILTER (
        WHERE EXISTS (SELECT 1 FROM "videoFile" vf WHERE vf."videoId" = v."id" AND vf."storage" = 1)
          OR EXISTS (
            SELECT 1 FROM "videoStreamingPlaylist" vsp
            JOIN "videoFile" vfhls ON vfhls."videoStreamingPlaylistId" = vsp."id"
            WHERE vsp."videoId" = v."id" AND vfhls."storage" = 1
          )
          OR EXISTS (SELECT 1 FROM "videoSource" vs WHERE vs."videoId" = v."id" AND vs."storage" = 1)
      )::int AS "objectStorageVideos",
      COUNT(*) FILTER (WHERE v."state" = ${VideoState.TRANSCODING_FAILED})::int AS "failedTranscodingVideos",
      COUNT(*) FILTER (WHERE v."state" = ${VideoState.TO_TRANSCODE})::int AS "notYetTranscodedVideos"
    FROM "video" v
    WHERE v."remote" IS FALSE
  `, { plain: true })

  return res.json(result)
}

async function pauseJobQueue (req: express.Request, res: express.Response) {
  await JobQueue.Instance.pause()

  return res.sendStatus(HttpStatusCode.NO_CONTENT_204)
}

async function resumeJobQueue (req: express.Request, res: express.Response) {
  await JobQueue.Instance.resume()

  return res.sendStatus(HttpStatusCode.NO_CONTENT_204)
}

async function listJobs (req: express.Request, res: express.Response) {
  const state = req.params.state as JobState
  const asc = req.query.sort === 'createdAt'
  const jobType = req.query.jobType
  const search = req.query.search as string

  const jobs = await JobQueue.Instance.listForApi({
    state,
    start: req.query.start,
    count: req.query.count,
    asc,
    jobType,
    search
  })
  const total = await JobQueue.Instance.count(state, jobType, search)

  const result: ResultList<Job> = {
    total,
    data: await Promise.all(jobs.map(j => formatJob(j, state)))
  }

  return res.json(result)
}

const CANCELLED_REASON = 'Video was deleted - transcoding job cancelled'

async function formatJob (job: BullJob, state?: JobState): Promise<Job> {
  let displayState = state || await job.getState()
  if (displayState === 'failed' && typeof job.failedReason === 'string' && job.failedReason.includes(CANCELLED_REASON)) {
    displayState = 'cancelled'
  }
  return {
    id: job.id,
    state: displayState,
    type: job.queueName as JobType,
    data: job.data,
    parent: job.parent
      ? { id: job.parent.id }
      : undefined,
    progress: job.progress as number,
    priority: job.opts.priority,
    error: getJobError(job),
    createdAt: new Date(job.timestamp),
    finishedOn: new Date(job.finishedOn),
    processedOn: new Date(job.processedOn)
  }
}

function getJobError (job: BullJob) {
  if (isArray(job.stacktrace) && job.stacktrace.length !== 0) return job.stacktrace[0]
  if (job.failedReason) return job.failedReason

  return null
}
