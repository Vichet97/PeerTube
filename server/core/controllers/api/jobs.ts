import { FileStorage, HttpStatusCode, Job, JobState, JobType, ResultList, UserRight, VideoFileStream, VideoState, VideoStateType } from '@peertube/peertube-models'
import { Job as BullJob } from 'bullmq'
import express from 'express'
import { isArray } from '../../helpers/custom-validators/misc.js'
import { logger } from '../../helpers/logger.js'
import { CONFIG } from '../../initializers/config.js'
import { sequelizeTypescript } from '../../initializers/database.js'
import { JobQueue } from '../../lib/job-queue/index.js'
import { hasVideoResourcesToBeMoved } from '../../lib/move-storage/shared/move-video.js'
import { buildMoveVideoJob, buildLocalStoryboardJobIfNeeded } from '../../lib/video-jobs.js'
import { moveToNextState } from '../../lib/video-state.js'
import { VideoCaptionModel } from '../../models/video/video-caption.js'
import { VideoJobInfoModel } from '../../models/video/video-job-info.js'
import { StoryboardModel } from '../../models/video/storyboard.js'
import { VideoModel } from '../../models/video/video.js'
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
import { createMoveStorageJobsValidator, createRetryTranscodingJobsValidator, listJobsValidator } from '../../middlewares/validators/jobs.js'

const jobsRouter = express.Router()

jobsRouter.use(apiRateLimiter)

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

jobsRouter.post('/recheck-videos-status',
  authenticate,
  ensureUserHasRight(UserRight.MANAGE_JOBS),
  asyncMiddleware(recheckVideosStatus)
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
  const targetStorage = storage === 'object-storage' ? FileStorage.OBJECT_STORAGE : FileStorage.FILE_SYSTEM

  if (storage === 'object-storage' && !CONFIG.OBJECT_STORAGE.ENABLED) {
    return res.fail({
      status: HttpStatusCode.BAD_REQUEST_400,
      message: 'Object storage is not enabled on this instance'
    })
  }

  const ids = await VideoModel.listLocalIds()
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
          await JobQueue.Instance.createJob(job)
          jobsCreated++
        }
      } catch (err) {
        // Continue with other videos if one fails
      }
    }
  }

  return res.json({ jobsCreated })
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
            try {
              await job.remove()
              cancelledCount++
            } catch {
              // Job might have been processed already, continue
            }
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

async function recheckVideosStatus (req: express.Request, res: express.Response) {
  const jobType = req.body.jobType as string | undefined

  const ids = await VideoModel.listLocalIds()

  const videoUUIDsWithPendingJobs = await JobQueue.Instance.listVideoUUIDsWithPendingTranscodingJobs()

  let videosChecked = 0
  let videosUpdated = 0

  for (const id of ids) {
    const video = await VideoModel.loadFull(id)
    if (!video || video.isLive) continue

    // If jobType filter is specified, only process videos with that job type
    if (jobType && jobType !== 'all') {
      if (jobType === 'transcoding' && video.state !== VideoState.TO_TRANSCODE && video.state !== VideoState.TRANSCODING_FAILED) {
        continue
      }
      if (jobType === 'move-to-object-storage' && video.state !== VideoState.TO_MOVE_TO_EXTERNAL_STORAGE &&
          video.state !== VideoState.TO_MOVE_TO_EXTERNAL_STORAGE_FAILED && video.state !== VideoState.TO_MOVE_TO_FILE_SYSTEM &&
          video.state !== VideoState.TO_MOVE_TO_FILE_SYSTEM_FAILED) {
        continue
      }
    }

    videosChecked++

    const info = await VideoJobInfoModel.load(video.id)

    // === TRANSCODING SYNC ===
    // If pendingTranscode > 0 but no jobs in queue, decrement counter
    if (info?.pendingTranscode > 0) {
      const hasActiveTranscodingJob = videoUUIDsWithPendingJobs.has(video.uuid)
      if (!hasActiveTranscodingJob) {
        // No active job, but counter is positive - decrement the counter
        await VideoJobInfoModel.decrease(video.uuid, 'pendingTranscode')
        logger.info(`Fixed pendingTranscode counter for video ${video.uuid}, was ${info.pendingTranscode}, now ${info.pendingTranscode - 1}`)
        videosUpdated++
      }
    }

    // If video state is TRANSCODING_FAILED but has no pending jobs, reset to TO_TRANSCODE
    if (video.state === VideoState.TRANSCODING_FAILED) {
      const hasActiveTranscodingJob = videoUUIDsWithPendingJobs.has(video.uuid)
      const infoAfterDecrease = await VideoJobInfoModel.load(video.id)
      if (!hasActiveTranscodingJob && (!infoAfterDecrease || infoAfterDecrease.pendingTranscode === 0)) {
        video.state = VideoState.TO_TRANSCODE
        await video.save()
        logger.info(`Reset video ${video.uuid} from TRANSCODING_FAILED to TO_TRANSCODE`)
        videosUpdated++
      }
    }

    // === MOVE TO STORAGE SYNC ===
    // Check if all files are on object storage (load video with files)
    const videoWithFiles = await VideoModel.loadWithFiles(video.id)
    const videoFiles = videoWithFiles?.VideoFiles || []
    const hasLocalFiles = videoFiles.some(f => f.storage === 0) // 0 = local
    const hasObjectStorageFiles = videoFiles.some(f => f.storage === 1) // 1 = object storage

    if (info?.pendingMove > 0 && !hasLocalFiles) {
      // All files moved to object storage, but counter still positive - decrement
      await VideoJobInfoModel.decrease(video.uuid, 'pendingMove')
      logger.info(`Fixed pendingMove counter for video ${video.uuid}, files now on object storage`)
      videosUpdated++
    }

    if (hasObjectStorageFiles && !hasLocalFiles) {
      // Fully moved to object storage - update state if still in pending state
      if (video.state === VideoState.TO_MOVE_TO_EXTERNAL_STORAGE) {
        // Move to next state (should be PUBLISHED)
        await moveToNextState({ video, previousVideoState: video.state })
        logger.info(`Video ${video.uuid} moved to next state after all files on object storage`)
        videosUpdated++
      }
    } else if (hasLocalFiles && !hasObjectStorageFiles) {
      // Fully on local storage - if in failed state, reset
      if (video.state === VideoState.TO_MOVE_TO_EXTERNAL_STORAGE_FAILED) {
        video.state = VideoState.TO_MOVE_TO_EXTERNAL_STORAGE
        await video.save()
        logger.info(`Reset video ${video.uuid} from TO_MOVE_TO_EXTERNAL_STORAGE_FAILED to TO_MOVE_TO_EXTERNAL_STORAGE`)
        videosUpdated++
      }
    }

    // === STORYBOARD SYNC ===
    // Note: Storyboard doesn't have a counter in VideoJobInfoModel
    // Storyboard status is tracked by whether a StoryboardModel record exists
    // and doesn't block video state - no action needed here

    // === TRANSCRIPTION SYNC ===
    // Note: Transcription status is tracked via pendingTranscription counter
    // and VideoCaptionModel - no action needed here since captions don't block video state
  }

  logger.info(`Recheck videos status completed: checked ${videosChecked}, updated ${videosUpdated}`)

  return res.json({ videosChecked, videosUpdated })
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
  const jobType = req.params.jobType as string
  const jobId = req.params.jobId as string

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

function resumeJobQueue (req: express.Request, res: express.Response) {
  JobQueue.Instance.resume()

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
