import { FileStorage, HttpStatusCode, Job, JobState, JobType, ResultList, UserRight, VideoFileStream, VideoState, VideoStateType } from '@peertube/peertube-models'
import { Job as BullJob } from 'bullmq'
import express from 'express'
import { isArray } from '../../helpers/custom-validators/misc.js'
import { CONFIG } from '../../initializers/config.js'
import { sequelizeTypescript } from '../../initializers/database.js'
import { JobQueue } from '../../lib/job-queue/index.js'
import { hasVideoResourcesToBeMoved } from '../../lib/move-storage/shared/move-video.js'
import { buildMoveVideoJob } from '../../lib/video-jobs.js'
import { VideoJobInfoModel } from '../../models/video/video-job-info.js'
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
        await JobQueue.Instance.createJob(job)
        jobsCreated++
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
