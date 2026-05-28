import {
  FileStorage,
  HttpStatusCode,
  Job as JobModel,
  JobState,
  JobType,
  MoveStoragePayload,
  ResultList,
  VideoStatsOverallQuery,
  VideoStatsTimeserieMetric,
  VideoStatsTimeserieQuery,
  VideoStatsUserAgentQuery,
  isMoveVideoStoragePayload
} from '@peertube/peertube-models'
import { Job } from 'bullmq'
import { CONFIG } from '../../../initializers/config.js'
import { LocalVideoViewerModel } from '@server/models/view/local-video-viewer.js'
import { VideoModel } from '../../../models/video/video.js'
import { VideoCaptionModel } from '../../../models/video/video-caption.js'
import { VideoImportModel } from '../../../models/video/video-import.js'
import { VideoPathManager } from '../../../lib/video-path-manager.js'
import express from 'express'
import {
  asyncMiddleware,
  authenticate,
  videoJobsValidator,
  videoRetryJobValidator,
  videoOverallOrUserAgentStatsValidator,
  videoRetentionStatsValidator,
  videoTimeseriesStatsValidator
} from '../../../middlewares/index.js'
import { JobQueue } from '../../../lib/job-queue/job-queue.js'

const statsRouter = express.Router()

statsRouter.get(
  '/:videoId/stats/overall',
  authenticate,
  asyncMiddleware(videoOverallOrUserAgentStatsValidator),
  asyncMiddleware(getOverallStats)
)

statsRouter.get(
  '/:videoId/stats/timeseries/:metric',
  authenticate,
  asyncMiddleware(videoTimeseriesStatsValidator),
  asyncMiddleware(getTimeseriesStats)
)

statsRouter.get(
  '/:videoId/stats/retention',
  authenticate,
  asyncMiddleware(videoRetentionStatsValidator),
  asyncMiddleware(getRetentionStats)
)

statsRouter.get(
  '/:videoId/stats/user-agent',
  authenticate,
  asyncMiddleware(videoOverallOrUserAgentStatsValidator),
  asyncMiddleware(getUserAgentStats)
)

statsRouter.get(
  '/:videoId/jobs',
  authenticate,
  asyncMiddleware(videoJobsValidator),
  asyncMiddleware(listRelatedJobs)
)

statsRouter.post(
  '/:videoId/jobs/:jobType/:jobId/retry',
  authenticate,
  asyncMiddleware(videoRetryJobValidator),
  asyncMiddleware(retryRelatedJob)
)

// ---------------------------------------------------------------------------

export {
  statsRouter
}

// ---------------------------------------------------------------------------

async function getOverallStats (req: express.Request, res: express.Response) {
  const video = res.locals.videoAll
  const query = req.query as VideoStatsOverallQuery

  const stats = await LocalVideoViewerModel.getOverallStats({
    video,
    startDate: query.startDate,
    endDate: query.endDate
  })

  return res.json(stats)
}

async function getUserAgentStats (req: express.Request, res: express.Response) {
  const video = res.locals.videoAll
  const query = req.query as VideoStatsUserAgentQuery

  const stats = await LocalVideoViewerModel.getUserAgentStats({
    video,
    startDate: query.startDate,
    endDate: query.endDate
  })

  return res.json(stats)
}

async function getRetentionStats (req: express.Request, res: express.Response) {
  const video = res.locals.videoAll

  const stats = await LocalVideoViewerModel.getRetentionStats(video)

  return res.json(stats)
}

async function getTimeseriesStats (req: express.Request, res: express.Response) {
  const video = res.locals.videoAll
  const metric = req.params.metric as VideoStatsTimeserieMetric

  const query = req.query as VideoStatsTimeserieQuery

  const stats = await LocalVideoViewerModel.getTimeserieStats({
    video,
    metric,
    startDate: query.startDate ?? video.createdAt.toISOString(),
    endDate: query.endDate ?? new Date().toISOString()
  })

  return res.json(stats)
}

async function listRelatedJobs (req: express.Request, res: express.Response) {
  const video = res.locals.videoAll
  const videoImport = await VideoImportModel.unscoped().findOne({
    attributes: [ 'id' ],
    where: { videoId: video.id }
  })

  const jobs = await JobQueue.Instance.listForApi({
    start: 0,
    count: 1000,
    asc: false,
    videoUUID: video.uuid,
    videoId: video.id,
    videoImportId: videoImport?.id
  })

  const result: ResultList<JobModel> = {
    total: jobs.length,
    data: await Promise.all(jobs.map(j => formatJob(j)))
  }

  return res.json(result)
}

async function retryRelatedJob (req: express.Request, res: express.Response) {
  const video = res.locals.videoAll

  const result = await JobQueue.Instance.retryFailedJob({
    jobType: req.params.jobType as JobType,
    jobId: req.params.jobId,
    videoUUID: video.uuid,
    videoId: video.id
  })

  if (result.status === 'not_found') {
    return res.fail({
      status: HttpStatusCode.NOT_FOUND_404,
      message: 'Job was not found'
    })
  }

  if (result.status === 'not_video_related') {
    return res.fail({
      status: HttpStatusCode.BAD_REQUEST_400,
      message: 'Job is not related to this video'
    })
  }

  if (result.status === 'not_failed') {
    return res.fail({
      status: HttpStatusCode.BAD_REQUEST_400,
      message: 'Only failed jobs can be retried'
    })
  }

  if (result.status !== 'retried') {
    return res.fail({
      status: HttpStatusCode.BAD_REQUEST_400,
      message: 'Cannot retry this job'
    })
  }

  return res.json({ jobId: result.newJobId })
}

async function formatJob (job: Job): Promise<JobModel> {
  const state = await job.getState()
  const payload = job.data as MoveStoragePayload

  const formattedJob: JobModel = {
    id: job.id,
    state: state as JobState,
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

  // Add source/destination info for move-to-object-storage jobs
  if (job.queueName === 'move-to-object-storage' && isMoveVideoStoragePayload(payload)) {
    const sourcePaths: string[] = []
    const destinationPaths: string[] = []

    // Load video with files to get file info
    const video = await VideoModel.loadWithFiles(payload.videoUUID)
    if (video) {
      // Web videos on file system
      for (const f of video.VideoFiles || []) {
        if (f.storage === FileStorage.FILE_SYSTEM) {
          sourcePaths.push(VideoPathManager.Instance.getFSVideoFileOutputPath(video, f))
          destinationPaths.push(
            `${CONFIG.OBJECT_STORAGE.ENDPOINT}/${CONFIG.OBJECT_STORAGE.WEB_VIDEOS.BUCKET_NAME}/` +
            `${CONFIG.OBJECT_STORAGE.WEB_VIDEOS.PREFIX || ''}${f.filename}`
          )
        }
      }

      // HLS files
      for (const playlist of video.VideoStreamingPlaylists || []) {
        for (const f of playlist.VideoFiles || []) {
          if (f.storage === FileStorage.FILE_SYSTEM) {
            sourcePaths.push(VideoPathManager.Instance.getFSHLSOutputPath(video, f.filename))
            destinationPaths.push(
              `${CONFIG.OBJECT_STORAGE.ENDPOINT}/${CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS.BUCKET_NAME}/` +
              `${CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS.PREFIX || ''}hls/${video.uuid}/${f.filename}`
            )
          }
        }
      }

      // Thumbnails
      for (const t of video.Thumbnails || []) {
        if (t.storage === FileStorage.FILE_SYSTEM) {
          sourcePaths.push(t.getFSPath())
          destinationPaths.push(
            `${CONFIG.OBJECT_STORAGE.ENDPOINT}/${CONFIG.OBJECT_STORAGE.THUMBNAILS.BUCKET_NAME}/` +
            `${CONFIG.OBJECT_STORAGE.THUMBNAILS.PREFIX || ''}${t.filename}`
          )
        }
      }

      // Captions
      const captions = await VideoCaptionModel.listVideoCaptions(video.id)
      for (const c of captions) {
        if (c.storage === FileStorage.FILE_SYSTEM) {
          sourcePaths.push(c.getFSFilePath())
          destinationPaths.push(
            `${CONFIG.OBJECT_STORAGE.ENDPOINT}/${CONFIG.OBJECT_STORAGE.CAPTIONS.BUCKET_NAME}/` +
            `${CONFIG.OBJECT_STORAGE.CAPTIONS.PREFIX || ''}${c.filename}`
          )
        }
      }
    }

    // Add as metadata
    formattedJob.data = {
      ...payload,
      _sourcePaths: sourcePaths,
      _destinationPaths: destinationPaths
    }
  }

  return formattedJob
}

function getJobError (job: Job) {
  if (Array.isArray(job.stacktrace) && job.stacktrace.length !== 0) return job.stacktrace[0]
  if (job.failedReason) return job.failedReason

  return null
}
