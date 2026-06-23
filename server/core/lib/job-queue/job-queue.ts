import { pick, timeoutPromise } from '@peertube/peertube-core-utils'
import {
  ActivitypubFollowPayload,
  ActivitypubHttpBroadcastPayload,
  ActivitypubHttpFetcherPayload,
  ActivitypubHttpUnicastPayload,
  ActorKeysPayload,
  AfterVideoChannelImportPayload,
  CreateUserExportPayload,
  EmailPayload,
  FederateVideoPayload,
  GenerateStoryboardPayload,
  ImportUserArchivePayload,
  JobState,
  JobType,
  VideoState,
  ManageVideoTorrentPayload,
  MoveStoragePayload,
  MoveVideoFilePayload,
  MoveHLSPlaylistPayload,
  MoveThumbnailPayload,
  MoveCaptionPayload,
  NotifyPayload,
  RefreshPayload,
  TranscodingJobBuilderPayload,
  VideoChannelImportPayload,
  VideoChannelResetPayload,
  VideoFileImportPayload,
  VideoImportPayload,
  VideoLiveEndingPayload,
  VideoRedundancyPayload,
  VideoStudioEditionPayload,
  VideoTranscodingPayload,
  VideoTranscriptionPayload
} from '@peertube/peertube-models'
import { jobStates } from '../../helpers/custom-validators/jobs.js'
import { toCompleteUUID } from '../../helpers/custom-validators/misc.js'
import { CONFIG, registerConfigChangedHandler } from '../../initializers/config.js'
import {
  DeletedVideoJobIdentifiers,
  listDeletedVideoJobTypes,
  shouldRemoveDeletedVideoJob
} from './deleted-video-job-matchers.js'
import { processVideoRedundancy } from './handlers/video-redundancy.js'
import { scheduleRetainedLocalFilesCleanup } from '../move-storage/move-to-object-storage.js'
import {
  FlowJob,
  FlowProducer,
  Job,
  JobsOptions,
  Queue,
  QueueEvents,
  QueueEventsOptions,
  QueueOptions,
  Worker,
  WorkerOptions
} from 'bullmq'
import { logger } from '../../helpers/logger.js'
import { JOB_ATTEMPTS, JOB_CONCURRENCY, JOB_REMOVAL_OPTIONS, JOB_TTL, REPEAT_JOBS, WEBSERVER } from '../../initializers/constants.js'
import { VideoModel } from '../../models/video/video.js'
import { VideoJobInfoModel } from '../../models/video/video-job-info.js'
import { Hooks } from '../plugins/hooks.js'
import { Redis } from '../redis.js'
import { processActivityPubCleaner } from './handlers/activitypub-cleaner.js'
import { processActivityPubFollow } from './handlers/activitypub-follow.js'
import {
  processActivityPubHttpSequentialBroadcast,
  processActivityPubParallelHttpBroadcast
} from './handlers/activitypub-http-broadcast.js'
import { processActivityPubHttpFetcher } from './handlers/activitypub-http-fetcher.js'
import { processActivityPubHttpUnicast } from './handlers/activitypub-http-unicast.js'
import { refreshAPObject } from './handlers/activitypub-refresher.js'
import { processActorKeys } from './handlers/actor-keys.js'
import { processAfterVideoChannelImport } from './handlers/after-video-channel-import.js'
import { processCreateUserExport } from './handlers/create-user-export.js'
import { processEmail } from './handlers/email.js'
import { processFederateVideo } from './handlers/federate-video.js'
import { processGenerateStoryboard } from './handlers/generate-storyboard.js'
import { processImportUserArchive } from './handlers/import-user-archive.js'
import { processManageVideoTorrent } from './handlers/manage-video-torrent.js'
import { onMoveToFileSystemFailure, processMoveToFileSystem } from './handlers/move-to-file-system.js'
import { onMoveToObjectStorageFailure, processMoveToObjectStorage } from './handlers/move-to-object-storage.js'
import { onGranularMoveToObjectStorageFailure, processGranularMoveToObjectStorage } from './handlers/granular-move-to-object-storage.js'
import { processNotify } from './handlers/notify.js'
import { processTranscodingJobBuilder } from './handlers/transcoding-job-builder.js'
import { processVideoChannelImport } from './handlers/video-channel-import.js'
import { processVideoChannelReset } from './handlers/video-channel-reset.js'
import { processVideoFileImport } from './handlers/video-file-import.js'
import { processVideoImport } from './handlers/video-import.js'
import { processVideoLiveEnding } from './handlers/video-live-ending.js'
import { processVideoStudioEdition } from './handlers/video-studio-edition.js'
import { processVideoTranscoding } from './handlers/video-transcoding.js'
import { processVideoTranscription } from './handlers/video-transcription.js'
import { processVideosViewsStats } from './handlers/video-views-stats.js'
import { Op } from 'sequelize'
import { Redis as IORedis } from 'ioredis'

const TRANSCODING_PROGRESS_CACHE_TTL_MS = 1000

export type CreateJobArgument =
  | { type: 'activitypub-http-broadcast', payload: ActivitypubHttpBroadcastPayload }
  | { type: 'activitypub-http-broadcast-parallel', payload: ActivitypubHttpBroadcastPayload }
  | { type: 'activitypub-http-unicast', payload: ActivitypubHttpUnicastPayload }
  | { type: 'activitypub-http-fetcher', payload: ActivitypubHttpFetcherPayload }
  | { type: 'activitypub-cleaner', payload: {} }
  | { type: 'activitypub-follow', payload: ActivitypubFollowPayload }
  | { type: 'video-file-import', payload: VideoFileImportPayload }
  | { type: 'video-transcoding', payload: VideoTranscodingPayload }
  | { type: 'email', payload: EmailPayload }
  | { type: 'transcoding-job-builder', payload: TranscodingJobBuilderPayload }
  | { type: 'video-import', payload: VideoImportPayload }
  | { type: 'activitypub-refresher', payload: RefreshPayload }
  | { type: 'videos-views-stats', payload: {} }
  | { type: 'video-live-ending', payload: VideoLiveEndingPayload }
  | { type: 'actor-keys', payload: ActorKeysPayload }
  | { type: 'video-redundancy', payload: VideoRedundancyPayload }
  | { type: 'video-studio-edition', payload: VideoStudioEditionPayload }
  | { type: 'manage-video-torrent', payload: ManageVideoTorrentPayload }
  | { type: 'move-to-object-storage', payload: MoveStoragePayload }
  | { type: 'move-to-file-system', payload: MoveStoragePayload }
  | { type: 'move-video-file-to-object-storage', payload: MoveVideoFilePayload }
  | { type: 'move-hls-playlist-to-object-storage', payload: MoveHLSPlaylistPayload }
  | { type: 'move-thumbnail-to-object-storage', payload: MoveThumbnailPayload }
  | { type: 'move-caption-to-object-storage', payload: MoveCaptionPayload }
  | { type: 'video-channel-import', payload: VideoChannelImportPayload }
  | { type: 'video-channel-reset', payload: VideoChannelResetPayload }
  | { type: 'after-video-channel-import', payload: AfterVideoChannelImportPayload }
  | { type: 'notify', payload: NotifyPayload }
  | { type: 'federate-video', payload: FederateVideoPayload }
  | { type: 'create-user-export', payload: CreateUserExportPayload }
  | { type: 'generate-video-storyboard', payload: GenerateStoryboardPayload }
  | { type: 'import-user-archive', payload: ImportUserArchivePayload }
  | { type: 'video-transcription', payload: VideoTranscriptionPayload }

export type CreateJobOptions = {
  delay?: number
  priority?: number
  customJobId?: string
  failParentOnFailure?: boolean
}

const handlers: { [id in JobType]: (job: Job) => Promise<any> } = {
  'activitypub-cleaner': processActivityPubCleaner,
  'activitypub-follow': processActivityPubFollow,
  'activitypub-http-broadcast-parallel': processActivityPubParallelHttpBroadcast,
  'activitypub-http-broadcast': processActivityPubHttpSequentialBroadcast,
  'activitypub-http-fetcher': processActivityPubHttpFetcher,
  'activitypub-http-unicast': processActivityPubHttpUnicast,
  'activitypub-refresher': refreshAPObject,
  'actor-keys': processActorKeys,
  'after-video-channel-import': processAfterVideoChannelImport,
  'email': processEmail,
  'federate-video': processFederateVideo,
  'transcoding-job-builder': processTranscodingJobBuilder,
  'manage-video-torrent': processManageVideoTorrent,
  'move-to-object-storage': processMoveToObjectStorage,
  'move-to-file-system': processMoveToFileSystem,
  'move-video-file-to-object-storage': processGranularMoveToObjectStorage,
  'move-hls-playlist-to-object-storage': processGranularMoveToObjectStorage,
  'move-thumbnail-to-object-storage': processGranularMoveToObjectStorage,
  'move-caption-to-object-storage': processMoveToObjectStorage,
  'notify': processNotify,
  'video-channel-import': processVideoChannelImport,
  'video-channel-reset': processVideoChannelReset,
  'video-file-import': processVideoFileImport,
  'video-import': processVideoImport,
  'video-live-ending': processVideoLiveEnding,
  'video-redundancy': processVideoRedundancy,
  'video-studio-edition': processVideoStudioEdition,
  'video-transcoding': processVideoTranscoding,
  'videos-views-stats': processVideosViewsStats,
  'generate-video-storyboard': processGenerateStoryboard,
  'create-user-export': processCreateUserExport,
  'import-user-archive': processImportUserArchive,
  'video-transcription': processVideoTranscription
}

const errorHandlers: { [id in JobType]?: (job: Job, err: any) => Promise<any> } = {
  'move-to-object-storage': onMoveToObjectStorageFailure,
  'move-to-file-system': onMoveToFileSystemFailure,
  'move-video-file-to-object-storage': onGranularMoveToObjectStorageFailure,
  'move-hls-playlist-to-object-storage': onGranularMoveToObjectStorageFailure,
  'move-thumbnail-to-object-storage': onGranularMoveToObjectStorageFailure,
  'move-caption-to-object-storage': onMoveToObjectStorageFailure
}

const jobTypes: JobType[] = [
  'activitypub-cleaner',
  'activitypub-follow',
  'activitypub-http-broadcast-parallel',
  'activitypub-http-broadcast',
  'activitypub-http-fetcher',
  'activitypub-http-unicast',
  'activitypub-refresher',
  'actor-keys',
  'after-video-channel-import',
  'email',
  'federate-video',
  'generate-video-storyboard',
  'manage-video-torrent',
  'move-to-object-storage',
  'move-to-file-system',
  'move-video-file-to-object-storage',
  'move-hls-playlist-to-object-storage',
  'move-thumbnail-to-object-storage',
  'move-caption-to-object-storage',
  'notify',
  'transcoding-job-builder',
  'video-channel-import',
  'video-channel-reset',
  'video-file-import',
  'video-import',
  'video-live-ending',
  'video-redundancy',
  'video-studio-edition',
  'video-transcription',
  'videos-views-stats',
  'create-user-export',
  'import-user-archive',
  'video-transcoding'
]

const silentFailure = new Set<JobType>([ 'activitypub-http-unicast' ])

const CANCELLED_REASON = 'Video was deleted - transcoding job cancelled'
const VIDEO_PIPELINE_JOB_TYPES_ON_RESET_HOLD = new Set<JobType>([
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
  'video-studio-edition'
])

class JobQueue {
  private static instance: JobQueue

  private workers: { [id in JobType]?: Worker } = {}
  private queues: { [id in JobType]?: Queue } = {}
  private queueEvents: { [id in JobType]?: QueueEvents } = {}
  private readonly startedQueueEvents = new Set<JobType>()

  private flowProducer: FlowProducer
  private sharedRedisClient: IORedis

  private initialized = false
  private jobRedisPrefix: string
  private transcodingProgressCache?: {
    expiresAt: number
    values: Map<string, number>
  }

  private constructor () {
  }

  init () {
    // Already initialized
    if (this.initialized === true) return
    this.initialized = true

    this.jobRedisPrefix = 'bull-' + WEBSERVER.HOST
    this.sharedRedisClient = new IORedis(Redis.getRedisClientOptions('BullMQShared', { maxRetriesPerRequest: null }))
    const disableWorkers = process.env.PEERTUBE_TEST_DISABLE_JOB_WORKERS === 'true'

    for (const handlerName of Object.keys(handlers)) {
      if (!disableWorkers) this.buildWorker(handlerName)
      this.buildQueue(handlerName)
    }

    this.flowProducer = new FlowProducer({
      connection: this.sharedRedisClient,
      prefix: this.jobRedisPrefix
    })
    this.flowProducer.on('error', err => {
      logger.error('Error in flow producer', { err })
    })

    this.addRepeatableJobs()

    registerConfigChangedHandler(() => {
      if (disableWorkers) return

      for (const handlerName of Object.keys(handlers)) {
        const worker = this.workers[handlerName]
        if (!worker) continue

        worker.concurrency = this.getJobConcurrency(handlerName)
      }
    })
  }

  private buildWorker (handlerName: JobType) {
    // Transcoding/transcription/move operations can be I/O-heavy and may briefly block lock renewals.
    // Use a longer lock duration to avoid false "stalled" loops where jobs bounce back to waiting.
    const longRunningHandlers = [
      'video-transcoding',
      'video-transcription',
      'move-to-object-storage',
      'move-to-file-system',
      'move-video-file-to-object-storage',
      'move-hls-playlist-to-object-storage',
      'move-thumbnail-to-object-storage',
      'move-caption-to-object-storage'
    ]
    const lockDuration = longRunningHandlers.includes(handlerName)
      ? 1000 * 60 * 10 // 10 minutes for long-running operations
      : 30000 // 30 seconds for quick operations

    const workerOptions: WorkerOptions = {
      autorun: false,
      concurrency: this.getJobConcurrency(handlerName),
      prefix: this.jobRedisPrefix,
      connection: this.sharedRedisClient,
      maxStalledCount: 10,
      lockDuration
    }

    const handler = function (job: Job) {
      const timeout = JOB_TTL[handlerName]
      const p = handlers[handlerName](job)

      if (!timeout) return p

      return timeoutPromise(p, timeout)
    }

    const processor = async (jobArg: Job) => {
      const job = await Hooks.wrapObject(jobArg, 'filter:job-queue.process.params', { type: handlerName })

      return Hooks.wrapPromiseFun(handler, job, 'filter:job-queue.process.result')
    }

    const worker = new Worker(handlerName, processor, workerOptions)

    // Handle stalled jobs: when lockDuration expires and the job hasn't been processed,
    // BullMQ marks it as stalled so it can be retried by another worker
    worker.on('stalled', (jobId) => {
      logger.warn('Job %s in queue %s has stalled (lock expired). It will be retried.', jobId, handlerName)
    })

    worker.on('failed', (job, err) => {
      let logLevel: 'debug' | 'info' | 'error' = silentFailure.has(handlerName)
        ? 'debug'
        : 'error'
      if (logLevel === 'error' && err?.message === CANCELLED_REASON) {
        logLevel = 'info'
      }
      logger.log(logLevel, 'Cannot execute job %s in queue %s.', job.id, handlerName, { payload: job.data, err })

      if (errorHandlers[handlerName]) {
        errorHandlers[handlerName](job, err)
          .catch(err => logger.error('Cannot run error handler for job failure %d in queue %s.', job.id, handlerName, { err }))
      }
    })

    worker.on('error', err => {
      logger.error('Error in job worker %s.', handlerName, { err })
    })

    this.workers[handlerName] = worker
  }

  private buildQueue (handlerName: JobType) {
    const queueOptions: QueueOptions = {
      connection: this.sharedRedisClient,
      prefix: this.jobRedisPrefix
    }

    const queue = new Queue(handlerName, queueOptions)
    queue.on('error', err => {
      logger.error('Error in job queue %s.', handlerName, { err })
    })

    this.queues[handlerName] = queue

    queue.removeDeprecatedPriorityKey()
      .catch(err => logger.error('Cannot remove bullmq deprecated priority keys of ' + handlerName, { err }))
  }

  private buildQueueEvent (handlerName: JobType) {
    const existing = this.queueEvents[handlerName]
    if (existing) return existing

    const queueEventsOptions: QueueEventsOptions = {
      autorun: false,
      connection: this.sharedRedisClient,
      prefix: this.jobRedisPrefix
    }

    const queueEvents = new QueueEvents(handlerName, queueEventsOptions)
    queueEvents.on('error', err => {
      logger.error('Error in job queue events %s.', handlerName, { err })
    })

    this.queueEvents[handlerName] = queueEvents
    return queueEvents
  }

  // ---------------------------------------------------------------------------

  async terminate () {
    const promises = Object.keys(this.workers)
      .map(handlerName => {
        const worker: Worker = this.workers[handlerName]
        const queue: Queue = this.queues[handlerName]
        const queueEvent: QueueEvents = this.queueEvents[handlerName]

        return Promise.all([
          worker.close(false),
          queue.close(),
          queueEvent?.close()
        ])
      })

    await Promise.all(promises)

    if (this.sharedRedisClient) {
      await this.sharedRedisClient.quit().catch(() => this.sharedRedisClient.disconnect())
    }
  }

  start () {
    scheduleRetainedLocalFilesCleanup()

    const promises = Object.keys(this.workers)
      .map(handlerName => {
        const worker: Worker = this.workers[handlerName]

        return Promise.all([
          worker.run()
        ])
      })

    return Promise.all(promises)
  }

  async pause (options: { doNotWaitActive?: boolean, jobTypes?: JobType[] } = {}) {
    const { doNotWaitActive = false, jobTypes } = options
    const workerNames = jobTypes ?? Object.keys(this.workers)

    for (const handlerName of workerNames) {
      const worker: Worker = this.workers[handlerName]
      if (!worker) continue

      await worker.pause(doNotWaitActive)
    }
  }

  async resume (jobTypes?: JobType[]) {
    const queueNames = jobTypes ?? Object.keys(this.queues)
    const workerNames = jobTypes ?? Object.keys(this.workers)

    for (const queueName of queueNames) {
      const queue: Queue = this.queues[queueName]
      if (!queue) continue

      await queue.resume()
    }

    for (const handlerName of workerNames) {
      const worker: Worker = this.workers[handlerName]
      if (!worker) continue

      worker.resume()
    }
  }

  async clearVideoPipelineSystemResetHoldAndResume () {
    await Redis.Instance.removeVideoPipelineSystemResetHold()
    await this.resume([ ...VIDEO_PIPELINE_JOB_TYPES_ON_RESET_HOLD ])
  }

  // ---------------------------------------------------------------------------

  createJobAsync (options: CreateJobArgument & CreateJobOptions): void {
    Promise.resolve(this.createJob(options))
      .catch(err => logger.error('Cannot create job.', { err, options }))
  }

  createJob (options: CreateJobArgument & CreateJobOptions | undefined) {
    if (!options) return

    if (VIDEO_PIPELINE_JOB_TYPES_ON_RESET_HOLD.has(options.type)) {
      return this.createVideoPipelineJobRespectingSystemResetHold(options)
    }

    const queue: Queue = this.queues[options.type]
    if (queue === undefined) {
      logger.error('Unknown queue %s: cannot create job.', options.type)
      return
    }

    const jobOptions = this.buildJobOptions(options.type as JobType, pick(options, [ 'priority', 'delay', 'customJobId' ]))

    return queue.add('job', options.payload, jobOptions)
  }

  private async createVideoPipelineJobRespectingSystemResetHold (options: CreateJobArgument & CreateJobOptions) {
    const isResetHoldEnabled = await Redis.Instance.isVideoPipelineSystemResetHoldSet()
    const isNewVideoTrigger = this.isNewVideoPipelineTrigger(options)

    if (isResetHoldEnabled && isNewVideoTrigger) {
      const payload = options.payload as { videoUUID?: string }

      logger.info('[JOB_QUEUE] Clearing video pipeline reset hold before creating %s job.', options.type, {
        videoUUID: payload?.videoUUID
      })

      await this.clearVideoPipelineSystemResetHoldAndResume()
    }

    if (isResetHoldEnabled && !isNewVideoTrigger) {
      logger.warn('[JOB_QUEUE] Blocking %s job creation because video pipeline reset hold is enabled.', options.type, {
        payload: options.payload
      })
      return undefined
    }

    const queue: Queue = this.queues[options.type]
    if (queue === undefined) {
      logger.error('Unknown queue %s: cannot create job.', options.type)
      return
    }

    const jobOptions = this.buildJobOptions(options.type as JobType, pick(options, [ 'priority', 'delay', 'customJobId' ]))

    return queue.add('job', options.payload, jobOptions)
  }

  private isNewVideoPipelineTrigger (job: CreateJobArgument & CreateJobOptions) {
    const payload = job.payload as {
      isNewVideo?: boolean
      videoUUID?: string
      optimizeJob?: { isNewVideo?: boolean }
      moveVideoState?: { isNewVideo?: boolean }
    }

    return job.type === 'video-import' ||
      payload?.isNewVideo === true ||
      payload?.optimizeJob?.isNewVideo === true ||
      payload?.moveVideoState?.isNewVideo === true
  }

  private async shouldAllowProtectedVideoPipelineFlowCreation (
    jobs: (CreateJobArgument & CreateJobOptions)[],
    options: {
      blockedMessage: string
      blockedContext: Record<string, unknown>
      resumeMessage: string
      resumeContext: Record<string, unknown>
    }
  ) {
    const hasProtectedJob = jobs.some(job => VIDEO_PIPELINE_JOB_TYPES_ON_RESET_HOLD.has(job.type))
    if (!hasProtectedJob) return true

    const isResetHoldEnabled = await Redis.Instance.isVideoPipelineSystemResetHoldSet()
    if (!isResetHoldEnabled) return true

    const hasNewVideoTrigger = jobs.some(job => this.isNewVideoPipelineTrigger(job))
    if (!hasNewVideoTrigger) {
      logger.warn(options.blockedMessage, options.blockedContext)
      return false
    }

    logger.info(options.resumeMessage, options.resumeContext)
    await this.clearVideoPipelineSystemResetHoldAndResume()

    return true
  }

  async retryFailedJob (options: {
    jobType: JobType
    jobId: string
    videoUUID?: string
    videoId?: number
  }): Promise<
    | { status: 'retried', newJobId: string | number }
    | { status: 'not_found' | 'not_failed' | 'not_video_related' }
    > {
    const { jobType, jobId, videoUUID, videoId } = options

    const queue = this.queues[jobType]
    if (!queue) return { status: 'not_found' }

    const job = await queue.getJob(jobId)
    if (!job) return { status: 'not_found' }

    if (videoUUID || videoId) {
      const data = job.data as { videoUUID?: string, videoId?: number }

      const matchUUID = typeof videoUUID === 'string' && data?.videoUUID === videoUUID
      const matchId = typeof videoId === 'number' && data?.videoId === videoId

      if (!matchUUID && !matchId) return { status: 'not_video_related' }
    }

    const state = await job.getState()
    if (state !== 'failed') return { status: 'not_failed' }

    const retry = await this.buildRetryJobData(jobType, job.data)

    let newJob: Job
    try {
      newJob = await queue.add('job', retry.data, this.buildJobOptions(jobType, { priority: job.opts.priority }))
    } catch (err) {
      if (retry.increasedPendingMove) {
        await VideoJobInfoModel.decrease(retry.data.videoUUID, 'pendingMove')
      }

      throw err
    }

    return { status: 'retried', newJobId: newJob.id }
  }

  private async buildRetryJobData (jobType: JobType, jobData: any) {
    if (!this.isObjectStorageMoveJobType(jobType) || !jobData?.videoUUID) {
      return { data: jobData, increasedPendingMove: false }
    }

    if (jobType === 'move-hls-playlist-to-object-storage' && jobData?.cleanupMode === 'cleanup') {
      return { data: jobData, increasedPendingMove: false }
    }

    await VideoJobInfoModel.increaseOrCreate(jobData.videoUUID, 'pendingMove')

    return {
      data: {
        ...jobData,
        retryOfFailedJob: true
      },
      increasedPendingMove: true
    }
  }

  private isObjectStorageMoveJobType (jobType: JobType) {
    return jobType === 'move-to-object-storage' ||
      jobType === 'move-video-file-to-object-storage' ||
      jobType === 'move-hls-playlist-to-object-storage' ||
      jobType === 'move-thumbnail-to-object-storage'
  }

  async hasPendingOrActiveJob (jobType: JobType, videoUUID: string, captionId?: number): Promise<boolean> {
    const queue = this.queues[jobType]
    if (!queue) return false

    // Check all not-finished states that may still need local files
    const states: ('waiting' | 'delayed' | 'prioritized' | 'waiting-children' | 'active')[] = [
      'waiting',
      'delayed',
      'prioritized',
      'waiting-children',
      'active'
    ]
    const jobs = await queue.getJobs(states, 0, 1000, true)

    if (captionId !== undefined) {
      // Check for caption-specific job
      return jobs.some((job: Job) => {
        const data = (job?.data ?? {}) as { captionId?: number; videoUUID?: string }
        return data?.captionId === captionId
      })
    }

    return jobs.some((job: Job) => {
      const data = (job?.data ?? {}) as { videoUUID?: string }
      return data?.videoUUID === videoUUID
    })
  }

  async hasPendingOrActiveHLSPlaylistMoveJob (options: {
    videoUUID: string
    excludeCleanupJobs?: boolean
    activeOnly?: boolean
  }): Promise<boolean> {
    const queue = this.queues['move-hls-playlist-to-object-storage']
    if (!queue) return false

    const { activeOnly = false } = options
    const states: ('waiting' | 'delayed' | 'prioritized' | 'waiting-children' | 'active')[] = activeOnly
      ? [ 'active' ]
      : [
          'waiting',
          'delayed',
          'prioritized',
          'waiting-children',
          'active'
        ]
    const jobs = await queue.getJobs(states, 0, 1000, true)

    const { videoUUID, excludeCleanupJobs = false } = options

    return jobs.some((job: Job) => {
      const data = (job?.data ?? {}) as { videoUUID?: string, cleanupMode?: 'move' | 'cleanup' }
      if (data?.videoUUID !== videoUUID) return false

      if (excludeCleanupJobs && data.cleanupMode === 'cleanup') return false

      return true
    })
  }

  async hasPendingOrActiveLocalFileConsumerJob (videoUUID: string): Promise<boolean> {
    const jobTypes: JobType[] = [
      'transcoding-job-builder',
      'video-transcoding',
      'video-transcription',
      'generate-video-storyboard',
      'video-studio-edition',
      'move-to-object-storage',
      'move-video-file-to-object-storage',
      'move-hls-playlist-to-object-storage',
      'move-thumbnail-to-object-storage'
    ]

    for (const jobType of jobTypes) {
      if (await this.hasPendingOrActiveJob(jobType, videoUUID)) return true
    }

    return false
  }

  async getLocalVideoPipelineBacklog () {
    const localPipelineJobTypes: JobType[] = [
      'transcoding-job-builder',
      'video-transcoding',
      'move-to-object-storage',
      'move-video-file-to-object-storage',
      'move-hls-playlist-to-object-storage',
      'move-thumbnail-to-object-storage',
      'move-caption-to-object-storage'
    ]
    const states: JobState[] = [ 'waiting', 'delayed', 'prioritized', 'waiting-children', 'active' ]
    const byType: Partial<Record<JobType, number>> = {}
    const importRelevantUniqueVideoUUIDsByType: Partial<Record<JobType, number>> = {}
    const uniqueVideoUUIDsByType: Partial<Record<JobType, number>> = {}
    const importRelevantUniqueVideoUUIDs = new Set<string>()
    const uniqueVideoUUIDs = new Set<string>()
    let total = 0

    for (const jobType of localPipelineJobTypes) {
      const queue = this.queues[jobType]
      if (!queue) continue

      const counts = await queue.getJobCounts()
      const count = states.reduce((sum, state) => sum + (counts[state] ?? 0), 0)

      if (count !== 0) byType[jobType] = count
      total += count

      if (jobType === 'move-caption-to-object-storage') continue

      const jobs = await queue.getJobs(states as Parameters<Queue['getJobs']>[0], 0, 10000, true)
      const importRelevantUUIDsForType = new Set<string>()
      const uuidsForType = new Set<string>()

      for (const job of jobs) {
        const data = (job?.data ?? {}) as {
          videoUUID?: string
          isNewVideo?: boolean
          previousVideoState?: number
          moveVideoState?: { isNewVideo?: boolean, previousVideoState?: number }
          optimizeJob?: { isNewVideo?: boolean }
          jobs?: { payload?: { isNewVideo?: boolean } }[]
          sequentialJobs?: { payload?: { isNewVideo?: boolean } }[][]
        }
        const videoUUID = data.videoUUID
        if (!videoUUID) continue

        uuidsForType.add(videoUUID)
        uniqueVideoUUIDs.add(videoUUID)

        if (this.isImportRelevantLocalPipelineJobData(jobType, data)) {
          importRelevantUUIDsForType.add(videoUUID)
          importRelevantUniqueVideoUUIDs.add(videoUUID)
        }
      }

      if (importRelevantUUIDsForType.size !== 0) importRelevantUniqueVideoUUIDsByType[jobType] = importRelevantUUIDsForType.size
      if (uuidsForType.size !== 0) uniqueVideoUUIDsByType[jobType] = uuidsForType.size
    }

    return {
      total,
      byType,
      importRelevantUniqueVideoUUIDTotal: importRelevantUniqueVideoUUIDs.size,
      importRelevantUniqueVideoUUIDsByType,
      uniqueVideoUUIDTotal: uniqueVideoUUIDs.size,
      uniqueVideoUUIDsByType
    }
  }

  // New-import pressure is primarily driven by videos still in their initial
  // transcode/move pipeline. Published-video follow-up cleanup (for example
  // late HLS object-storage moves) should not throttle fresh imports as
  // aggressively.
  private isImportRelevantLocalPipelineJobData (jobType: JobType, data: {
    isNewVideo?: boolean
    previousVideoState?: number
    moveVideoState?: { isNewVideo?: boolean, previousVideoState?: number }
    optimizeJob?: { isNewVideo?: boolean }
    jobs?: { payload?: { isNewVideo?: boolean } }[]
    sequentialJobs?: { payload?: { isNewVideo?: boolean } }[][]
  }) {
    if (jobType === 'generate-video-storyboard') return false

    if (jobType === 'transcoding-job-builder') {
      if (data.optimizeJob?.isNewVideo === true) return true
      if (data.jobs?.some(job => job.payload?.isNewVideo === true)) return true
      if (data.sequentialJobs?.some(group => group.some(job => job.payload?.isNewVideo === true))) return true
      return false
    }

    const previousVideoState = data.previousVideoState ?? data.moveVideoState?.previousVideoState

    if (jobType === 'video-transcoding') {
      return data.isNewVideo === true
    }

    if (jobType === 'move-to-object-storage') {
      if (data.isNewVideo === true || data.moveVideoState?.isNewVideo === true) return true
      return previousVideoState !== undefined && previousVideoState !== VideoState.PUBLISHED
    }

    if (
      jobType === 'move-video-file-to-object-storage' ||
      jobType === 'move-hls-playlist-to-object-storage' ||
      jobType === 'move-thumbnail-to-object-storage'
    ) {
      if (data.isNewVideo === true) return true
      return previousVideoState !== undefined && previousVideoState !== VideoState.PUBLISHED
    }

    return false
  }

  async getExistingMoveJob (jobType: JobType, videoUUID: string, options?: {
    isFollowUp?: boolean
    fileId?: number
    thumbnailId?: number
    captionId?: number
  }) {
    const queue = this.queues[jobType]
    if (!queue) return null

    // Check all states: waiting, delayed, active, failed
    const states: ('waiting' | 'delayed' | 'active' | 'failed')[] = [ 'waiting', 'delayed', 'active', 'failed' ]
    const jobs = await queue.getJobs(states, 0, 10000, true)

    const matchingJobs = jobs.filter((job: Job) => {
      const data = (job?.data ?? {}) as {
        videoUUID?: string
        isFollowUp?: boolean
        fileId?: number
        thumbnailId?: number
        captionId?: number
      }
      if (data.videoUUID !== videoUUID) return false
      if (options?.isFollowUp !== undefined && (data.isFollowUp === true) !== options.isFollowUp) return false
      if (options?.fileId !== undefined && data.fileId !== options.fileId) return false
      if (options?.thumbnailId !== undefined && data.thumbnailId !== options.thumbnailId) return false
      if (options?.captionId !== undefined && data.captionId !== options.captionId) return false

      return true
    })

    return matchingJobs.find(job => job.failedReason === undefined || job.failedReason === null) || matchingJobs[0] || null
  }

  async getExistingCaptionMoveJob (captionId: number) {
    const queues = [
      this.queues['move-to-object-storage'],
      this.queues['move-caption-to-object-storage']
    ].filter(Boolean)
    if (queues.length === 0) return null

    const states: ('waiting' | 'delayed' | 'active' | 'failed')[] = [ 'waiting', 'delayed', 'active', 'failed' ]
    const jobs = (await Promise.all(queues.map(queue => queue.getJobs(states, 0, 10000, true)))).flat()

    const matchingJobs = jobs.filter((job: Job) => ((job?.data ?? {}) as { captionId?: number }).captionId === captionId)

    return matchingJobs.find(job => job.failedReason === undefined || job.failedReason === null) || matchingJobs[0] || null
  }

  async getExistingCaptionMoveJobByVideoUUID (videoUUID: string) {
    const queues = [
      this.queues['move-to-object-storage'],
      this.queues['move-caption-to-object-storage']
    ].filter(Boolean)
    if (queues.length === 0) return null

    const states: ('waiting' | 'delayed' | 'active' | 'failed')[] = [ 'waiting', 'delayed', 'active', 'failed' ]
    const jobs = (await Promise.all(queues.map(queue => queue.getJobs(states, 0, 10000, true)))).flat()

    const matchingJobs = jobs.filter((job: Job) => ((job?.data ?? {}) as { videoUUID?: string }).videoUUID === videoUUID)

    return matchingJobs.find(job => job.failedReason === undefined || job.failedReason === null) || matchingJobs[0] || null
  }

  async getExistingHLSPlaylistMoveJobs (videoUUID: string, playlistId: number, fileIds: number[], options?: {
    match?: 'exact' | 'overlap'
  }) {
    const queue = this.queues['move-hls-playlist-to-object-storage']
    if (!queue) return []

    const states: ('waiting' | 'delayed' | 'active' | 'failed')[] = [ 'waiting', 'delayed', 'active', 'failed' ]
    const jobs = await queue.getJobs(states, 0, 10000, true)

    const { match = 'exact' } = options ?? {}

    return jobs.filter((job: Job) => {
      const data = job.data as {
        videoUUID?: string
        playlistId?: number
        fileIds?: number[]
        cleanupMode?: 'move' | 'cleanup'
      }
      const jobFileIds = data?.fileIds
      if (data?.cleanupMode === 'cleanup') return false
      if (data?.videoUUID !== videoUUID || data?.playlistId !== playlistId || !jobFileIds) return false

      if (match === 'overlap') {
        return fileIds.some(id => jobFileIds.includes(id))
      }

      return fileIds.length === jobFileIds.length &&
             fileIds.every(id => jobFileIds.includes(id))
    })
  }

  async waitForJobCompletion (options: {
    jobType: JobType
    jobId: string | number
    timeoutMs?: number
  }) {
    const { jobType, jobId, timeoutMs } = options

    const queue = this.queues[jobType]
    if (!queue) return

    const queueEvents = this.queueEvents[jobType] ?? this.buildQueueEvent(jobType)

    if (!this.startedQueueEvents.has(jobType)) {
      await queueEvents.waitUntilReady()
      void queueEvents.run()
        .catch(err => logger.error('Error while running on-demand job queue events %s.', jobType, { err }))
      this.startedQueueEvents.add(jobType)
    }

    const job = await queue.getJob(String(jobId))
    if (!job) {
      logger.debug(
        '[JOB_QUEUE] Job %s in queue %s not found while waiting for completion, skipping wait',
        jobId,
        jobType
      )
      return
    }

    await job.waitUntilFinished(queueEvents, timeoutMs)
  }

  async createSequentialJobFlow (...jobs: ((CreateJobArgument & CreateJobOptions) | undefined)[]) {
    const filteredJobs = jobs.filter(job => !!job)
    if (filteredJobs.length === 0) return undefined

    const isAllowed = await this.shouldAllowProtectedVideoPipelineFlowCreation(filteredJobs, {
      blockedMessage:
        '[JOB_QUEUE] Blocking sequential job flow creation because video pipeline reset hold is enabled ' +
        'and no new-video trigger exists.',
      blockedContext: {
        jobTypes: filteredJobs.map(job => job.type)
      },
      resumeMessage: '[JOB_QUEUE] Clearing video pipeline reset hold before creating protected sequential job flow.',
      resumeContext: {
        jobTypes: filteredJobs.map(job => job.type)
      }
    })
    if (!isAllowed) return undefined

    let lastJob: FlowJob

    logger.debug('Creating jobs in local job queue', { jobs: filteredJobs })

    for (const job of filteredJobs) {
      lastJob = {
        ...this.buildJobFlowOption(job),

        children: lastJob
          ? [ lastJob ]
          : []
      }
    }

    return this.flowProducer.add(lastJob)
  }

  async createJobWithChildren (parent: CreateJobArgument & CreateJobOptions, children: (CreateJobArgument & CreateJobOptions)[]) {
    const flowJobs = [ parent, ...children ]
    const isAllowed = await this.shouldAllowProtectedVideoPipelineFlowCreation(flowJobs, {
      blockedMessage:
        '[JOB_QUEUE] Blocking parent/children job flow creation because video pipeline reset hold is enabled ' +
        'and no new-video trigger exists.',
      blockedContext: {
        parentType: parent.type,
        childTypes: children.map(job => job.type)
      },
      resumeMessage:
        '[JOB_QUEUE] Clearing video pipeline reset hold before creating protected parent/children job flow.',
      resumeContext: {
        parentType: parent.type,
        childTypes: children.map(job => job.type)
      }
    })
    if (!isAllowed) return undefined

    return this.flowProducer.add({
      ...this.buildJobFlowOption(parent),

      children: children.map(c => this.buildJobFlowOption(c))
    })
  }

  private buildJobFlowOption (job: CreateJobArgument & CreateJobOptions): FlowJob {
    return {
      name: 'job',
      data: job.payload,
      queueName: job.type,
      opts: {
        failParentOnFailure: true,

        ...this.buildJobOptions(job.type as JobType, pick(job, [ 'priority', 'delay', 'failParentOnFailure', 'customJobId' ]))
      }
    }
  }

  private buildJobOptions (type: JobType, options: CreateJobOptions = {}): JobsOptions {
    return {
      backoff: { delay: 60 * 1000, type: 'exponential' },
      attempts: JOB_ATTEMPTS[type],
      priority: options.priority,
      delay: options.delay,
      jobId: options.customJobId,

      ...this.buildJobRemovalOptions(type)
    }
  }

  // ---------------------------------------------------------------------------

  async listForApi (options: {
    state?: JobState
    start: number
    count: number
    asc?: boolean
    jobType?: JobType
    search?: string
    videoUUID?: string
    videoId?: number
    videoImportId?: number
  }): Promise<Job[]> {
    const { state, start, count, asc, jobType, search, videoUUID, videoId, videoImportId } = options

    const states = this.buildStateFilter(state)
    const filteredJobTypes = this.buildTypeFilter(jobType)

    // When filtering failed/cancelled we over-fetch because we filter by failedReason
    const fetchLimit = search || videoUUID || videoId || videoImportId
      ? 10000
      : (state === 'failed' || state === 'cancelled')
          ? Math.min(10000, start + count + 2000)
          : start + count

    let results: Job[] = []

    for (const jobType of filteredJobTypes) {
      const queue: Queue = this.queues[jobType]

      if (queue === undefined) {
        logger.error('Unknown queue %s to list jobs.', jobType)
        continue
      }

      const jobs = await queue.getJobs(states as Parameters<Queue['getJobs']>[0], 0, fetchLimit, asc)

      results = results.concat(jobs)
    }

    if (state === 'failed' || state === 'cancelled') {
      const wantCancelled = state === 'cancelled'
      results = results.filter((j: Job) => {
        const isCancelled = typeof j.failedReason === 'string' &&
          j.failedReason.includes('Video was deleted - transcoding job cancelled')
        return wantCancelled ? isCancelled : !isCancelled
      })
    }

    results = await this.filterJobsByVideoOptions(results, { search, videoUUID, videoId, videoImportId })

    results.sort((j1: any, j2: any) => {
      if (j1.timestamp < j2.timestamp) return -1
      else if (j1.timestamp === j2.timestamp) return 0

      return 1
    })

    if (asc === false) results.reverse()

    return results.slice(start, start + count)
  }

  async count (
    state: JobState,
    jobType?: JobType,
    search?: string,
    videoUUID?: string,
    videoId?: number,
    videoImportId?: number
  ): Promise<number> {
    const filteredJobTypes = this.buildTypeFilter(jobType)
    const hasVideoFilter = !!search || !!videoUUID || !!videoId || !!videoImportId

    if (state === 'failed' || state === 'cancelled' || hasVideoFilter) {
      const states = this.buildStateFilter(state)
      let total = 0
      for (const type of filteredJobTypes) {
        const queue = this.queues[type]
        if (!queue) continue
        const jobs = await queue.getJobs(states as Parameters<Queue['getJobs']>[0], 0, 10000, true)
        const wantCancelled = state === 'cancelled'
        const count = jobs.filter((j: Job) => {
          const isCancelled = typeof j.failedReason === 'string' &&
            j.failedReason.includes('Video was deleted - transcoding job cancelled')
          return wantCancelled ? isCancelled : !isCancelled
        })

        const filteredJobs = await this.filterJobsByVideoOptions(count, { search, videoUUID, videoId, videoImportId })
        const countTotal = filteredJobs.length
        total += countTotal
      }
      return total
    }

    const states = this.buildStateFilter(state)

    let total = 0

    for (const type of filteredJobTypes) {
      const queue = this.queues[type]
      if (queue === undefined) {
        logger.error('Unknown queue %s to count jobs.', type)
        continue
      }

      const counts = await queue.getJobCounts()

      for (const s of states) {
        total += counts[s]
      }
    }

    return total
  }

  private buildStateFilter (state?: JobState) {
    if (!state) return Array.from(jobStates)

    // Cancelled jobs are stored as 'failed' in BullMQ; we filter by failedReason in listForApi/count
    if (state === 'cancelled') return [ 'failed' ]

    const states = [ state ]

    // Include parent and prioritized if filtering on waiting
    if (state === 'waiting') {
      states.push('waiting-children')
      states.push('prioritized')
    }

    return states
  }

  private buildTypeFilter (jobType?: JobType) {
    if (!jobType) return jobTypes

    return jobTypes.filter(t => t === jobType)
  }

  private async filterJobsByVideoOptions (jobs: Job[], options: {
    search?: string
    videoUUID?: string
    videoId?: number
    videoImportId?: number
  }) {
    const { search, videoUUID, videoId, videoImportId } = options

    if (!search && !videoUUID && !videoId && !videoImportId) return jobs

    const trimmedSearch = search?.trim()
    const loweredSearch = trimmedSearch?.toLowerCase() ?? ''

    const filteredVideoUUIDs = new Set<string>()
    const filteredVideoIds = new Set<number>()

    if (videoUUID) {
      filteredVideoUUIDs.add(videoUUID)
    }

    if (videoId) {
      filteredVideoIds.add(videoId)
    }

    if (trimmedSearch) {
      const searchResults = await this.resolveVideoSearch(trimmedSearch)
      for (const uuid of searchResults.videoUUIDs) filteredVideoUUIDs.add(uuid)
      for (const id of searchResults.videoIds) filteredVideoIds.add(id)
    }

    const hasVideoMatches = filteredVideoUUIDs.size !== 0 || filteredVideoIds.size !== 0
    const hasSearch = !!trimmedSearch

    return jobs.filter(job => {
      if (!job?.data) return false

      if (hasSearch && String(job.id).toLowerCase().includes(loweredSearch)) return true

      if (!hasVideoMatches) return false

      const data = job.data as { videoUUID?: string, videoId?: number, videoImportId?: number }

      if (typeof data?.videoUUID === 'string' && filteredVideoUUIDs.has(data.videoUUID)) return true
      if (typeof data?.videoId === 'number' && filteredVideoIds.has(data.videoId)) return true
      if (typeof data?.videoImportId === 'number' && videoImportId && data.videoImportId === videoImportId) return true

      return false
    })
  }

  private async resolveVideoSearch (search: string) {
    const trimmedSearch = search?.trim()
    if (!trimmedSearch) return { videoUUIDs: new Set<string>(), videoIds: new Set<number>() }

    const uuidOr: string[] = []
    try {
      uuidOr.push(toCompleteUUID(trimmedSearch))
    } catch {
      // Ignore invalid UUIDs and keep title search
    }

    const where = {
      remote: false,
      [Op.or]: [
        { name: { [Op.iLike]: '%' + trimmedSearch + '%' } },
        ...(uuidOr.length !== 0 ? [ { uuid: { [Op.in]: uuidOr } } ] : [])
      ]
    }

    const videos = await VideoModel.findAll({
      attributes: [ 'id', 'uuid' ],
      where,
      limit: 200
    })

    return {
      videoUUIDs: new Set(videos.map(v => v.uuid)),
      videoIds: new Set(videos.map(v => v.id))
    }
  }

  async getStats () {
    const promises = jobTypes.map(async t => ({ jobType: t, counts: await this.queues[t].getJobCounts() }))

    return Promise.all(promises)
  }

  async getTranscodingProgressForVideo (videoUUID: string): Promise<number | null> {
    const progressByVideoUUID = await this.getTranscodingProgressSnapshot()

    return progressByVideoUUID.get(videoUUID) ?? null
  }

  private async getTranscodingProgressSnapshot () {
    const now = Date.now()
    if (this.transcodingProgressCache && this.transcodingProgressCache.expiresAt > now) {
      return this.transcodingProgressCache.values
    }

    const states: ('waiting' | 'delayed' | 'prioritized' | 'waiting-children' | 'active')[] = [
      'waiting',
      'delayed',
      'prioritized',
      'waiting-children',
      'active'
    ]
    const values = new Map<string, number>()

    const queue = this.queues['video-transcoding']
    if (queue) {
      const jobs = await queue.getJobs(states, 0, 10000, true)

      const progressesByUUID = new Map<string, number[]>()
      for (const job of jobs) {
        const uuid = (job.data as { videoUUID?: string }).videoUUID
        if (!uuid) continue

        if (!progressesByUUID.has(uuid)) progressesByUUID.set(uuid, [])

        if (typeof job.progress === 'number') {
          progressesByUUID.get(uuid).push(job.progress)
        }
      }

      for (const [ uuid, progresses ] of progressesByUUID) {
        values.set(
          uuid,
          progresses.length === 0
            ? 0
            : Math.round(progresses.reduce((a, b) => a + b, 0) / progresses.length)
        )
      }
    }

    const builderQueue = this.queues['transcoding-job-builder']
    if (builderQueue) {
      const builderJobs = await builderQueue.getJobs(states, 0, 10000, true)
      for (const job of builderJobs) {
        const uuid = (job.data as { videoUUID?: string }).videoUUID
        if (uuid && !values.has(uuid)) values.set(uuid, 0)
      }
    }

    this.transcodingProgressCache = {
      expiresAt: Date.now() + TRANSCODING_PROGRESS_CACHE_TTL_MS,
      values
    }

    return values
  }

  async listVideoUUIDsWithPendingTranscodingJobs (): Promise<Set<string>> {
    return this.listVideoUUIDsWithPendingVideoJobs([ 'transcoding-job-builder', 'video-transcoding' ])
  }

  async listVideoUUIDsWithPendingTranscriptionJobs (): Promise<Set<string>> {
    return this.listVideoUUIDsWithPendingVideoJobs([ 'video-transcription' ])
  }

  private async listVideoUUIDsWithPendingVideoJobs (queueNames: JobType[]): Promise<Set<string>> {
    const states = [ 'waiting', 'delayed', 'prioritized', 'waiting-children', 'active' ] as const
    const uuids = new Set<string>()

    for (const queueName of queueNames) {
      const queue = this.queues[queueName]
      if (!queue) continue

      for (const state of states) {
        const jobs = await queue.getJobs([ state ], 0, 10000, true)

        for (const job of jobs) {
          const videoUUID = (job.data as { videoUUID?: string })?.videoUUID
          if (videoUUID) uuids.add(videoUUID)
        }
      }
    }

    return uuids
  }

  /**
   * Remove all jobs relevant to a video (waiting, delayed, active).
   * Called when a video is deleted so jobs are gracefully cancelled.
   * Active jobs are attempted; if removal fails (e.g. job locked), the worker will exit and throw
   * "Video was deleted - transcoding job cancelled" so the job appears as failed with that reason.
   */
  async removeAllVideoJobsForVideo (videoUUID: string, videoId: number, videoImportId?: number): Promise<void> {
    const CANCELLED_REASON = 'Video was deleted - transcoding job cancelled'
    const states = [ 'waiting', 'delayed', 'prioritized', 'waiting-children', 'active' ] as const
    let removedCount = 0
    const identifiers: DeletedVideoJobIdentifiers = { videoUUID, videoId, videoImportId }

    const queueConfigs = listDeletedVideoJobTypes()
      .map(name => ({
        name,
        match: (data: any) => shouldRemoveDeletedVideoJob(name, data, identifiers)
      }))

    for (const { name: queueName, match } of queueConfigs) {
      const queue = this.queues[queueName]
      if (!queue) continue

      for (const state of states) {
        try {
          const jobs = await queue.getJobs([ state ], 0, 500, true)
          const matchingJobs = jobs.filter((j: Job) => match(j.data))

          for (const job of matchingJobs) {
            const removed = await this.removeJobForDeletedVideo({
              job,
              state,
              videoUUID,
              cancelledReason: CANCELLED_REASON
            })

            if (removed) removedCount++
          }
        } catch (err) {
          const errMessage = err instanceof Error ? err.message : String(err)
          if (!errMessage.includes('Could not find queue')) {
            logger.warn('Cannot list %s jobs for video %s removal.', queueName, videoUUID, { err })
          }
        }
      }
    }

    if (removedCount > 0) {
      logger.info(
        'Cancelled %d job(s) for deleted video %s (reason: %s).',
        removedCount,
        videoUUID,
        CANCELLED_REASON
      )
    }
  }

  private async removeJobForDeletedVideo (options: {
    job: Job
    state: JobState
    videoUUID: string
    cancelledReason: string
  }) {
    const { job, state, videoUUID, cancelledReason } = options

    try {
      await job.remove()
      return true
    } catch (err) {
      if (state !== 'active') {
        logger.warn('Cannot remove job %s for deleted video %s.', job.id, videoUUID, { err })
        return false
      }

      logger.debug(
        'Could not remove active job %s for video %s (worker may still be processing; it will fail with: %s).',
        job.id,
        videoUUID,
        cancelledReason
      )
      return false
    }
  }

  // ---------------------------------------------------------------------------

  private addRepeatableJobs () {
    this.queues['videos-views-stats'].add('job', {}, {
      repeat: REPEAT_JOBS['videos-views-stats'],

      ...this.buildJobRemovalOptions('videos-views-stats')
    }).catch(err => logger.error('Cannot add repeatable job.', { err }))

    if (CONFIG.FEDERATION.VIDEOS.CLEANUP_REMOTE_INTERACTIONS) {
      this.queues['activitypub-cleaner'].add('job', {}, {
        repeat: REPEAT_JOBS['activitypub-cleaner'],

        ...this.buildJobRemovalOptions('activitypub-cleaner')
      }).catch(err => logger.error('Cannot add repeatable job.', { err }))
    }
  }

  private getJobConcurrency (jobType: JobType) {
    if (jobType === 'video-transcoding') return CONFIG.TRANSCODING.CONCURRENCY
    if (jobType === 'video-import') return CONFIG.IMPORT.VIDEOS.CONCURRENCY
    if (jobType === 'move-to-object-storage') return CONFIG.OBJECT_STORAGE.CONCURRENCY
    if (jobType === 'move-to-file-system') return CONFIG.OBJECT_STORAGE.MOVE_TO_FILE_SYSTEM_CONCURRENCY
    if (jobType === 'federate-video') return CONFIG.FEDERATION.CONCURRENCY
    if (jobType === 'generate-video-storyboard') return CONFIG.STORYBOARDS.CONCURRENCY
    if (jobType === 'video-studio-edition') return CONFIG.VIDEO_STUDIO.CONCURRENCY
    if (jobType === 'video-transcription') return CONFIG.VIDEO_TRANSCRIPTION.CONCURRENCY
    if (jobType === 'video-file-import') return CONFIG.VIDEO_FILE.IMPORT.CONCURRENCY
    if (jobType === 'transcoding-job-builder') return CONFIG.TRANSCODING.CONCURRENCY

    return JOB_CONCURRENCY[jobType]
  }

  private buildJobRemovalOptions (queueName: string) {
    return {
      removeOnComplete: {
        // Wants seconds
        age: (JOB_REMOVAL_OPTIONS.SUCCESS[queueName] || JOB_REMOVAL_OPTIONS.SUCCESS.DEFAULT) / 1000,

        count: JOB_REMOVAL_OPTIONS.COUNT
      },
      removeOnFail: {
        // Wants seconds
        age: (JOB_REMOVAL_OPTIONS.FAILURE[queueName] || JOB_REMOVAL_OPTIONS.FAILURE.DEFAULT) / 1000,

        count: JOB_REMOVAL_OPTIONS.COUNT / 1000
      }
    }
  }

  static get Instance () {
    return this.instance || (this.instance = new this())
  }

  getQueues () {
    return this.queues
  }
}

// ---------------------------------------------------------------------------

export {
  JobQueue,
  jobTypes
}
