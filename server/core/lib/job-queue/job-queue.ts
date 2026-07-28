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
  ManageVideoTorrentPayload,
  MoveStoragePayload,
  MoveVideoFilePayload,
  MoveHLSPlaylistPayload,
  MoveThumbnailPayload,
  MoveCaptionPayload,
  NotifyPayload,
  RefreshPayload,
  RunnerJobState,
  RunnerJobType,
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
  getLocalStorageImportCapacity,
  getSharedLocalStorageImportCapacity,
  onLocalStorageImportCapacityAvailable,
  shouldDeferVideoImportForLocalStorage,
  startLocalStorageImportCapacityTracking,
  stopLocalStorageImportCapacityTracking
} from '../local-storage-import-admission.js'
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
import { VideoCaptionModel } from '../../models/video/video-caption.js'
import { VideoImportModel } from '../../models/video/video-import.js'
import { VideoJobInfoModel } from '../../models/video/video-job-info.js'
import { RunnerJobModel } from '../../models/runner/runner-job.js'
import { VideoStreamingPlaylistModel } from '../../models/video/video-streaming-playlist.js'
import {
  CLEANUP_LOCK_HEARTBEAT_MS,
  CLEANUP_LOCK_TTL_MS,
  LocalFileLease,
  LocalFileLeaseManager
} from '../local-file-lease-manager.js'
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
import { onVideoTranscodingFailure, processVideoTranscoding } from './handlers/video-transcoding.js'
import { onVideoTranscriptionFailure, processVideoTranscription } from './handlers/video-transcription.js'
import { processVideosViewsStats } from './handlers/video-views-stats.js'
import { Op } from 'sequelize'
import { Redis as IORedis } from 'ioredis'

const TRANSCODING_PROGRESS_CACHE_TTL_MS = 5000
const LOCAL_FILE_LEASE_HEARTBEAT_MS = 60 * 60 * 1000
const LOCAL_FILE_LEASE_RECONCILIATION_BATCH_SIZE = 1000
const VIDEO_PIPELINE_COUNTER_WATCHDOG_INTERVAL_MS = 60 * 60 * 1000
const VIDEO_IMPORT_LOCAL_STORAGE_CAPACITY_JOB_ID_PREFIX = 'video-import-local-storage-capacity-'
const LOCAL_STORAGE_IMPORT_ADMISSION_LOCK_ID = '__local-storage-import-admission__'
const LOCAL_STORAGE_IMPORT_RESERVATIONS_KEY_SUFFIX = 'local-storage-import-reservations'

class LocalStorageImportAdmissionLockUnavailableError extends Error {}

const LOCAL_FILE_CONSUMER_JOB_TYPES: JobType[] = [
  'video-import',
  'video-file-import',
  'transcoding-job-builder',
  'video-transcoding',
  'video-transcription',
  'generate-video-storyboard',
  'video-studio-edition',
  'manage-video-torrent',
  'video-live-ending',
  'move-to-object-storage',
  'move-to-file-system',
  'move-video-file-to-object-storage',
  'move-hls-playlist-to-object-storage',
  'move-thumbnail-to-object-storage',
  'move-caption-to-object-storage'
]

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
  localFileLeaseId?: string
  localFileLeaseVideoUUID?: string
}

type PeerTubeJobOptions = JobsOptions & {
  localFileLeaseId?: string
  localFileLeaseVideoUUID?: string
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
  'move-caption-to-object-storage': onMoveToObjectStorageFailure,
  'video-transcoding': onVideoTranscodingFailure,
  'video-transcription': onVideoTranscriptionFailure
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

const MAX_STORED_JOB_ERROR_MESSAGE_LENGTH = 8000
const MAX_STORED_JOB_ERROR_STACK_LENGTH = 16000

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
const VIDEO_PIPELINE_COUNTER_WATCHDOG_STATES = [ 'waiting', 'delayed', 'prioritized', 'waiting-children', 'active' ] as const
const VIDEO_PIPELINE_COUNTER_WATCHDOG_RUNNER_TYPES: RunnerJobType[] = [
  'vod-web-video-transcoding',
  'vod-hls-transcoding',
  'vod-audio-merge-transcoding',
  'video-studio-transcoding',
  'video-transcription'
]
const VIDEO_PIPELINE_COUNTER_WATCHDOG_RUNNER_STATES = [
  RunnerJobState.PENDING,
  RunnerJobState.PROCESSING,
  RunnerJobState.WAITING_FOR_PARENT_JOB,
  RunnerJobState.COMPLETING
]

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
  private transcodingProgressRefreshPromise?: Promise<Map<string, number>>
  private videoPipelineCounterWatchdogTimer?: NodeJS.Timeout
  private stopLocalStorageImportAdmissionListener?: () => void
  private stopLocalStorageImportAdmissionLockListener?: () => void
  private stopLocalStorageImportAdmissionRedisListener?: () => void
  private localStorageCapacityImportPromotionRunning = false
  private localStorageCapacityImportReleasePending = false
  private localStorageCapacityImportPromotionRetryOnRedisReady = false
  private localStorageCapacityImportPromotionRetryOnLockRelease = false
  private localStorageCapacityImportLockReleaseVersion = 0
  private readonly localStorageCapacityImportReservations = new Map<string, number>()

  private constructor () {
  }

  init () {
    // Already initialized
    if (this.initialized === true) return
    this.initialized = true

    this.jobRedisPrefix = 'bull-' + WEBSERVER.HOST
    this.sharedRedisClient = new IORedis(Redis.getRedisClientOptions('BullMQShared', { maxRetriesPerRequest: null }))
    this.stopLocalStorageImportAdmissionRedisListener = Redis.Instance.onConnected(() => {
      if (
        !this.localStorageCapacityImportPromotionRetryOnRedisReady &&
        !this.localStorageCapacityImportPromotionRetryOnLockRelease
      ) return

      this.localStorageCapacityImportPromotionRetryOnRedisReady = false
      this.localStorageCapacityImportPromotionRetryOnLockRelease = false
      this.localStorageCapacityImportReleasePending = true
      this.promoteLocalStorageCapacityVideoImports()
    })
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
      if (!disableWorkers) {
        for (const handlerName of Object.keys(handlers)) {
          const worker = this.workers[handlerName]
          if (!worker) continue

          worker.concurrency = this.getJobConcurrency(handlerName)
        }
      }

      // An admin update can enable object storage, increase the limit, or
      // reduce headroom. Reconfigure the event listener before evaluating
      // parked imports so a live enablement cannot leave them delayed forever.
      void this.configureLocalStorageImportAdmission()
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
      try {
        const timeout = JOB_TTL[handlerName]
        const p = handlers[handlerName](job)

        if (!timeout) {
          return p.catch(err => {
            throw sanitizeJobErrorForStorage(err)
          })
        }

        return timeoutPromise(p, timeout).catch(err => {
          throw sanitizeJobErrorForStorage(err)
        })
      } catch (err) {
        throw sanitizeJobErrorForStorage(err)
      }
    }

    const processor = async (jobArg: Job) => {
      const job = await Hooks.wrapObject(jobArg, 'filter:job-queue.process.params', { type: handlerName })

      const lease = await this.ensureLocalFileLeaseForJob(job, handlerName)
      const stopLeaseHeartbeat = this.startLocalFileLeaseHeartbeat(lease, handlerName, job.id)

      try {
        return await Hooks.wrapPromiseFun(handler, job, 'filter:job-queue.process.result')
      } finally {
        stopLeaseHeartbeat()
      }
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

      void this.handleJobFailure(job, handlerName, err)
    })

    worker.on('completed', job => {
      void this.releaseLocalFileLeaseForJob(job, handlerName)
      void this.onLocalStorageCapacityVideoImportSettled(handlerName, job)
    })

    worker.on('error', err => {
      logger.error('Error in job worker %s.', handlerName, { err })
    })

    this.workers[handlerName] = worker
  }

  private async handleJobFailure (job: Job, handlerName: JobType, err: Error) {
    try {
      const errorHandler = errorHandlers[handlerName]
      if (errorHandler) await errorHandler(job, err)
    } catch (errorHandlerErr) {
      logger.error('Cannot run error handler for job failure %d in queue %s.', job.id, handlerName, { err: errorHandlerErr })
    } finally {
      if (this.isFinalJobAttempt(job)) {
        await this.releaseLocalFileLeaseForJob(job, handlerName)
        await this.onLocalStorageCapacityVideoImportSettled(handlerName, job)
      }
    }
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
    if (this.videoPipelineCounterWatchdogTimer) {
      clearTimeout(this.videoPipelineCounterWatchdogTimer)
      this.videoPipelineCounterWatchdogTimer = undefined
    }

    this.stopLocalStorageImportAdmissionListener?.()
    this.stopLocalStorageImportAdmissionListener = undefined
    this.stopLocalStorageImportAdmissionLockListener?.()
    this.stopLocalStorageImportAdmissionLockListener = undefined
    this.stopLocalStorageImportAdmissionRedisListener?.()
    this.stopLocalStorageImportAdmissionRedisListener = undefined
    this.localStorageCapacityImportReleasePending = false
    this.localStorageCapacityImportPromotionRetryOnRedisReady = false
    this.localStorageCapacityImportPromotionRetryOnLockRelease = false
    this.localStorageCapacityImportReservations.clear()
    stopLocalStorageImportCapacityTracking()

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

  async start () {
    await this.reconcileLocalFileLeases()
    await this.configureLocalStorageImportAdmission()

    scheduleRetainedLocalFilesCleanup()
    this.scheduleVideoPipelineCounterWatchdog()

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

  async createJob (options: CreateJobArgument & CreateJobOptions | undefined) {
    if (!options) return

    if (VIDEO_PIPELINE_JOB_TYPES_ON_RESET_HOLD.has(options.type)) {
      return this.createVideoPipelineJobRespectingSystemResetHold(options)
    }

    const queue: Queue = this.queues[options.type]
    if (queue === undefined) {
      logger.error('Unknown queue %s: cannot create job.', options.type)
      return
    }

    return this.addJobToQueue(queue, options)
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

    return this.addJobToQueue(queue, options)
  }

  private async addJobToQueue (queue: Queue, options: CreateJobArgument & CreateJobOptions) {
    if (options.customJobId) {
      const existingJob = await queue.getJob(options.customJobId)
      if (existingJob) return existingJob
    }

    const prepared = await this.prepareLocalFileLease(options)
    const jobOptions = this.buildJobOptions(options.type as JobType, pick(prepared.options, [
      'priority',
      'delay',
      'customJobId',
      'localFileLeaseId',
      'localFileLeaseVideoUUID'
    ]))

    try {
      return await queue.add('job', prepared.options.payload, jobOptions)
    } catch (err) {
      // A concurrent creator may have won the custom job-id race after the
      // preflight lookup. Do not release its shared deterministic lease.
      if (options.customJobId) {
        const existingJob = await queue.getJob(options.customJobId).catch(() => undefined)
        if (existingJob) return existingJob
      }

      await prepared.lease?.release()

      throw err
    }
  }

  private async prepareLocalFileLease (options: CreateJobArgument & CreateJobOptions) {
    if (options.localFileLeaseId || !this.isLocalFileLeaseJobType(options.type)) {
      return { options, lease: undefined }
    }

    const videoUUID = await this.getJobVideoUUID(options)
    if (!videoUUID) return { options, lease: undefined }

    const localFileLeaseId = options.customJobId
      ? `job:${options.type}:${videoUUID}:${options.customJobId}`
      : LocalFileLeaseManager.Instance.createLeaseId(`job:${options.type}`)
    const lease = await LocalFileLeaseManager.Instance.acquire({
      videoUUID,
      leaseId: localFileLeaseId,
      persistent: true
    })

    if (!lease) throw new Error(`Cannot acquire local file lease before creating ${options.type} job for video ${videoUUID}`)

    return {
      options: { ...options, localFileLeaseId, localFileLeaseVideoUUID: videoUUID },
      lease
    }
  }

  private isLocalFileLeaseJobType (jobType: JobType) {
    return LOCAL_FILE_CONSUMER_JOB_TYPES.includes(jobType)
  }

  private async getJobVideoUUID (job: { payload: any, localFileLeaseVideoUUID?: string }): Promise<string | undefined> {
    const videoUUID = job.localFileLeaseVideoUUID ?? job.payload?.videoUUID

    if (typeof videoUUID === 'string' && videoUUID.length !== 0) return videoUUID

    const payload = job.payload as {
      captionId?: number
      videoImportId?: number
      videoId?: number
      streamingPlaylistId?: number
    }

    if (typeof payload?.captionId === 'number') {
      return (await VideoCaptionModel.loadWithVideo(payload.captionId))?.Video?.uuid
    }

    if (typeof payload?.videoImportId === 'number') {
      return (await VideoImportModel.loadAndPopulateVideo(payload.videoImportId))?.Video?.uuid
    }

    if (typeof payload?.videoId === 'number') {
      return (await VideoModel.load(payload.videoId))?.uuid
    }

    if (typeof payload?.streamingPlaylistId === 'number') {
      return (await VideoStreamingPlaylistModel.loadWithVideo(payload.streamingPlaylistId))?.Video?.uuid
    }

    return undefined
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
    let retryLease: LocalFileLease | undefined
    let localFileLeaseId: string | undefined
    let localFileLeaseVideoUUID: string | undefined

    if (this.isLocalFileLeaseJobType(jobType)) {
      const videoUUID = await this.getJobVideoUUID({
        payload: retry.data,
        localFileLeaseVideoUUID: this.getLocalFileLeaseVideoUUID(job)
      })
      if (videoUUID) {
        localFileLeaseVideoUUID = videoUUID
        localFileLeaseId = LocalFileLeaseManager.Instance.createLeaseId(`job:${jobType}`)
        retryLease = await LocalFileLeaseManager.Instance.acquire({
          videoUUID,
          leaseId: localFileLeaseId,
          persistent: true
        })
        if (!retryLease) throw new Error(`Cannot acquire local file lease before retrying ${jobType} job for video ${videoUUID}`)
      }
    }

    try {
      newJob = await queue.add(
        'job',
        retry.data,
        this.buildJobOptions(jobType, { priority: job.opts.priority, localFileLeaseId, localFileLeaseVideoUUID })
      )
    } catch (err) {
      await retryLease?.release()

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

  private registerLocalStorageImportAdmissionListener () {
    this.stopLocalStorageImportAdmissionListener?.()
    this.stopLocalStorageImportAdmissionListener = onLocalStorageImportCapacityAvailable(() => {
      this.localStorageCapacityImportReleasePending = true
      this.promoteLocalStorageCapacityVideoImports()
    })

    this.stopLocalStorageImportAdmissionLockListener?.()
    this.stopLocalStorageImportAdmissionLockListener = LocalFileLeaseManager.Instance.onCleanupLockReleased(lockId => {
      if (lockId !== LOCAL_STORAGE_IMPORT_ADMISSION_LOCK_ID) return

      this.localStorageCapacityImportLockReleaseVersion++

      // A fail-closed decision can be blocked by another PM2 process while
      // the disk is already below the resume threshold. The owner releasing
      // the distributed lock is the event that makes its delayed import
      // eligible for another decision; it is not a retry timer.
      this.localStorageCapacityImportPromotionRetryOnLockRelease = false
      this.localStorageCapacityImportPromotionRetryOnRedisReady = false
      this.localStorageCapacityImportReleasePending = true
      this.promoteLocalStorageCapacityVideoImports()
    })
  }

  private async configureLocalStorageImportAdmission () {
    if (!CONFIG.OBJECT_STORAGE.ENABLED) {
      this.stopLocalStorageImportAdmissionListener?.()
      this.stopLocalStorageImportAdmissionListener = undefined
      this.stopLocalStorageImportAdmissionLockListener?.()
      this.stopLocalStorageImportAdmissionLockListener = undefined
      this.localStorageCapacityImportPromotionRetryOnLockRelease = false
      stopLocalStorageImportCapacityTracking()
      this.localStorageCapacityImportReservations.clear()

      // A configuration change that disables object storage also disables the
      // disk admission gate. Release one held import; completion of that job
      // will release the next one without a timer loop.
      this.localStorageCapacityImportReleasePending = true
      this.promoteLocalStorageCapacityVideoImports()
      return
    }

    await startLocalStorageImportCapacityTracking()
    this.registerLocalStorageImportAdmissionListener()

    // Startup and a config change are state transitions, not retry polling.
    // They can make held imports eligible even if no file was just removed.
    this.localStorageCapacityImportReleasePending = true
    this.promoteLocalStorageCapacityVideoImports()
  }

  private async onLocalStorageCapacityVideoImportSettled (handlerName: JobType, job: Job) {
    if (handlerName !== 'video-import' || !this.isLocalStorageCapacityVideoImportJob(job)) return

    await this.releaseLocalStorageCapacityVideoImportReservation(job.id)

    // A promoted job can be stale, canceled, or fail before it changes local
    // media. Its terminal state is itself an event: evaluate the next held
    // import so the queue cannot remain parked for the one-year safety delay.
    this.localStorageCapacityImportReleasePending = true
    this.promoteLocalStorageCapacityVideoImports()
  }

  async hasLocalStorageCapacityVideoImports () {
    return !!await this.getFirstLocalStorageCapacityVideoImportJob([ 'waiting', 'delayed', 'prioritized', 'active' ])
  }

  async hasLocalStorageCapacityVideoImportForImport (videoImportId: number) {
    return !!await this.getFirstLocalStorageCapacityVideoImportJob(
      [ 'waiting', 'delayed', 'prioritized', 'active' ],
      job => (job.data as { videoImportId?: number }).videoImportId === videoImportId
    )
  }

  async reserveLocalStorageCapacityVideoImport (options: {
    jobId: string | number | undefined
    isPromotedDelayedImport: boolean
  }) {
    try {
      return await this.runLocalStorageCapacityImportAdmissionExclusive(async () => {
        const capacity = await getSharedLocalStorageImportCapacity()
        const jobId = String(options.jobId)
        const reservationBytes = Math.max(0, capacity.limitBytes - capacity.resumeUsageBytes)
        const reservations = await this.getLocalStorageCapacityImportReservations()
        const existingReservationBytes = reservations.get(jobId) ?? 0
        const otherReservationBytes = this.getLocalStorageCapacityImportReservationBytes(reservations) - existingReservationBytes
        const effectiveCapacity = {
          ...capacity,
          usageBytes: capacity.usageBytes + otherReservationBytes
        }
        const hasDelayedImports = options.isPromotedDelayedImport || await this.hasLocalStorageCapacityVideoImports()
        const shouldDefer = options.isPromotedDelayedImport
          ? this.shouldDeferPromotedLocalStorageCapacityImport(effectiveCapacity, otherReservationBytes)
          : hasDelayedImports || shouldDeferVideoImportForLocalStorage(effectiveCapacity, { hasDeferredImports: false })

        if (!shouldDefer && existingReservationBytes === 0) {
          await this.setLocalStorageCapacityImportReservation(jobId, reservationBytes)
        }

        return { capacity, shouldDefer, admissionUnavailable: false, retryOnLockRelease: false }
      })
    } catch (err) {
      // Admission is a safety gate. If the shared lock cannot be acquired,
      // park the import instead of letting multiple workers overcommit disk.
      logger.warn('Cannot acquire local-storage import admission lock. Deferring import.', { err, jobId: options.jobId })

      let capacity: Awaited<ReturnType<typeof getLocalStorageImportCapacity>>
      try {
        capacity = await getLocalStorageImportCapacity()
      } catch (capacityErr) {
        logger.warn('Cannot read local-storage import capacity after admission lock failure.', { err: capacityErr })
        capacity = this.buildFailClosedLocalStorageImportCapacity()
      }

      return {
        capacity,
        shouldDefer: true,
        admissionUnavailable: true,
        retryOnLockRelease: err instanceof LocalStorageImportAdmissionLockUnavailableError
      }
    }
  }

  async releaseLocalStorageCapacityVideoImportReservation (jobId: string | number | undefined) {
    // Direct import path notifications are serialized through the tracker. Do
    // not drop the admission reservation until their updated directory totals
    // have been observed by the next capacity read.
    try {
      await getLocalStorageImportCapacity()
    } catch (err) {
      // Keep the durable reservation and let the reconciliation path remove it
      // later. A Redis snapshot problem must not turn a completed import into
      // a failed/retried BullMQ job.
      logger.warn('Cannot refresh local-storage capacity before releasing import reservation.', { err, jobId })
      return
    }

    try {
      await this.runLocalStorageCapacityImportAdmissionExclusive(async () => {
        await this.deleteLocalStorageCapacityImportReservation(String(jobId))
      })
    } catch (err) {
      // A stale reservation is reconciled against BullMQ job state before the
      // next admission decision. Keeping it is safer than admitting work.
      logger.warn('Cannot release local-storage import admission reservation.', { err, jobId })
    }
  }

  private promoteLocalStorageCapacityVideoImports () {
    if (this.localStorageCapacityImportPromotionRunning) return

    this.localStorageCapacityImportPromotionRunning = true
    const run = this.promoteLocalStorageCapacityVideoImportsNow()

    run
      .catch(err => logger.warn('Cannot promote local-storage deferred video imports.', { err }))
      .finally(() => {
        this.localStorageCapacityImportPromotionRunning = false
        if (
          this.localStorageCapacityImportReleasePending &&
          !this.localStorageCapacityImportPromotionRetryOnRedisReady &&
          !this.localStorageCapacityImportPromotionRetryOnLockRelease
        ) {
          this.promoteLocalStorageCapacityVideoImports()
        }
      })
  }

  private async promoteLocalStorageCapacityVideoImportsNow () {
    const shouldPromote = this.localStorageCapacityImportReleasePending
    this.localStorageCapacityImportReleasePending = false
    if (!shouldPromote) return

    const queue = this.queues['video-import']
    if (!queue) return

    const deferredJob = await this.getFirstLocalStorageCapacityVideoImportJob([ 'delayed' ])
    if (!deferredJob) return

    if (CONFIG.OBJECT_STORAGE.ENABLED) {
      const lockReleaseVersion = this.localStorageCapacityImportLockReleaseVersion
      const { capacity, shouldDefer, admissionUnavailable, retryOnLockRelease } = await this.reserveLocalStorageCapacityVideoImport({
        jobId: deferredJob.id,
        isPromotedDelayedImport: true
      })
      if (shouldDefer) {
        if (admissionUnavailable) {
          // Keep the event latch armed. A Redis reconnect or the owner that
          // currently holds the admission lock will re-evaluate this job.
          this.localStorageCapacityImportReleasePending = true
          const lockWasReleasedDuringDecision =
            retryOnLockRelease && this.localStorageCapacityImportLockReleaseVersion !== lockReleaseVersion
          this.localStorageCapacityImportPromotionRetryOnLockRelease = retryOnLockRelease && !lockWasReleasedDuringDecision
          this.localStorageCapacityImportPromotionRetryOnRedisReady = !retryOnLockRelease
        }

        return
      }

      // Promote one job per local-file-removal event. The next removal event,
      // or the promoted job's terminal state, decides whether there is room
      // for another import; no timer/retry loop is involved.
      try {
        await deferredJob.promote()
        this.localStorageCapacityImportPromotionRetryOnRedisReady = false
      } catch (err) {
        await this.releaseLocalStorageCapacityVideoImportReservation(deferredJob.id)
        this.localStorageCapacityImportReleasePending = true
        this.localStorageCapacityImportPromotionRetryOnRedisReady = true
        throw err
      }

      logger.info(
        '[JOB_QUEUE] Promoted delayed import %s after local storage fell to %d/%d bytes (resume threshold: %d).',
        deferredJob.id,
        capacity.usageBytes,
        capacity.limitBytes,
        capacity.resumeUsageBytes
      )
      return
    }

    await deferredJob.promote()
    logger.info('[JOB_QUEUE] Promoted delayed import %s because object storage is disabled.', deferredJob.id)
  }

  private isLocalStorageCapacityVideoImportJob (job: Job) {
    return String(job.id).startsWith(VIDEO_IMPORT_LOCAL_STORAGE_CAPACITY_JOB_ID_PREFIX)
  }

  private getLocalStorageCapacityImportReservationBytes (reservations = this.localStorageCapacityImportReservations) {
    let total = 0
    for (const bytes of reservations.values()) total += bytes
    return total
  }

  private shouldDeferPromotedLocalStorageCapacityImport (
    capacity: Awaited<ReturnType<typeof getLocalStorageImportCapacity>>,
    otherReservationBytes: number
  ) {
    // The first release is allowed exactly at the resume threshold. Further
    // release/config events must not promote another import until the first
    // promotion's immutable admission reservation has been consumed or settled.
    if (otherReservationBytes > 0 && capacity.usageBytes >= capacity.resumeUsageBytes) return true

    return shouldDeferVideoImportForLocalStorage(capacity, { hasDeferredImports: true })
  }

  private async runLocalStorageCapacityImportAdmissionExclusive<T> (fn: () => Promise<T>) {
    if (!Redis.Instance.isConnected()) {
      throw new Error('Redis is unavailable for local-storage import admission')
    }

    const distributedLock = await LocalFileLeaseManager.Instance.acquireCleanupLock(
      LOCAL_STORAGE_IMPORT_ADMISSION_LOCK_ID,
      CLEANUP_LOCK_TTL_MS + CLEANUP_LOCK_HEARTBEAT_MS
    )
    if (!distributedLock) {
      throw new LocalStorageImportAdmissionLockUnavailableError('Cannot acquire the shared local-storage import admission lock')
    }

    let lockLost = false
    let refreshInFlight = false
    const refreshLock = () => {
      if (lockLost || refreshInFlight) return

      refreshInFlight = true
      void distributedLock.refresh()
        .then(refreshed => {
          if (refreshed) return

          lockLost = true
          logger.warn('Lost the shared local-storage import admission lock.')
        })
        .catch(err => {
          lockLost = true
          logger.warn('Cannot refresh the shared local-storage import admission lock.', { err })
        })
        .finally(() => {
          refreshInFlight = false
        })
    }
    const heartbeat = setInterval(refreshLock, CLEANUP_LOCK_HEARTBEAT_MS)
    heartbeat.unref?.()

    try {
      const result = await fn()
      // A final synchronous refresh closes the interval race just before the
      // caller acts on the decision. If the lock was lost, fail closed so the
      // import remains delayed rather than admitting against stale state.
      if (lockLost || !await distributedLock.refresh()) {
        throw new LocalStorageImportAdmissionLockUnavailableError('Lost the shared local-storage import admission lock')
      }

      return result
    } finally {
      clearInterval(heartbeat)
      await distributedLock.release()
    }
  }

  private async getLocalStorageCapacityImportReservations () {
    const client = Redis.Instance.isConnected() ? Redis.Instance.getClient() : undefined
    if (!client) return new Map(this.localStorageCapacityImportReservations)

    const key = this.buildLocalStorageCapacityImportReservationsKey()
    const rawReservations = await client.hgetall(key)
    const reservations = new Map<string, number>()
    const staleJobIds: string[] = []
    const queue = this.queues['video-import']

    for (const [ jobId, rawBytes ] of Object.entries(rawReservations)) {
      const bytes = Number(rawBytes)
      if (!Number.isFinite(bytes) || bytes < 0) {
        staleJobIds.push(jobId)
        continue
      }

      const job = await queue?.getJob(jobId)
      const state = job ? await job.getState() : undefined
      if (!job || state === 'completed' || state === 'failed' || state === 'unknown') {
        staleJobIds.push(jobId)
        continue
      }

      reservations.set(jobId, bytes)
    }

    if (staleJobIds.length !== 0) await client.hdel(key, ...staleJobIds)
    return reservations
  }

  private async setLocalStorageCapacityImportReservation (jobId: string, bytes: number) {
    this.localStorageCapacityImportReservations.set(jobId, bytes)

    const client = Redis.Instance.isConnected() ? Redis.Instance.getClient() : undefined
    if (!client) return

    await client.hset(this.buildLocalStorageCapacityImportReservationsKey(), jobId, String(bytes))
  }

  private async deleteLocalStorageCapacityImportReservation (jobId: string) {
    this.localStorageCapacityImportReservations.delete(jobId)

    const client = Redis.Instance.isConnected() ? Redis.Instance.getClient() : undefined
    if (!client) return

    await client.hdel(this.buildLocalStorageCapacityImportReservationsKey(), jobId)
  }

  private buildLocalStorageCapacityImportReservationsKey () {
    return Redis.Instance.getPrefix() + LOCAL_STORAGE_IMPORT_RESERVATIONS_KEY_SUFFIX
  }

  private buildFailClosedLocalStorageImportCapacity () {
    const limitBytes = Math.max(1, CONFIG.IMPORT.VIDEOS.LOCAL_STORAGE_LIMIT_GB) * 1024 ** 3
    const headroomBytes = Math.max(0, CONFIG.IMPORT.VIDEOS.LOCAL_STORAGE_FREE_SPACE_FOR_IMPORT_GB) * 1024 ** 3

    return {
      usageBytes: Number.MAX_SAFE_INTEGER,
      limitBytes,
      resumeUsageBytes: Math.max(0, limitBytes - headroomBytes)
    }
  }

  private async getFirstLocalStorageCapacityVideoImportJob (
    states: ('waiting' | 'delayed' | 'prioritized' | 'active')[],
    matches = (_job: Job) => true
  ) {
    const queue = this.queues['video-import']
    if (!queue) return undefined

    const pageSize = 1000
    for (let start = 0; ; start += pageSize) {
      const jobs = await queue.getJobs(states, start, start + pageSize - 1, true)
      const job = jobs.find(candidate => this.isLocalStorageCapacityVideoImportJob(candidate) && matches(candidate))
      if (job) return job
      if (jobs.length < pageSize) return undefined
    }
  }

  async hasPendingOrActiveLocalFileConsumerJob (videoUUID: string): Promise<boolean> {
    for (const jobType of LOCAL_FILE_CONSUMER_JOB_TYPES) {
      if (await this.hasPendingOrActiveJob(jobType, videoUUID)) return true
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

    const preparedJobs = await this.prepareLocalFileLeases(filteredJobs)
    let lastJob: FlowJob

    logger.debug('Creating jobs in local job queue', { jobs: preparedJobs.map(job => job.options) })

    for (const preparedJob of preparedJobs) {
      lastJob = {
        ...this.buildJobFlowOption(preparedJob.options),

        children: lastJob
          ? [ lastJob ]
          : []
      }
    }

    try {
      return await this.flowProducer.add(lastJob)
    } catch (err) {
      await this.releasePreparedJobLeases(preparedJobs)

      throw err
    }
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

    const preparedJobs = await this.prepareLocalFileLeases(flowJobs)
    const [ preparedParent, ...preparedChildren ] = preparedJobs

    try {
      return await this.flowProducer.add({
        ...this.buildJobFlowOption(preparedParent.options),

        children: preparedChildren.map(c => this.buildJobFlowOption(c.options))
      })
    } catch (err) {
      await this.releasePreparedJobLeases(preparedJobs)

      throw err
    }
  }

  private async releasePreparedJobLeases (preparedJobs: { lease?: LocalFileLease }[]) {
    await Promise.all(preparedJobs.map(job => job.lease?.release()))
  }

  private async prepareLocalFileLeases (jobs: (CreateJobArgument & CreateJobOptions)[]) {
    const preparedJobs: { options: CreateJobArgument & CreateJobOptions, lease?: LocalFileLease }[] = []

    try {
      for (const job of jobs) preparedJobs.push(await this.prepareLocalFileLease(job))
    } catch (err) {
      await this.releasePreparedJobLeases(preparedJobs)

      throw err
    }

    return preparedJobs
  }

  private buildJobFlowOption (job: CreateJobArgument & CreateJobOptions): FlowJob {
    return {
      name: 'job',
      data: job.payload,
      queueName: job.type,
      opts: {
        failParentOnFailure: true,

        ...this.buildJobOptions(job.type as JobType, pick(job, [
          'priority',
          'delay',
          'failParentOnFailure',
          'customJobId',
          'localFileLeaseId',
          'localFileLeaseVideoUUID'
        ]))
      }
    }
  }

  private buildJobOptions (type: JobType, options: CreateJobOptions = {}): PeerTubeJobOptions {
    return {
      backoff: { delay: 60 * 1000, type: 'exponential' },
      attempts: JOB_ATTEMPTS[type],
      priority: options.priority,
      delay: options.delay,
      jobId: options.customJobId,
      localFileLeaseId: options.localFileLeaseId,
      localFileLeaseVideoUUID: options.localFileLeaseVideoUUID,

      ...this.buildJobRemovalOptions(type)
    }
  }

  private async ensureLocalFileLeaseForJob (job: Job, jobType: JobType): Promise<LocalFileLease | undefined> {
    if (!this.isLocalFileLeaseJobType(jobType)) return undefined

    const videoUUID = await this.getJobVideoUUID({
      payload: job.data,
      localFileLeaseVideoUUID: this.getLocalFileLeaseVideoUUID(job)
    })
    if (!videoUUID) return undefined

    const leaseId = LocalFileLeaseManager.Instance.getLeaseId(job) ??
      LocalFileLeaseManager.Instance.buildLegacyJobLeaseId(jobType, job.id)
    const lease = await LocalFileLeaseManager.Instance.acquire({ videoUUID, leaseId, persistent: true })

    if (!lease) throw new Error(`Cannot acquire local file lease while processing ${jobType} job ${job.id}`)

    return lease
  }

  private async releaseLocalFileLeaseForJob (job: Job, jobType: JobType) {
    if (!this.isLocalFileLeaseJobType(jobType)) return

    const leaseId = LocalFileLeaseManager.Instance.getLeaseId(job) ??
      LocalFileLeaseManager.Instance.buildLegacyJobLeaseId(jobType, job.id)

    if (await LocalFileLeaseManager.Instance.releaseLeaseById(leaseId)) return

    const videoUUID = await this.getJobVideoUUID({
      payload: job.data,
      localFileLeaseVideoUUID: this.getLocalFileLeaseVideoUUID(job)
    })
    if (videoUUID) await LocalFileLeaseManager.Instance.releaseLeaseById(leaseId, videoUUID)
  }

  private startLocalFileLeaseHeartbeat (lease: LocalFileLease | undefined, jobType: JobType, jobId: string | number) {
    if (!lease) return () => {}

    const timer = setInterval(() => {
      lease.refresh()
        .then(refreshed => {
          if (!refreshed) {
            logger.warn('Cannot refresh local file lease while processing %s job %s.', jobType, jobId)
          }
        })
        .catch(err => logger.warn('Cannot refresh local file lease while processing job.', { err, jobType, jobId }))
    }, LOCAL_FILE_LEASE_HEARTBEAT_MS)
    timer.unref?.()

    return () => {
      clearInterval(timer)
      lease.deactivate?.()
    }
  }

  async releaseLocalFileLeaseForRemovedJob (job: Job, jobType?: JobType) {
    const resolvedJobType = jobType ?? job.queueName as JobType

    await this.releaseLocalFileLeaseForJob(job, resolvedJobType)
  }

  /**
   * Waiting and delayed jobs never reach their handler, so deleting one must
   * release the counter that was reserved when it was queued. Terminal jobs
   * are deliberately excluded: their handler/final-failure path owns their
   * accounting and a second decrement would hide real concurrent work.
   */
  async releaseVideoPipelineCounterForRemovedJob (options: {
    job: Job
    jobType?: JobType
    state: JobState | 'unknown'
  }) {
    const { job, state } = options
    const jobType = options.jobType ?? job.queueName as JobType
    if (!this.isQueuedCounterOwningState(state)) return

    const counterColumn = this.getVideoPipelineCounterColumn(jobType, job.data)
    if (!counterColumn) return

    const videoUUID = await this.getJobVideoUUID({
      payload: job.data,
      localFileLeaseVideoUUID: this.getLocalFileLeaseVideoUUID(job)
    })
    if (!videoUUID) return

    const pending = await VideoJobInfoModel.decrease(videoUUID, counterColumn)
    logger.info(
      '[JOB_QUEUE] Released %s counter for removed %s job %s of video %s. Remaining: %d.',
      counterColumn,
      jobType,
      job.id,
      videoUUID,
      pending
    )
  }

  private getLocalFileLeaseVideoUUID (job: { opts?: object }) {
    return (job.opts as { localFileLeaseVideoUUID?: string } | undefined)?.localFileLeaseVideoUUID
  }

  private isQueuedCounterOwningState (state: JobState | 'unknown') {
    return state === 'waiting' || state === 'delayed' || state === 'prioritized' || state === 'waiting-children' || state === 'paused'
  }

  private getVideoPipelineCounterColumn (jobType: JobType, data: unknown) {
    const payload = data as { isFollowUp?: boolean, cleanupMode?: string }

    if (jobType === 'video-transcoding') return 'pendingTranscode' as const
    if (jobType === 'video-transcription') return 'pendingTranscription' as const

    if (jobType === 'move-to-object-storage' || jobType === 'move-to-file-system') {
      return payload?.isFollowUp === true ? undefined : 'pendingMove' as const
    }

    if (jobType === 'move-video-file-to-object-storage' || jobType === 'move-thumbnail-to-object-storage') {
      return 'pendingMove' as const
    }

    if (jobType === 'move-hls-playlist-to-object-storage') {
      return payload?.cleanupMode === 'cleanup' ? undefined : 'pendingMove' as const
    }

    return undefined
  }

  async reconcileLocalFileLeasesNow () {
    await this.reconcileLocalFileLeases()
  }

  private isFinalJobAttempt (job: Job) {
    return (job.opts?.attempts ?? 1) <= job.attemptsMade
  }

  private async reconcileLocalFileLeases () {
    const reconciliationStartedAt = Date.now()
    const states: ('waiting' | 'delayed' | 'prioritized' | 'waiting-children' | 'active')[] = [
      'waiting',
      'delayed',
      'prioritized',
      'waiting-children',
      'active'
    ]
    const liveLeases = new Map<string, string>()

    for (const jobType of LOCAL_FILE_CONSUMER_JOB_TYPES) {
      const queue = this.queues[jobType]
      if (!queue) continue

      try {
        await this.collectLiveLocalFileLeases({ queue, jobType, states, liveLeases })
      } catch (err) {
        logger.warn('Cannot reconcile local file leases because queue %s could not be listed.', jobType, { err })

        return
      }
    }

    await LocalFileLeaseManager.Instance.reconcile(liveLeases, reconciliationStartedAt)
  }

  private async collectLiveLocalFileLeases (options: {
    queue: Queue
    jobType: JobType
    states: ('waiting' | 'delayed' | 'prioritized' | 'waiting-children' | 'active')[]
    liveLeases: Map<string, string>
  }) {
    const { queue, jobType, states, liveLeases } = options

    for (const state of states) {
      let start = 0

      while (true) {
        const jobs = await queue.getJobs([ state ], start, start + LOCAL_FILE_LEASE_RECONCILIATION_BATCH_SIZE - 1, true)
        for (const job of jobs) {
          await this.addLiveLocalFileLease({ job, jobType, liveLeases })
        }

        if (jobs.length < LOCAL_FILE_LEASE_RECONCILIATION_BATCH_SIZE) break

        start += jobs.length
      }
    }
  }

  private async addLiveLocalFileLease (options: {
    job: Job
    jobType: JobType
    liveLeases: Map<string, string>
  }) {
    const { job, jobType, liveLeases } = options
    const videoUUID = await this.getJobVideoUUID({
      payload: job.data,
      localFileLeaseVideoUUID: this.getLocalFileLeaseVideoUUID(job)
    })
    if (!videoUUID) return

    const leaseId = LocalFileLeaseManager.Instance.getLeaseId(job) ??
      LocalFileLeaseManager.Instance.buildLegacyJobLeaseId(jobType, job.id)
    liveLeases.set(leaseId, videoUUID)
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

    if (this.transcodingProgressCache) {
      void this.refreshTranscodingProgressSnapshot()
        .catch(err => logger.warn('Cannot refresh transcoding progress snapshot.', { err }))

      return this.transcodingProgressCache.values
    }

    return this.refreshTranscodingProgressSnapshot()
  }

  private async refreshTranscodingProgressSnapshot () {
    if (this.transcodingProgressRefreshPromise !== undefined) return this.transcodingProgressRefreshPromise

    const promise = this.buildTranscodingProgressSnapshot()
    this.transcodingProgressRefreshPromise = promise

    try {
      return await promise
    } finally {
      if (this.transcodingProgressRefreshPromise === promise) {
        this.transcodingProgressRefreshPromise = undefined
      }
    }
  }

  private async buildTranscodingProgressSnapshot () {

    const states: ('waiting' | 'delayed' | 'prioritized' | 'waiting-children' | 'active')[] = [
      'waiting',
      'delayed',
      'prioritized',
      'waiting-children',
      'active'
    ]
    const values = new Map<string, number>()

    const queue = this.queues['video-transcoding']
    const builderQueue = this.queues['transcoding-job-builder']
    const [ jobs, builderJobs ] = await Promise.all([
      queue ? queue.getJobs(states, 0, 10000, true) : Promise.resolve([]),
      builderQueue ? builderQueue.getJobs(states, 0, 10000, true) : Promise.resolve([])
    ])

    if (queue) {
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

    if (builderQueue) {
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

  async listVideoUUIDsWithPendingLocalFileConsumerJobs (): Promise<Set<string>> {
    return this.listVideoUUIDsWithPendingVideoJobs(LOCAL_FILE_CONSUMER_JOB_TYPES)
  }

  private async listVideoUUIDsWithPendingVideoJobs (queueNames: JobType[]): Promise<Set<string>> {
    const states: ('waiting' | 'delayed' | 'prioritized' | 'waiting-children' | 'active')[] = [
      'waiting',
      'delayed',
      'prioritized',
      'waiting-children',
      'active'
    ]
    const uuids = new Set<string>()

    const jobGroups = await Promise.all(queueNames.map(async queueName => {
      const queue = this.queues[queueName]
      if (!queue) return []

      return queue.getJobs(states, 0, 10000, true)
    }))

    for (const jobs of jobGroups) {
      for (const job of jobs) {
        const videoUUID = (job?.data as { videoUUID?: string })?.videoUUID
        if (videoUUID) uuids.add(videoUUID)
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
              jobType: queueName,
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
    jobType: JobType
    state: JobState
    videoUUID: string
    cancelledReason: string
  }) {
    const { job, jobType, state, videoUUID, cancelledReason } = options

    try {
      await job.remove()
      await this.releaseLocalFileLeaseForJob(job, jobType)

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

  private scheduleVideoPipelineCounterWatchdog () {
    if (process.env.NODE_ENV === 'test' || this.videoPipelineCounterWatchdogTimer) return

    const run = () => {
      this.videoPipelineCounterWatchdogTimer = undefined

      this.reconcileStaleVideoPipelineCounters()
        .catch(err => logger.warn('[JOB_QUEUE] Cannot reconcile stale video pipeline counters.', { err }))
        .finally(() => this.scheduleVideoPipelineCounterWatchdog())
    }

    this.videoPipelineCounterWatchdogTimer = setTimeout(run, VIDEO_PIPELINE_COUNTER_WATCHDOG_INTERVAL_MS)
    this.videoPipelineCounterWatchdogTimer.unref?.()
  }

  private async reconcileStaleVideoPipelineCounters () {
    const activeCounterOwners = await this.collectActiveVideoPipelineCounterOwners()
    const infos = await VideoJobInfoModel.findAll({
      include: [ {
        model: VideoModel.unscoped(),
        attributes: [ 'uuid' ],
        required: true
      } ],
      where: {
        [Op.or]: [
          { pendingMove: { [Op.gt]: 0 } },
          { pendingTranscode: { [Op.gt]: 0 } },
          { pendingTranscription: { [Op.gt]: 0 } }
        ]
      }
    })
    let repaired = 0

    for (const info of infos) {
      const videoUUID = (info.Video as { uuid?: string } | undefined)?.uuid
      if (!videoUUID) continue
      if (await LocalFileLeaseManager.Instance.hasActiveLeases(videoUUID)) continue

      const activeColumns = activeCounterOwners.get(videoUUID) ?? new Set<string>()
      const expected = {
        pendingMove: info.pendingMove,
        pendingTranscode: info.pendingTranscode,
        pendingTranscription: info.pendingTranscription
      }
      const next = {
        pendingMove: activeColumns.has('pendingMove') ? expected.pendingMove : 0,
        pendingTranscode: activeColumns.has('pendingTranscode') ? expected.pendingTranscode : 0,
        pendingTranscription: activeColumns.has('pendingTranscription') ? expected.pendingTranscription : 0
      }
      const amount =
        (expected.pendingMove - next.pendingMove) +
        (expected.pendingTranscode - next.pendingTranscode) +
        (expected.pendingTranscription - next.pendingTranscription)
      if (amount === 0) continue

      const replaced = await VideoJobInfoModel.replaceCountersIfUnchanged({ videoUUID, expected, next })
      if (replaced) repaired += amount
    }

    if (repaired !== 0) {
      logger.warn('[JOB_QUEUE] Counter watchdog cleared %d stale video pipeline counter(s).', repaired)
    }
  }

  private async collectActiveVideoPipelineCounterOwners () {
    const owners = new Map<string, Set<string>>()
    const addOwner = (videoUUID: string, column: string) => {
      if (!owners.has(videoUUID)) owners.set(videoUUID, new Set())
      owners.get(videoUUID).add(column)
    }
    const jobTypes: JobType[] = [
      'transcoding-job-builder',
      'video-transcoding',
      'video-transcription',
      'move-to-object-storage',
      'move-to-file-system',
      'move-video-file-to-object-storage',
      'move-hls-playlist-to-object-storage',
      'move-thumbnail-to-object-storage'
    ]

    for (const jobType of jobTypes) {
      const queue = this.queues[jobType]
      if (!queue) continue

      for (const state of VIDEO_PIPELINE_COUNTER_WATCHDOG_STATES) {
        for (let start = 0; ; start += LOCAL_FILE_LEASE_RECONCILIATION_BATCH_SIZE) {
          const jobs = await queue.getJobs([ state ], start, start + LOCAL_FILE_LEASE_RECONCILIATION_BATCH_SIZE - 1, true)
          for (const job of jobs) {
            const videoUUID = (job.data as { videoUUID?: unknown })?.videoUUID
            if (typeof videoUUID !== 'string') continue

            if (jobType === 'transcoding-job-builder' || jobType === 'video-transcoding') {
              addOwner(videoUUID, 'pendingTranscode')
              continue
            }
            if (jobType === 'video-transcription') {
              addOwner(videoUUID, 'pendingTranscription')
              continue
            }
            if (this.getVideoPipelineCounterColumn(jobType, job.data) === 'pendingMove') {
              addOwner(videoUUID, 'pendingMove')
            }
          }

          if (jobs.length < LOCAL_FILE_LEASE_RECONCILIATION_BATCH_SIZE) break
        }
      }
    }

    const runnerJobs = await RunnerJobModel.findAll({
      attributes: [ 'type', 'privatePayload' ],
      where: {
        type: { [Op.in]: VIDEO_PIPELINE_COUNTER_WATCHDOG_RUNNER_TYPES },
        state: { [Op.in]: VIDEO_PIPELINE_COUNTER_WATCHDOG_RUNNER_STATES }
      }
    })
    for (const runnerJob of runnerJobs) {
      const videoUUID = (runnerJob.privatePayload as { videoUUID?: unknown })?.videoUUID
      if (typeof videoUUID !== 'string') continue

      addOwner(videoUUID, runnerJob.type === 'video-transcription' ? 'pendingTranscription' : 'pendingTranscode')
    }

    return owners
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
    if (
      jobType === 'move-to-object-storage' ||
      jobType === 'move-video-file-to-object-storage' ||
      jobType === 'move-hls-playlist-to-object-storage' ||
      jobType === 'move-thumbnail-to-object-storage' ||
      jobType === 'move-caption-to-object-storage'
    ) {
      return CONFIG.OBJECT_STORAGE.CONCURRENCY
    }
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

function sanitizeJobErrorForStorage (err: unknown) {
  const error = err instanceof Error
    ? err
    : new Error(typeof err === 'string' ? err : JSON.stringify(err))

  if (error.message.length > MAX_STORED_JOB_ERROR_MESSAGE_LENGTH) {
    const originalLength = error.message.length
    error.message = error.message.slice(0, MAX_STORED_JOB_ERROR_MESSAGE_LENGTH) +
      `... [truncated ${originalLength - MAX_STORED_JOB_ERROR_MESSAGE_LENGTH} chars from job error]`
  }

  if (error.stack && error.stack.length > MAX_STORED_JOB_ERROR_STACK_LENGTH) {
    error.stack = error.stack.slice(0, MAX_STORED_JOB_ERROR_STACK_LENGTH) +
      `... [truncated ${error.stack.length - MAX_STORED_JOB_ERROR_STACK_LENGTH} chars from job stack]`
  }

  return error
}

// ---------------------------------------------------------------------------

export {
  JobQueue,
  jobTypes
}
