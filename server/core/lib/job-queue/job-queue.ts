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
  NotifyPayload,
  RefreshPayload,
  TranscodingJobBuilderPayload,
  VideoChannelImportPayload,
  VideoFileImportPayload,
  VideoImportPayload,
  VideoLiveEndingPayload,
  VideoRedundancyPayload,
  VideoStudioEditionPayload,
  VideoTranscodingPayload,
  VideoTranscriptionPayload
} from '@peertube/peertube-models'
import { jobStates } from '@server/helpers/custom-validators/jobs.js'
import { toCompleteUUID } from '@server/helpers/custom-validators/misc.js'
import { CONFIG, registerConfigChangedHandler } from '@server/initializers/config.js'
import { processVideoRedundancy } from '@server/lib/job-queue/handlers/video-redundancy.js'
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
import { processNotify } from './handlers/notify.js'
import { processTranscodingJobBuilder } from './handlers/transcoding-job-builder.js'
import { processVideoChannelImport } from './handlers/video-channel-import.js'
import { processVideoFileImport } from './handlers/video-file-import.js'
import { processVideoImport } from './handlers/video-import.js'
import { processVideoLiveEnding } from './handlers/video-live-ending.js'
import { processVideoStudioEdition } from './handlers/video-studio-edition.js'
import { processVideoTranscoding } from './handlers/video-transcoding.js'
import { processVideoTranscription } from './handlers/video-transcription.js'
import { processVideosViewsStats } from './handlers/video-views-stats.js'
import { Op } from 'sequelize'

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
  | { type: 'video-channel-import', payload: VideoChannelImportPayload }
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
  'notify': processNotify,
  'video-channel-import': processVideoChannelImport,
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
  'move-to-file-system': onMoveToFileSystemFailure
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
  'notify',
  'transcoding-job-builder',
  'video-channel-import',
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

class JobQueue {
  private static instance: JobQueue

  private workers: { [id in JobType]?: Worker } = {}
  private queues: { [id in JobType]?: Queue } = {}
  private queueEvents: { [id in JobType]?: QueueEvents } = {}

  private flowProducer: FlowProducer

  private initialized = false
  private jobRedisPrefix: string

  private constructor () {
  }

  init () {
    // Already initialized
    if (this.initialized === true) return
    this.initialized = true

    this.jobRedisPrefix = 'bull-' + WEBSERVER.HOST

    for (const handlerName of Object.keys(handlers)) {
      this.buildWorker(handlerName)
      this.buildQueue(handlerName)
      this.buildQueueEvent(handlerName)
    }

    this.flowProducer = new FlowProducer({
      connection: Redis.getRedisClientOptions('FlowProducer'),
      prefix: this.jobRedisPrefix
    })
    this.flowProducer.on('error', err => {
      logger.error('Error in flow producer', { err })
    })

    this.addRepeatableJobs()

    registerConfigChangedHandler(() => {
      for (const handlerName of Object.keys(handlers)) {
        this.workers[handlerName].concurrency = this.getJobConcurrency(handlerName)
      }
    })
  }

  private buildWorker (handlerName: JobType) {
    const workerOptions: WorkerOptions = {
      autorun: false,
      concurrency: this.getJobConcurrency(handlerName),
      prefix: this.jobRedisPrefix,
      connection: Redis.getRedisClientOptions('Worker'),
      maxStalledCount: 10,

      // Transcoding/transcription can be CPU-heavy and may briefly block lock renewals.
      // Use a longer lock duration to avoid false "stalled" loops where jobs bounce back to waiting.
      lockDuration: (handlerName === 'video-transcoding' || handlerName === 'video-transcription')
        ? 1000 * 60 * 10
        : 30000
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
      connection: Redis.getRedisClientOptions('Queue'),
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
    const queueEventsOptions: QueueEventsOptions = {
      autorun: false,
      connection: Redis.getRedisClientOptions('QueueEvent'),
      prefix: this.jobRedisPrefix
    }

    const queueEvents = new QueueEvents(handlerName, queueEventsOptions)
    queueEvents.on('error', err => {
      logger.error('Error in job queue events %s.', handlerName, { err })
    })

    this.queueEvents[handlerName] = queueEvents
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
          queueEvent.close()
        ])
      })

    return Promise.all(promises)
  }

  start () {
    const promises = Object.keys(this.workers)
      .map(handlerName => {
        const worker: Worker = this.workers[handlerName]
        const queueEvent: QueueEvents = this.queueEvents[handlerName]

        return Promise.all([
          worker.run(),
          queueEvent.run()
        ])
      })

    return Promise.all(promises)
  }

  async pause () {
    for (const handlerName of Object.keys(this.workers)) {
      const worker: Worker = this.workers[handlerName]

      await worker.pause()
    }
  }

  resume () {
    for (const handlerName of Object.keys(this.workers)) {
      const worker: Worker = this.workers[handlerName]

      worker.resume()
    }
  }

  // ---------------------------------------------------------------------------

  createJobAsync (options: CreateJobArgument & CreateJobOptions): void {
    this.createJob(options)
      .catch(err => logger.error('Cannot create job.', { err, options }))
  }

  createJob (options: CreateJobArgument & CreateJobOptions | undefined) {
    if (!options) return

    const queue: Queue = this.queues[options.type]
    if (queue === undefined) {
      logger.error('Unknown queue %s: cannot create job.', options.type)
      return
    }

    const jobOptions = this.buildJobOptions(options.type as JobType, pick(options, [ 'priority', 'delay' ]))

    return queue.add('job', options.payload, jobOptions)
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

    const newJob = await queue.add('job', job.data, this.buildJobOptions(jobType, { priority: job.opts.priority }))

    return { status: 'retried', newJobId: newJob.id }
  }

  createSequentialJobFlow (...jobs: ((CreateJobArgument & CreateJobOptions) | undefined)[]) {
    let lastJob: FlowJob

    logger.debug('Creating jobs in local job queue', { jobs })

    for (const job of jobs) {
      if (!job) continue

      lastJob = {
        ...this.buildJobFlowOption(job),

        children: lastJob
          ? [ lastJob ]
          : []
      }
    }

    return this.flowProducer.add(lastJob)
  }

  createJobWithChildren (parent: CreateJobArgument & CreateJobOptions, children: (CreateJobArgument & CreateJobOptions)[]) {
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

        ...this.buildJobOptions(job.type as JobType, pick(job, [ 'priority', 'delay', 'failParentOnFailure' ]))
      }
    }
  }

  private buildJobOptions (type: JobType, options: CreateJobOptions = {}): JobsOptions {
    return {
      backoff: { delay: 60 * 1000, type: 'exponential' },
      attempts: JOB_ATTEMPTS[type],
      priority: options.priority,
      delay: options.delay,

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
  }): Promise<Job[]> {
    const { state, start, count, asc, jobType, search, videoUUID } = options

    const states = this.buildStateFilter(state)
    const filteredJobTypes = this.buildTypeFilter(jobType)

    // When filtering failed/cancelled we over-fetch because we filter by failedReason
    const fetchLimit = search || videoUUID
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

    results = await this.filterJobsByVideoOptions(results, { search, videoUUID })

    results.sort((j1: any, j2: any) => {
      if (j1.timestamp < j2.timestamp) return -1
      else if (j1.timestamp === j2.timestamp) return 0

      return 1
    })

    if (asc === false) results.reverse()

    return results.slice(start, start + count)
  }

  async count (state: JobState, jobType?: JobType, search?: string, videoUUID?: string): Promise<number> {
    const filteredJobTypes = this.buildTypeFilter(jobType)
    const hasVideoFilter = !!search || !!videoUUID

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

        const filteredJobs = await this.filterJobsByVideoOptions(count, { search, videoUUID })
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
  }) {
    const { search, videoUUID } = options

    if (!search && !videoUUID) return jobs

    const trimmedSearch = search?.trim()
    const loweredSearch = trimmedSearch?.toLowerCase() ?? ''

    const filteredVideoUUIDs = new Set<string>()
    const filteredVideoIds = new Set<number>()

    if (videoUUID) {
      filteredVideoUUIDs.add(videoUUID)
    }

    if (trimmedSearch) {
      const searchResults = await this.resolveVideoSearch(trimmedSearch)
      for (const uuid of searchResults.videoUUIDs) filteredVideoUUIDs.add(uuid)
      for (const id of searchResults.videoIds) filteredVideoIds.add(id)
    }

    const hasVideoMatches = filteredVideoUUIDs.size !== 0 || filteredVideoIds.size !== 0
    const hasSearch = !!trimmedSearch

    return jobs.filter(job => {
      if (hasSearch && String(job.id).toLowerCase().includes(loweredSearch)) return true

      if (!hasVideoMatches) return false

      const data = job.data as { videoUUID?: string, videoId?: number }

      if (typeof data?.videoUUID === 'string' && filteredVideoUUIDs.has(data.videoUUID)) return true
      if (typeof data?.videoId === 'number' && filteredVideoIds.has(data.videoId)) return true

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
    const queue = this.queues['video-transcoding']
    if (!queue) return null

    const jobs = await queue.getJobs([ 'active' ], 0, 100, true)
    const matchingJobs = jobs.filter((j: Job) => (j.data as { videoUUID?: string }).videoUUID === videoUUID)
    if (matchingJobs.length === 0) return null

    const progresses = matchingJobs
      .map((j: Job) => j.progress)
      .filter((p: unknown): p is number => typeof p === 'number')

    if (progresses.length === 0) return 0
    return Math.round(progresses.reduce((a, b) => a + b, 0) / progresses.length)
  }

  async listVideoUUIDsWithPendingTranscodingJobs (): Promise<Set<string>> {
    const queueNames: JobType[] = [ 'transcoding-job-builder', 'video-transcoding' ]
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
  async removeAllVideoJobsForVideo (videoUUID: string, videoId: number): Promise<void> {
    const CANCELLED_REASON = 'Video was deleted - transcoding job cancelled'
    const states = [ 'waiting', 'delayed', 'prioritized', 'waiting-children', 'active' ] as const
    let removedCount = 0

    const queueConfigs: { name: JobType; match: (data: any) => boolean }[] = [
      { name: 'video-transcoding', match: (d) => d.videoUUID === videoUUID },
      { name: 'transcoding-job-builder', match: (d) => d.videoUUID === videoUUID },
      { name: 'move-to-object-storage', match: (d) => 'videoUUID' in d && d.videoUUID === videoUUID },
      { name: 'move-to-file-system', match: (d) => 'videoUUID' in d && d.videoUUID === videoUUID },
      { name: 'video-transcription', match: (d) => d.videoUUID === videoUUID },
      { name: 'generate-video-storyboard', match: (d) => d.videoUUID === videoUUID },
      { name: 'federate-video', match: (d) => d.videoUUID === videoUUID },
      { name: 'video-studio-edition', match: (d) => d.videoUUID === videoUUID },
      { name: 'manage-video-torrent', match: (d) => d.videoId === videoId }
    ]

    for (const { name: queueName, match } of queueConfigs) {
      const queue = this.queues[queueName]
      if (!queue) continue

      for (const state of states) {
        try {
          const jobs = await queue.getJobs([ state ], 0, 500, true)
          const matchingJobs = jobs.filter((j: Job) => match(j.data))

          for (const job of matchingJobs) {
            try {
              await job.remove()
              removedCount++
            } catch (err) {
              if (state === 'active') {
                logger.debug(
                  'Could not remove active job %s for video %s (worker may still be processing; it will fail with: %s).',
                  job.id,
                  videoUUID,
                  CANCELLED_REASON
                )
              } else {
                logger.warn('Cannot remove job %s for deleted video %s.', job.id, videoUUID, { err })
              }
            }
          }
        } catch (err) {
          if ((err as any)?.message?.includes('Could not find queue') !== true) {
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
