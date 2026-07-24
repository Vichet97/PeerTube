import {
  FileStorage,
  JobType,
  RunnerJobState,
  RunnerJobType,
  VideoImportState,
  type VideoImportStateType,
  VideoState,
  VideoStateType
} from '@peertube/peertube-models'
import { Job as BullJob } from 'bullmq'
import { stat } from 'fs/promises'
import { pathExists } from 'fs-extra/esm'
import { Op } from 'sequelize'
import { logger, loggerTagsFactory } from '@server/helpers/logger.js'
import { CONFIG } from '@server/initializers/config.js'
import { sequelizeTypescript } from '@server/initializers/database.js'
import { JobQueue } from '@server/lib/job-queue/index.js'
import {
  generateHLSObjectStorageKey,
  generateWebVideoObjectStorageKey,
  getObjectStorageFileSize,
  type BucketInfo
} from '@server/lib/object-storage/index.js'
import { getHLSResolutionPlaylistFilename } from '@server/lib/paths.js'
import { Redis } from '@server/lib/redis.js'
import { moveToExternalStorageState, moveToFileSystemState, moveToNextState } from '@server/lib/video-state.js'
import { VideoPathManager } from '@server/lib/video-path-manager.js'
import { LocalFileLeaseManager } from '@server/lib/local-file-lease-manager.js'
import { RunnerJobModel } from '@server/models/runner/runner-job.js'
import { VideoImportModel } from '@server/models/video/video-import.js'
import { VideoJobInfoColumnType, VideoJobInfoModel } from '@server/models/video/video-job-info.js'
import { VideoModel } from '@server/models/video/video.js'
import { MVideoWithAllFiles } from '@server/types/models/index.js'

const lTags = loggerTagsFactory('video-pipeline-reconciliation')
const JOB_SCAN_BATCH_SIZE = 1000
const VIDEO_RECONCILIATION_BATCH_SIZE = 3

const LIVE_JOB_STATES = [ 'waiting', 'delayed', 'prioritized', 'waiting-children', 'active' ] as const
const PIPELINE_JOB_TYPES: JobType[] = [
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
const ACTIVE_RUNNER_STATES = [
  RunnerJobState.PENDING,
  RunnerJobState.PROCESSING,
  RunnerJobState.WAITING_FOR_PARENT_JOB,
  RunnerJobState.COMPLETING
] as const
const VIDEO_PIPELINE_RUNNER_TYPES: RunnerJobType[] = [
  'vod-web-video-transcoding',
  'vod-hls-transcoding',
  'vod-audio-merge-transcoding',
  'video-studio-transcoding',
  'video-transcription',
  'generate-video-storyboard'
]

type VideoJobRef = {
  job: BullJob
  jobType: JobType
}

type VideoJobIndex = {
  byVideoUUID: Map<string, VideoJobRef[]>
  byVideoId: Map<number, VideoJobRef[]>
  byVideoImportId: Map<number, VideoJobRef[]>
}

type MediaVerification = {
  hasPlayableRecord: boolean
  hasAvailablePlayableMedia: boolean
  hasAvailableLocalPlayableMedia: boolean
  hasAvailableObjectStoragePlayableMedia: boolean
  hasObjectStoragePlayableRecords: boolean
  missing: string[]
  objectStorageErrors: string[]
}

export type VideoPipelineReconciliationResult = {
  videosChecked: number
  videosWithActiveWork: number
  videosSkippedForActiveLease: number
  countersCleared: number
  counterRaceSkips: number
  jobsRecreated: number
  videosPublished: number
  videosFailed: number
  importsFailed: number
  failedJobsRemoved: number
  objectStorageFilesChecked: number
  objectStorageVerificationErrors: number
  videosAwaitingObjectStorageVerification: number
  completedRemoteVideos: number
}

export type VideoPipelineReconciliationProgress = VideoPipelineReconciliationResult & {
  currentPhase: string
}

export async function runVideoPipelineReconciliation (options: {
  onProgress?: (progress: VideoPipelineReconciliationProgress) => void | Promise<void>
} = {}): Promise<VideoPipelineReconciliationResult> {
  const ids = await VideoModel.listLocalIds()
  const index = await buildLiveVideoJobIndex()
  const activeRunnerVideoUUIDs = await listActiveRunnerVideoUUIDs()
  const result: VideoPipelineReconciliationResult = {
    videosChecked: 0,
    videosWithActiveWork: 0,
    videosSkippedForActiveLease: 0,
    countersCleared: 0,
    counterRaceSkips: 0,
    jobsRecreated: 0,
    videosPublished: 0,
    videosFailed: 0,
    importsFailed: 0,
    failedJobsRemoved: 0,
    objectStorageFilesChecked: 0,
    objectStorageVerificationErrors: 0,
    videosAwaitingObjectStorageVerification: 0,
    completedRemoteVideos: 0
  }

  await emitProgress(options.onProgress, result, 'building reconciliation snapshot')

  for (let start = 0; start < ids.length; start += VIDEO_RECONCILIATION_BATCH_SIZE) {
    const batch = ids.slice(start, start + VIDEO_RECONCILIATION_BATCH_SIZE)
    await Promise.all(batch.map(id => reconcileVideo({ id, index, activeRunnerVideoUUIDs, result })))

    await emitProgress(options.onProgress, result, `checked ${Math.min(start + batch.length, ids.length)}/${ids.length} videos`)
  }

  logger.info('[VIDEO_RECONCILIATION] Completed safe video pipeline reconciliation.', result)

  return result
}

// ---------------------------------------------------------------------------

async function reconcileVideo (options: {
  id: number
  index: VideoJobIndex
  activeRunnerVideoUUIDs: Set<string>
  result: VideoPipelineReconciliationResult
}) {
  const { id, index, activeRunnerVideoUUIDs, result } = options
  const video = await VideoModel.loadWithFiles(id)
  if (!video || video.isLive) return

  result.videosChecked++

  const videoImport = await VideoImportModel.unscoped().findOne({ where: { videoId: video.id } })
  const liveJobs = getVideoJobRefs(index, video.uuid, video.id, videoImport?.id)
  const hasLiveRunnerJob = activeRunnerVideoUUIDs.has(video.uuid)

  if (liveJobs.length !== 0 || hasLiveRunnerJob) {
    result.videosWithActiveWork++
    return
  }

  const info = await VideoJobInfoModel.load(video.id)
  const isIncomplete = isIncompleteState(video.state)
  const hasStaleCounters = !!info && hasPositiveCounter(info)
  if (!isIncomplete && !hasStaleCounters) return

  if (await LocalFileLeaseManager.Instance.hasActiveLeases(video.uuid)) {
    result.videosSkippedForActiveLease++
    return
  }

  const cleared = await clearCountersFromStableSnapshot(video.uuid, info)
  if (cleared === undefined) {
    result.counterRaceSkips++
    return
  }
  result.countersCleared += cleared

  if (video.state === VideoState.TO_IMPORT) {
    await failImportAndVideo({
      video,
      videoImport,
      videoState: VideoState.TO_IMPORT_FAILED,
      reason: 'Video import has no live BullMQ job, runner job, or local-file lease.'
    })
    result.videosFailed++
    if (videoImport && isIncompleteImportState(videoImport.state)) result.importsFailed++
    result.failedJobsRemoved += await removeFailedVideoJobs(video.uuid, video.id, videoImport?.id)
    return
  }

  const media = await verifyPlayableMedia(video)
  result.objectStorageFilesChecked += media.checkedObjectStorageFiles
  result.objectStorageVerificationErrors += media.objectStorageErrors.length

  if (media.objectStorageErrors.length !== 0) {
    // A transport/auth error is not proof that a remote file is missing. Keep
    // the state untouched so a later run can make the decision safely.
    result.videosAwaitingObjectStorageVerification++
    return
  }

  if (video.state === VideoState.TO_TRANSCODE) {
    await reconcileStaleTranscoding({ video, videoImport, media, result })
    return
  }

  if (video.state === VideoState.TO_MOVE_TO_EXTERNAL_STORAGE || video.state === VideoState.TO_MOVE_TO_FILE_SYSTEM) {
    await reconcileStaleMove({ video, videoImport, media, result })
    return
  }

  if (video.state === VideoState.PUBLISHED) {
    if (!media.hasAvailablePlayableMedia) {
      await failImportAndVideo({
        video,
        videoImport,
        videoState: VideoState.TO_MOVE_TO_EXTERNAL_STORAGE_FAILED,
        reason:
          'Published video has no verified playable local or object-storage media: ' +
          (media.missing.slice(0, 5).join(', ') || 'no media records')
      })
      result.videosFailed++
      if (videoImport && isIncompleteImportState(videoImport.state)) result.importsFailed++
      result.failedJobsRemoved += await removeFailedVideoJobs(video.uuid, video.id, videoImport?.id)
      return
    }

    if (media.hasAvailableObjectStoragePlayableMedia) result.completedRemoteVideos++
  }
}

async function reconcileStaleTranscoding (options: {
  video: MVideoWithAllFiles
  videoImport: VideoImportModel | null
  media: MediaVerification & { checkedObjectStorageFiles: number }
  result: VideoPipelineReconciliationResult
}) {
  const { video, videoImport, media, result } = options
  if (!media.hasAvailableLocalPlayableMedia) {
    await failImportAndVideo({
      video,
      videoImport,
      videoState: VideoState.TRANSCODING_FAILED,
      reason:
        'Stale transcoding state has no usable local input media: ' +
        (media.missing.slice(0, 5).join(', ') || 'no local playable media')
    })
    result.videosFailed++
    if (videoImport && isIncompleteImportState(videoImport.state)) result.importsFailed++
    result.failedJobsRemoved += await removeFailedVideoJobs(video.uuid, video.id, videoImport?.id)
    return
  }

  if (CONFIG.TRANSCODING.ENABLED !== true) {
    await moveToNextState({ video: { uuid: video.uuid }, isNewVideo: false })
    result.videosPublished++
    return
  }

  if (await Redis.Instance.isVideoPipelineSystemResetHoldSet()) return

  const job = await JobQueue.Instance.createJob({
    type: 'transcoding-job-builder',
    // BullMQ reserves ':' for its internal Redis key format, so custom IDs
    // must use a safe separator.
    customJobId: `reconcile-transcoding-${video.uuid}`,
    payload: {
      videoUUID: video.uuid,
      optimizeJob: { isNewVideo: false }
    }
  })

  if (job) result.jobsRecreated++
}

async function reconcileStaleMove (options: {
  video: MVideoWithAllFiles
  videoImport: VideoImportModel | null
  media: MediaVerification & { checkedObjectStorageFiles: number }
  result: VideoPipelineReconciliationResult
}) {
  const { video, videoImport, media, result } = options
  const isObjectStorageMove = video.state === VideoState.TO_MOVE_TO_EXTERNAL_STORAGE

  if (!media.hasAvailablePlayableMedia) {
    await failImportAndVideo({
      video,
      videoImport,
      videoState: isObjectStorageMove
        ? VideoState.TO_MOVE_TO_EXTERNAL_STORAGE_FAILED
        : VideoState.TO_MOVE_TO_FILE_SYSTEM_FAILED,
      reason: `Stale storage-move state has no verified playable media: ${media.missing.slice(0, 5).join(', ') || 'no media records'}`
    })
    result.videosFailed++
    if (videoImport && isIncompleteImportState(videoImport.state)) result.importsFailed++
    result.failedJobsRemoved += await removeFailedVideoJobs(video.uuid, video.id, videoImport?.id)
    return
  }

  if (isObjectStorageMove && media.hasAvailableLocalPlayableMedia && CONFIG.OBJECT_STORAGE.ENABLED) {
    if (await Redis.Instance.isVideoPipelineSystemResetHoldSet()) return

    const recreated = await sequelizeTypescript.transaction(async transaction => {
      const freshVideo = await VideoModel.loadFull(video.uuid, transaction)
      if (!freshVideo || freshVideo.state !== VideoState.TO_MOVE_TO_EXTERNAL_STORAGE) return false

      return moveToExternalStorageState({ video: freshVideo, isNewVideo: false, transaction })
    })
    if (recreated) result.jobsRecreated++
    return
  }

  if (!isObjectStorageMove && media.hasObjectStoragePlayableRecords) {
    if (await Redis.Instance.isVideoPipelineSystemResetHoldSet()) return

    const recreated = await sequelizeTypescript.transaction(async transaction => {
      const freshVideo = await VideoModel.loadFull(video.uuid, transaction)
      if (!freshVideo || freshVideo.state !== VideoState.TO_MOVE_TO_FILE_SYSTEM) return false

      return moveToFileSystemState({ video: freshVideo, isNewVideo: false, transaction })
    })
    if (recreated) result.jobsRecreated++
    return
  }

  await moveToNextState({ video: { uuid: video.uuid }, isNewVideo: false })
  result.videosPublished++
  if (media.hasAvailableObjectStoragePlayableMedia) result.completedRemoteVideos++
}

async function clearCountersFromStableSnapshot (videoUUID: string, info: VideoJobInfoModel | null) {
  if (!info || !hasPositiveCounter(info)) return 0

  const expected = getCounters(info)
  const replaced = await VideoJobInfoModel.replaceCountersIfUnchanged({
    videoUUID,
    expected,
    next: {
      pendingMove: 0,
      pendingTranscode: 0,
      pendingTranscription: 0
    }
  })
  if (!replaced) return undefined

  return expected.pendingMove + expected.pendingTranscode + expected.pendingTranscription
}

async function failImportAndVideo (options: {
  video: MVideoWithAllFiles
  videoImport: VideoImportModel | null
  videoState: VideoStateType
  reason: string
}) {
  const { video, videoImport, videoState, reason } = options
  const shouldFailImport = videoImport && isIncompleteImportState(videoImport.state)

  if (shouldFailImport) {
    videoImport.state = VideoImportState.FAILED
    videoImport.progress = null
    videoImport.error = reason
    await videoImport.save()
  }

  if (video.state !== videoState || video.waitTranscoding !== false) {
    video.state = videoState
    video.waitTranscoding = false
    await video.save()
  }

  logger.warn(
    '[VIDEO_RECONCILIATION] Marked video %s as failed after verified stale pipeline state: %s',
    video.uuid,
    reason,
    lTags(video.uuid)
  )
}

// ---------------------------------------------------------------------------

async function verifyPlayableMedia (video: MVideoWithAllFiles): Promise<MediaVerification & { checkedObjectStorageFiles: number }> {
  const result: MediaVerification & { checkedObjectStorageFiles: number } = {
    hasPlayableRecord: false,
    hasAvailablePlayableMedia: false,
    hasAvailableLocalPlayableMedia: false,
    hasAvailableObjectStoragePlayableMedia: false,
    hasObjectStoragePlayableRecords: false,
    missing: [],
    objectStorageErrors: [],
    checkedObjectStorageFiles: 0
  }

  const checkLocal = async (label: string, path: string) => {
    result.hasPlayableRecord = true
    try {
      if (!await pathExists(path) || (await stat(path)).size <= 0) {
        result.missing.push(label)
        return
      }

      result.hasAvailablePlayableMedia = true
      result.hasAvailableLocalPlayableMedia = true
    } catch {
      result.missing.push(label)
    }
  }

  const checkObjectStorage = async (label: string, key: string, bucketInfo: BucketInfo) => {
    result.hasPlayableRecord = true
    result.hasObjectStoragePlayableRecords = true
    result.checkedObjectStorageFiles++

    if (CONFIG.OBJECT_STORAGE.ENABLED !== true) {
      result.objectStorageErrors.push(`${label}: object storage is disabled`)
      return
    }

    try {
      const size = await getObjectStorageFileSize({ key, bucketInfo })
      if (typeof size !== 'number' || size <= 0) {
        result.missing.push(label)
        return
      }

      result.hasAvailablePlayableMedia = true
      result.hasAvailableObjectStoragePlayableMedia = true
    } catch (err) {
      if (isObjectStorageNotFound(err)) {
        result.missing.push(label)
        return
      }

      result.objectStorageErrors.push(`${label}: ${getErrorMessage(err)}`)
    }
  }

  for (const file of video.VideoFiles || []) {
    if (file.storage === FileStorage.FILE_SYSTEM) {
      await checkLocal(`web video file ${file.id}`, VideoPathManager.Instance.getFSVideoFileOutputPath(video, file))
    } else if (file.storage === FileStorage.OBJECT_STORAGE) {
      await checkObjectStorage(
        `web video file ${file.id}`,
        generateWebVideoObjectStorageKey(file.filename),
        CONFIG.OBJECT_STORAGE.WEB_VIDEOS
      )
    }
  }

  for (const playlist of video.VideoStreamingPlaylists || []) {
    if (playlist.storage === FileStorage.FILE_SYSTEM) {
      await checkLocal(
        `HLS master playlist ${playlist.id}`,
        VideoPathManager.Instance.getFSHLSOutputPath(video, playlist.playlistFilename)
      )
    } else if (playlist.storage === FileStorage.OBJECT_STORAGE) {
      await checkObjectStorage(
        `HLS master playlist ${playlist.id}`,
        generateHLSObjectStorageKey(video, playlist.playlistFilename),
        CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS
      )
    }

    for (const file of playlist.VideoFiles || []) {
      if (file.storage === FileStorage.FILE_SYSTEM) {
        await checkLocal(`HLS media file ${file.id}`, VideoPathManager.Instance.getFSHLSOutputPath(video, file.filename))
        await checkLocal(
          `HLS resolution playlist ${file.id}`,
          VideoPathManager.Instance.getFSHLSOutputPath(video, getHLSResolutionPlaylistFilename(file.filename))
        )
      } else if (file.storage === FileStorage.OBJECT_STORAGE) {
        await checkObjectStorage(
          `HLS media file ${file.id}`,
          generateHLSObjectStorageKey(video, file.filename),
          CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS
        )
        await checkObjectStorage(
          `HLS resolution playlist ${file.id}`,
          generateHLSObjectStorageKey(video, getHLSResolutionPlaylistFilename(file.filename)),
          CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS
        )
      }
    }
  }

  return result
}

// ---------------------------------------------------------------------------

async function buildLiveVideoJobIndex (): Promise<VideoJobIndex> {
  const index: VideoJobIndex = {
    byVideoUUID: new Map(),
    byVideoId: new Map(),
    byVideoImportId: new Map()
  }
  const queues = JobQueue.Instance.getQueues()

  for (const jobType of PIPELINE_JOB_TYPES) {
    const queue = queues[jobType]
    if (!queue) continue

    for (const state of LIVE_JOB_STATES) {
      for (let start = 0; ; start += JOB_SCAN_BATCH_SIZE) {
        const jobs = await queue.getJobs([ state ], start, start + JOB_SCAN_BATCH_SIZE - 1, true)
        for (const job of jobs) addJobToIndex(index, { job, jobType })
        if (jobs.length < JOB_SCAN_BATCH_SIZE) break
      }
    }
  }

  return index
}

async function listActiveRunnerVideoUUIDs () {
  const runnerJobs = await RunnerJobModel.findAll({
    attributes: [ 'type', 'privatePayload' ],
    where: {
      type: { [Op.in]: VIDEO_PIPELINE_RUNNER_TYPES },
      state: { [Op.in]: ACTIVE_RUNNER_STATES }
    }
  })
  const videoUUIDs = new Set<string>()

  for (const runnerJob of runnerJobs) {
    const privatePayload = runnerJob.privatePayload as { videoUUID?: unknown }
    if (typeof privatePayload?.videoUUID === 'string') videoUUIDs.add(privatePayload.videoUUID)
  }

  return videoUUIDs
}

function addJobToIndex (index: VideoJobIndex, ref: VideoJobRef) {
  const data = ref.job.data as { videoUUID?: unknown, videoId?: unknown, videoImportId?: unknown }
  if (typeof data?.videoUUID === 'string') pushMapValue(index.byVideoUUID, data.videoUUID, ref)

  const videoId = toPositiveInteger(data?.videoId)
  if (videoId) pushMapValue(index.byVideoId, videoId, ref)

  const videoImportId = toPositiveInteger(data?.videoImportId)
  if (videoImportId) pushMapValue(index.byVideoImportId, videoImportId, ref)
}

function getVideoJobRefs (index: VideoJobIndex, videoUUID: string, videoId: number, videoImportId?: number) {
  const refs = [
    ...(index.byVideoUUID.get(videoUUID) || []),
    ...(index.byVideoId.get(videoId) || []),
    ...(videoImportId ? index.byVideoImportId.get(videoImportId) || [] : [])
  ]
  const seen = new Set<string>()

  return refs.filter(ref => {
    const key = `${ref.jobType}:${ref.job.id}`
    if (seen.has(key)) return false
    seen.add(key)
    return true
  })
}

async function removeFailedVideoJobs (videoUUID: string, videoId: number, videoImportId?: number) {
  let removed = 0
  const queues = JobQueue.Instance.getQueues()

  for (const jobType of PIPELINE_JOB_TYPES) {
    const queue = queues[jobType]
    if (!queue) continue

    const failedJobs: BullJob[] = []
    for (let start = 0; ; start += JOB_SCAN_BATCH_SIZE) {
      const jobs = await queue.getJobs([ 'failed' ], start, start + JOB_SCAN_BATCH_SIZE - 1, true)
      failedJobs.push(...jobs)
      if (jobs.length < JOB_SCAN_BATCH_SIZE) break
    }

    for (const job of failedJobs) {
      const refIndex: VideoJobIndex = { byVideoUUID: new Map(), byVideoId: new Map(), byVideoImportId: new Map() }
      addJobToIndex(refIndex, { job, jobType })
      if (getVideoJobRefs(refIndex, videoUUID, videoId, videoImportId).length === 0) continue

      try {
        await job.remove()
        await JobQueue.Instance.releaseLocalFileLeaseForRemovedJob(job, jobType)
        removed++
      } catch (err) {
        logger.warn(
          '[VIDEO_RECONCILIATION] Cannot remove failed %s job %s for %s.',
          jobType,
          job.id,
          videoUUID,
          { err, ...lTags(videoUUID) }
        )
      }
    }
  }

  return removed
}

// ---------------------------------------------------------------------------

function getCounters (info: VideoJobInfoModel): Record<VideoJobInfoColumnType, number> {
  return {
    pendingMove: info.pendingMove,
    pendingTranscode: info.pendingTranscode,
    pendingTranscription: info.pendingTranscription
  }
}

function hasPositiveCounter (info: VideoJobInfoModel) {
  return info.pendingMove > 0 || info.pendingTranscode > 0 || info.pendingTranscription > 0
}

function isIncompleteState (state: VideoStateType) {
  return state === VideoState.TO_IMPORT ||
    state === VideoState.TO_TRANSCODE ||
    state === VideoState.TO_MOVE_TO_EXTERNAL_STORAGE ||
    state === VideoState.TO_MOVE_TO_FILE_SYSTEM
}

function isIncompleteImportState (state: VideoImportStateType) {
  return state === VideoImportState.PENDING || state === VideoImportState.PROCESSING
}

function isObjectStorageNotFound (err: unknown) {
  if (!err || typeof err !== 'object') return false
  const value = err as {
    name?: string
    code?: string
    $metadata?: { httpStatusCode?: number }
    $response?: { statusCode?: number }
  }
  const statusCode = value.$metadata?.httpStatusCode ?? value.$response?.statusCode

  return statusCode === 404 || value.name === 'NoSuchKey' || value.name === 'NotFound' || value.code === 'NoSuchKey'
}

function getErrorMessage (err: unknown) {
  return err instanceof Error ? err.message : String(err)
}

function toPositiveInteger (value: unknown) {
  if (typeof value === 'number' && Number.isInteger(value) && value > 0) return value
  if (typeof value !== 'string' || value.length === 0) return undefined

  const parsed = Number(value)
  return Number.isInteger(parsed) && parsed > 0 ? parsed : undefined
}

function pushMapValue<K, V> (map: Map<K, V[]>, key: K, value: V) {
  const values = map.get(key)
  if (values) values.push(value)
  else map.set(key, [ value ])
}

async function emitProgress (
  onProgress: ((progress: VideoPipelineReconciliationProgress) => void | Promise<void>) | undefined,
  result: VideoPipelineReconciliationResult,
  currentPhase: string
) {
  if (!onProgress) return

  await onProgress({ ...result, currentPhase })
}
