import { buildAspectRatio } from '@peertube/peertube-core-utils'
import { ffprobePromise, getChaptersFromContainer, getVideoStreamDuration } from '@peertube/peertube-ffmpeg'
import {
  VideoImportPayload,
  VideoImportPreventExceptionResult,
  VideoImportState,
  VideoImportTorrentPayload,
  VideoImportTorrentPayloadType,
  VideoImportYoutubeDLPayload,
  VideoImportYoutubeDLPayloadType,
  VideoState
} from '@peertube/peertube-models'
import { retryTransactionWrapper } from '@server/helpers/database-utils.js'
import { downloadWithNm3u8dlRe } from '@server/helpers/n-m3u8dl-re/index.js'
import { customHeadersToYoutubeDLArgs, YoutubeDLWrapper } from '@server/helpers/youtube-dl/index.js'
import { CONFIG } from '@server/initializers/config.js'
import { AutomaticTagger } from '@server/lib/automatic-tags/automatic-tagger.js'
import { setAndSaveVideoAutomaticTags } from '@server/lib/automatic-tags/automatic-tags.js'
import { isPostImportVideoAccepted } from '@server/lib/moderation.js'
import { Hooks } from '@server/lib/plugins/hooks.js'
import { ServerConfigManager } from '@server/lib/server-config-manager.js'
import { createOptimizeOrMergeAudioJobs } from '@server/lib/transcoding/create-transcoding-job.js'
import { isUserQuotaValid } from '@server/lib/user.js'
import { createTranscriptionTaskIfNeeded } from '@server/lib/video-captions.js'
import { replaceChaptersIfNotExist } from '@server/lib/video-chapters.js'
import { buildNewFile } from '@server/lib/video-file.js'
import { addLocalOrRemoteStoryboardJobIfNeeded, buildMoveVideoJob, createMoveJobWithPendingMoveRollback } from '@server/lib/video-jobs.js'
import { VideoPathManager } from '@server/lib/video-path-manager.js'
import { buildNextVideoState } from '@server/lib/video-state.js'
import { createTorrentAndSetInfoHash, downloadWebTorrentVideo } from '@server/lib/webtorrent.js'
import { JobQueue } from '@server/lib/job-queue/index.js'
import { VideoCaptionModel } from '@server/models/video/video-caption.js'
import { MUserId, MVideoFile, MVideoFullLight } from '@server/types/models/index.js'
import { MVideoImport, MVideoImportDefault, MVideoImportDefaultFiles, MVideoImportVideo } from '@server/types/models/video/video-import.js'
import { Job } from 'bullmq'
import { FfprobeData } from 'fluent-ffmpeg'
import { move, remove } from 'fs-extra/esm'
import { stat } from 'fs/promises'
import { basename, dirname, join } from 'path'
import { runDrmDecryption } from '../../../helpers/drm-decrypt/index.js'
import { logger } from '../../../helpers/logger.js'
import { getSecureTorrentName } from '../../../helpers/utils.js'
import { CONSTRAINTS_FIELDS, JOB_TTL } from '../../../initializers/constants.js'
import { sequelizeTypescript } from '../../../initializers/database.js'
import { VideoFileModel } from '../../../models/video/video-file.js'
import { VideoImportModel } from '../../../models/video/video-import.js'
import { VideoModel } from '../../../models/video/video.js'
import { federateVideoIfNeeded } from '../../activitypub/videos/index.js'
import { Notifier } from '../../notifier/index.js'
import { createLocalVideoThumbnailsFromVideo } from '../../thumbnail.js'
import { UserModel } from '@server/models/user/user.js'

const VIDEO_IMPORT_LOCAL_PIPELINE_BACKPRESSURE_DELAY_MS = 10 * 60 * 1000
const VIDEO_IMPORT_LOCAL_PIPELINE_BACKPRESSURE_MIN_DELAY_MS = 2 * 60 * 1000

async function processVideoImport (job: Job): Promise<VideoImportPreventExceptionResult> {
  const payload = job.data as VideoImportPayload

  const videoImport = await getVideoImportOrDie(payload)
  if (videoImport.state === VideoImportState.CANCELLED) {
    logger.info('Do not process import since it has been cancelled', { payload })
    return { resultType: 'success' }
  }

  if (videoImport.attempts >= CONFIG.IMPORT.VIDEOS.MAX_ATTEMPTS) {
    logger.info('Do not process import since it has reached the maximum number of attempts', { payload, attempts: videoImport.attempts })

    return { resultType: 'error' }
  }

  if (await maybeDeferVideoImportForLocalPipeline(job, videoImport, payload)) {
    return { resultType: 'success' }
  }

  videoImport.attempts += 1
  videoImport.state = VideoImportState.PROCESSING
  videoImport.progress = 0
  await videoImport.save()

  try {
    if (payload.type === 'youtube-dl') await processYoutubeDLImport(job, videoImport, payload)
    if (payload.type === 'magnet-uri' || payload.type === 'torrent-file') await processTorrentImport(job, videoImport, payload)

    return { resultType: 'success' }
  } catch (err) {
    // Processors already handle video import state change on error

    if (!payload.preventException) throw err

    logger.warn('Catch error in video import to send value to parent job.', { payload, err })
    return { resultType: 'error' }
  }
}

// ---------------------------------------------------------------------------

export {
  processVideoImport
}

// ---------------------------------------------------------------------------

type LocalVideoPipelineBacklogSnapshot = {
  total: number
  byType: Partial<Record<string, number>>
  uniqueVideoUUIDTotal?: number
  uniqueVideoUUIDsByType?: Partial<Record<string, number>>
}

async function maybeDeferVideoImportForLocalPipeline (
  job: Job,
  videoImport: MVideoImportDefault,
  payload: VideoImportPayload
) {
  // Remote imports materialize the source file in local web-videos before
  // transcoding/move jobs can delete it. When many imports are accepted faster
  // than the downstream local pipeline can drain, local storage fills and Redis
  // can enter MISCONF/stop-writes mode. Keep imports pending instead of
  // downloading more local files while the downstream queue is already backed up.
  if (!CONFIG.OBJECT_STORAGE.ENABLED || !CONFIG.TRANSCODING.ENABLED) return false

  const backlog = await JobQueue.Instance.getLocalVideoPipelineBacklog()
  const maxJobs = buildVideoImportLocalPipelineBackpressureMaxJobs({
    transcodingConcurrency: CONFIG.TRANSCODING.CONCURRENCY,
    objectStorageConcurrency: CONFIG.OBJECT_STORAGE.CONCURRENCY
  })
  const effectiveBacklogTotal = getVideoImportLocalPipelineBackpressureTotal(backlog)

  if (!isVideoImportLocalPipelineBacklogged({ total: effectiveBacklogTotal, maxJobs })) return false

  const delayMs = buildVideoImportLocalPipelineBackpressureDelayMs({
    total: effectiveBacklogTotal,
    maxJobs
  })
  const customJobId = buildVideoImportBackpressureJobId(videoImport.id, Date.now(), delayMs)

  logger.warn(
    '[VIDEO_IMPORT] Deferring import %d from job %s for %d ms because effective local video pipeline backlog is %d/%d.',
    videoImport.id,
    job.id,
    delayMs,
    effectiveBacklogTotal,
    maxJobs,
    {
      backlogTotal: backlog.total,
      backlogByType: backlog.byType
    }
  )

  videoImport.state = VideoImportState.PENDING
  videoImport.progress = null
  await videoImport.save()

  try {
    const delayedJob = await JobQueue.Instance.createJob({
      type: 'video-import',
      payload,
      delay: delayMs,
      customJobId
    })

    if (!delayedJob) {
      throw new Error(`Cannot create delayed video import job for import ${videoImport.id}`)
    }
  } catch (err) {
    if (isDuplicateVideoImportBackpressureJobError(err)) {
      logger.info(
        '[VIDEO_IMPORT] Delayed import backpressure job %s already exists for import %d.',
        customJobId,
        videoImport.id
      )
      return true
    }

    throw err
  }

  return true
}

export function buildVideoImportLocalPipelineBackpressureMaxJobs (options: {
  transcodingConcurrency: number
  objectStorageConcurrency: number
}) {
  const transcodingConcurrency = Math.max(1, options.transcodingConcurrency || 1)
  const objectStorageConcurrency = Math.max(1, options.objectStorageConcurrency || 1)
  const moveDrainHeadroom = Math.max(5, Math.ceil(objectStorageConcurrency / 2))

  // We already dedupe backlog by video UUID, so use a little extra headroom for
  // short bursts while the object-storage movers drain. Without this, imports
  // tend to bounce into repeated 10-minute deferrals when the pipeline is busy
  // but still healthy.
  return Math.max(10, transcodingConcurrency * 2 + objectStorageConcurrency + moveDrainHeadroom)
}

export function buildVideoImportLocalPipelineBackpressureDelayMs (options: {
  total: number
  maxJobs: number
}) {
  const { total, maxJobs } = options

  if (maxJobs <= 0) return VIDEO_IMPORT_LOCAL_PIPELINE_BACKPRESSURE_MIN_DELAY_MS

  const overload = Math.max(0, total - maxJobs)
  if (overload === 0) return VIDEO_IMPORT_LOCAL_PIPELINE_BACKPRESSURE_MIN_DELAY_MS

  const saturationWindow = Math.max(5, Math.ceil(maxJobs / 2))
  const ratio = Math.min(1, overload / saturationWindow)

  return Math.round(
    VIDEO_IMPORT_LOCAL_PIPELINE_BACKPRESSURE_MIN_DELAY_MS +
    ratio * (VIDEO_IMPORT_LOCAL_PIPELINE_BACKPRESSURE_DELAY_MS - VIDEO_IMPORT_LOCAL_PIPELINE_BACKPRESSURE_MIN_DELAY_MS)
  )
}

export function getVideoImportLocalPipelineBackpressureTotal (backlog: LocalVideoPipelineBacklogSnapshot) {
  // Count distinct in-flight videos, not every granular child move job.
  // A single video can fan out to many HLS/caption/object-storage jobs, and
  // counting all of them makes the import backpressure gate trigger far too
  // early, leaving new imports stuck in repeated delayed "To import" cycles.
  //
  // Caption-only moves are intentionally excluded in JobQueue because they do
  // not consume the critical local web-video/transcoding pipeline capacity.
  if (typeof backlog.uniqueVideoUUIDTotal === 'number') {
    return backlog.uniqueVideoUUIDTotal
  }

  return Math.max(0, backlog.total - (backlog.byType['move-caption-to-object-storage'] ?? 0))
}

export function isVideoImportLocalPipelineBacklogged (options: {
  total: number
  maxJobs: number
}) {
  return options.maxJobs > 0 && options.total >= options.maxJobs
}

export function buildVideoImportBackpressureJobId (
  videoImportId: number,
  nowMs = Date.now(),
  delayMs = VIDEO_IMPORT_LOCAL_PIPELINE_BACKPRESSURE_DELAY_MS
) {
  const bucket = Math.floor(nowMs / delayMs)

  return `video-import-backpressure-${videoImportId}-${bucket}`
}

function isDuplicateVideoImportBackpressureJobError (err: unknown) {
  const message = err instanceof Error ? err.message : String(err)

  return message.includes('Job is already waiting') ||
    (message.includes('JobId') && message.includes('already exists'))
}

// ---------------------------------------------------------------------------

async function processTorrentImport (job: Job, videoImport: MVideoImportDefault, payload: VideoImportTorrentPayload) {
  logger.info('Processing torrent video import in job %s.', job.id)

  const options = { type: payload.type, generateTranscription: payload.generateTranscription, videoImportId: payload.videoImportId }

  const target = {
    torrentName: videoImport.torrentName
      ? getSecureTorrentName(videoImport.torrentName)
      : undefined,
    uri: videoImport.magnetUri
  }
  return processFile(() => downloadWebTorrentVideo(target, JOB_TTL['video-import']), videoImport, options)
}

async function processYoutubeDLImport (job: Job, videoImport: MVideoImportDefault, payload: VideoImportYoutubeDLPayload) {
  logger.info('Processing youtubeDL video import in job %s.', job.id)

  const options = {
    type: payload.type,
    generateTranscription: payload.generateTranscription,
    videoImportId: videoImport.id,
    licenseServerUrl: payload.licenseServerUrl,
    drmType: payload.drmType,
    clearkeys: payload.clearkeys,
    useNm3u8dlRe: payload.useNm3u8dlRe
  }

  const onProgress = async (percent: number) => {
    job.updateProgress(percent).catch(err => logger.error('Cannot update video import job progress', { err }))
    videoImport.progress = percent
    await videoImport.save()
  }

  let downloader: () => Promise<string>

  if (payload.useNm3u8dlRe) {
    downloader = () => downloadWithNm3u8dlRe({
      url: videoImport.targetUrl,
      clearkeys: payload.clearkeys,
      customHeaders: payload.customHeaders,
      timeout: JOB_TTL['video-import'],
      onProgress
    })
  } else {
    const youtubeDL = new YoutubeDLWrapper(
      videoImport.targetUrl,
      ServerConfigManager.Instance.getEnabledResolutions('vod'),
      CONFIG.TRANSCODING.ALWAYS_TRANSCODE_ORIGINAL_RESOLUTION
    )
    const youtubeDLArgs = customHeadersToYoutubeDLArgs(payload.customHeaders)
    downloader = () => youtubeDL.downloadVideo(payload.fileExt ?? '.mp4', JOB_TTL['video-import'], onProgress, youtubeDLArgs)
  }

  return processFile(downloader, videoImport, options)
}

async function getVideoImportOrDie (payload: VideoImportPayload) {
  const videoImport = await VideoImportModel.loadAndPopulateVideo(payload.videoImportId)
  if (!videoImport) throw new Error('Video import not found')

  if (!videoImport.Video) {
    const err = new Error(
      `Cannot process video import ${payload.videoImportId}: the video import or video linked to this import does not exist anymore.`
    )

    await onImportError(err, null, videoImport)

    throw err
  }

  return videoImport
}

type ProcessFileOptions = {
  type: VideoImportYoutubeDLPayloadType | VideoImportTorrentPayloadType
  generateTranscription: boolean
  videoImportId: number
  licenseServerUrl?: string | null
  drmType?: string | null
  clearkeys?: string | null
  useNm3u8dlRe?: boolean
}
async function processFile (downloader: () => Promise<string>, videoImport: MVideoImportDefault, options: ProcessFileOptions) {
  let tmpVideoPath: string
  let videoFile: MVideoFile

  try {
    // Download video from youtubeDL or torrent
    tmpVideoPath = await downloader()

    // Optional DRM decryption step (before transcoding). Skip when N_m3u8DL-RE already decrypted.
    if (CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION.ENABLED && options.type === 'youtube-dl' && !options.useNm3u8dlRe) {
      const ext = tmpVideoPath.match(/\.[^/.]+$/)?.[0] ?? '.mp4'
      const baseName = basename(tmpVideoPath, ext)
      const decryptedPath = join(dirname(tmpVideoPath), `${baseName}-decrypted${ext}`)

      try {
        await runDrmDecryption({
          inputPath: tmpVideoPath,
          outputPath: decryptedPath,
          licenseServerUrl: options.licenseServerUrl,
          drmType: options.drmType,
          clearkeys: options.clearkeys
        })
        await remove(tmpVideoPath).catch(() => {})
        tmpVideoPath = decryptedPath
      } catch (err) {
        logger.warn('DRM decryption failed, continuing with original file.', { err, tmpVideoPath })
        // Continue with the original (possibly encrypted) file - transcoding may fail later
      }
    }

    // Get information about this video
    const stats = await stat(tmpVideoPath)
    const user = videoImport.User ?? await UserModel.loadByVideoId(videoImport.videoId)
    if (!user) {
      throw new Error(`Cannot process video import ${videoImport.id}: owner user could not be loaded.`)
    }

    const isAble = await isUserQuotaValid({ channelUserId: user.id, uploadSize: stats.size })
    if (isAble === false) {
      throw new Error('The user video quota is exceeded with this video to import.')
    }

    const ffprobe = await ffprobePromise(tmpVideoPath)
    const duration = await getVideoStreamDuration(tmpVideoPath, ffprobe)

    const containerChapters = await getChaptersFromContainer({
      path: tmpVideoPath,
      maxTitleLength: CONSTRAINTS_FIELDS.VIDEO_CHAPTERS.TITLE.max,
      ffprobe
    })

    videoFile = await buildNewFile({ mode: 'web-video', ffprobe, path: tmpVideoPath })
    videoFile.videoId = videoImport.videoId

    const hookName = options.type === 'youtube-dl'
      ? 'filter:api.video.post-import-url.accept.result'
      : 'filter:api.video.post-import-torrent.accept.result'

    // Check we accept this video
    const acceptParameters = {
      videoImport,
      video: videoImport.Video,
      videoFilePath: tmpVideoPath,
      videoFile: videoFile as VideoFileModel,
      user: videoImport.User
    }
    const acceptedResult = await Hooks.wrapFun(isPostImportVideoAccepted, acceptParameters, hookName)

    if (acceptedResult.accepted !== true) {
      logger.info('Refused imported video.', { acceptedResult, acceptParameters })

      videoImport.state = VideoImportState.REJECTED
      await videoImport.save()

      throw new Error(acceptedResult.errorMessage)
    }

    // Video is accepted, resuming preparation
    const videoFileLockReleaser = await VideoPathManager.Instance.lockFiles(videoImport.Video.uuid)

    try {
      const videoImportWithFiles = await refreshVideoImportFromDB(videoImport, videoFile)

      // Move file
      const videoDestFile = VideoPathManager.Instance.getFSVideoFileOutputPath(videoImportWithFiles.Video, videoFile)
      await move(tmpVideoPath, videoDestFile)

      tmpVideoPath = null // This path is not used anymore

      const [ thumbnails ] = await Promise.all([
        generateThumbnails({ videoImportWithFiles, videoFile, ffprobe }),
        createTorrentAndSetInfoHash(videoImportWithFiles.Video, videoFile)
      ])

      const { videoImportUpdated, videoUUID } = await retryTransactionWrapper(() => {
        return sequelizeTypescript.transaction(async t => {
          // Refresh video
          const video = await VideoModel.load(videoImportWithFiles.videoId, t)
          if (!video) throw new Error('Video linked to import ' + videoImportWithFiles.videoId + ' does not exist anymore.')

          const persistedVideoFile = await VideoFileModel.customUpsert(videoFile, 'video', t)

          // Update video DB object
          video.duration = duration
          video.state = buildNextVideoState(video.state)
          video.aspectRatio = buildAspectRatio({ width: persistedVideoFile.width, height: persistedVideoFile.height })
          await video.save({ transaction: t })

          if (thumbnails.length !== 0) {
            await video.replaceAndSaveThumbnails(thumbnails, t)
          }

          await replaceChaptersIfNotExist({ video, chapters: containerChapters, transaction: t })

          const automaticTags = await new AutomaticTagger().buildVideoAutomaticTags({ video, transaction: t })
          await setAndSaveVideoAutomaticTags({ video, automaticTags, transaction: t })

          // Update video import object
          videoImportWithFiles.state = VideoImportState.SUCCESS
          videoImportWithFiles.progress = 100
          const videoImportUpdated = await videoImportWithFiles.save({ transaction: t }) as MVideoImport

          logger.info('Video %s imported.', video.uuid)

          return { videoImportUpdated, videoUUID: video.uuid }
        })
      })

      const video = await VideoModel.loadFull(videoUUID)
      if (!video) {
        logger.warn(
          '[VIDEO_IMPORT] Video %s disappeared after import transaction commit. Skipping federation and post-import tasks.',
          videoUUID
        )
        return
      }

      await ensureImportMediaPersistedOrThrow({
        videoUUID,
        videoImport: videoImportUpdated
      })

      await federateVideoIfNeeded(video, true)

      await afterImportSuccess({
        videoImport: videoImportUpdated,
        video,
        videoFile,
        user: videoImport.User,
        generateTranscription: options.generateTranscription
      })
    } finally {
      videoFileLockReleaser()
    }
  } catch (err) {
    await onImportError(err, tmpVideoPath, videoImport)

    throw err
  }
}

async function refreshVideoImportFromDB (videoImport: MVideoImportDefault, videoFile: MVideoFile): Promise<MVideoImportDefaultFiles> {
  // Refresh video, privacy may have changed
  const video = await videoImport.Video.reload()
  const videoWithFiles = Object.assign(video, { VideoFiles: [ videoFile ], VideoStreamingPlaylists: [] })

  return Object.assign(videoImport, { Video: videoWithFiles })
}

async function generateThumbnails (options: {
  videoImportWithFiles: MVideoImportDefaultFiles
  videoFile: MVideoFile
  ffprobe: FfprobeData
}) {
  const { ffprobe, videoFile, videoImportWithFiles } = options

  if (videoImportWithFiles.Video.Thumbnails.length !== 0) return []

  return createLocalVideoThumbnailsFromVideo({ video: videoImportWithFiles.Video, videoFile, ffprobe })
}

async function afterImportSuccess (options: {
  videoImport: MVideoImport
  video: MVideoFullLight
  videoFile: MVideoFile
  user: MUserId

  generateTranscription: boolean
}) {
  const { video, videoFile, videoImport, user, generateTranscription } = options

  Notifier.Instance.notifyOnFinishedVideoImport({ videoImport: Object.assign(videoImport, { Video: video }), success: true })

  if (video.isBlacklisted()) {
    const videoBlacklist = Object.assign(video.VideoBlacklist, { Video: video })

    Notifier.Instance.notifyOnVideoAutoBlacklist(videoBlacklist)
  } else {
    Notifier.Instance.notifyOnNewVideoOrLiveIfNeeded(video)
  }

  const postImportTasks: Promise<unknown>[] = []

  if (generateTranscription === true) {
    postImportTasks.push(
      VideoCaptionModel.hasVideoCaption(video.id)
        .then(hasCaption => {
          if (hasCaption) return

          return createTranscriptionTaskIfNeeded(video)
        })
    )
  }

  if (video.state === VideoState.TO_MOVE_TO_EXTERNAL_STORAGE) {
    postImportTasks.push(
      (async () => {
        const job = await buildMoveVideoJob({
          type: 'move-to-object-storage',
          video,
          moveVideoState: {
            isNewVideo: true,
            previousVideoState: VideoState.TO_IMPORT
          }
        })
        if (job) {
          await createMoveJobWithPendingMoveRollback(job)
        } else {
          logger.info(`[VIDEO_IMPORT] Move job skipped (already pending/active) for video ${video.uuid}`)
        }
      })()
    )

    await Promise.all(postImportTasks)
    return
  }

  if (video.state === VideoState.TO_TRANSCODE) { // Create transcoding jobs?
    postImportTasks.push((async () => {
      const videoWithFiles = await VideoModel.loadWithFiles(video.uuid)
      await createOptimizeOrMergeAudioJobs({
        video: (videoWithFiles || video) as typeof video,
        videoFile,
        isNewVideo: true,
        user
      })
    })())
  }

  await Promise.all(postImportTasks)

  // Storyboard generation can be expensive (especially with remote runners).
  // Do not block transcription/transcoding job scheduling on it.
  void addLocalOrRemoteStoryboardJobIfNeeded({ video, federate: true })
    .catch(err => logger.error('Cannot create storyboard job after video import.', { err, videoUUID: video.uuid }))
}

async function onImportError (err: Error, tempVideoPath: string, videoImport: MVideoImportVideo) {
  try {
    if (tempVideoPath) await remove(tempVideoPath)
  } catch (errUnlink) {
    logger.warn('Cannot cleanup files after a video import error.', { err: errUnlink })
  }

  await sequelizeTypescript.transaction(async t => {
    videoImport.error = err.message
    videoImport.progress = null

    if (videoImport.state !== VideoImportState.REJECTED) {
      videoImport.state = VideoImportState.FAILED
    }

    await videoImport.save({ transaction: t })

    const video = await VideoModel.load(videoImport.videoId, t)

    if (video) {
      video.state = VideoState.TO_IMPORT_FAILED
      await video.save({ transaction: t })
    }
  })

  Notifier.Instance.notifyOnFinishedVideoImport({ videoImport, success: false })
}

async function ensureImportMediaPersistedOrThrow (options: {
  videoUUID: string
  videoImport: MVideoImport
}) {
  const { videoUUID, videoImport } = options

  const persistedVideo = await VideoModel.loadWithFiles(videoUUID)
  if (!persistedVideo) {
    throw new Error(`Imported video ${videoUUID} disappeared before post-import verification.`)
  }

  const hasPersistedMedia =
    (persistedVideo.VideoFiles?.length || 0) > 0 ||
    persistedVideo.VideoStreamingPlaylists?.some(playlist => (playlist.VideoFiles?.length || 0) > 0) === true

  if (hasPersistedMedia) return

  const err = new Error(
    `Imported media for video ${videoUUID} was not durably persisted before transcoding scheduling.`
  )

  await onImportError(err, null, videoImport as MVideoImportVideo)
  throw err
}
