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
import { addLocalOrRemoteStoryboardJobIfNeeded, buildMoveVideoJob } from '@server/lib/video-jobs.js'
import { VideoPathManager } from '@server/lib/video-path-manager.js'
import { buildNextVideoState } from '@server/lib/video-state.js'
import { createTorrentAndSetInfoHash, downloadWebTorrentVideo } from '@server/lib/webtorrent.js'
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
import { JobQueue } from '../job-queue.js'
import { UserModel } from '@server/models/user/user.js'

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
    const user = await UserModel.loadByVideoId(videoImport.videoId)

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

      const { videoImportUpdated, video } = await retryTransactionWrapper(() => {
        return sequelizeTypescript.transaction(async t => {
          // Refresh video
          const video = await VideoModel.load(videoImportWithFiles.videoId, t)
          if (!video) throw new Error('Video linked to import ' + videoImportWithFiles.videoId + ' does not exist anymore.')

          await videoFile.save({ transaction: t })

          // Update video DB object
          video.duration = duration
          video.state = buildNextVideoState(video.state)
          video.aspectRatio = buildAspectRatio({ width: videoFile.width, height: videoFile.height })
          await video.save({ transaction: t })

          if (thumbnails.length !== 0) {
            await video.replaceAndSaveThumbnails(thumbnails, t)
          }

          await replaceChaptersIfNotExist({ video, chapters: containerChapters, transaction: t })

          const automaticTags = await new AutomaticTagger().buildVideoAutomaticTags({ video, transaction: t })
          await setAndSaveVideoAutomaticTags({ video, automaticTags, transaction: t })

          // Now we can federate the video (reload from database, we need more attributes)
          const videoForFederation = await VideoModel.loadFull(video.uuid, t)
          await federateVideoIfNeeded(videoForFederation, true, t)

          // Update video import object
          videoImportWithFiles.state = VideoImportState.SUCCESS
          videoImportWithFiles.progress = 100
          const videoImportUpdated = await videoImportWithFiles.save({ transaction: t }) as MVideoImport

          logger.info('Video %s imported.', video.uuid)

          return { videoImportUpdated, video: videoForFederation }
        })
      })

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
      buildMoveVideoJob({
        type: 'move-to-object-storage',
        video,
        moveVideoState: {
          isNewVideo: true,
          previousVideoState: VideoState.TO_IMPORT
        }
      }).then(job => JobQueue.Instance.createJob(job))
    )

    await Promise.all(postImportTasks)
    return
  }

  if (video.state === VideoState.TO_TRANSCODE) { // Create transcoding jobs?
    postImportTasks.push(createOptimizeOrMergeAudioJobs({ video, videoFile, isNewVideo: true, user }))
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
