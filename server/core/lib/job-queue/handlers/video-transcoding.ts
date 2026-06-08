import {
  HLSTranscodingPayload,
  MergeAudioTranscodingPayload,
  NewWebVideoResolutionTranscodingPayload,
  OptimizeTranscodingPayload,
  VideoResolution,
  VideoTranscodingPayload
} from '@peertube/peertube-models'
import { CONFIG } from '@server/initializers/config.js'
import { hasMissingHLSStreams } from '@server/lib/runners/job-handlers/shared/utils.js'
import { onTranscodingEnded } from '@server/lib/transcoding/ended-transcoding.js'
import { generateHlsPlaylistResolution } from '@server/lib/transcoding/hls-transcoding.js'
import { mergeAudioVideofile, optimizeOriginalVideofile, transcodeNewWebVideoResolution } from '@server/lib/transcoding/web-transcoding.js'
import { removeAllWebVideoFiles } from '@server/lib/video-file.js'
import { JobQueue } from '@server/lib/job-queue/index.js'
import { VideoPathManager } from '@server/lib/video-path-manager.js'
import { moveToFailedTranscodingState } from '@server/lib/video-state.js'
import { UserModel } from '@server/models/user/user.js'
import { VideoJobInfoModel } from '@server/models/video/video-job-info.js'
import { MUser, MUserId, MVideoFullLight } from '@server/types/models/index.js'
import { Job } from 'bullmq'
import { logger, loggerTagsFactory } from '../../../helpers/logger.js'
import { VideoModel } from '../../../models/video/video.js'
import { publishVideoAfterFirstTranscodingBatchIfNeededWithDeps } from './video-transcoding-publish.js'

type HandlerFunction = (job: Job, payload: VideoTranscodingPayload, video: MVideoFullLight, user: MUser) => Promise<void>

const handlers: { [id in VideoTranscodingPayload['type']]: HandlerFunction } = {
  'new-resolution-to-hls': handleHLSJob,
  'new-resolution-to-web-video': handleNewWebVideoResolutionJob,
  'merge-audio-to-web-video': handleWebVideoMergeAudioJob,
  'optimize-to-web-video': handleWebVideoOptimizeJob
}

const lTags = loggerTagsFactory('transcoding')

async function processVideoTranscoding (job: Job) {
  const payload = job.data as VideoTranscodingPayload
  logger.info('Processing transcoding job %s.', job.id, lTags(payload.videoUUID))

  const video = await VideoModel.loadFull(payload.videoUUID)
  if (!video) {
    logger.info(
      'Transcoding job %s cancelled: video %s does not exist (video was deleted).',
      job.id,
      payload.videoUUID,
      lTags(payload.videoUUID)
    )
    throw new Error('Video was deleted - transcoding job cancelled')
  }

  const user = await UserModel.loadByChannelActorId(video.VideoChannel.Actor.id)

  const handler = handlers[payload.type]

  if (!handler) {
    await moveToFailedTranscodingState(video)
    await VideoJobInfoModel.decrease(video.uuid, 'pendingTranscode')

    throw new Error('Cannot find transcoding handler for ' + payload.type)
  }

  try {
    await handler(job, payload, video, user)
  } catch (error) {
    // Video may have been deleted during transcoding; treat as graceful cancellation, not failure
    const videoStillExists = await VideoModel.loadFull(payload.videoUUID)
    if (!videoStillExists) {
      logger.info(
        'Transcoding job %s cancelled: video %s was deleted during transcoding.',
        job.id,
        payload.videoUUID,
        lTags(payload.videoUUID)
      )
      throw new Error('Video was deleted - transcoding job cancelled', { cause: error })
    }

    await moveToFailedTranscodingState(videoStillExists)
    await VideoJobInfoModel.decrease(video.uuid, 'pendingTranscode')

    throw error
  }

  return video
}

// ---------------------------------------------------------------------------

export {
  processVideoTranscoding
}

// ---------------------------------------------------------------------------
// Job handlers
// ---------------------------------------------------------------------------

async function handleWebVideoMergeAudioJob (job: Job, payload: MergeAudioTranscodingPayload, video: MVideoFullLight, user: MUserId) {
  logger.info('Handling merge audio transcoding job for %s.', video.uuid, lTags(video.uuid), { payload })

  // Early exit: check if file already exists (audio already merged)
  const existingFile = video.getMaxQualityAudioAndVideoFiles().videoFile
  if (existingFile && existingFile.fps === payload.fps && existingFile.resolution === payload.resolution) {
    logger.info(`Merged audio file already exists for video ${video.uuid} at ${payload.resolution}p, skipping`, lTags(video.uuid))
    await VideoJobInfoModel.decrease(video.uuid, 'pendingTranscode')
    return
  }

  await mergeAudioVideofile({ video, resolution: payload.resolution, fps: payload.fps, job })

  logger.info('Merge audio transcoding job for %s ended.', video.uuid, lTags(video.uuid), { payload })

  if (CONFIG.OBJECT_STORAGE.ENABLED) {
    await publishVideoAfterFirstTranscodingBatchIfNeeded({ videoUUID: video.uuid, isNewVideo: payload.isNewVideo })
  }

  await onTranscodingEnded({ isNewVideo: payload.isNewVideo, moveVideoToNextState: payload.canMoveVideoState, video })
}

async function handleWebVideoOptimizeJob (job: Job, payload: OptimizeTranscodingPayload, video: MVideoFullLight, user: MUserId) {
  logger.info('Handling optimize transcoding job for %s.', video.uuid, lTags(video.uuid), { payload })

  // Early exit: check if optimized file already exists
  const optimizedFile = video.getWebVideoFileResolution(VideoResolution.H_NOVIDEO)
  if (optimizedFile) {
    logger.info(`Optimized file already exists for video ${video.uuid}, skipping`, lTags(video.uuid))
    await VideoJobInfoModel.decrease(video.uuid, 'pendingTranscode')
    return
  }

  await optimizeOriginalVideofile({ video, job })

  logger.info('Optimize transcoding job for %s ended.', video.uuid, lTags(video.uuid), { payload })

  if (CONFIG.OBJECT_STORAGE.ENABLED) {
    await publishVideoAfterFirstTranscodingBatchIfNeeded({ videoUUID: video.uuid, isNewVideo: payload.isNewVideo })
  }

  await onTranscodingEnded({ isNewVideo: payload.isNewVideo, moveVideoToNextState: payload.canMoveVideoState, video })
}

// ---------------------------------------------------------------------------

async function handleNewWebVideoResolutionJob (job: Job, payload: NewWebVideoResolutionTranscodingPayload, video: MVideoFullLight) {
  logger.info('Handling Web Video transcoding job for %s.', video.uuid, lTags(video.uuid), { payload })

  // Early exit: check if resolution already exists
  const existingFile = video.getWebVideoFileResolution(payload.resolution)
  if (existingFile) {
    logger.info(`Resolution ${payload.resolution}p already exists for video ${video.uuid}, skipping`, lTags(video.uuid))
    await VideoJobInfoModel.decrease(video.uuid, 'pendingTranscode')
    return
  }

  await transcodeNewWebVideoResolution({ video, resolution: payload.resolution, fps: payload.fps, job })

  logger.info('Web Video transcoding job for %s ended.', video.uuid, lTags(video.uuid), { payload })

  if (CONFIG.OBJECT_STORAGE.ENABLED) {
    await publishVideoAfterFirstTranscodingBatchIfNeeded({ videoUUID: video.uuid, isNewVideo: payload.isNewVideo })
  }

  // Always move video to next state, we're ready enough with this resolution
  await onTranscodingEnded({ isNewVideo: payload.isNewVideo, moveVideoToNextState: payload.canMoveVideoState, video })
}

// ---------------------------------------------------------------------------

async function handleHLSJob (job: Job, payload: HLSTranscodingPayload, videoArg: MVideoFullLight) {
  // [LOGGER] HLS job started
  logger.info('[TRANSCODE_HANDLER] HLS transcoding job started for %s', videoArg.uuid, { payload })

  if (CONFIG.OBJECT_STORAGE.ENABLED) {
    await waitForPreviousHLSMoveJobs(videoArg.uuid)
  }

  const inputFileMutexReleaser = await VideoPathManager.Instance.lockFiles(videoArg.uuid)
  let video: MVideoFullLight

  try {
    video = await VideoModel.loadFull(videoArg.uuid)

    const { videoFile, separatedAudioFile } = video.getMaxQualityAudioAndVideoFiles()
    const webVideoFile = video.getWebVideoFileResolution(payload.resolution)

    // [LOGGER] Input files selected
    logger.info('[TRANSCODE_HANDLER] Input files for %s', video.uuid, {
      hasVideoFile: !!videoFile,
      hasSeparatedAudio: !!separatedAudioFile,
      hasWebVideoFile: !!webVideoFile
    })

    // [LOGGER] Starting FFmpeg transcoding
    logger.info('[TRANSCODE_HANDLER] Starting FFmpeg HLS transcoding for %s', video.uuid)

    await VideoPathManager.Instance.makeAvailableMaxQualityFiles(video, async ({ videoPath, separatedAudioPath }) => {
      await generateHlsPlaylistResolution({
        video,

        videoInputPath: videoPath,
        separatedAudioInputPath: separatedAudioPath,

        filesLockedInParent: true,
        resolution: payload.resolution,
        fps: payload.fps,
        separatedAudio: payload.separatedAudio,
        job
      })
    })

    // [LOGGER] FFmpeg transcoding completed
    logger.info('[TRANSCODE_HANDLER] FFmpeg HLS transcoding completed for %s', video.uuid)
  } finally {
    // [LOGGER] Releasing lock
    logger.info('[TRANSCODE_HANDLER] Releasing file lock for %s', videoArg.uuid)
    inputFileMutexReleaser()
  }

  // Video may have been deleted during transcoding; re-load to avoid operating on stale/deleted data
  const videoStillExists = await VideoModel.loadFull(videoArg.uuid)
  if (!videoStillExists) {
    logger.info('Transcoding job cancelled: video %s was deleted during HLS transcoding.', videoArg.uuid, lTags(videoArg.uuid))
    throw new Error('Video was deleted - transcoding job cancelled')
  }

  // [LOGGER] HLS transcoding job ended
  logger.info('[TRANSCODE_HANDLER] HLS transcoding job ended for %s', videoStillExists.uuid, { payload })

  const missingStream = await hasMissingHLSStreams({
    inputStreams: payload.inputStreams,
    transcodingRequestAt: payload.transcodingRequestAt,
    videoId: videoArg.uuid
  })

  // [LOGGER] Missing stream check
  logger.info('[TRANSCODE_HANDLER] Missing stream check for %s: %s', videoStillExists.uuid, !!missingStream)

  if (CONFIG.OBJECT_STORAGE.ENABLED && !missingStream) {
    await publishVideoAfterFirstTranscodingBatchIfNeeded({
      videoUUID: videoStillExists.uuid,
      isNewVideo: payload.isNewVideo
    })
  }

  if (!missingStream && payload.deleteWebVideoFiles === true) {
    const resolutionExceptions = CONFIG.TRANSCODING.ALWAYS_TRANSCODE_PODCAST_OPTIMIZED_AUDIO
      ? [ VideoResolution.H_NOVIDEO ]
      : []

    // [LOGGER] Removing web video files
    logger.info('[TRANSCODE_HANDLER] Removing Web Video files for %s', videoStillExists.uuid, { resolutionExceptions })

    await removeAllWebVideoFiles(videoStillExists, { resolutionExceptions })
  }

  // Splitted audio, wait audio & video generation before moving the video in its next state
  const moveVideoToNextState = payload.canMoveVideoState && !missingStream

  // [LOGGER] Calling onTranscodingEnded
  logger.info('[TRANSCODE_HANDLER] Calling onTranscodingEnded for %s, moveVideoToNextState=%s', videoStillExists.uuid, moveVideoToNextState)

  await onTranscodingEnded({ isNewVideo: payload.isNewVideo, moveVideoToNextState, video: videoStillExists })

  // [LOGGER] HLS job handler complete
  logger.info('[TRANSCODE_HANDLER] HLS job handler complete for %s', videoStillExists.uuid)

}

async function waitForPreviousHLSMoveJobs (videoUUID: string) {
  const startedAt = Date.now()
  const maxWaitMs = 1000 * 60 * 60 * 24 // Align with move-hls-playlist job TTL (24h)
  const pollEveryMs = 2000
  let lastLogAt = 0

  while (await JobQueue.Instance.hasPendingOrActiveHLSPlaylistMoveJob({
    videoUUID,
    excludeCleanupJobs: true,
    activeOnly: true
  })) {
    const elapsed = Date.now() - startedAt
    if (elapsed >= maxWaitMs) {
      throw new Error(`Timed out after ${elapsed}ms waiting for previous HLS move jobs of video ${videoUUID}`)
    }

    // Keep logs informative without flooding.
    if (Date.now() - lastLogAt >= 30000) {
      logger.info(
        '[TRANSCODE_HANDLER] Waiting for previous HLS move jobs to finish before transcoding next resolution for %s (elapsed=%dms)',
        videoUUID,
        elapsed
      )
      lastLogAt = Date.now()
    }

    await new Promise(resolve => setTimeout(resolve, pollEveryMs))
  }
}

export async function publishVideoAfterFirstTranscodingBatchIfNeeded (options: {
  videoUUID: string
  isNewVideo: boolean
}) {
  const { sequelizeTypescript } = await import('@server/initializers/database.js')

  return publishVideoAfterFirstTranscodingBatchIfNeededWithDeps(options, {
    loadVideo: (id, transaction) => VideoModel.loadFull(id, transaction as any),
    runTransaction: fn => sequelizeTypescript.transaction(transaction => fn(transaction))
  })
}
