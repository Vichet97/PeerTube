import { FileStorage, VideoState, VideoStateType } from '@peertube/peertube-models'
import { retryTransactionWrapper } from '@server/helpers/database-utils.js'
import { logger, loggerTagsFactory } from '@server/helpers/logger.js'
import { CONFIG } from '@server/initializers/config.js'
import { sequelizeTypescript } from '@server/initializers/database.js'
import { VideoJobInfoModel } from '@server/models/video/video-job-info.js'
import { VideoModel } from '@server/models/video/video.js'
import { MVideo, MVideoFullLight, MVideoUUID } from '@server/types/models/index.js'
import { Transaction } from 'sequelize'
import { federateVideoIfNeeded } from './activitypub/videos/index.js'
import { JobQueue } from './job-queue/index.js'
import { hasVideoResourcesToBeMoved } from './move-storage/shared/move-video.js'
import { Notifier } from './notifier/index.js'
import { buildGranularMoveJobs, buildMoveVideoJob } from './video-jobs.js'

const lTags = loggerTagsFactory('video-state')

export function buildNextVideoState (currentState?: VideoStateType) {
  if (currentState === VideoState.PUBLISHED) {
    throw new Error('Video is already in its final state')
  }

  // Move succeeded after previous failure: next state is PUBLISHED
  if (
    currentState === VideoState.TO_MOVE_TO_EXTERNAL_STORAGE_FAILED ||
    currentState === VideoState.TO_MOVE_TO_FILE_SYSTEM_FAILED
  ) {
    return VideoState.PUBLISHED
  }

  if (
    currentState !== VideoState.TO_EDIT &&
    currentState !== VideoState.TO_TRANSCODE &&
    currentState !== VideoState.TO_MOVE_TO_EXTERNAL_STORAGE &&
    currentState !== VideoState.TO_MOVE_TO_FILE_SYSTEM &&
    CONFIG.TRANSCODING.ENABLED
  ) {
    return VideoState.TO_TRANSCODE
  }

  if (
    currentState !== VideoState.TO_MOVE_TO_EXTERNAL_STORAGE &&
    currentState !== VideoState.TO_MOVE_TO_FILE_SYSTEM &&
    CONFIG.OBJECT_STORAGE.ENABLED
  ) {
    return VideoState.TO_MOVE_TO_EXTERNAL_STORAGE
  }

  return VideoState.PUBLISHED
}

export function moveToNextState (options: {
  video: MVideoUUID
  previousVideoState?: VideoStateType
  isNewVideo?: boolean // Default true
}) {
  const { video, previousVideoState, isNewVideo = true } = options

  return retryTransactionWrapper(() => {
    return sequelizeTypescript.transaction(async t => {
      // Maybe the video changed in database, refresh it
      const videoDatabase = await VideoModel.loadFull(video.uuid, t)
      // Video does not exist anymore
      if (!videoDatabase) return undefined

      // Already in its final state
      if (videoDatabase.state === VideoState.PUBLISHED) {
        await federateVideoIfNeeded(videoDatabase, false, t)

        const queuedMoveJobs = await enqueueMissingObjectStorageMoveJobsForPublishedVideo({
          video: videoDatabase,
          isNewVideo,
          previousVideoState
        })

        logger.debug(`Video ${videoDatabase.uuid} is already published, no state change.`, lTags(videoDatabase.uuid))
        if (queuedMoveJobs > 0) {
          logger.info(
            '[MOVE_JOB] Video %s already published but queued %d missing object-storage move job(s).',
            videoDatabase.uuid,
            queuedMoveJobs
          )
        }

        return false
      }

      const newState = buildNextVideoState(videoDatabase.state)

      if (newState === VideoState.PUBLISHED) {
        await moveToPublishedState({ video: videoDatabase, previousVideoState, isNewVideo, transaction: t })
        return true
      }

      if (newState === VideoState.TO_MOVE_TO_EXTERNAL_STORAGE) {
        await moveToExternalStorageState({ video: videoDatabase, isNewVideo, transaction: t })
        return true
      }

      throw new Error('Unknown next state for video ' + videoDatabase.uuid + ': ' + newState)
    })
  })
}

// ---------------------------------------------------------------------------

export async function moveToExternalStorageState (options: {
  video: MVideoFullLight
  isNewVideo: boolean
  transaction: Transaction
}) {
  const { video, isNewVideo, transaction } = options

  const previousVideoState = video.state

  if (video.state !== VideoState.TO_MOVE_TO_EXTERNAL_STORAGE) {
    await video.setNewState(VideoState.TO_MOVE_TO_EXTERNAL_STORAGE, isNewVideo, transaction)
  }

  // DEBUG: Log state transition and job creation
  logger.debug(`[DEBUG] moveToExternalStorageState for ${video.uuid}`, {
    previousVideoState,
    newVideoState: VideoState.TO_MOVE_TO_EXTERNAL_STORAGE,
    isNewVideo,
    objectStorageEnabled: CONFIG.OBJECT_STORAGE.ENABLED
  })

  logger.info('Creating granular external storage move jobs for video %s.', video.uuid, lTags(video.uuid))

  try {
    // Create granular move jobs (per file/playlist) instead of one monolithic job
    const jobs = await buildGranularMoveJobs({
      videoUUID: video.uuid,
      isNewVideo,
      previousVideoState
    })

    if (jobs.length > 0) {
      // Increment pendingMove once per job that was actually created
      // (duplicate jobs were already filtered out by the builders)
      await VideoJobInfoModel.increaseOrCreate(video.uuid, 'pendingMove', jobs.length)

      logger.info('[MOVE_JOB] Created %d granular move jobs for %s (pendingMove now reflects job count)', jobs.length, video.uuid)
      for (const job of jobs) {
        await JobQueue.Instance.createJob(job)
      }
    } else {
      // No granular jobs. We can still have leftover resources like local torrents,
      // so fallback to the legacy move job when needed.
      const hasFallbackResources = await hasVideoResourcesToBeMoved(video, FileStorage.OBJECT_STORAGE)

      if (hasFallbackResources) {
        const fallbackJob = await buildMoveVideoJob({
          type: 'move-to-object-storage',
          video,
          moveVideoState: {
            previousVideoState,
            isNewVideo
          }
        })

        if (fallbackJob) {
          logger.info('[MOVE_JOB] No granular jobs for %s, created fallback move-to-object-storage job.', video.uuid)
          await JobQueue.Instance.createJob(fallbackJob)
        } else {
          logger.info('[MOVE_JOB] Fallback move job already pending/active for %s, skipping duplicate.', video.uuid)
        }

        return true
      }

      logger.info('[MOVE_JOB] No files to move for %s, transitioning to published', video.uuid)
      await moveToNextState({ video: { uuid: video.uuid }, isNewVideo, previousVideoState })
    }

    return true
  } catch (err) {
    logger.error('Cannot add move to object storage jobs', { err, ...lTags(video.uuid) })

    return false
  }
}

async function enqueueMissingObjectStorageMoveJobsForPublishedVideo (options: {
  video: MVideoFullLight
  isNewVideo: boolean
  previousVideoState?: VideoStateType
}) {
  const { video, isNewVideo, previousVideoState } = options
  if (!CONFIG.OBJECT_STORAGE.ENABLED) return 0

  const jobs = await buildGranularMoveJobs({
    videoUUID: video.uuid,
    isNewVideo,
    previousVideoState: previousVideoState ?? video.state
  })

  if (jobs.length === 0) {
    const hasFallbackResources = await hasVideoResourcesToBeMoved(video, FileStorage.OBJECT_STORAGE)
    if (!hasFallbackResources) return 0

    const fallbackJob = await buildMoveVideoJob({
      type: 'move-to-object-storage',
      video: { uuid: video.uuid }
    })

    if (!fallbackJob) return 0

    await JobQueue.Instance.createJob(fallbackJob)
    return 1
  }

  await VideoJobInfoModel.increaseOrCreate(video.uuid, 'pendingMove', jobs.length)

  for (const job of jobs) {
    await JobQueue.Instance.createJob(job)
  }

  return jobs.length
}

export async function moveToFileSystemState (options: {
  video: MVideoFullLight
  isNewVideo: boolean
  transaction: Transaction
}) {
  const { video, isNewVideo, transaction } = options

  const previousVideoState = video.state

  if (video.state !== VideoState.TO_MOVE_TO_FILE_SYSTEM) {
    await video.setNewState(VideoState.TO_MOVE_TO_FILE_SYSTEM, false, transaction)
  }

  logger.info('Creating move to file system job for video %s.', video.uuid, { tags: [ video.uuid ] })

  try {
    const job = await buildMoveVideoJob({
      type: 'move-to-file-system',
      video,
      moveVideoState: {
        previousVideoState,
        isNewVideo
      }
    })

    if (!job) {
      // Job was skipped due to existing pending/active job
      return true
    }

    await JobQueue.Instance.createJob(job)

    return true
  } catch (err) {
    logger.error('Cannot add move to file system job', { err, ...lTags(video.uuid) })

    return false
  }
}

// ---------------------------------------------------------------------------

export function moveToFailedTranscodingState (video: MVideo) {
  if (video.state === VideoState.TRANSCODING_FAILED) return

  return video.setNewState(VideoState.TRANSCODING_FAILED, false, undefined)
}

export function moveToFailedMoveToObjectStorageState (video: MVideo) {
  if (video.state === VideoState.TO_MOVE_TO_EXTERNAL_STORAGE_FAILED) return

  return video.setNewState(VideoState.TO_MOVE_TO_EXTERNAL_STORAGE_FAILED, false, undefined)
}

export function moveToFailedMoveToFileSystemState (video: MVideo) {
  if (video.state === VideoState.TO_MOVE_TO_FILE_SYSTEM_FAILED) return

  return video.setNewState(VideoState.TO_MOVE_TO_FILE_SYSTEM_FAILED, false, undefined)
}

// ---------------------------------------------------------------------------
// Private
// ---------------------------------------------------------------------------

async function moveToPublishedState (options: {
  video: MVideoFullLight
  isNewVideo: boolean
  transaction: Transaction
  previousVideoState?: VideoStateType
}) {
  const { video, isNewVideo, transaction, previousVideoState } = options
  const previousState = previousVideoState ?? video.state

  logger.info('Publishing video %s.', video.uuid, { isNewVideo, previousState, ...lTags(video.uuid) })

  await video.setNewState(VideoState.PUBLISHED, isNewVideo, transaction)

  await federateVideoIfNeeded(video, isNewVideo, transaction)

  if (previousState === VideoState.TO_EDIT) {
    Notifier.Instance.notifyOfFinishedVideoStudioEdition(video)
    return
  }

  if (isNewVideo) {
    Notifier.Instance.notifyOnNewVideoOrLiveIfNeeded(video)

    if (previousState === VideoState.TO_TRANSCODE) {
      Notifier.Instance.notifyOnVideoPublishedAfterTranscoding(video)
    }
  }
}
