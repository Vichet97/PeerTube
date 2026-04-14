import { isMoveCaptionPayload, isMoveVideoStoragePayload, MoveStoragePayload } from '@peertube/peertube-models'
import { logger, loggerTagsFactory } from '@server/helpers/logger.js'
import {
  moveCaptionToObjectStorage,
  moveVideoToObjectStorage,
  onMoveVideoToObjectStorageFailure
} from '@server/lib/move-storage/move-to-object-storage.js'
import { VideoModel } from '@server/models/video/video.js'
import { Job } from 'bullmq'

const lTagsBase = loggerTagsFactory('move-object-storage')

export async function processMoveToObjectStorage (job: Job) {
  const payload = job.data as MoveStoragePayload

  if (isMoveVideoStoragePayload(payload)) { // Move all video related files
    const video = await VideoModel.loadWithFiles(payload.videoUUID)
    if (!video) {
      logger.info('Move-to-object-storage job %s cancelled: video %s does not exist (video was deleted).', job.id, payload.videoUUID, lTagsBase(payload.videoUUID))
      throw new Error('Video was deleted - transcoding job cancelled')
    }

    logger.info(`Moving video ${payload.videoUUID} to object storage in job ${job.id}`, lTagsBase(payload.videoUUID))

    // Determine moveVideoState
    // When retrying a failed job, use the video's CURRENT state as previousVideoState
    // so that buildNextVideoState can correctly transition to PUBLISHED
    let moveVideoState = payload.moveVideoState

    if (moveVideoState && payload.isNewVideo === undefined) {
      // This is a retry of a failed job (not a new job creation)
      // Use the current video state as previousVideoState to ensure proper state transition
      moveVideoState = {
        ...moveVideoState,
        previousVideoState: video.state
      }
    } else if (payload.isNewVideo !== undefined) {
      moveVideoState = {
        isNewVideo: payload.isNewVideo,
        previousVideoState: payload.previousVideoState
      }
    }

    await moveVideoToObjectStorage({
      videoUUID: payload.videoUUID,
      moveVideoState,
      loggerTags: lTagsBase().tags
    })
  } else if (isMoveCaptionPayload(payload)) { // Only caption file
    logger.info(`Moving video caption ${payload.captionId} to object storage in job ${job.id}.`, lTagsBase(payload.captionId))

    return moveCaptionToObjectStorage({
      captionId: payload.captionId,
      loggerTags: lTagsBase().tags
    })
  } else {
    throw new Error('Unknown payload type')
  }
}

export async function onMoveToObjectStorageFailure (job: Job, err: any) {
  const payload = job.data as MoveStoragePayload

  if (!isMoveVideoStoragePayload(payload)) return

  await onMoveVideoToObjectStorageFailure({
    videoUUID: payload.videoUUID,
    err,
    loggerTags: lTagsBase().tags
  })
}
