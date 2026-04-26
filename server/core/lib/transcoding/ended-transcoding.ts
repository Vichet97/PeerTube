import { retryTransactionWrapper } from '@server/helpers/database-utils.js'
import { logger } from '@server/helpers/logger.js'
import { CONFIG } from '@server/initializers/config.js'
import { VideoJobInfoModel } from '@server/models/video/video-job-info.js'
import { MVideo } from '@server/types/models/index.js'
import { moveToNextState } from '../video-state.js'

// NOTE: onTranscodingEnded only triggers the state machine.
// moveToNextState may queue object-storage move jobs when needed (including when
// a video is already published and new files still need to be moved).
export async function onTranscodingEnded (options: {
  video: MVideo
  isNewVideo: boolean
  moveVideoToNextState: boolean
}) {
  const { video, isNewVideo, moveVideoToNextState } = options

  await VideoJobInfoModel.decrease(video.uuid, 'pendingTranscode')

  logger.info('[TRANSCODE_END] Transcoding ended for video %s, moveToNextState=%s', video.uuid, moveVideoToNextState)

  if (moveVideoToNextState) {
    // Trigger the state machine. The state machine (moveToNextState) will decide
    // the next state and create move jobs accordingly (via moveToExternalStorageState).
    // We do NOT create move jobs here to avoid double-creation when video is already published.
    const changedState = await retryTransactionWrapper(moveToNextState, { video, isNewVideo })

    logger.info('[TRANSCODE_END] State change for %s: changedState=%s, isNewVideo=%s', video.uuid, changedState, isNewVideo)

    if (!changedState && !CONFIG.OBJECT_STORAGE.ENABLED) {
      logger.info('[TRANSCODE_END] Video %s already published and object storage disabled, no further action needed', video.uuid)
    }
  } else {
    logger.info('[TRANSCODE_END] moveVideoToNextState=false for %s, skipping state transition', video.uuid)
  }
}
