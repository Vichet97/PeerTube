import { FederateVideoPayload } from '@peertube/peertube-models'
import { retryTransactionWrapper } from '@server/helpers/database-utils.js'
import { sequelizeTypescript } from '@server/initializers/database.js'
import { federateVideoIfNeeded } from '@server/lib/activitypub/videos/index.js'
import { VideoModel } from '@server/models/video/video.js'
import { Job } from 'bullmq'
import { logger } from '../../../helpers/logger.js'

export function processFederateVideo (job: Job) {
  const payload = job.data as FederateVideoPayload

  logger.info('Processing video federation in job %s.', job.id)

  return retryTransactionWrapper(() => {
    return sequelizeTypescript.transaction(async t => {
      const video = await VideoModel.loadFull(payload.videoUUID, t)
      if (!video) {
        logger.info('Federate video job %s cancelled: video %s does not exist (video was deleted).', job.id, payload.videoUUID)
        throw new Error('Video was deleted - transcoding job cancelled')
      }

      return federateVideoIfNeeded(video, payload.isNewVideoForFederation, t)
    })
  })
}
