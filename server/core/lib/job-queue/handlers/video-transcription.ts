import { VideoTranscriptionPayload } from '@peertube/peertube-models'
import { generateSubtitle } from '@server/lib/video-captions.js'
import { Job } from 'bullmq'
import { logger, loggerTagsFactory } from '../../../helpers/logger.js'
import { VideoModel } from '../../../models/video/video.js'

const lTags = loggerTagsFactory('transcription')

export async function processVideoTranscription (job: Job) {
  const payload = job.data as VideoTranscriptionPayload

  logger.info('Processing video transcription in job %s.', job.id)

  const video = await VideoModel.load(payload.videoUUID)
  if (!video) {
    logger.info('Transcription job %s cancelled: video %s does not exist (video was deleted).', job.id, payload.videoUUID, lTags(payload.videoUUID))
    throw new Error('Video was deleted - transcoding job cancelled')
  }

  return generateSubtitle({ video })
}
