import { HttpStatusCode, VideoCaptionGenerate, VideoCaptionImport, VideoChannelActivityAction } from '@peertube/peertube-models'
import { buildSUUID } from '@peertube/peertube-node-utils'
import { retryTransactionWrapper } from '@server/helpers/database-utils.js'
import { doRequestAndSaveToFile } from '@server/helpers/requests.js'
import { Hooks } from '@server/lib/plugins/hooks.js'
import { createLocalCaption, createTranscriptionTaskIfNeeded, updateHLSMasterOnCaptionChangeIfNeeded } from '@server/lib/video-captions.js'
import { VideoJobInfoModel } from '@server/models/video/video-job-info.js'
import express from 'express'
import { remove } from 'fs-extra/esm'
import { join } from 'path'
import { createReqFiles } from '../../../helpers/express-utils.js'
import { logger, loggerTagsFactory } from '../../../helpers/logger.js'
import { getFormattedObjects } from '../../../helpers/utils.js'
import { CONSTRAINTS_FIELDS, MIMETYPES } from '../../../initializers/constants.js'
import { CONFIG } from '../../../initializers/config.js'
import { sequelizeTypescript } from '../../../initializers/database.js'
import { federateVideoIfNeeded } from '../../../lib/activitypub/videos/index.js'
import { cleanupStagedTranscriptionAudio } from '../../../lib/transcription-audio-staging.js'
import { asyncMiddleware, asyncRetryTransactionMiddleware, authenticate } from '../../../middlewares/index.js'
import { isCaptionFileValid } from '../../../helpers/custom-validators/video-captions.js'
import {
  addVideoCaptionImportValidator,
  addVideoCaptionValidator,
  deleteVideoCaptionValidator,
  generateVideoCaptionValidator,
  listVideoCaptionsValidator
} from '../../../middlewares/validators/index.js'
import { VideoCaptionModel } from '../../../models/video/video-caption.js'
import { VideoChannelActivityModel } from '@server/models/video/video-channel-activity.js'

const lTags = loggerTagsFactory('api', 'video-caption')

const reqVideoCaptionAdd = createReqFiles([ 'captionfile' ], MIMETYPES.VIDEO_CAPTIONS.MIMETYPE_EXT)

const videoCaptionsRouter = express.Router()

videoCaptionsRouter.post(
  '/:videoId/captions/generate',
  authenticate,
  asyncMiddleware(generateVideoCaptionValidator),
  asyncMiddleware(createGenerateVideoCaption)
)

videoCaptionsRouter.get('/:videoId/captions', asyncMiddleware(listVideoCaptionsValidator), asyncMiddleware(listVideoCaptions))

videoCaptionsRouter.post(
  '/:videoId/captions/import',
  authenticate,
  asyncMiddleware(addVideoCaptionImportValidator),
  asyncMiddleware(createVideoCaptionFromImport)
)

videoCaptionsRouter.put(
  '/:videoId/captions/:captionLanguage',
  authenticate,
  reqVideoCaptionAdd,
  asyncMiddleware(addVideoCaptionValidator),
  asyncMiddleware(createVideoCaption)
)

videoCaptionsRouter.delete(
  '/:videoId/captions/:captionLanguage',
  authenticate,
  asyncMiddleware(deleteVideoCaptionValidator),
  asyncRetryTransactionMiddleware(deleteVideoCaption)
)

// ---------------------------------------------------------------------------

export {
  videoCaptionsRouter
}

// ---------------------------------------------------------------------------

async function createGenerateVideoCaption (req: express.Request, res: express.Response) {
  const video = res.locals.videoAll

  const body = req.body as VideoCaptionGenerate
  if (body.forceTranscription === true) {
    await VideoJobInfoModel.abortAllTasks(video.uuid, 'pendingTranscription')
    await cleanupStagedTranscriptionAudio(video.uuid)
  }

  await createTranscriptionTaskIfNeeded(video)

  return res.sendStatus(HttpStatusCode.NO_CONTENT_204)
}

async function listVideoCaptions (req: express.Request, res: express.Response) {
  const data = await VideoCaptionModel.listVideoCaptions(res.locals.onlyVideo.id)

  return res.json(await getFormattedObjects(data, data.length))
}

async function createVideoCaptionFromImport (req: express.Request, res: express.Response) {
  const body = req.body as VideoCaptionImport
  const video = res.locals.videoAll
  const { targetUrl, language, customHeaders } = body

  const bodyKBLimit = Math.ceil(CONSTRAINTS_FIELDS.VIDEO_CAPTIONS.CAPTION_FILE.FILE_SIZE.max / 1000)
  const ext = targetUrl.toLowerCase().endsWith('.srt') ? '.srt' : '.vtt'
  const tmpPath = join(CONFIG.STORAGE.TMP_DIR, `caption-import-${buildSUUID()}${ext}`)

  try {
    const requestHeaders = customHeaders && Object.keys(customHeaders).length > 0
      ? { ...customHeaders }
      : undefined

    await doRequestAndSaveToFile(targetUrl, tmpPath, {
      headers: requestHeaders,
      bodyKBLimit,
      timeout: 30000
    })

    if (!await isCaptionFileValid(tmpPath)) {
      return res.fail({
        status: HttpStatusCode.BAD_REQUEST_400,
        message: 'The downloaded file is not a valid VTT or SRT caption file'
      })
    }

    const videoCaption = await createLocalCaption({
      video,
      language,
      path: tmpPath,
      automaticallyGenerated: false
    })

    if (videoCaption.m3u8Filename) {
      await updateHLSMasterOnCaptionChangeIfNeeded(video)
    }

    await retryTransactionWrapper(() => {
      return sequelizeTypescript.transaction(async t => {
        await VideoChannelActivityModel.addVideoActivity({
          action: VideoChannelActivityAction.UPDATE_CAPTIONS,
          user: res.locals.oauth.token.User,
          channel: video.VideoChannel,
          video,
          transaction: t
        })

        return federateVideoIfNeeded(video, false, t)
      })
    })

    Hooks.runAction('action:api.video-caption.created', { caption: videoCaption, req, res })

    logger.info('Video caption %s imported from URL for video %s.', language, video.uuid, lTags(video.uuid))

    return res.status(HttpStatusCode.NO_CONTENT_204).end()
  } finally {
    await remove(tmpPath).catch(() => {})
  }
}

async function createVideoCaption (req: express.Request, res: express.Response) {
  const videoCaptionPhysicalFile: Express.Multer.File = req.files['captionfile'][0]
  const video = res.locals.videoAll

  const captionLanguage = req.params.captionLanguage

  const videoCaption = await createLocalCaption({
    video,
    language: captionLanguage,
    path: videoCaptionPhysicalFile.path,
    automaticallyGenerated: false
  })

  if (videoCaption.m3u8Filename) {
    await updateHLSMasterOnCaptionChangeIfNeeded(video)
  }

  await retryTransactionWrapper(() => {
    return sequelizeTypescript.transaction(async t => {
      await VideoChannelActivityModel.addVideoActivity({
        action: VideoChannelActivityAction.UPDATE_CAPTIONS,
        user: res.locals.oauth.token.User,
        channel: video.VideoChannel,
        video,
        transaction: t
      })

      return federateVideoIfNeeded(video, false, t)
    })
  })

  Hooks.runAction('action:api.video-caption.created', { caption: videoCaption, req, res })

  return res.status(HttpStatusCode.NO_CONTENT_204).end()
}

async function deleteVideoCaption (req: express.Request, res: express.Response) {
  const video = res.locals.videoAll
  const videoCaption = res.locals.videoCaption
  const hasM3U8 = !!videoCaption.m3u8Filename

  await sequelizeTypescript.transaction(async t => {
    await videoCaption.destroy({ transaction: t })
  })

  if (hasM3U8) {
    await updateHLSMasterOnCaptionChangeIfNeeded(video)
  }

  await retryTransactionWrapper(() => {
    return sequelizeTypescript.transaction(async t => {
      await VideoChannelActivityModel.addVideoActivity({
        action: VideoChannelActivityAction.UPDATE_CAPTIONS,
        user: res.locals.oauth.token.User,
        channel: video.VideoChannel,
        video,
        transaction: t
      })

      return federateVideoIfNeeded(video, false, t)
    })
  })

  logger.info('Video caption %s of video %s deleted.', videoCaption.language, video.uuid, lTags(video.uuid))

  Hooks.runAction('action:api.video-caption.deleted', { caption: videoCaption, req, res })

  return res.type('json').status(HttpStatusCode.NO_CONTENT_204).end()
}
