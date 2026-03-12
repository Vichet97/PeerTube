import { OBJECT_STORAGE_PROXY_PATHS } from '@server/initializers/constants.js'
import { proxifyCaption, proxifyHLS, proxifyStoryboard, proxifyThumbnail, proxifyWebVideoFile } from '@server/lib/object-storage/index.js'
import {
  asyncMiddleware,
  ensureCanAccessPrivateVideoHLSFiles,
  ensureCanAccessVideoPrivateWebVideoFiles,
  ensurePrivateObjectStorageProxyIsEnabled,
  optionalAuthenticate
} from '@server/middlewares/index.js'
import cors from 'cors'
import express from 'express'
import { doReinjectVideoFileToken } from './shared/m3u8-playlist.js'

const objectStorageProxyRouter = express.Router()

objectStorageProxyRouter.use(cors())

objectStorageProxyRouter.get(
  [ OBJECT_STORAGE_PROXY_PATHS.PRIVATE_WEB_VIDEOS + ':filename', OBJECT_STORAGE_PROXY_PATHS.LEGACY_PRIVATE_WEB_VIDEOS + ':filename' ],
  ensurePrivateObjectStorageProxyIsEnabled,
  optionalAuthenticate,
  asyncMiddleware(ensureCanAccessVideoPrivateWebVideoFiles),
  asyncMiddleware(proxifyWebVideoController)
)

objectStorageProxyRouter.get(
  OBJECT_STORAGE_PROXY_PATHS.STREAMING_PLAYLISTS.PRIVATE_HLS + ':videoUUID/:filename',
  ensurePrivateObjectStorageProxyIsEnabled,
  optionalAuthenticate,
  asyncMiddleware(ensureCanAccessPrivateVideoHLSFiles),
  asyncMiddleware(proxifyHLSController)
)

objectStorageProxyRouter.get(
  OBJECT_STORAGE_PROXY_PATHS.THUMBNAILS.PRIVATE + ':videoUUID/:filename',
  ensurePrivateObjectStorageProxyIsEnabled,
  asyncMiddleware(proxifyThumbnailController)
)

objectStorageProxyRouter.get(
  OBJECT_STORAGE_PROXY_PATHS.STORYBOARDS.PRIVATE + ':videoUUID/:filename',
  ensurePrivateObjectStorageProxyIsEnabled,
  asyncMiddleware(proxifyStoryboardController)
)

objectStorageProxyRouter.get(
  OBJECT_STORAGE_PROXY_PATHS.CAPTIONS.PRIVATE + ':videoUUID/:filename',
  ensurePrivateObjectStorageProxyIsEnabled,
  asyncMiddleware(proxifyCaptionController)
)

// ---------------------------------------------------------------------------

export {
  objectStorageProxyRouter
}

function proxifyWebVideoController (req: express.Request, res: express.Response) {
  const filename = req.params.filename

  return proxifyWebVideoFile({ req, res, filename })
}

function proxifyHLSController (req: express.Request, res: express.Response) {
  const video = res.locals.onlyVideo
  const filename = req.params.filename

  const reinjectVideoFileToken = filename.endsWith('.m3u8') && doReinjectVideoFileToken(req)

  return proxifyHLS({
    req,
    res,
    video,
    filename,
    reinjectVideoFileToken
  })
}

function proxifyThumbnailController (req: express.Request, res: express.Response) {
  const filename = req.params.filename

  return proxifyThumbnail({ req, res, filename })
}

function proxifyStoryboardController (req: express.Request, res: express.Response) {
  const filename = req.params.filename

  return proxifyStoryboard({ req, res, filename })
}

function proxifyCaptionController (req: express.Request, res: express.Response) {
  const filename = req.params.filename

  return proxifyCaption({ req, res, filename })
}
