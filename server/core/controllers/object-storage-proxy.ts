import { CONFIG } from '@server/initializers/config.js'
import { OBJECT_STORAGE_PROXY_PATHS } from '@server/initializers/constants.js'
import {
  generatePresignedRedirect,
  validateProxyToken,
  getObjectContent,
  getCachedHLSPlaylistResponse,
  ObjectStoragePublicFileType
} from '@server/lib/object-storage/presigned-redirect.js'
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

// Public presigned URL redirect endpoints
objectStorageProxyRouter.get(
  OBJECT_STORAGE_PROXY_PATHS.PUBLIC.THUMBNAILS + ':key(*)',
  asyncMiddleware(presignedRedirectController('thumbnails'))
)

objectStorageProxyRouter.get(
  OBJECT_STORAGE_PROXY_PATHS.PUBLIC.STORYBOARDS + ':key(*)',
  asyncMiddleware(presignedRedirectController('storyboards'))
)

objectStorageProxyRouter.get(
  OBJECT_STORAGE_PROXY_PATHS.PUBLIC.WEB_VIDEOS + ':key(*)',
  asyncMiddleware(presignedRedirectController('web-videos'))
)

// HLS m3u8 proxy with presigned segment URLs (must be before :key(*) catch-all)
// Uses regex to match path: /content/public/streaming-playlists/hls/<videoUUID>/<playlistName>
// Supports ?expires=<token> query parameter for expiration validation
objectStorageProxyRouter.get(
  OBJECT_STORAGE_PROXY_PATHS.PUBLIC.STREAMING_PLAYLISTS + 'hls/:playlistPath(*)',
  asyncMiddleware(hlsProxyController)
)

// Segment proxy (legacy format - kept for backward compatibility)
// Segments now use direct S3 presigned URLs from transformM3U8ToProxy
objectStorageProxyRouter.get(
  OBJECT_STORAGE_PROXY_PATHS.PUBLIC.STREAMING_PLAYLISTS + 'segment/:masterKey/:segmentIndex',
  asyncMiddleware(segmentPresignedRedirectController)
)

objectStorageProxyRouter.get(
  OBJECT_STORAGE_PROXY_PATHS.PUBLIC.STREAMING_PLAYLISTS + ':key(*)',
  asyncMiddleware(presignedRedirectController('streaming-playlists'))
)

objectStorageProxyRouter.get(
  OBJECT_STORAGE_PROXY_PATHS.PUBLIC.TORRENTS + ':key(*)',
  asyncMiddleware(presignedRedirectController('torrents'))
)

objectStorageProxyRouter.get(
  OBJECT_STORAGE_PROXY_PATHS.PUBLIC.CAPTIONS + ':key(*)',
  asyncMiddleware(presignedRedirectController('captions'))
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

// ---------------------------------------------------------------------------
// Public presigned URL redirect

function presignedRedirectController (fileType: ObjectStoragePublicFileType) {
  return async (req: express.Request, res: express.Response) => {
    const key = req.params.key

    return generatePresignedRedirect({ res, key, fileType })
  }
}

// ---------------------------------------------------------------------------
// HLS Proxy with presigned segment URLs and expiration validation

async function hlsProxyController (req: express.Request, res: express.Response) {
  const { playlistPath } = req.params
  const expiresParam = req.query.expires as string

  // Validate expiration - token is required
  if (!expiresParam) {
    return res.status(403).json({ error: 'Proxy token required' })
  }

  // Token path is the generic URL path that both sides can compute independently
  // Format: /content/public/streaming-playlists/hls/<uuid>/<filename>
  // Route pattern: PROXY_PATH + 'hls/:playlistPath(*)'
  // For URL /content/public/streaming-playlists/hls/uuid/file.m3u8
  // Express captures: playlistPath = "uuid/file.m3u8" (after the literal "hls/")
  // So we reconstruct: proxyPath + "hls/" + playlistPath = /.../hls/uuid/file.m3u8
  const proxyPath = OBJECT_STORAGE_PROXY_PATHS.PUBLIC.STREAMING_PLAYLISTS
  const tokenPath = `${proxyPath}hls/${playlistPath}`
  const validation = validateProxyToken(expiresParam, tokenPath)
  if (!validation.valid) {
    return res.status(403).json({ error: 'Link expired or invalid' })
  }

  // Decode the path (e.g., "uuid/playlist.m3u8")
  const decodedPath = decodeURIComponent(playlistPath)
  const lastSlash = decodedPath.lastIndexOf('/')

  if (lastSlash === -1) {
    return res.status(400).json({ error: 'Invalid playlist path' })
  }

  const videoUUID = decodedPath.substring(0, lastSlash)
  const playlistName = decodedPath.substring(lastSlash + 1)
  // S3 key format: "hls/<uuid>/<filename>" (without streaming-playlists prefix)
  // The bucket prefix will be added by getObjectContent via buildKey
  const playlistKey = `hls/${videoUUID}/${playlistName}`

  // Use timeout from config (request_timeout in seconds, converted to ms)
  const timeoutMs = CONFIG.OBJECT_STORAGE.PROXY?.REQUEST_TIMEOUT_MS

  if (playlistName.endsWith('.m3u8')) {
    // Cache only the expensive transformed response. Token validation remains above
    // the cache lookup, so a cached playlist never bypasses access validation.
    const transformed = await getCachedHLSPlaylistResponse({
      playlistKey,
      getContent: () => getObjectContent({
        key: playlistKey,
        fileType: 'streaming-playlists',
        timeoutMs
      })
    })

    if (transformed === null) {
      return res.status(504).json({ error: 'Failed to fetch playlist from object storage' })
    }

    const expiresInSeconds = 3600 * CONFIG.OBJECT_STORAGE.PRESIGNED_PUBLIC_URLS_EXPIRATION_HOURS
    res.set('Cache-Control', `public, max-age=${expiresInSeconds}`)
    return res.set('content-type', 'application/x-mpegurl; charset=utf-8').send(transformed).end()
  }

  // Keep the legacy non-playlist route behaviour unchanged.
  const content = await getObjectContent({
    key: playlistKey,
    fileType: 'streaming-playlists',
    timeoutMs
  })

  if (!content) {
    return res.status(504).json({ error: 'Failed to fetch playlist from object storage' })
  }

  // Return non-playlist content as-is.
  const expiresInSeconds = 3600 * CONFIG.OBJECT_STORAGE.PRESIGNED_PUBLIC_URLS_EXPIRATION_HOURS
  res.set('Cache-Control', `public, max-age=${expiresInSeconds}`)
  return res.set('content-type', 'application/x-mpegurl; charset=utf-8').send(content).end()
}

async function segmentPresignedRedirectController (req: express.Request, res: express.Response) {
  const { masterKey, segmentIndex } = req.params
  const tokenParam = req.query.token as string

  // Validate expiration - token is required
  if (!tokenParam) {
    return res.status(403).json({ error: 'Proxy token required' })
  }

  // Validate token with masterKey path
  const validation = validateProxyToken(tokenParam, `segment:${masterKey}`)
  if (!validation.valid) {
    return res.status(403).json({ error: 'Link expired or invalid' })
  }

  const index = parseInt(segmentIndex.replace('segment-', ''), 10)

  const decodedKey = decodeURIComponent(masterKey)
  // Use timeout from config (request_timeout in seconds, converted to ms)
  const timeoutMs = CONFIG.OBJECT_STORAGE.PROXY?.REQUEST_TIMEOUT_MS
  const content = await getObjectContent({
    key: decodedKey,
    fileType: 'streaming-playlists',
    timeoutMs
  })

  if (!content) {
    return res.status(504).json({ error: 'Failed to fetch playlist from object storage' })
  }

  const lines = content.toString('utf-8').split('\n')
  const segmentKeys: string[] = []

  for (const line of lines) {
    const trimmed = line.trim()
    if (trimmed.endsWith('.ts') || trimmed.endsWith('.mp4') || trimmed.endsWith('.webm')) {
      segmentKeys.push(trimmed)
    }
  }

  if (index < 0 || index >= segmentKeys.length) {
    return res.status(404).json({ error: 'Segment not found' })
  }

  const segmentKey = segmentKeys[index]
  return generatePresignedRedirect({ res, key: segmentKey, fileType: 'streaming-playlists' })
}
