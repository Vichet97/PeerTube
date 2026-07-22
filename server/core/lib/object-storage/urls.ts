import { logger } from '@server/helpers/logger.js'
import { CONFIG } from '@server/initializers/config.js'
import { OBJECT_STORAGE_PROXY_PATHS, WEBSERVER } from '@server/initializers/constants.js'
import { MVideoUUID } from '@server/types/models/index.js'
import { buildKey, getEndpoint, getReadClient, getReadEndpoint, lTags } from './shared/index.js'
import { ObjectStoragePublicFileType, generateProxyToken, keepSignedQueryEncoded } from './presigned-redirect.js'
import { applyReadBucketNameReplacement, getReadBucketNameForSigning, getReadForcePathStyle } from './read-url.js'

// ---------------------------------------------------------------------------

export type { ObjectStoragePublicFileType }

// ---------------------------------------------------------------------------

export async function buildObjectStoragePublicFileUrl (options: {
  bucket: BucketInfo
  key: string
  fileType?: ObjectStoragePublicFileType
}): Promise<string> {
  const { bucket, key, fileType } = options

  if (CONFIG.OBJECT_STORAGE.USE_PRESIGNED_PUBLIC_URLS) {
    // When presigned URLs are required:
    // - streaming-playlists m3u8 files: use proxy URL (will be transformed)
    // - static public assets: use compact PeerTube redirect URLs to avoid embedding many long S3 signatures in API responses
    // - playback URLs: use direct S3 presigned URLs
    if (fileType === 'streaming-playlists' && (key.endsWith('.m3u8') || key.endsWith('.mp4'))) {
      return buildPresignedProxyUrl(fileType, key)
    }

    const directFileType = fileType || 'thumbnails'

    if (shouldUsePublicStaticRedirectUrl(directFileType)) {
      return buildPublicStaticRedirectUrl(directFileType, key)
    }

    // Do not reuse playback URLs or URLs signed by rotating credentials.
    if (directFileType === 'web-videos' || !CONFIG.OBJECT_STORAGE.CREDENTIALS.ACCESS_KEY_ID) {
      return generatePresignedUrlFromFileType(key, directFileType)
    }

    return generateCachedPresignedUrlFromFileType(key, directFileType)
  }

  return applyReadBucketNameReplacement(buildBaseUrl(bucket, 'read') + buildKey(key, bucket), bucket.BUCKET_NAME)
}

// ---------------------------------------------------------------------------

// Generates proxy URL with token (for m3u8 files that need content transformation)
function buildPresignedProxyUrl (fileType: ObjectStoragePublicFileType, key: string): string {
  if (fileType === 'streaming-playlists' && key.startsWith('hls/')) {
    // Key format: "hls/<uuid>/<filename>"
    // Token path is the full generic URL path: /content/public/streaming-playlists/hls/<uuid>/<filename>
    // Both generator and validator use this same path format
    const proxyPath = OBJECT_STORAGE_PROXY_PATHS.PUBLIC.STREAMING_PLAYLISTS
    const tokenPath = `${proxyPath}hls/${key.substring(4)}`
    const token = generateProxyToken(tokenPath)
    return `${WEBSERVER.URL}${proxyPath}${key}?expires=${token}`
  }

  const encodedKey = encodeURIComponent(key)
  const path = `${fileType}:${encodedKey}`
  const token = generateProxyToken(path)
  const pathMap: Record<ObjectStoragePublicFileType, string> = {
    'thumbnails': OBJECT_STORAGE_PROXY_PATHS.PUBLIC.THUMBNAILS,
    'storyboards': OBJECT_STORAGE_PROXY_PATHS.PUBLIC.STORYBOARDS,
    'web-videos': OBJECT_STORAGE_PROXY_PATHS.PUBLIC.WEB_VIDEOS,
    'streaming-playlists': OBJECT_STORAGE_PROXY_PATHS.PUBLIC.STREAMING_PLAYLISTS,
    'torrents': OBJECT_STORAGE_PROXY_PATHS.PUBLIC.TORRENTS,
    'captions': OBJECT_STORAGE_PROXY_PATHS.PUBLIC.CAPTIONS
  }
  return `${WEBSERVER.URL}${pathMap[fileType]}${encodedKey}?expires=${token}`
}

function shouldUsePublicStaticRedirectUrl (fileType: ObjectStoragePublicFileType) {
  return fileType === 'thumbnails' ||
    fileType === 'storyboards' ||
    fileType === 'captions'
}

function buildPublicStaticRedirectUrl (fileType: 'thumbnails' | 'storyboards' | 'captions', key: string) {
  const encodedKey = encodeURIComponent(key)
  const pathMap: Record<typeof fileType, string> = {
    thumbnails: OBJECT_STORAGE_PROXY_PATHS.PUBLIC.THUMBNAILS,
    storyboards: OBJECT_STORAGE_PROXY_PATHS.PUBLIC.STORYBOARDS,
    captions: OBJECT_STORAGE_PROXY_PATHS.PUBLIC.CAPTIONS
  }

  return `${WEBSERVER.URL}${pathMap[fileType]}${encodedKey}`
}

const presignedPublicUrlCache = new Map<string, { expiresAt: number, promise: Promise<string> }>()
const PRESIGNED_PUBLIC_URL_CACHE_MAX_ITEMS = 20_000

function generateCachedPresignedUrlFromFileType (key: string, fileType: ObjectStoragePublicFileType): Promise<string> {
  const cacheKey = `${fileType}:${key}`
  const now = Date.now()
  const cached = presignedPublicUrlCache.get(cacheKey)

  if (cached && cached.expiresAt > now) return cached.promise
  if (cached) presignedPublicUrlCache.delete(cacheKey)

  const expiresInMs = 3600 * CONFIG.OBJECT_STORAGE.PRESIGNED_PUBLIC_URLS_EXPIRATION_HOURS * 1000
  const expiresAt = now + Math.max(0, Math.floor(expiresInMs / 2))

  const promise: Promise<string> = generatePresignedUrlFromFileType(key, fileType)
    .catch(err => {
      if (presignedPublicUrlCache.get(cacheKey)?.promise === promise) {
        presignedPublicUrlCache.delete(cacheKey)
      }

      throw err
    })

  presignedPublicUrlCache.set(cacheKey, { expiresAt, promise })
  prunePresignedPublicUrlCache()

  return promise
}

function prunePresignedPublicUrlCache () {
  while (presignedPublicUrlCache.size > PRESIGNED_PUBLIC_URL_CACHE_MAX_ITEMS) {
    const oldestCacheKey = presignedPublicUrlCache.keys().next().value
    if (!oldestCacheKey) return

    presignedPublicUrlCache.delete(oldestCacheKey)
  }
}

// Generates S3 presigned URL
async function generatePresignedUrlFromFileType (key: string, fileType: ObjectStoragePublicFileType): Promise<string> {
  const bucketInfo = getBucketInfoForFileType(fileType)
  const fullKey = buildKey(key, bucketInfo)

  const { GetObjectCommand } = await import('@aws-sdk/client-s3')
  const { getSignedUrl } = await import('@aws-sdk/s3-request-presigner')

  const command = new GetObjectCommand({
    Bucket: getReadBucketNameForSigning(bucketInfo.BUCKET_NAME),
    Key: fullKey
  })

  const signedUrl = await getSignedUrl(
    await getReadClient(),
    command,
    { expiresIn: 3600 * CONFIG.OBJECT_STORAGE.PRESIGNED_PUBLIC_URLS_EXPIRATION_HOURS }
  )

  return applyReadBucketNameReplacement(keepSignedQueryEncoded(signedUrl), bucketInfo.BUCKET_NAME)
}

function getBucketInfoForFileType (fileType: ObjectStoragePublicFileType) {
  switch (fileType) {
    case 'thumbnails':
      return CONFIG.OBJECT_STORAGE.THUMBNAILS
    case 'storyboards':
      return CONFIG.OBJECT_STORAGE.STORYBOARDS
    case 'web-videos':
      return CONFIG.OBJECT_STORAGE.WEB_VIDEOS
    case 'streaming-playlists':
      return CONFIG.OBJECT_STORAGE.STREAMING_PLAYLISTS
    case 'torrents':
      return CONFIG.OBJECT_STORAGE.TORRENTS
    case 'captions':
      return CONFIG.OBJECT_STORAGE.CAPTIONS
    default:
      return CONFIG.OBJECT_STORAGE.THUMBNAILS
  }
}

// ---------------------------------------------------------------------------

export function buildObjectStorageHLSPrivateFileUrl (video: MVideoUUID, filename: string) {
  return WEBSERVER.URL + OBJECT_STORAGE_PROXY_PATHS.STREAMING_PLAYLISTS.PRIVATE_HLS + video.uuid + `/${filename}`
}

export function buildObjectStorageWebVideoPrivateFileUrl (filename: string) {
  return WEBSERVER.URL + OBJECT_STORAGE_PROXY_PATHS.PRIVATE_WEB_VIDEOS + filename
}

export function buildObjectStorageThumbnailPrivateFileUrl (video: MVideoUUID, filename: string) {
  return WEBSERVER.URL + OBJECT_STORAGE_PROXY_PATHS.THUMBNAILS.PRIVATE + video.uuid + `/${filename}`
}

export function buildObjectStorageStoryboardPrivateFileUrl (video: MVideoUUID, filename: string) {
  return WEBSERVER.URL + OBJECT_STORAGE_PROXY_PATHS.STORYBOARDS.PRIVATE + video.uuid + `/${filename}`
}

export function buildObjectStorageCaptionPrivateFileUrl (video: MVideoUUID, filename: string) {
  return WEBSERVER.URL + OBJECT_STORAGE_PROXY_PATHS.CAPTIONS.PRIVATE + video.uuid + `/${filename}`
}

// Returns the raw storage URL regardless of presigned mode (for logging/internal use only)
export function buildObjectStorageRawUrl (bucket: BucketInfo, key: string): string {
  return buildBaseUrl(bucket, 'write') + buildKey(key, bucket)
}

// ---------------------------------------------------------------------------
// Private
// ---------------------------------------------------------------------------

function buildBaseUrl (bucketInfo: BucketInfo, endpointType: 'read' | 'write') {
  const endpointParsed = getEndpointParsed(endpointType)
  if (!endpointParsed) return ''

  let baseUrlConfig = bucketInfo.BASE_URL
  if (baseUrlConfig && !baseUrlConfig.endsWith('/')) baseUrlConfig += '/'

  if (endpointType === 'read'
    ? getReadForcePathStyle()
    : CONFIG.OBJECT_STORAGE.FORCE_PATH_STYLE) {
    const baseUrl = baseUrlConfig || `${endpointParsed.protocol}//${endpointParsed.host}/`

    return baseUrl + `${bucketInfo.BUCKET_NAME}/`
  }

  if (baseUrlConfig) return baseUrlConfig

  return `${endpointParsed.protocol}//${bucketInfo.BUCKET_NAME}.${endpointParsed.host}/`
}

const parsedEndpointCache = new Map<'read' | 'write', { endpoint: string, parsed: URL }>()

function getEndpointParsed (endpointType: 'read' | 'write') {
  const endpoint = endpointType === 'read' ? getReadEndpoint() : getEndpoint()
  const cached = parsedEndpointCache.get(endpointType)
  if (cached?.endpoint === endpoint) return cached.parsed

  try {
    const parsed = new URL(endpoint)
    parsedEndpointCache.set(endpointType, { endpoint, parsed })
    return parsed
  } catch (error) {
    const configKey = endpointType === 'read'
      ? 'object_storage.read_endpoint'
      : 'object_storage.endpoint'

    logger.error(
      `Invalid object storage ${endpointType} endpoint URL: ${endpoint}. ` +
        `If you enabled object storage, ensure ${configKey} is correctly configured. ` +
        `Otherwise, check that you have correctly moved all your videos to your local filesystem.`,
      lTags()
    )

    return undefined
  }
}

interface BucketInfo {
  BUCKET_NAME: string
  PREFIX: string
  BASE_URL: string
}
