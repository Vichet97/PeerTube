import { logger } from '@server/helpers/logger.js'
import { CONFIG } from '@server/initializers/config.js'
import { OBJECT_STORAGE_PROXY_PATHS, WEBSERVER } from '@server/initializers/constants.js'
import { MVideoUUID } from '@server/types/models/index.js'
import { buildKey, getClient, getEndpoint, lTags } from './shared/index.js'
import { ObjectStoragePublicFileType, generateProxyToken } from './presigned-redirect.js'

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
    // - All other files: use direct S3 presigned URLs
    if (fileType === 'streaming-playlists' && (key.endsWith('.m3u8') || key.endsWith('.mp4'))) {
      return buildPresignedProxyUrl(fileType, key)
    }
    return await generatePresignedUrlFromFileType(key, fileType || 'thumbnails')
  }

  return buildBaseUrl(bucket) + buildKey(key, bucket)
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

// Generates S3 presigned URL
async function generatePresignedUrlFromFileType (key: string, fileType: ObjectStoragePublicFileType): Promise<string> {
  const bucketInfo = getBucketInfoForFileType(fileType)
  const fullKey = buildKey(key, bucketInfo)

  const { GetObjectCommand } = await import('@aws-sdk/client-s3')
  const { getSignedUrl } = await import('@aws-sdk/s3-request-presigner')

  const command = new GetObjectCommand({
    Bucket: bucketInfo.BUCKET_NAME,
    Key: fullKey
  })

  return getSignedUrl(
    await getClient(),
    command,
    { expiresIn: 3600 * CONFIG.OBJECT_STORAGE.PRESIGNED_PUBLIC_URLS_EXPIRATION_HOURS }
  )
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
  return buildBaseUrl(bucket) + buildKey(key, bucket)
}

// ---------------------------------------------------------------------------
// Private
// ---------------------------------------------------------------------------

function buildBaseUrl (bucketInfo: BucketInfo) {
  const endpointParsed = getEndpointParsed()
  if (!endpointParsed) return ''

  let baseUrlConfig = bucketInfo.BASE_URL
  if (baseUrlConfig && !baseUrlConfig.endsWith('/')) baseUrlConfig += '/'

  if (CONFIG.OBJECT_STORAGE.FORCE_PATH_STYLE) {
    const baseUrl = baseUrlConfig || `${endpointParsed.protocol}//${endpointParsed.host}/`

    return baseUrl + `${bucketInfo.BUCKET_NAME}/`
  }

  if (baseUrlConfig) return baseUrlConfig

  return `${endpointParsed.protocol}//${bucketInfo.BUCKET_NAME}.${endpointParsed.host}/`
}

let endpointParsed: URL

function getEndpointParsed () {
  if (!endpointParsed) {
    try {
      endpointParsed = new URL(getEndpoint())
    } catch (error) {
      logger.error(
        `Invalid object storage endpoint URL: ${getEndpoint()}. ` +
          `If you enabled object storage, ensure object_storage.endpoint is correctly configured. ` +
          `Otherwise, check that you have correctly moved all your videos to your local filesystem.`,
        lTags()
      )

      return undefined
    }
  }

  return endpointParsed
}

interface BucketInfo {
  BUCKET_NAME: string
  PREFIX: string
  BASE_URL: string
}
