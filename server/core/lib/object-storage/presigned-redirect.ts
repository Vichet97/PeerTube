import type express from 'express'
import { createHmac, timingSafeEqual } from 'node:crypto'
import { CONFIG } from '@server/initializers/config.js'
import { OBJECT_STORAGE_PROXY_PATHS } from '@server/initializers/constants.js'
import { getClient, getReadClient, buildKey, lTags } from './shared/index.js'
import { logger } from '@server/helpers/logger.js'
import { applyReadBucketNameReplacement, getReadBucketNameForSigning } from './read-url.js'

export type ObjectStoragePublicFileType = 'thumbnails' | 'storyboards' | 'web-videos' | 'streaming-playlists' | 'torrents' | 'captions'

// Secret key for proxy token signing (derived from secret or fallback)
function getProxyTokenSecret (): string {
  return CONFIG.SECRETS.PEERTUBE || 'default-proxy-secret'
}

// Generate a proxy token for URL expiration validation
// Token format: base64(path.signature)
// Signature includes both the path and expiration time to prevent URL hijacking
export function generateProxyToken (path: string): string {
  const timestamp = Date.now()
  const expiresInMs = 3600 * 1000 * CONFIG.OBJECT_STORAGE.PRESIGNED_PUBLIC_URLS_EXPIRATION_HOURS
  const expiresAt = timestamp + expiresInMs

  const secret = getProxyTokenSecret()
  const data = `${path}:${expiresAt}`
  const signature = createHmac('sha256', secret).update(data).digest('base64url')

  return Buffer.from(`${path}:${expiresAt}.${signature}`).toString('base64url')
}

// Validate a proxy token against a specific path
// Returns { valid: true, expiresAt: number } or { valid: false }
export function validateProxyToken (token: string, path?: string): { valid: boolean; expiresAt?: number } {
  try {
    const decoded = Buffer.from(token, 'base64url').toString('utf-8')
    const lastDotIndex = decoded.lastIndexOf('.')
    if (lastDotIndex === -1) return { valid: false }

    const dataPart = decoded.substring(0, lastDotIndex)
    const signature = decoded.substring(lastDotIndex + 1)

    const colonIndex = dataPart.indexOf(':')
    if (colonIndex === -1) return { valid: false }

    const tokenPath = dataPart.substring(0, colonIndex)
    const timestampStr = dataPart.substring(colonIndex + 1)

    const expiresAt = parseInt(timestampStr, 10)
    if (isNaN(expiresAt)) return { valid: false }

    // If path is provided, verify it matches
    if (path && path !== tokenPath) return { valid: false }

    // Check if token is expired
    const now = Date.now()
    if (now > expiresAt) return { valid: false }

    // Verify signature with path + timestamp
    const secret = getProxyTokenSecret()
    const dataToVerify = `${tokenPath}:${timestampStr}`
    const expectedSignature = createHmac('sha256', secret).update(dataToVerify).digest('base64url')

    // Use timing-safe comparison to prevent timing attacks
    const signatureBuffer = Buffer.from(signature)
    const expectedBuffer = Buffer.from(expectedSignature)

    if (signatureBuffer.length !== expectedBuffer.length) return { valid: false }

    if (!timingSafeEqual(signatureBuffer, expectedBuffer)) return { valid: false }

    return { valid: true, expiresAt }
  } catch {
    return { valid: false }
  }
}

export async function generatePresignedRedirect (options: {
  res: express.Response
  key: string
  fileType: ObjectStoragePublicFileType
}) {
  const { res, key, fileType } = options

  const decodedKey = decodeURIComponent(key)
  const bucketInfo = getBucketInfo(fileType)
  const fullKey = buildKey(decodedKey, bucketInfo)

  try {
    const { GetObjectCommand } = await import('@aws-sdk/client-s3')
    const { getSignedUrl } = await import('@aws-sdk/s3-request-presigner')

    const command = new GetObjectCommand({
      Bucket: getReadBucketNameForSigning(bucketInfo.BUCKET_NAME),
      Key: fullKey
    })

    const presignedUrl = await getSignedUrl(
      await getReadClient(),
      command,
      { expiresIn: 3600 * CONFIG.OBJECT_STORAGE.PRESIGNED_PUBLIC_URLS_EXPIRATION_HOURS }
    )

    return res.redirect(applyReadBucketNameReplacement(keepSignedQueryEncoded(presignedUrl), bucketInfo.BUCKET_NAME))
  } catch (err) {
    return res.status(500).json({ error: 'Failed to generate presigned URL' })
  }
}

export async function generatePresignedUrl (options: {
  key: string
  fileType: ObjectStoragePublicFileType
}): Promise<string> {
  const { key, fileType } = options

  const bucketInfo = getBucketInfo(fileType)
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

export async function getObjectContent (options: {
  key: string
  fileType: ObjectStoragePublicFileType
  timeoutMs?: number
}): Promise<Buffer | null> {
  const { key, fileType, timeoutMs } = options

  const bucketInfo = getBucketInfo(fileType)
  const fullKey = buildKey(key, bucketInfo)

  try {
    const { GetObjectCommand } = await import('@aws-sdk/client-s3')
    const client = await getClient()

    // Create AbortController for timeout
    const abortController = new AbortController()
    let timeoutId: ReturnType<typeof setTimeout> | undefined

    if (timeoutMs) {
      timeoutId = setTimeout(() => abortController.abort(), timeoutMs)
    }

    const command = new GetObjectCommand({ Bucket: bucketInfo.BUCKET_NAME, Key: fullKey })

    let response
    try {
      response = await client.send(command, { abortSignal: abortController.signal })
    } finally {
      if (timeoutId) clearTimeout(timeoutId)
    }

    const chunks: Buffer[] = []
    for await (const chunk of response.Body) {
      chunks.push(chunk)
    }
    return Buffer.concat(chunks)
  } catch (err) {
    // Check if it's an abort/timeout error
    if (err?.name === 'AbortError' || err?.message?.includes('abort')) {
      logger.warn(`Timeout fetching object ${fullKey} from object storage after ${timeoutMs}ms`, { ...lTags() })
    } else {
      logger.warn(`Cannot get object ${fullKey} from object storage`, { err, ...lTags() })
    }
    return null
  }
}

export async function transformM3U8ToProxy (options: {
  masterPlaylistKey: string
  videoUUID: string
  masterPlaylistContent: string
}) : Promise<string | null> {
  const { masterPlaylistKey, masterPlaylistContent } = options

  const lines = masterPlaylistContent.split('\n')
  const resultLines: string[] = []

  const baseDir = getM3U8BaseDir(masterPlaylistKey)

  // Collect all file URLs that need presigned URLs
  const fileRefs: { originalLine: string; fileKey: string; isInitSegment: boolean }[] = []
  for (const line of lines) {
    const l = line.trim()

    // Sub-playlists (.m3u8)
    if (l.endsWith('.m3u8') && !l.startsWith('#')) {
      const fileKey = baseDir ? `${baseDir}/${l}` : l
      fileRefs.push({ originalLine: line, fileKey, isInitSegment: false })
    }
    // Segments (.ts, .mp4, .webm)
    else if ((l.endsWith('.ts') || l.endsWith('.mp4') || l.endsWith('.webm')) && !l.startsWith('#')) {
      const fileKey = baseDir ? `${baseDir}/${l}` : l
      fileRefs.push({ originalLine: line, fileKey, isInitSegment: false })
    }
    // EXT-X-MAP:URI="..." - init segments
    else if (l.startsWith('#EXT-X-MAP:')) {
      const uriMatch = l.match(/URI="([^"]+)"/)
      if (uriMatch) {
        const uri = uriMatch[1]
        const fileKey = baseDir ? `${baseDir}/${uri}` : uri
        fileRefs.push({ originalLine: line, fileKey, isInitSegment: true })
      }
    }
  }

  // Generate all presigned URLs in parallel
  const signedUrls = await Promise.all(
    fileRefs.map(r => buildSegmentPresignedUrl(r.fileKey, 'streaming-playlists'))
  )

  // Build a map for quick lookup
  const fileUrlMap = new Map<string, string>()
  fileRefs.forEach((r, i) => {
    fileUrlMap.set(r.fileKey, signedUrls[i])
  })

  for (const line of lines) {
    const l = line.trim()
    const originalLine = line

    if (l.endsWith('.m3u8') && !l.startsWith('#')) {
      // Sub-playlist (.m3u8) references: need individual token for each playlist
      // Original content has full path like "hls/uuid/filename.m3u8"
      // Extract basename for relative path
      const lastSlash = l.lastIndexOf('/')
      const basename = lastSlash === -1 ? l : l.substring(lastSlash + 1)

      // Generate token with the correct path for this sub-playlist
      // Full URL path: /content/public/streaming-playlists/<fullPath>
      const proxyPath = OBJECT_STORAGE_PROXY_PATHS.PUBLIC.STREAMING_PLAYLISTS
      const fileKey = baseDir ? `${baseDir}/${l}` : l // "hls/uuid/variant.m3u8"
      const tokenPath = `${proxyPath}${fileKey}`
      const token = generateProxyToken(tokenPath)

      resultLines.push(originalLine.replace(l, `${basename}?expires=${token}`))
    }
    else if ((l.endsWith('.ts') || l.endsWith('.mp4') || l.endsWith('.webm')) && !l.startsWith('#')) {
      // Segment files: use direct S3 presigned URLs
      const fileKey = baseDir ? `${baseDir}/${l}` : l
      const signedUrl = fileUrlMap.get(fileKey) || ''
      resultLines.push(originalLine.replace(l, signedUrl))
    }
    else if (l.startsWith('#EXT-X-MAP:')) {
      // EXT-X-MAP:URI="..." - init segments, use direct S3 presigned URLs
      // Preserve other attributes like BYTERANGE
      const uriMatch = l.match(/(URI=")([^"]+)(")/)
      if (uriMatch) {
        const uri = uriMatch[2]
        const fileKey = baseDir ? `${baseDir}/${uri}` : uri
        const signedUrl = fileUrlMap.get(fileKey) || ''
        // Only replace the URI="..." part, preserve rest of the line
        resultLines.push(l.replace(`URI="${uri}"`, `URI="${signedUrl}"`))
      } else {
        resultLines.push(originalLine)
      }
    }
    else if (l.includes('URI=')) {
      // Other tags with URI attributes (e.g., EXT-X-MEDIA:TYPE=SUBTITLES,URI="...")
      // Need individual token for each referenced playlist
      const uriMatch = l.match(/(URI=")([^"]+)(")/)
      if (uriMatch) {
        const uri = uriMatch[2]
        // Only proxy if it's a file path (not already a URL)
        if (!uri.startsWith('http://') && !uri.startsWith('https://')) {
          // Extract basename for relative path
          const lastSlash = uri.lastIndexOf('/')
          const basename = lastSlash === -1 ? uri : uri.substring(lastSlash + 1)

          // Generate token with the correct path for this sub-playlist
          const proxyPath = OBJECT_STORAGE_PROXY_PATHS.PUBLIC.STREAMING_PLAYLISTS
          const fileKey = baseDir ? `${baseDir}/${uri}` : uri
          const tokenPath = `${proxyPath}${fileKey}`
          const token = generateProxyToken(tokenPath)

          resultLines.push(originalLine.replace(l, l.replace(uriMatch[2], `${basename}?expires=${token}`)))
        } else {
          resultLines.push(originalLine)
        }
      } else {
        resultLines.push(originalLine)
      }
    }
    else {
      resultLines.push(originalLine)
    }
  }

  return resultLines.join('\n')
}

async function buildSegmentPresignedUrl (key: string, fileType: ObjectStoragePublicFileType): Promise<string> {
  const bucketInfo = getBucketInfo(fileType)
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

function getM3U8BaseDir (key: string): string {
  const lastSlash = key.lastIndexOf('/')
  return lastSlash === -1 ? '' : key.substring(0, lastSlash)
}

export function keepSignedQueryEncoded (url: string): string {
  const questionMarkIndex = url.indexOf('?')
  if (questionMarkIndex === -1) return url

  const base = url.substring(0, questionMarkIndex)
  const query = url.substring(questionMarkIndex + 1)

  const normalizedQuery = query.split('&').map(param => {
    const equalIndex = param.indexOf('=')
    if (equalIndex === -1) return param

    const key = param.substring(0, equalIndex)
    let value = param.substring(equalIndex + 1)

    if (key.startsWith('X-Amz-')) {
      // Keep reserved chars encoded in SigV4 params to avoid intermediary rewriting.
      value = value.replace(/\+/g, '%2B').replace(/\//g, '%2F')
    }

    return `${key}=${value}`
  }).join('&')

  return `${base}?${normalizedQuery}`
}

function getBucketInfo (fileType: ObjectStoragePublicFileType) {
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
