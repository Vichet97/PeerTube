import { CONFIG } from '@server/initializers/config.js'

export function getReadForcePathStyle () {
  if (CONFIG.OBJECT_STORAGE.READ_FORCE_PATH_STYLE !== undefined) {
    return CONFIG.OBJECT_STORAGE.READ_FORCE_PATH_STYLE
  }

  return false
}

export function getReadBucketNameForSigning (bucketName: string) {
  if (getReadForcePathStyle()) return bucketName

  const replacement = CONFIG.OBJECT_STORAGE.REPLACE_READ_BUCKET_NAME
  if (replacement === undefined) return bucketName

  return replacement
}

export function applyReadBucketNameReplacement (url: string, bucketName: string) {
  const replacement = CONFIG.OBJECT_STORAGE.REPLACE_READ_BUCKET_NAME
  if (replacement === undefined) return url

  const parsed = new URL(url)

  if (getReadForcePathStyle()) {
    const bucketSegment = `/${bucketName}`

    if (parsed.pathname === bucketSegment) {
      parsed.pathname = replacement === ''
        ? '/'
        : `/${replacement}`

      return parsed.toString()
    }

    const bucketSegmentWithSlash = `${bucketSegment}/`
    if (parsed.pathname.startsWith(bucketSegmentWithSlash)) {
      parsed.pathname = replacement === ''
        ? parsed.pathname.slice(bucketSegment.length)
        : `/${replacement}${parsed.pathname.slice(bucketSegment.length)}`
    }

    return parsed.toString()
  }

  const bucketHostPrefix = `${bucketName}.`
  if (parsed.hostname.startsWith(bucketHostPrefix)) {
    parsed.hostname = replacement === ''
      ? parsed.hostname.slice(bucketHostPrefix.length)
      : `${replacement}.${parsed.hostname.slice(bucketHostPrefix.length)}`
  }

  return parsed.toString()
}
