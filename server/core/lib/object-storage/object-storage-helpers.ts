import { pipelinePromise } from '@server/helpers/core-utils.js'
import { isArray } from '@server/helpers/custom-validators/misc.js'
import { logger } from '@server/helpers/logger.js'
import { CONFIG } from '@server/initializers/config.js'
import Bluebird from 'bluebird'
import { createReadStream, createWriteStream } from 'fs'
import { ensureDir } from 'fs-extra/esm'
import { dirname } from 'path'
import { Readable } from 'stream'
import { getClient } from './shared/client.js'
import { lTags } from './shared/logger.js'

import type { _Object, ObjectCannedACL, PutObjectCommandInput, S3Client } from '@aws-sdk/client-s3'

type BucketInfo = {
  BUCKET_NAME: string
  BASE_URL: string
  PREFIX?: string
}

async function listKeysOfPrefix (prefix: string, bucketInfo: BucketInfo, continuationToken?: string) {
  const s3Client = await getClient()

  const { ListObjectsV2Command } = await import('@aws-sdk/client-s3')

  const bucketPrefix = bucketInfo.PREFIX ?? ''
  const commandPrefix = prefix.startsWith(bucketPrefix) ? prefix : bucketPrefix + prefix
  const listCommand = new ListObjectsV2Command({
    Bucket: bucketInfo.BUCKET_NAME,
    Prefix: commandPrefix,
    ContinuationToken: continuationToken
  })

  const listedObjects = await s3Client.send(listCommand)
    .catch(err => {
      throw parseS3Error(err)
    })

  if (isArray(listedObjects.Contents) !== true) return []

  let keys = listedObjects.Contents.map(c => c.Key)

  if (listedObjects.IsTruncated) {
    keys = keys.concat(await listKeysOfPrefix(prefix, bucketInfo, listedObjects.NextContinuationToken))
  }

  return keys
}

// ---------------------------------------------------------------------------

async function storeObject (options: {
  inputPath: string
  objectStorageKey: string
  bucketInfo: BucketInfo
  isPrivate: boolean
  contentType: string
}): Promise<void> {
  const { inputPath, objectStorageKey, bucketInfo, isPrivate, contentType } = options

  logger.debug('Uploading file %s to %s%s in bucket %s', inputPath, bucketInfo.PREFIX, objectStorageKey, bucketInfo.BUCKET_NAME, lTags())

  const fileStream = createReadStream(inputPath)

  return uploadToStorage({ objectStorageKey, content: fileStream, bucketInfo, isPrivate, contentType })
}

async function storeContent (options: {
  content: string
  objectStorageKey: string
  bucketInfo: BucketInfo
  isPrivate: boolean
  contentType: string
}): Promise<void> {
  const { content, objectStorageKey, bucketInfo, isPrivate, contentType } = options

  logger.debug('Uploading %s content to %s%s in bucket %s', content, bucketInfo.PREFIX, objectStorageKey, bucketInfo.BUCKET_NAME, lTags())

  return uploadToStorage({ objectStorageKey, content, bucketInfo, isPrivate, contentType })
}

async function storeStream (options: {
  stream: Readable
  objectStorageKey: string
  bucketInfo: BucketInfo
  isPrivate: boolean
  contentType: string
}): Promise<void> {
  const { stream, objectStorageKey, bucketInfo, isPrivate, contentType } = options

  logger.debug('Streaming file to %s%s in bucket %s', bucketInfo.PREFIX, objectStorageKey, bucketInfo.BUCKET_NAME, lTags())

  return uploadToStorage({ objectStorageKey, content: stream, bucketInfo, isPrivate, contentType })
}

// ---------------------------------------------------------------------------

async function updateObjectACL (options: {
  objectStorageKey: string
  bucketInfo: BucketInfo
  isPrivate: boolean
}) {
  const { objectStorageKey, bucketInfo, isPrivate } = options

  const acl = getACL(isPrivate)
  if (!acl) return

  const key = buildKey(objectStorageKey, bucketInfo)

  logger.debug('Updating ACL file %s in bucket %s', key, bucketInfo.BUCKET_NAME, lTags())

  const { PutObjectAclCommand } = await import('@aws-sdk/client-s3')

  const command = new PutObjectAclCommand({
    Bucket: bucketInfo.BUCKET_NAME,
    Key: key,
    ACL: acl
  })

  const client = await getClient()
  await client.send(command)
    .catch(err => {
      throw parseS3Error(err)
    })
}

async function updatePrefixACL (options: {
  prefix: string
  bucketInfo: BucketInfo
  isPrivate: boolean
}) {
  const { prefix, bucketInfo, isPrivate } = options

  const acl = getACL(isPrivate)
  if (!acl) return

  const { PutObjectAclCommand } = await import('@aws-sdk/client-s3')

  logger.debug('Updating ACL of files in prefix %s in bucket %s', prefix, bucketInfo.BUCKET_NAME, lTags())

  return applyOnPrefix({
    prefix,
    bucketInfo,
    commandBuilder: obj => {
      logger.debug('Updating ACL of %s inside prefix %s in bucket %s', obj.Key, prefix, bucketInfo.BUCKET_NAME, lTags())

      return new PutObjectAclCommand({
        Bucket: bucketInfo.BUCKET_NAME,
        Key: obj.Key,
        ACL: acl
      })
    }
  })
}

// ---------------------------------------------------------------------------

function removeObject (objectStorageKey: string, bucketInfo: BucketInfo) {
  const key = buildKey(objectStorageKey, bucketInfo)

  return removeObjectByFullKey(key, bucketInfo)
}

async function removeObjectByFullKey (fullKey: string, bucketInfo: Pick<BucketInfo, 'BUCKET_NAME'>) {
  logger.debug('Removing file %s in bucket %s', fullKey, bucketInfo.BUCKET_NAME, lTags())

  const { DeleteObjectCommand } = await import('@aws-sdk/client-s3')

  const command = new DeleteObjectCommand({
    Bucket: bucketInfo.BUCKET_NAME,
    Key: fullKey
  })

  const client = await getClient()

  return client.send(command)
    .catch(err => {
      throw parseS3Error(err)
    })
}

async function removePrefix (prefix: string, bucketInfo: BucketInfo) {
  logger.debug('Removing prefix %s in bucket %s', prefix, bucketInfo.BUCKET_NAME, lTags())

  const { DeleteObjectCommand } = await import('@aws-sdk/client-s3')

  return applyOnPrefix({
    prefix,
    bucketInfo,
    commandBuilder: obj => {
      logger.debug('Removing %s inside prefix %s in bucket %s', obj.Key, prefix, bucketInfo.BUCKET_NAME, lTags())

      return new DeleteObjectCommand({
        Bucket: bucketInfo.BUCKET_NAME,
        Key: obj.Key
      })
    }
  })
}

// ---------------------------------------------------------------------------

async function makeAvailable (options: {
  key: string
  destination: string
  bucketInfo: BucketInfo
}) {
  const { key, destination, bucketInfo } = options
  const requestTimeoutMs = CONFIG.OBJECT_STORAGE.PROXY.REQUEST_TIMEOUT_MS

  for (let attempt = 1; attempt <= 2; attempt++) {
    let timeoutId: ReturnType<typeof setTimeout> | undefined
    let responseBody: Readable | undefined
    let cleanupStreamTimeout: (() => void) | undefined
    const abortController = requestTimeoutMs
      ? new AbortController()
      : undefined
    const timeoutError = requestTimeoutMs
      ? new Error(`Object storage fetch ${bucketInfo.PREFIX ?? ''}${key} timed out after ${requestTimeoutMs}ms`)
      : undefined
    if (timeoutError) timeoutError.name = 'TimeoutError'

    try {
      await ensureDir(dirname(options.destination))

      const { GetObjectCommand } = await import('@aws-sdk/client-s3')

      const command = new GetObjectCommand({
        Bucket: bucketInfo.BUCKET_NAME,
        Key: buildKey(key, bucketInfo)
      })

      const client = await getClient()
      if (abortController && requestTimeoutMs) {
        timeoutId = setTimeout(() => {
          abortController.abort()
          responseBody?.destroy(timeoutError)
        }, requestTimeoutMs)
        timeoutId.unref?.()
      }

      const response = await client.send(command, abortController ? { abortSignal: abortController.signal } : {})
        .catch(err => {
          throw parseS3Error(err)
        })
      if (timeoutId) clearTimeout(timeoutId)

      const file = createWriteStream(destination)
      responseBody = response.Body as Readable
      if (requestTimeoutMs && timeoutError) {
        cleanupStreamTimeout = watchReadableInactivity({
          stream: responseBody,
          timeoutMs: requestTimeoutMs,
          timeoutError,
          onTimeout: err => {
            abortController?.abort()
            responseBody?.destroy(err)
          }
        })
      }
      await pipelinePromise(responseBody, file)

      file.close()
      return
    } catch (err) {
      if (attempt < 2 && isTransientObjectStorageError(err)) {
        logger.warn(
          'Transient object storage error while fetching %s%s from bucket %s, retrying in 5s (attempt %d/2)',
          bucketInfo.PREFIX,
          key,
          bucketInfo.BUCKET_NAME,
          attempt
        )

        await new Promise(resolve => setTimeout(resolve, 5000))
        continue
      }

      throw err
    } finally {
      if (timeoutId) clearTimeout(timeoutId)
      cleanupStreamTimeout?.()
    }
  }
}

function buildKey (key: string, bucketInfo: BucketInfo) {
  const prefix = bucketInfo.PREFIX ?? ''

  return key.startsWith(prefix) ? key : prefix + key
}

// ---------------------------------------------------------------------------

function isTransientObjectStorageError (err: unknown) {
  if (!err || typeof err !== 'object') return false

  const e = err as any
  const message = [
    e.name,
    e.Code,
    e.code,
    e.message,
    e.$response?.statusCode,
    e.$metadata?.httpStatusCode
  ].filter(Boolean).join(' ')

  const statusCode = e.$response?.statusCode ?? e.$metadata?.httpStatusCode
  if (statusCode && [ 500, 502, 503, 504 ].includes(statusCode)) return true

  return [
    'XMinioBackendDown',
    'Object storage backend is unreachable',
    'ServiceUnavailable',
    'SlowDown',
    'RequestTimeout',
    'TimeoutError',
    'AbortError',
    'ECONNRESET',
    'ETIMEDOUT',
    'EPIPE',
    'aborted',
    '500 Internal Server Error',
    '503 Service Unavailable'
  ].some(token => message.includes(token))
}

// ---------------------------------------------------------------------------

async function createObjectReadStream (options: {
  key: string
  bucketInfo: BucketInfo
  rangeHeader?: string
  abortSignal?: AbortSignal
  requestTimeoutMs?: number
}) {
  const { key, bucketInfo, rangeHeader, abortSignal, requestTimeoutMs } = options

  const { GetObjectCommand } = await import('@aws-sdk/client-s3')

  const timeoutController = requestTimeoutMs ? new AbortController() : null
  const effectiveSignal = (() => {
    if (abortSignal && timeoutController) {
      const combined = new AbortController()
      const onAbort = () => combined.abort()
      abortSignal.addEventListener('abort', onAbort)
      timeoutController.signal.addEventListener('abort', onAbort)
      return combined.signal
    }
    return abortSignal ?? timeoutController?.signal
  })()

  let timeoutId: ReturnType<typeof setTimeout> | undefined
  if (timeoutController && requestTimeoutMs) {
    const controller = timeoutController
    timeoutId = setTimeout(() => controller.abort(), requestTimeoutMs)
    timeoutId.unref?.()
  }

  const command = new GetObjectCommand({
    Bucket: bucketInfo.BUCKET_NAME,
    Key: buildKey(key, bucketInfo),
    ...(rangeHeader && { Range: rangeHeader })
  })

  const client = await getClient()
  const sendOptions = effectiveSignal ? { abortSignal: effectiveSignal } : {}
  const response = await client.send(command, sendOptions)
    .catch(err => {
      throw parseS3Error(err)
    })
    .finally(() => {
      if (timeoutId) clearTimeout(timeoutId)
    })

  const stream = response.Body as Readable
  if (stream && requestTimeoutMs) {
    const streamTimeoutError = new Error(`Object storage stream ${bucketInfo.PREFIX ?? ''}${key} timed out after ${requestTimeoutMs}ms`)
    streamTimeoutError.name = 'TimeoutError'

    watchReadableInactivity({
      stream,
      timeoutMs: requestTimeoutMs,
      timeoutError: streamTimeoutError,
      onTimeout: err => stream.destroy(err)
    })
  }

  return {
    response,
    stream
  }
}

// ---------------------------------------------------------------------------

function watchReadableInactivity (options: {
  stream: Readable
  timeoutMs: number
  timeoutError: Error
  onTimeout: (err: Error) => void
}) {
  const { stream, timeoutMs, timeoutError, onTimeout } = options
  let timeoutId: ReturnType<typeof setTimeout> | undefined

  const arm = () => {
    if (timeoutId) clearTimeout(timeoutId)
    timeoutId = setTimeout(() => onTimeout(timeoutError), timeoutMs)
    timeoutId.unref?.()
  }

  const cleanup = () => {
    if (timeoutId) clearTimeout(timeoutId)
    stream.off('data', arm)
    stream.off('end', cleanup)
    stream.off('close', cleanup)
    stream.off('error', cleanup)
  }

  stream.on('data', arm)
  stream.once('end', cleanup)
  stream.once('close', cleanup)
  stream.once('error', cleanup)

  arm()

  return cleanup
}

// ---------------------------------------------------------------------------

async function getObjectStorageFileSize (options: {
  key: string
  bucketInfo: BucketInfo
}) {
  const { key, bucketInfo } = options

  const { HeadObjectCommand } = await import('@aws-sdk/client-s3')

  const command = new HeadObjectCommand({
    Bucket: bucketInfo.BUCKET_NAME,
    Key: buildKey(key, bucketInfo)
  })

  const client = await getClient()
  const response = await client.send(command)
    .catch(err => {
      throw parseS3Error(err)
    })

  return response.ContentLength
}

async function checkObjectStorageReadiness (options: {
  key: string
  bucketInfo: BucketInfo
  maxRetries?: number
  retryIntervalMs?: number
  logNotReadyAsDebug?: boolean
}): Promise<boolean> {
  const { key, bucketInfo, maxRetries = 30, retryIntervalMs = 10000, logNotReadyAsDebug = false } = options
  const requestTimeoutMs = 10000
  const progressTags = {
    objectStorageKey: key,
    bucket: bucketInfo.BUCKET_NAME,
    prefix: bucketInfo.PREFIX,
    ...lTags()
  }

  const { GetObjectCommand } = await import('@aws-sdk/client-s3')

  logger.debug(
    'Checking object storage readiness for %s%s in bucket %s',
    bucketInfo.PREFIX,
    key,
    bucketInfo.BUCKET_NAME,
    progressTags
  )

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const command = new GetObjectCommand({
        Bucket: bucketInfo.BUCKET_NAME,
        Key: buildKey(key, bucketInfo),
        Range: 'bytes=0-10'
      })

      const client = await getClient()

      const timeoutController = new AbortController()
      const timeoutId = setTimeout(() => timeoutController.abort(), requestTimeoutMs)
      timeoutId.unref?.()

      try {
        const response = await client.send(command, { abortSignal: timeoutController.signal })
        clearTimeout(timeoutId)
        ;(response.Body as Readable | undefined)?.destroy()

        if (response.$metadata.httpStatusCode === 200 || response.$metadata.httpStatusCode === 206) {
          logger.debug('Object storage file %s is ready (attempt %d)', key, attempt, { ...progressTags, attempt })
          return true
        }

        logger.debug(
          'Object storage file %s returned unexpected status %d (attempt %d/%d)',
          key,
          response.$metadata.httpStatusCode,
          attempt,
          maxRetries,
          { ...progressTags, attempt, statusCode: response.$metadata.httpStatusCode }
        )
      } catch (innerErr) {
        clearTimeout(timeoutId)
        throw innerErr
      }
    } catch (err) {
      if (attempt < maxRetries) {
        logger.debug(
          'Object storage file %s not ready yet (attempt %d/%d), retrying in %dms',
          key,
          attempt,
          maxRetries,
          retryIntervalMs,
          { ...progressTags, attempt, retryIntervalMs, err: err?.message }
        )
        await new Promise(resolve => setTimeout(resolve, retryIntervalMs))
      } else {
        logger[logNotReadyAsDebug ? 'debug' : 'warn'](
          'Object storage file %s did not become ready after %d attempts',
          key,
          maxRetries,
          { ...progressTags, attempt, err: err?.message }
        )
        return false
      }
    }
  }

  return false
}

// ---------------------------------------------------------------------------

export {
  buildKey,
  checkObjectStorageReadiness,
  createObjectReadStream,
  getObjectStorageFileSize,
  isTransientObjectStorageError,
  listKeysOfPrefix,
  makeAvailable,
  removeObject,
  removeObjectByFullKey,
  removePrefix,
  storeContent,
  storeObject,
  storeStream,
  updateObjectACL,
  updatePrefixACL,
  type BucketInfo
}

// ---------------------------------------------------------------------------

async function uploadToStorage (options: {
  content: Readable | string
  objectStorageKey: string
  bucketInfo: BucketInfo
  isPrivate: boolean

  contentType?: string
}) {
  const { content, objectStorageKey, bucketInfo, isPrivate, contentType } = options

  const input: PutObjectCommandInput = {
    Body: content,
    Bucket: bucketInfo.BUCKET_NAME,
    Key: buildKey(objectStorageKey, bucketInfo),
    ContentType: contentType
  }

  const acl = getACL(isPrivate)
  if (acl) input.ACL = acl
  const progressTags = {
    bucket: bucketInfo.BUCKET_NAME,
    key: input.Key,
    objectStorageKey,
    prefix: bucketInfo.PREFIX,
    contentType,
    acl: input.ACL,
    queueSize: CONFIG.OBJECT_STORAGE.UPLOAD_PART_QUEUE_SIZE,
    partSize: CONFIG.OBJECT_STORAGE.MAX_UPLOAD_PART,
    ...lTags()
  }

  const { Upload } = await import('@aws-sdk/lib-storage')

  const parallelUploads3 = new Upload({
    client: await getClient(),
    queueSize: CONFIG.OBJECT_STORAGE.UPLOAD_PART_QUEUE_SIZE,
    partSize: CONFIG.OBJECT_STORAGE.MAX_UPLOAD_PART,

    // `leavePartsOnError` must be set to `true` to avoid silently dropping failed parts
    // More detailed explanation:
    // https://github.com/aws/aws-sdk-js-v3/blob/v3.164.0/lib/lib-storage/src/Upload.ts#L274
    // https://github.com/aws/aws-sdk-js-v3/issues/2311#issuecomment-939413928
    leavePartsOnError: true,
    params: input
  })

  parallelUploads3.on('httpUploadProgress', progress => {
    const loaded = progress.loaded ?? 0
    const total = progress.total
    const percent = total ? Math.round((loaded / total) * 10000) / 100 : undefined

    logger.debug('Object storage upload progress', {
      ...progressTags,
      loaded,
      total,
      percent,
      part: progress.part,
      eventBucket: progress.Bucket,
      eventKey: progress.Key
    })
  })

  try {
    const response = await parallelUploads3.done()
    // Check is needed even if the HTTP status code is 200 OK
    // For more information, see https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
    if (!response.Bucket) {
      const message = `Error uploading ${objectStorageKey} to bucket ${bucketInfo.BUCKET_NAME}`
      logger.error(message, { ...progressTags, response })
      throw new Error(message)
    }

    logger.debug(
      'Completed %s%s in bucket %s',
      bucketInfo.PREFIX,
      objectStorageKey,
      bucketInfo.BUCKET_NAME,
      { ...progressTags, responseMetadata: response.$metadata }
    )
  } catch (err) {
    // eslint-disable-next-line @typescript-eslint/only-throw-error
    throw parseS3Error(err)
  }
}

async function applyOnPrefix (options: {
  prefix: string
  bucketInfo: BucketInfo
  commandBuilder: (obj: _Object) => Parameters<S3Client['send']>[0]

  continuationToken?: string
}) {
  const { prefix, bucketInfo, commandBuilder, continuationToken } = options

  const s3Client = await getClient()

  const { ListObjectsV2Command } = await import('@aws-sdk/client-s3')

  const commandPrefix = buildKey(prefix, bucketInfo)
  const listCommand = new ListObjectsV2Command({
    Bucket: bucketInfo.BUCKET_NAME,
    Prefix: commandPrefix,
    ContinuationToken: continuationToken
  })

  const listedObjects = await s3Client.send(listCommand)
    .catch(err => {
      throw parseS3Error(err)
    })

  // Empty prefix (no files) is success, not an error (e.g. HLS not yet uploaded)
  const contents = listedObjects.Contents
  if (!isArray(contents) || contents.length === 0) {
    logger.debug('No files in prefix %s in bucket %s, nothing to apply.', commandPrefix, bucketInfo.BUCKET_NAME, lTags())
    return
  }

  await Bluebird.map(contents, object => {
    const command = commandBuilder(object)

    return s3Client.send(command)
      .catch(err => {
        throw parseS3Error(err)
      })
  }, { concurrency: 10 })

  // Repeat if not all objects could be listed at once (limit of 1000?)
  if (listedObjects.IsTruncated) {
    await applyOnPrefix({ ...options, continuationToken: listedObjects.ContinuationToken })
  }
}

function getACL (isPrivate: boolean) {
  return isPrivate
    ? CONFIG.OBJECT_STORAGE.UPLOAD_ACL.PRIVATE as ObjectCannedACL
    : CONFIG.OBJECT_STORAGE.UPLOAD_ACL.PUBLIC as ObjectCannedACL
}

// Prevent logging too much information, in particular the body request
function parseS3Error (err: any) {
  if (err.$response?.body) {
    const body = err.$response.body

    err.$response.body = {
      rawHeaders: body.rawHeaders,
      req: {
        _header: body.req?._header
      }
    }
  }

  return err as Error
}
