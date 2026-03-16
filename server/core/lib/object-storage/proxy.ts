import { HttpStatusCode } from '@peertube/peertube-models'
import { buildReinjectVideoFileTokenQuery } from '@server/controllers/shared/m3u8-playlist.js'
import { logger } from '@server/helpers/logger.js'
import { StreamReplacer } from '@server/helpers/stream-replacer.js'
import { MVideo } from '@server/types/models/index.js'
import express from 'express'
import { PassThrough, pipeline } from 'stream'
import { injectQueryToPlaylistUrls } from '../hls.js'
import { CONFIG } from '@server/initializers/config.js'
import { createRequestAbortContext } from './proxy-utils.js'
import { getCaptionReadStream, getHLSFileReadStream, getStoryboardReadStream, getThumbnailReadStream, getWebVideoFileReadStream } from './videos.js'

import type { GetObjectCommandOutput } from '@aws-sdk/client-s3'

export async function proxifyWebVideoFile (options: {
  req: express.Request
  res: express.Response
  filename: string
}) {
  const { req, res, filename } = options

  logger.debug('Proxifying Web Video file %s from object storage.', filename)

  const { abortController, cleanup, registerStream } = createRequestAbortContext(req, res)
  const timeoutMs = CONFIG.OBJECT_STORAGE.PROXY?.REQUEST_TIMEOUT_MS

  try {
    const { response: s3Response, stream } = await getWebVideoFileReadStream({
      filename,
      rangeHeader: req.header('range'),
      abortSignal: abortController.signal,
      requestTimeoutMs: timeoutMs
    })

    registerStream(stream)
    setS3Headers(res, s3Response)

    return pipeline(
      stream,
      res,
      err => {
        cleanup()
        if (err) handleObjectStorageFailure(res, err)
      }
    )
  } catch (err) {
    cleanup()
    return handleObjectStorageFailure(res, err)
  }
}

export async function proxifyHLS (options: {
  req: express.Request
  res: express.Response
  video: MVideo
  filename: string
  reinjectVideoFileToken: boolean
}) {
  const { req, res, video, filename, reinjectVideoFileToken } = options

  logger.debug('Proxifying HLS file %s from object storage.', filename)

  const { abortController, cleanup, registerStream } = createRequestAbortContext(req, res)
  const timeoutMs = CONFIG.OBJECT_STORAGE.PROXY?.REQUEST_TIMEOUT_MS

  try {
    const { response: s3Response, stream } = await getHLSFileReadStream({
      video,
      filename,
      rangeHeader: req.header('range'),
      abortSignal: abortController.signal,
      requestTimeoutMs: timeoutMs
    })

    registerStream(stream)
    setS3Headers(res, s3Response, { allowContentLength: !reinjectVideoFileToken })

    const streamReplacer = reinjectVideoFileToken
      ? new StreamReplacer(line => injectQueryToPlaylistUrls(line, buildReinjectVideoFileTokenQuery(req, filename.endsWith('master.m3u8'))))
      : new PassThrough()

    return pipeline(
      stream,
      streamReplacer,
      res,
      err => {
        cleanup()
        if (err) handleObjectStorageFailure(res, err)
      }
    )
  } catch (err) {
    cleanup()
    return handleObjectStorageFailure(res, err)
  }
}

export async function proxifyThumbnail (options: {
  req: express.Request
  res: express.Response
  filename: string
}) {
  const { req, res, filename } = options

  logger.debug('Proxifying thumbnail file %s from object storage.', filename)

  const { abortController, cleanup, registerStream } = createRequestAbortContext(req, res)
  const timeoutMs = CONFIG.OBJECT_STORAGE.PROXY?.REQUEST_TIMEOUT_MS

  try {
    const { response: s3Response, stream } = await getThumbnailReadStream({
      filename,
      rangeHeader: req.header('range'),
      abortSignal: abortController.signal,
      requestTimeoutMs: timeoutMs
    })

    registerStream(stream)
    setS3Headers(res, s3Response)

    return pipeline(
      stream,
      res,
      err => {
        cleanup()
        if (err) handleObjectStorageFailure(res, err)
      }
    )
  } catch (err) {
    cleanup()
    return handleObjectStorageFailure(res, err)
  }
}

export async function proxifyStoryboard (options: {
  req: express.Request
  res: express.Response
  filename: string
}) {
  const { req, res, filename } = options

  logger.debug('Proxifying storyboard file %s from object storage.', filename)

  const { abortController, cleanup, registerStream } = createRequestAbortContext(req, res)
  const timeoutMs = CONFIG.OBJECT_STORAGE.PROXY?.REQUEST_TIMEOUT_MS

  try {
    const { response: s3Response, stream } = await getStoryboardReadStream({
      filename,
      rangeHeader: req.header('range'),
      abortSignal: abortController.signal,
      requestTimeoutMs: timeoutMs
    })

    registerStream(stream)
    setS3Headers(res, s3Response)

    return pipeline(
      stream,
      res,
      err => {
        cleanup()
        if (err) handleObjectStorageFailure(res, err)
      }
    )
  } catch (err) {
    cleanup()
    return handleObjectStorageFailure(res, err)
  }
}

export async function proxifyCaption (options: {
  req: express.Request
  res: express.Response
  filename: string
}) {
  const { req, res, filename } = options

  logger.debug('Proxifying caption file %s from object storage.', filename)

  const { abortController, cleanup, registerStream } = createRequestAbortContext(req, res)
  const timeoutMs = CONFIG.OBJECT_STORAGE.PROXY?.REQUEST_TIMEOUT_MS

  try {
    const { response: s3Response, stream } = await getCaptionReadStream({
      filename,
      rangeHeader: req.header('range'),
      abortSignal: abortController.signal,
      requestTimeoutMs: timeoutMs
    })

    registerStream(stream)
    setS3Headers(res, s3Response)

    return pipeline(
      stream,
      res,
      err => {
        cleanup()
        if (err) handleObjectStorageFailure(res, err)
      }
    )
  } catch (err) {
    cleanup()
    return handleObjectStorageFailure(res, err)
  }
}

// ---------------------------------------------------------------------------
// Private
// ---------------------------------------------------------------------------

function handleObjectStorageFailure (res: express.Response, err: Error) {
  if (res.writableEnded || res.headersSent) {
    logger.debug('Skipping error response: client disconnected or response already sent', { err: err?.message })
    return
  }

  // Client disconnect / abort: do not log as error
  const errWithCode = err as NodeJS.ErrnoException
  const isClientDisconnect = err?.name === 'AbortError' ||
    errWithCode?.code === 'ERR_STREAM_PREMATURE_CLOSE'
  if (isClientDisconnect) {
    logger.debug('Client disconnected during object storage proxy', { err: err?.message })
    return
  }

  if (err?.name === 'NoSuchKey') {
    logger.debug('Could not find key in object storage to proxify private HLS video file.', { err })
    return res.sendStatus(HttpStatusCode.NOT_FOUND_404)
  }

  logger.error('Object storage failure', { err })

  return res.fail({
    status: HttpStatusCode.INTERNAL_SERVER_ERROR_500,
    message: err.message
  })
}

function setS3Headers (
  res: express.Response,
  s3Response: GetObjectCommandOutput,
  options: { allowContentLength?: boolean } = {}
) {
  const { allowContentLength = true } = options

  setHeaderIfDefined(res, 'Content-Type', s3Response.ContentType)
  setHeaderIfDefined(res, 'Accept-Ranges', s3Response.AcceptRanges)
  setHeaderIfDefined(res, 'ETag', s3Response.ETag)
  if (s3Response.LastModified) {
    res.setHeader('Last-Modified', s3Response.LastModified.toUTCString())
  }

  if (allowContentLength && s3Response.ContentLength !== undefined) {
    res.setHeader('Content-Length', String(s3Response.ContentLength))
  }

  if (s3Response.$metadata.httpStatusCode === HttpStatusCode.PARTIAL_CONTENT_206) {
    setHeaderIfDefined(res, 'Content-Range', s3Response.ContentRange)
    res.status(HttpStatusCode.PARTIAL_CONTENT_206)
  }
}

function setHeaderIfDefined (res: express.Response, key: string, value: string | undefined) {
  if (!value) return

  res.setHeader(key, value)
}
