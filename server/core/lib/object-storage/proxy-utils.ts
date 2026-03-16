import type { Request, Response } from 'express'
import type { Readable } from 'stream'

export type ProxyStreamContext = {
  abortController: AbortController
  cleanup: () => void
  registerStream: (stream: Readable) => void
}

/**
 * Creates an AbortController and registers cleanup when the client disconnects.
 * Call registerStream() after getting the S3 stream to destroy it on client abort.
 * Call cleanup() to remove listeners (from pipeline callback or catch block).
 */
export function createRequestAbortContext (req: Request, res: Response): ProxyStreamContext {
  const abortController = new AbortController()
  let streamRef: Readable | null = null

  const onAbortOrClose = () => {
    streamRef?.destroy()
    if (!abortController.signal.aborted) {
      abortController.abort()
    }
  }

  const cleanup = () => {
    req.off('aborted', onAbortOrClose)
    res.off('close', onAbortOrClose)
    streamRef = null
  }

  req.on('aborted', onAbortOrClose)
  res.on('close', onAbortOrClose)

  const registerStream = (stream: Readable) => {
    streamRef = stream
  }

  return { abortController, cleanup, registerStream }
}
