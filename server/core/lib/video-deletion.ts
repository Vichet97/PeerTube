import { VideoModel } from '@server/models/video/video.js'
import { Redis } from './redis.js'

const DEFAULT_CONFIRM_DELAY_MS = 750

export async function isVideoDeletionPending (videoUUID: string, options?: {
  confirmDelayMs?: number
  clearStaleFlag?: boolean
}) {
  const flagState = await Redis.Instance.getVideoDeletionFlagState(videoUUID)
  if (!flagState) return false

  // Structured flags are written by current code right before a real delete.
  // Trust them and never auto-clear them here based on a short timing heuristic.
  if (flagState.legacy === false) return true

  const confirmDelayMs = options?.confirmDelayMs ?? DEFAULT_CONFIRM_DELAY_MS
  const clearStaleFlag = options?.clearStaleFlag ?? true

  if (!await videoExists(videoUUID)) return true

  if (confirmDelayMs > 0) {
    await wait(confirmDelayMs)

    const refreshedFlagState = await Redis.Instance.getVideoDeletionFlagState(videoUUID)
    if (!refreshedFlagState) return false
    if (refreshedFlagState.legacy === false) return true
    if (!await videoExists(videoUUID)) return true
  }

  if (clearStaleFlag) {
    await Redis.Instance.clearVideoDeletionFlag(videoUUID)
  }

  return false
}

function videoExists (videoUUID: string) {
  return VideoModel.load(videoUUID)
    .then(video => !!video)
}

function wait (ms: number) {
  return new Promise<void>(resolve => setTimeout(resolve, ms))
}
