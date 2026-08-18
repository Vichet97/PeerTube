export function isMinimalMoveToObjectStoragePayload (payload: unknown): payload is { videoUUID: string, retryOfFailedJob?: true } {
  if (!payload || typeof payload !== 'object') return false

  const candidate = payload as Record<string, unknown>
  const keys = Object.keys(candidate)

  return typeof candidate.videoUUID === 'string' &&
    keys.every(key => key === 'videoUUID' || key === 'retryOfFailedJob') &&
    (candidate.retryOfFailedJob === undefined || candidate.retryOfFailedJob === true)
}
