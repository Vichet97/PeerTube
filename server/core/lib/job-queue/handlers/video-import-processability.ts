import { VideoImportState, VideoState } from '@peertube/peertube-models'

export function isVideoImportBackpressureJobId (jobId: string | number | undefined) {
  return String(jobId || '').startsWith('video-import-backpressure-')
}

export function getVideoImportSkipReason (options: {
  importState: number
  videoState?: number
}) {
  const { importState, videoState } = options

  if (importState === VideoImportState.CANCELLED) return 'cancelled'
  if (importState === VideoImportState.REJECTED) return 'rejected'
  if (importState === VideoImportState.PROCESSING) return 'already-processing'
  if (importState === VideoImportState.SUCCESS) return 'already-succeeded'

  if (
    videoState !== undefined &&
    videoState !== VideoState.TO_IMPORT &&
    videoState !== VideoState.TO_IMPORT_FAILED
  ) {
    return 'video-already-past-import-stage'
  }

  return undefined
}
