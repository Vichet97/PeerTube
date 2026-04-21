import { Job } from 'bullmq'
import { getFFmpegCommandWrapperOptions } from '@server/helpers/ffmpeg/index.js'
import { logger } from '@server/helpers/logger.js'
import { Redis } from '@server/lib/redis.js'
import { FFmpegVOD } from '@peertube/peertube-ffmpeg'
import { VideoTranscodingProfilesManager } from '../default-transcoding-profiles.js'

const CANCELLED_REASON = 'Video was deleted - transcoding job cancelled'

// Module-level cache: when Redis reports video deleted, we set this so the sync progress callback can throw
const cancelledVideoUUIDs = new Set<string>()
const CHECK_INTERVAL_MS = 500 // Check every 500ms; lower = faster abort, less orphaned files when video deleted during transcoding

// FFmpeg command timeout: 4 hours by default
// This is a safety net to prevent FFmpeg from hanging indefinitely
const DEFAULT_FFMPEG_TIMEOUT_MS = 4 * 60 * 60 * 1000

export function buildFFmpegVOD (jobOrOptions?: Job | { job?: Job, videoUUID?: string, timeoutMs?: number }) {
  // BullMQ Job has 'data' and 'updateProgress'; our options object has 'job' or 'videoUUID'
  const isJob = jobOrOptions && typeof jobOrOptions === 'object' && 'data' in jobOrOptions && 'updateProgress' in jobOrOptions
  const options = isJob
    ? { job: jobOrOptions as Job, videoUUID: undefined as string | undefined, timeoutMs: DEFAULT_FFMPEG_TIMEOUT_MS }
    : (jobOrOptions as { job?: Job, videoUUID?: string, timeoutMs?: number }) || { timeoutMs: DEFAULT_FFMPEG_TIMEOUT_MS }
  const job = options?.job
  const videoUUID = options?.videoUUID
  const timeoutMs = options?.timeoutMs ?? DEFAULT_FFMPEG_TIMEOUT_MS
  let lastCheckTime = 0
  let checkInProgress = false
  let lastProgress = 0
  let lastProgressUpdateTime = Date.now()
  const PROGRESS_STALL_THRESHOLD_MS = 60000 // Consider stalled if progress doesn't change for 60 seconds

  // Timer-based abort: check Redis every 150ms independent of ffmpeg progress (which can be sparse for HLS)
  let abortController: AbortController | undefined
  let redisCheckInterval: ReturnType<typeof setInterval> | undefined
  if (videoUUID) {
    abortController = new AbortController()
    redisCheckInterval = setInterval(() => {
      Redis.Instance.isVideoDeletionFlagSet(videoUUID)
        .then(flagged => { if (flagged) abortController!.abort() })
        .catch(() => { /* ignore */ })
    }, 150)
  }

  const updateJobProgress = (progress: number) => {
    if (!job) return

    // Sync check: if a previous Redis check found the video deleted, throw to abort transcoding
    if (videoUUID && cancelledVideoUUIDs.has(videoUUID)) {
      throw new Error(CANCELLED_REASON)
    }

    // Progress stall detection: if progress reaches 100% but doesn't complete, we have a stuck job
    // Track progress changes to detect stalls
    if (progress !== lastProgress) {
      lastProgress = progress
      lastProgressUpdateTime = Date.now()
    } else if (progress < 100 && Date.now() - lastProgressUpdateTime > PROGRESS_STALL_THRESHOLD_MS) {
      // Progress hasn't changed for too long - possible stall
      logger.warn('FFmpeg progress stalled at %d%% for %d seconds for video %s',
        progress, Math.floor((Date.now() - lastProgressUpdateTime) / 1000), videoUUID)
    }

    // Throttled async check: poll Redis periodically; when video is deleted, set cache so next progress call throws
    if (videoUUID && Date.now() - lastCheckTime >= CHECK_INTERVAL_MS && !checkInProgress) {
      checkInProgress = true
      Redis.Instance.isVideoDeletionFlagSet(videoUUID)
        .then(flagged => {
          lastCheckTime = Date.now()
          if (flagged) cancelledVideoUUIDs.add(videoUUID)
        })
        .catch(() => { /* ignore Redis errors */ })
        .finally(() => { checkInProgress = false })
    }

    job.updateProgress(progress)
      .catch(err => logger.error('Cannot update ffmpeg job progress', { err }))
  }

  return new FFmpegVOD({
    ...getFFmpegCommandWrapperOptions('vod', VideoTranscodingProfilesManager.Instance.getAvailableEncoders()),

    updateJobProgress,

    abortSignal: abortController?.signal,
    onSettled: () => { if (redisCheckInterval) clearInterval(redisCheckInterval) }
  }, timeoutMs)
}
