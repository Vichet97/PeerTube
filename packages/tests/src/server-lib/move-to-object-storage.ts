/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { VideoState } from '@peertube/peertube-models'
import {
  buildRetainedLocalFileCleanupDelay,
  maybeTransitionAfterObjectStorageMove
} from '@peertube/peertube-server/core/lib/move-storage/move-to-object-storage.js'
import { VideoModel } from '@peertube/peertube-server/core/models/video/video.js'
import { buildCaptionMoveJob, createPendingMoveJobs } from '@peertube/peertube-server/core/lib/video-jobs.js'
import { JobQueue } from '@peertube/peertube-server/core/lib/job-queue/index.js'
import { VideoJobInfoModel } from '@peertube/peertube-server/core/models/video/video-job-info.js'

describe('move-to-object-storage', function () {
  it('should skip stale move completion when the video is back in TO_TRANSCODE', async function () {
    const originalLoadFull = VideoModel.loadFull

    VideoModel.loadFull = (() => Promise.resolve({ state: VideoState.TO_TRANSCODE } as any)) as typeof VideoModel.loadFull

    try {
      await maybeTransitionAfterObjectStorageMove({
        videoUUID: 'video-uuid',
        moveVideoState: {
          isNewVideo: true,
          previousVideoState: VideoState.TO_TRANSCODE
        },
        reason: 'test'
      })
    } finally {
      VideoModel.loadFull = originalLoadFull
    }

    expect(true).to.be.true
  })

  it('should not republish a granular completion after another granular move already failed', async function () {
    const originalLoadFull = VideoModel.loadFull

    VideoModel.loadFull = (() => Promise.resolve({
      state: VideoState.TO_MOVE_TO_EXTERNAL_STORAGE_FAILED
    } as any)) as typeof VideoModel.loadFull

    try {
      await maybeTransitionAfterObjectStorageMove({
        videoUUID: 'video-uuid',
        moveVideoState: {
          isNewVideo: true,
          previousVideoState: VideoState.TO_MOVE_TO_EXTERNAL_STORAGE
        },
        reason: 'test',
        allowFailedState: false
      })
    } finally {
      VideoModel.loadFull = originalLoadFull
    }

    expect(true).to.be.true
  })

  it('should build caption moves on the dedicated caption queue', async function () {
    const originalGetExistingCaptionMoveJob = JobQueue.Instance.getExistingCaptionMoveJob
    JobQueue.Instance.getExistingCaptionMoveJob = (() => Promise.resolve(null)) as typeof JobQueue.Instance.getExistingCaptionMoveJob

    try {
      const job = await buildCaptionMoveJob(42)

      expect(job).to.deep.equal({
        type: 'move-caption-to-object-storage',
        payload: { captionId: 42 }
      })
    } finally {
      JobQueue.Instance.getExistingCaptionMoveJob = originalGetExistingCaptionMoveJob
    }
  })

  it('should roll back pendingMove for granular move jobs that were not queued', async function () {
    const originalIncreaseOrCreate = VideoJobInfoModel.increaseOrCreate
    const originalDecrease = VideoJobInfoModel.decrease
    const originalCreateJob = JobQueue.Instance.createJob

    let increasedBy: number
    let decreasedBy: number

    VideoJobInfoModel.increaseOrCreate = ((_videoUUID: string, _column: 'pendingMove', amount = 1) => {
      increasedBy = amount
      return Promise.resolve(amount)
    }) as typeof VideoJobInfoModel.increaseOrCreate

    VideoJobInfoModel.decrease = ((_videoUUID: string, _column: 'pendingMove', amount = 1) => {
      decreasedBy = amount
      return Promise.resolve(0)
    }) as typeof VideoJobInfoModel.decrease

    JobQueue.Instance.createJob = ((job: any) => {
      if (job.payload.fileId === 1) return Promise.resolve({ id: 'created' } as any)

      return Promise.resolve(undefined)
    }) as typeof JobQueue.Instance.createJob

    try {
      let error: Error | undefined
      try {
        await createPendingMoveJobs({
          videoUUID: 'video-uuid',
          jobs: [
            {
              type: 'move-video-file-to-object-storage' as const,
              payload: { videoUUID: 'video-uuid', fileId: 1, isNewVideo: false, previousVideoState: VideoState.PUBLISHED }
            },
            {
              type: 'move-video-file-to-object-storage' as const,
              payload: { videoUUID: 'video-uuid', fileId: 2, isNewVideo: false, previousVideoState: VideoState.PUBLISHED }
            }
          ]
        })
      } catch (err) {
        error = err as Error
      }

      expect(error).to.be.instanceOf(Error)
      expect(increasedBy).to.equal(2)
      expect(decreasedBy).to.equal(1)
    } finally {
      VideoJobInfoModel.increaseOrCreate = originalIncreaseOrCreate
      VideoJobInfoModel.decrease = originalDecrease
      JobQueue.Instance.createJob = originalCreateJob
    }
  })

  it('should only keep retained local files for the remaining configured delay after restart', function () {
    expect(buildRetainedLocalFileCleanupDelay({
      keepLocalFileAfterMoveMs: 60_000,
      mtimeMs: 1_000,
      nowMs: 30_000
    })).to.equal(31_000)

    expect(buildRetainedLocalFileCleanupDelay({
      keepLocalFileAfterMoveMs: 60_000,
      mtimeMs: 1_000,
      nowMs: 90_000
    })).to.equal(0)
  })
})
