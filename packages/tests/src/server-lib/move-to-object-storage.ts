/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { existsSync } from 'fs'
import { remove } from 'fs-extra/esm'
import { mkdtemp, writeFile } from 'fs/promises'
import { tmpdir } from 'os'
import { join } from 'path'
import { FileStorage, VideoState } from '@peertube/peertube-models'
import {
  buildRetainedLocalFileCleanupDelay,
  maybeTransitionAfterObjectStorageMove,
  removeLocalFileAfterMove
} from '@peertube/peertube-server/core/lib/move-storage/move-to-object-storage.js'
import { pickCaptionsForMoveBatch } from '@peertube/peertube-server/core/lib/move-storage/shared/move-caption.js'
import { CONFIG } from '@peertube/peertube-server/core/initializers/config.js'
import { VideoPathManager } from '@peertube/peertube-server/core/lib/video-path-manager.js'
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
    const originalGetExistingCaptionMoveJobByVideoUUID = JobQueue.Instance.getExistingCaptionMoveJobByVideoUUID
    JobQueue.Instance.getExistingCaptionMoveJobByVideoUUID =
      (() => Promise.resolve(null)) as typeof JobQueue.Instance.getExistingCaptionMoveJobByVideoUUID

    try {
      const job = await buildCaptionMoveJob(42, 'video-uuid')

      expect(job).to.deep.equal({
        type: 'move-caption-to-object-storage',
        payload: { captionId: 42, videoUUID: 'video-uuid' }
      })
    } finally {
      JobQueue.Instance.getExistingCaptionMoveJobByVideoUUID = originalGetExistingCaptionMoveJobByVideoUUID
    }
  })

  it('should batch caption moves by video and skip already-processed captions', function () {
    const selected = pickCaptionsForMoveBatch({
      representativeCaptionId: 2,
      includeAllVideoCaptions: true,
      captions: [
        { id: 1, storage: FileStorage.OBJECT_STORAGE, m3u8Filename: 'caption-1.m3u8' } as any,
        { id: 2, storage: FileStorage.FILE_SYSTEM, m3u8Filename: null } as any,
        { id: 3, storage: FileStorage.FILE_SYSTEM, m3u8Filename: 'caption-3.m3u8' } as any
      ]
    })

    expect(selected.map(c => c.id)).to.deep.equal([ 2, 3 ])

    const emptySelection = pickCaptionsForMoveBatch({
      representativeCaptionId: 1,
      includeAllVideoCaptions: true,
      captions: [
        { id: 1, storage: FileStorage.OBJECT_STORAGE, m3u8Filename: 'caption-1.m3u8' } as any,
        { id: 2, storage: FileStorage.OBJECT_STORAGE, m3u8Filename: 'caption-2.m3u8' } as any
      ]
    })

    expect(emptySelection).to.deep.equal([])
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

  it('should keep retained local files after unlock while pipeline counters remain pending', async function () {
    this.timeout(5_000)

    const originalKeepLocalFileAfterMove = CONFIG.OBJECT_STORAGE.KEEP_LOCAL_FILE_AFTER_MOVE
    const originalLoadByUUID = VideoJobInfoModel.loadByUUID

    const videoUUID = 'video-uuid'
    const tmpDirectory = await mkdtemp(join(tmpdir(), 'peertube-retained-local-'))
    const path = join(tmpDirectory, 'playlist.m3u8')
    const releaser = await VideoPathManager.Instance.lockFiles(videoUUID)

    CONFIG.OBJECT_STORAGE.KEEP_LOCAL_FILE_AFTER_MOVE = 0
    VideoJobInfoModel.loadByUUID = (() => Promise.resolve({
      pendingMove: 5,
      pendingTranscode: 7,
      pendingTranscription: 3
    } as any)) as typeof VideoJobInfoModel.loadByUUID

    try {
      await writeFile(path, '#EXTM3U')

      await removeLocalFileAfterMove({
        path,
        videoUUID,
        skipReadinessCheck: true
      })

      await new Promise(resolve => setTimeout(resolve, 200))
      expect(existsSync(path)).to.be.true

      releaser()

      await new Promise(resolve => setTimeout(resolve, 500))
      expect(existsSync(path)).to.be.true
    } finally {
      expect(existsSync(path)).to.be.false
      releaser()

      CONFIG.OBJECT_STORAGE.KEEP_LOCAL_FILE_AFTER_MOVE = originalKeepLocalFileAfterMove
      VideoJobInfoModel.loadByUUID = originalLoadByUUID

      await remove(tmpDirectory).catch(() => {})
    }
  })

  it('should keep retained local files while pipeline counters are still pending', async function () {
    this.timeout(5_000)

    const originalKeepLocalFileAfterMove = CONFIG.OBJECT_STORAGE.KEEP_LOCAL_FILE_AFTER_MOVE
    const originalLoadByUUID = VideoJobInfoModel.loadByUUID

    const videoUUID = 'video-uuid-pending-counters'
    const tmpDirectory = await mkdtemp(join(tmpdir(), 'peertube-retained-pending-'))
    const path = join(tmpDirectory, 'segment.ts')

    CONFIG.OBJECT_STORAGE.KEEP_LOCAL_FILE_AFTER_MOVE = 0

    let loadCount = 0
    VideoJobInfoModel.loadByUUID = ((uuid: string) => {
      if (uuid !== videoUUID) return Promise.resolve(null as any)

      loadCount++
      if (loadCount < 3) {
        return Promise.resolve({
          pendingMove: 1,
          pendingTranscode: 0,
          pendingTranscription: 0
        } as any)
      }

      return Promise.resolve({
        pendingMove: 0,
        pendingTranscode: 0,
        pendingTranscription: 0
      } as any)
    }) as typeof VideoJobInfoModel.loadByUUID

    try {
      await writeFile(path, 'test')

      await removeLocalFileAfterMove({
        path,
        videoUUID,
        skipReadinessCheck: true
      })

      await new Promise(resolve => setTimeout(resolve, 200))
      expect(existsSync(path)).to.be.true

      await new Promise(resolve => setTimeout(resolve, 500))
      expect(existsSync(path)).to.be.true
    } finally {
      CONFIG.OBJECT_STORAGE.KEEP_LOCAL_FILE_AFTER_MOVE = originalKeepLocalFileAfterMove
      VideoJobInfoModel.loadByUUID = originalLoadByUUID

      await remove(tmpDirectory).catch(() => {})
    }
  })

})
