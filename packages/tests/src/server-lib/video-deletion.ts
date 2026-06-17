/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { isVideoDeletionPending } from '@server/lib/video-deletion.js'
import { Redis } from '@peertube/peertube-server/core/lib/redis.js'
import { VideoModel } from '@peertube/peertube-server/core/models/video/video.js'

describe('video deletion flag validation', function () {
  it('should clear stale legacy deletion flags when the video still exists', async function () {
    const originalGetFlagState = Redis.Instance.getVideoDeletionFlagState
    const originalClearFlag = Redis.Instance.clearVideoDeletionFlag
    const originalLoad = VideoModel.load

    let cleared = false

    Redis.Instance.getVideoDeletionFlagState = (() => Promise.resolve({
      legacy: true,
      setAt: undefined
    })) as typeof Redis.Instance.getVideoDeletionFlagState
    Redis.Instance.clearVideoDeletionFlag = ((videoUUID: string) => {
      cleared = videoUUID === 'video-uuid'
      return Promise.resolve(undefined as any)
    }) as typeof Redis.Instance.clearVideoDeletionFlag
    VideoModel.load = (() => Promise.resolve({ id: 1 } as any)) as typeof VideoModel.load

    try {
      const result = await isVideoDeletionPending('video-uuid', { confirmDelayMs: 0 })

      expect(result).to.be.false
      expect(cleared).to.be.true
    } finally {
      Redis.Instance.getVideoDeletionFlagState = originalGetFlagState
      Redis.Instance.clearVideoDeletionFlag = originalClearFlag
      VideoModel.load = originalLoad
    }
  })

  it('should keep deletion pending when the legacy flagged video is already gone', async function () {
    const originalGetFlagState = Redis.Instance.getVideoDeletionFlagState
    const originalClearFlag = Redis.Instance.clearVideoDeletionFlag
    const originalLoad = VideoModel.load

    let cleared = false

    Redis.Instance.getVideoDeletionFlagState = (() => Promise.resolve({
      legacy: true,
      setAt: undefined
    })) as typeof Redis.Instance.getVideoDeletionFlagState
    Redis.Instance.clearVideoDeletionFlag = (() => {
      cleared = true
      return Promise.resolve(undefined as any)
    }) as typeof Redis.Instance.clearVideoDeletionFlag
    VideoModel.load = (() => Promise.resolve(undefined as any)) as typeof VideoModel.load

    try {
      const result = await isVideoDeletionPending('video-uuid', { confirmDelayMs: 0 })

      expect(result).to.be.true
      expect(cleared).to.be.false
    } finally {
      Redis.Instance.getVideoDeletionFlagState = originalGetFlagState
      Redis.Instance.clearVideoDeletionFlag = originalClearFlag
      VideoModel.load = originalLoad
    }
  })

  it('should trust structured deletion flags without clearing them while the video still exists', async function () {
    const originalGetFlagState = Redis.Instance.getVideoDeletionFlagState
    const originalClearFlag = Redis.Instance.clearVideoDeletionFlag
    const originalLoad = VideoModel.load

    let cleared = false

    Redis.Instance.getVideoDeletionFlagState = (() => Promise.resolve({
      legacy: false,
      setAt: Date.now()
    })) as typeof Redis.Instance.getVideoDeletionFlagState
    Redis.Instance.clearVideoDeletionFlag = (() => {
      cleared = true
      return Promise.resolve(undefined as any)
    }) as typeof Redis.Instance.clearVideoDeletionFlag
    VideoModel.load = (() => Promise.resolve({ id: 1 } as any)) as typeof VideoModel.load

    try {
      const result = await isVideoDeletionPending('video-uuid', { confirmDelayMs: 0 })

      expect(result).to.be.true
      expect(cleared).to.be.false
    } finally {
      Redis.Instance.getVideoDeletionFlagState = originalGetFlagState
      Redis.Instance.clearVideoDeletionFlag = originalClearFlag
      VideoModel.load = originalLoad
    }
  })
})
