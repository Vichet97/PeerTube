/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { FileStorage, VideoFileFormatFlag, VideoFileStream } from '@peertube/peertube-models'
import { buildRetryableImportedModelFactory } from '@server/lib/job-queue/handlers/video-import-retryable-model.js'

type RetryableVideoFile = {
  id?: number
  isNewRecord: boolean
  videoId: number
  resolution: number
  width: number
  height: number
  size: number
  extname: string
  fps: number
  formatFlags: number
  streams: number
  filename: string
  storage: number
}

type RetryableThumbnail = {
  id?: number
  isNewRecord: boolean
  videoId: number
  filename: string
  width: number
  height: number
  aspectRatio: string
  automaticallyGenerated: boolean
  cached: boolean
  storage: number
}

describe('video-import persistence retries', function () {
  it('should build a fresh video file for each retry so rolled-back inserts do not leak stale identity', function () {
    const createRetryVideoFile = buildRetryableImportedModelFactory<RetryableVideoFile>({
      toJSON: () => ({
        videoId: 42,
        resolution: 720,
        width: 1280,
        height: 720,
        size: 1024,
        extname: '.mp4',
        fps: 30,
        formatFlags: VideoFileFormatFlag.WEB_VIDEO,
        streams: VideoFileStream.AUDIO | VideoFileStream.VIDEO,
        filename: '720.mp4',
        storage: FileStorage.FILE_SYSTEM
      })
    } as any, attributes => ({
      ...(attributes as Omit<RetryableVideoFile, 'id' | 'isNewRecord'>),
      isNewRecord: true
    }))

    const firstAttempt = createRetryVideoFile()
    firstAttempt.id = 999 as any
    firstAttempt.isNewRecord = false

    const secondAttempt = createRetryVideoFile()

    expect(secondAttempt).to.not.equal(firstAttempt)
    expect(secondAttempt.id).to.be.undefined
    expect(secondAttempt.isNewRecord).to.be.true
    expect(secondAttempt.videoId).to.equal(42)
    expect(secondAttempt.filename).to.equal('720.mp4')
    expect(secondAttempt.resolution).to.equal(720)
    expect(secondAttempt.fps).to.equal(30)
  })

  it('should build fresh thumbnails for each retry so rolled-back inserts do not leak stale identity', function () {
    const createRetryThumbnail = buildRetryableImportedModelFactory<RetryableThumbnail>({
      toJSON: () => ({
        videoId: 42,
        filename: 'thumb.jpg',
        width: 1280,
        height: 720,
        aspectRatio: '16:9',
        automaticallyGenerated: true,
        cached: false,
        storage: FileStorage.FILE_SYSTEM
      })
    } as any, attributes => ({
      ...(attributes as Omit<RetryableThumbnail, 'id' | 'isNewRecord'>),
      isNewRecord: true
    }))

    const firstAttempt = createRetryThumbnail()
    firstAttempt.id = 321 as any
    firstAttempt.isNewRecord = false

    const secondAttempt = createRetryThumbnail()

    expect(secondAttempt).to.not.equal(firstAttempt)
    expect(secondAttempt.id).to.be.undefined
    expect(secondAttempt.isNewRecord).to.be.true
    expect(secondAttempt.videoId).to.equal(42)
    expect(secondAttempt.filename).to.equal('thumb.jpg')
  })
})
