/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import * as videoImportBackpressure from '@server/lib/job-queue/handlers/video-import.js'
import { JobQueue } from '@server/lib/job-queue/job-queue.js'
import {
  buildVideoImportBackpressureJobId,
  buildVideoImportLocalPipelineBackpressureMaxJobs,
  isVideoImportLocalPipelineBacklogged,
  shouldDeferVideoImportForLocalPipeline
} from '@server/lib/job-queue/handlers/video-import.js'

describe('video-import local pipeline backpressure', function () {
  it('should derive a conservative downstream backlog limit from processing concurrency', function () {
    expect(buildVideoImportLocalPipelineBackpressureMaxJobs({
      transcodingConcurrency: 5,
      objectStorageConcurrency: 10
    })).to.equal(55)

    expect(buildVideoImportLocalPipelineBackpressureMaxJobs({
      transcodingConcurrency: 1,
      objectStorageConcurrency: 1
    })).to.equal(16)
  })

  it('should defer imports when the downstream local pipeline reaches the limit', function () {
    expect(isVideoImportLocalPipelineBacklogged({ total: 24, maxJobs: 25 })).to.be.false
    expect(isVideoImportLocalPipelineBacklogged({ total: 25, maxJobs: 25 })).to.be.false
    expect(isVideoImportLocalPipelineBacklogged({ total: 26, maxJobs: 25 })).to.be.true
  })

  it('should retry sooner when the backlog only slightly exceeds the threshold', function () {
    const buildVideoImportLocalPipelineBackpressureDelayMs =
      (videoImportBackpressure as any).buildVideoImportLocalPipelineBackpressureDelayMs as (options: {
        total: number
        maxJobs: number
      }) => number

    expect(buildVideoImportLocalPipelineBackpressureDelayMs({
      total: 25,
      maxJobs: 25
    })).to.equal(120000)

    expect(buildVideoImportLocalPipelineBackpressureDelayMs({
      total: 26,
      maxJobs: 25
    })).to.be.lessThan(10 * 60 * 1000)

    expect(buildVideoImportLocalPipelineBackpressureDelayMs({
      total: 40,
      maxJobs: 25
    })).to.equal(10 * 60 * 1000)
  })

  it('should ignore caption-only backlog when deciding whether to defer new imports', function () {
    const getVideoImportLocalPipelineBackpressureTotal =
      (videoImportBackpressure as any).getVideoImportLocalPipelineBackpressureTotal as (backlog: {
        total: number
        byType: Record<string, number>
        uniqueVideoUUIDTotal?: number
      }) => number

    expect(getVideoImportLocalPipelineBackpressureTotal({
      total: 110,
      byType: {
        'move-caption-to-object-storage': 110
      }
    })).to.equal(0)

    expect(getVideoImportLocalPipelineBackpressureTotal({
      total: 25,
      byType: {
        'move-caption-to-object-storage': 10,
        'video-transcoding': 10,
        'move-to-object-storage': 5
      }
    })).to.equal(15)
  })

  it('should prefer unique in-flight video counts over raw granular job totals', function () {
    const getVideoImportLocalPipelineBackpressureTotal =
      (videoImportBackpressure as any).getVideoImportLocalPipelineBackpressureTotal as (backlog: {
        total: number
        byType: Record<string, number>
        uniqueVideoUUIDTotal?: number
      }) => number

    expect(getVideoImportLocalPipelineBackpressureTotal({
      total: 250,
      byType: {
        'video-transcoding': 40,
        'move-hls-playlist-to-object-storage': 200,
        'move-caption-to-object-storage': 10
      },
      uniqueVideoUUIDTotal: 18
    })).to.equal(18)
  })

  it('should prefer import-relevant in-flight video counts over all follow-up pipeline work', function () {
    const getVideoImportLocalPipelineBackpressureTotal =
      (videoImportBackpressure as any).getVideoImportLocalPipelineBackpressureTotal as (backlog: {
        total: number
        byType: Record<string, number>
        importRelevantUniqueVideoUUIDTotal?: number
        uniqueVideoUUIDTotal?: number
      }) => number

    expect(getVideoImportLocalPipelineBackpressureTotal({
      total: 250,
      byType: {
        'video-transcoding': 40,
        'move-hls-playlist-to-object-storage': 200,
        'move-caption-to-object-storage': 10
      },
      importRelevantUniqueVideoUUIDTotal: 12,
      uniqueVideoUUIDTotal: 48
    })).to.equal(12)
  })

  it('should count optional new-video transcodes because they retain local HLS input', function () {
    const isImportRelevantLocalPipelineJobData =
      (JobQueue.prototype as any).isImportRelevantLocalPipelineJobData as (
        jobType: string,
        data: Record<string, any>
      ) => boolean

    expect(isImportRelevantLocalPipelineJobData.call({}, 'video-transcoding', {
      isNewVideo: true,
      transcodingPriority: 'required'
    })).to.be.true

    expect(isImportRelevantLocalPipelineJobData.call({}, 'video-transcoding', {
      isNewVideo: true,
      transcodingPriority: 'optional'
    })).to.be.true

    expect(isImportRelevantLocalPipelineJobData.call({}, 'video-transcoding', {
      isNewVideo: false,
      transcodingPriority: 'optional'
    })).to.be.false

    expect(isImportRelevantLocalPipelineJobData.call({}, 'transcoding-job-builder', {
      optimizeJob: { isNewVideo: true }
    })).to.be.true

    expect(isImportRelevantLocalPipelineJobData.call({}, 'transcoding-job-builder', {
      sequentialJobs: [
        [
          { payload: { isNewVideo: true, transcodingPriority: 'optional' } }
        ]
      ]
    })).to.be.true

    expect(isImportRelevantLocalPipelineJobData.call({}, 'transcoding-job-builder', {
      jobs: [
        { payload: { isNewVideo: true, transcodingPriority: 'optional' } }
      ]
    })).to.be.true

    expect(isImportRelevantLocalPipelineJobData.call({}, 'transcoding-job-builder', {
      sequentialJobs: [
        [
          { payload: { isNewVideo: true, transcodingPriority: 'required' } }
        ]
      ]
    })).to.be.true
  })

  it('should bucket delayed import job IDs so repeated deferrals can be requeued later', function () {
    expect(buildVideoImportBackpressureJobId(42, 1_234_567, 600_000)).to.equal('video-import-backpressure-42-2')
    expect(buildVideoImportBackpressureJobId(42, 1_834_567, 600_000)).to.equal('video-import-backpressure-42-3')
  })

  it('should keep a backpressure retry pending until downstream work has drained', function () {
    expect(shouldDeferVideoImportForLocalPipeline({
      total: 30,
      maxJobs: 25
    })).to.be.true

    expect(shouldDeferVideoImportForLocalPipeline({
      total: 30,
      maxJobs: 25
    })).to.be.true
  })
})
