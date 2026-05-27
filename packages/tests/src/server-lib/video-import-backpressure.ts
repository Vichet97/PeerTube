/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import {
  buildVideoImportBackpressureJobId,
  buildVideoImportLocalPipelineBackpressureMaxJobs,
  isVideoImportLocalPipelineBacklogged
} from '@peertube/peertube-server/core/lib/job-queue/handlers/video-import.js'

describe('video-import local pipeline backpressure', function () {
  it('should derive a conservative downstream backlog limit from processing concurrency', function () {
    expect(buildVideoImportLocalPipelineBackpressureMaxJobs({
      transcodingConcurrency: 5,
      objectStorageConcurrency: 10
    })).to.equal(20)

    expect(buildVideoImportLocalPipelineBackpressureMaxJobs({
      transcodingConcurrency: 1,
      objectStorageConcurrency: 1
    })).to.equal(10)
  })

  it('should defer imports when the downstream local pipeline reaches the limit', function () {
    expect(isVideoImportLocalPipelineBacklogged({ total: 19, maxJobs: 20 })).to.be.false
    expect(isVideoImportLocalPipelineBacklogged({ total: 20, maxJobs: 20 })).to.be.true
    expect(isVideoImportLocalPipelineBacklogged({ total: 21, maxJobs: 20 })).to.be.true
  })

  it('should bucket delayed import job IDs so repeated deferrals can be requeued later', function () {
    expect(buildVideoImportBackpressureJobId(42, 1_234_567, 600_000)).to.equal('video-import-backpressure-42-2')
    expect(buildVideoImportBackpressureJobId(42, 1_834_567, 600_000)).to.equal('video-import-backpressure-42-3')
  })
})
