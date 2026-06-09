/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { VideoImportState, VideoState } from '@peertube/peertube-models'
import {
  getVideoImportSkipReason,
  isVideoImportBackpressureJobId
} from '@server/lib/job-queue/handlers/video-import-processability.js'

describe('video-import processability', function () {
  it('should detect delayed backpressure job ids', function () {
    expect(isVideoImportBackpressureJobId('video-import-backpressure-120013-5214422')).to.be.true
    expect(isVideoImportBackpressureJobId('135422')).to.be.false
  })

  it('should skip stale delayed imports that already succeeded or are already processing', function () {
    expect(getVideoImportSkipReason({
      importState: VideoImportState.SUCCESS,
      videoState: VideoState.PUBLISHED
    })).to.equal('already-succeeded')

    expect(getVideoImportSkipReason({
      importState: VideoImportState.PROCESSING,
      videoState: VideoState.TO_IMPORT
    })).to.equal('already-processing')
  })

  it('should skip imports whose linked video already moved past the import stage', function () {
    expect(getVideoImportSkipReason({
      importState: VideoImportState.PENDING,
      videoState: VideoState.PUBLISHED
    })).to.equal('video-already-past-import-stage')

    expect(getVideoImportSkipReason({
      importState: VideoImportState.FAILED,
      videoState: VideoState.TRANSCODING_FAILED
    })).to.equal('video-already-past-import-stage')
  })

  it('should still allow legitimate retries for imports that are still in import states', function () {
    expect(getVideoImportSkipReason({
      importState: VideoImportState.PENDING,
      videoState: VideoState.TO_IMPORT
    })).to.be.undefined

    expect(getVideoImportSkipReason({
      importState: VideoImportState.FAILED,
      videoState: VideoState.TO_IMPORT_FAILED
    })).to.be.undefined
  })
})
