import { expect } from 'chai'
import { isMinimalMoveToObjectStoragePayload } from '@server/lib/job-queue/move-job-payload.js'

describe('move-job payloads', function () {
  it('accepts the retry marker on a minimal move payload', function () {
    expect(isMinimalMoveToObjectStoragePayload({ videoUUID: 'video-uuid' })).to.equal(true)
    expect(isMinimalMoveToObjectStoragePayload({ videoUUID: 'video-uuid', retryOfFailedJob: true })).to.equal(true)
    expect(isMinimalMoveToObjectStoragePayload({ videoUUID: 'video-uuid', retryOfFailedJob: false })).to.equal(false)
    expect(isMinimalMoveToObjectStoragePayload({ videoUUID: 'video-uuid', fileId: 42 })).to.equal(false)
  })
})
