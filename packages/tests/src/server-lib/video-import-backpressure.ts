/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import {
  buildVideoImportLocalStorageCapacityJobId,
  isDeferredVideoImportJobId,
  isVideoImportBackpressureJobId,
  isVideoImportLocalStorageCapacityJobId
} from '@server/lib/job-queue/handlers/video-import-processability.js'
import {
  LocalStorageImportCapacity,
  shouldDeferVideoImportForLocalStorage
} from '@server/lib/local-storage-import-admission.js'

const GB = 1024 ** 3

function capacity (usageGB: number): LocalStorageImportCapacity {
  return {
    usageBytes: usageGB * GB,
    limitBytes: 1024 * GB,
    resumeUsageBytes: 1022 * GB
  }
}

describe('video-import local storage admission', function () {
  it('should accept imports below the configured local storage limit', function () {
    expect(shouldDeferVideoImportForLocalStorage(capacity(1023.99), { hasDeferredImports: false })).to.be.false
  })

  it('should park imports once local storage reaches the configured limit', function () {
    expect(shouldDeferVideoImportForLocalStorage(capacity(1024), { hasDeferredImports: false })).to.be.true
  })

  it('should keep delayed imports parked until the configured headroom is free', function () {
    expect(shouldDeferVideoImportForLocalStorage(capacity(1022.01), { hasDeferredImports: true })).to.be.true
    expect(shouldDeferVideoImportForLocalStorage(capacity(1022), { hasDeferredImports: true })).to.be.false
  })

  it('should use a distinct delayed-job identity for local-storage admission', function () {
    const jobId = buildVideoImportLocalStorageCapacityJobId(42, 1_234_567)

    expect(jobId).to.equal('video-import-local-storage-capacity-42-1234567')
    expect(isVideoImportLocalStorageCapacityJobId(jobId)).to.be.true
    expect(isDeferredVideoImportJobId(jobId)).to.be.true
    expect(isVideoImportBackpressureJobId(jobId)).to.be.false
  })
})
