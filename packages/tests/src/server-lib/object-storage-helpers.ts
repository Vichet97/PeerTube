/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { isTransientObjectStorageError } from '@peertube/peertube-server/core/lib/object-storage/object-storage-helpers.js'

describe('object-storage-helpers', function () {
  it('should detect transient object storage backend errors', function () {
    expect(isTransientObjectStorageError(new Error('XMinioBackendDown: Object storage backend is unreachable'))).to.be.true
    expect(isTransientObjectStorageError({ $response: { statusCode: 500 } })).to.be.true
    expect(isTransientObjectStorageError({ message: 'NoSuchKey: The specified key does not exist' })).to.be.false
  })
})
