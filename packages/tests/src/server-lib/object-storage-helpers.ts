/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { buildKey, isTransientObjectStorageError } from '@peertube/peertube-server/core/lib/object-storage/object-storage-helpers.js'

describe('object-storage-helpers', function () {
  it('should detect transient object storage backend errors', function () {
    expect(isTransientObjectStorageError(new Error('XMinioBackendDown: Object storage backend is unreachable'))).to.be.true
    expect(isTransientObjectStorageError({ $response: { statusCode: 500 } })).to.be.true
    expect(isTransientObjectStorageError({ message: 'NoSuchKey: The specified key does not exist' })).to.be.false
  })

  it('should only treat bucket prefixes at the start of keys as already applied', function () {
    const bucketInfo = {
      BUCKET_NAME: 'bucket',
      BASE_URL: 'https://example.com',
      PREFIX: 'prefix/'
    }

    expect(buildKey('prefix/video.mp4', bucketInfo)).to.equal('prefix/video.mp4')
    expect(buildKey('videos/prefix/video.mp4', bucketInfo)).to.equal('prefix/videos/prefix/video.mp4')
    expect(buildKey('video.mp4', { ...bucketInfo, PREFIX: undefined })).to.equal('video.mp4')
  })
})
