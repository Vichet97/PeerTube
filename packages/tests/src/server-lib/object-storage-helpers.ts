/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import {
  buildKey,
  isTransientObjectStorageError,
  retryTransientObjectStorageOperation
} from '@peertube/peertube-server/core/lib/object-storage/object-storage-helpers.js'

describe('object-storage-helpers', function () {
  it('should detect transient object storage backend errors', function () {
    expect(isTransientObjectStorageError(new Error('XMinioBackendDown: Object storage backend is unreachable'))).to.be.true
    expect(isTransientObjectStorageError({ $response: { statusCode: 500 } })).to.be.true
    expect(isTransientObjectStorageError(new Error('socket hang up'))).to.be.true
    expect(
      isTransientObjectStorageError(new Error('Client network socket disconnected before secure TLS connection was established'))
    ).to.be.true
    expect(isTransientObjectStorageError(new Error('AggregateError: internalConnectMultipleTimeout'))).to.be.true
    expect(isTransientObjectStorageError(new Error('AggregateError: permanent validation failure'))).to.be.false
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

  it('should retry transient object storage operations before failing', async function () {
    let attempts = 0

    const result = await retryTransientObjectStorageOperation({
      description: 'upload test file',
      delayMs: 0,
      run: () => {
        attempts++

        if (attempts < 3) {
          return Promise.reject(new Error('Client network socket disconnected before secure TLS connection was established'))
        }

        return Promise.resolve('ok')
      }
    })

    expect(result).to.equal('ok')
    expect(attempts).to.equal(3)
  })

  it('should not retry permanent object storage errors', async function () {
    let attempts = 0

    try {
      await retryTransientObjectStorageOperation({
        description: 'upload test file',
        delayMs: 0,
        run: () => {
          attempts++
          return Promise.reject(new Error('NoSuchKey: The specified key does not exist'))
        }
      })

      expect.fail('Expected operation to throw')
    } catch (err) {
      expect((err as Error).message).to.include('NoSuchKey')
      expect(attempts).to.equal(1)
    }
  })
})
