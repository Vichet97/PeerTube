/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { buildObjectStorageNodeHttpHandlerOptions } from '@peertube/peertube-server/core/lib/object-storage/shared/client.js'

describe('object-storage S3 client handler options', function () {
  it('should use keep-alive agents and bounded timeouts without a proxy', function () {
    const originalHttpProxy = process.env.HTTP_PROXY
    const originalHttpsProxy = process.env.HTTPS_PROXY
    delete process.env.HTTP_PROXY
    delete process.env.HTTPS_PROXY

    try {
      const options = buildObjectStorageNodeHttpHandlerOptions()

      expect(options.connectionTimeout).to.equal(10_000)
      expect(options.socketTimeout).to.be.a('number')
      expect((options.httpAgent as any).options.keepAlive).to.equal(true)
      expect((options.httpsAgent as any).options.keepAlive).to.equal(true)
      expect((options.httpAgent as any).maxSockets).to.be.greaterThan(0)
      expect((options.httpsAgent as any).maxSockets).to.be.greaterThan(0)
    } finally {
      if (originalHttpProxy !== undefined) process.env.HTTP_PROXY = originalHttpProxy
      if (originalHttpsProxy !== undefined) process.env.HTTPS_PROXY = originalHttpsProxy
    }
  })
})
