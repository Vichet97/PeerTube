/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import {
  buildObjectStorageNodeHttpHandlerOptions,
  getClient,
  getEndpoint,
  getReadClient,
  getReadEndpoint
} from '@peertube/peertube-server/core/lib/object-storage/shared/client.js'
import { CONFIG } from '@peertube/peertube-server/core/initializers/config.js'
import { buildObjectStoragePublicFileUrl, buildObjectStorageRawUrl } from '@peertube/peertube-server/core/lib/object-storage/urls.js'

describe('object-storage S3 client handler options', function () {
  it('should use keep-alive agents and bounded timeouts without a proxy', function () {
    const originalHttpProxy = process.env.HTTP_PROXY
    const originalHttpsProxy = process.env.HTTPS_PROXY
    delete process.env.HTTP_PROXY
    delete process.env.HTTPS_PROXY

    try {
      const options = buildObjectStorageNodeHttpHandlerOptions()

      expect(options.connectionTimeout).to.equal(30_000)
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

  it('should keep read and write S3 clients isolated when endpoints are identical', async function () {
    const originalEndpoint = CONFIG.OBJECT_STORAGE.ENDPOINT
    const originalReadEndpoint = CONFIG.OBJECT_STORAGE.READ_ENDPOINT
    const originalForcePathStyle = CONFIG.OBJECT_STORAGE.FORCE_PATH_STYLE
    const originalReadForcePathStyle = CONFIG.OBJECT_STORAGE.READ_FORCE_PATH_STYLE

    try {
      CONFIG.OBJECT_STORAGE.ENDPOINT = 'https://same-object-storage.example'
      CONFIG.OBJECT_STORAGE.READ_ENDPOINT = 'https://same-object-storage.example'
      CONFIG.OBJECT_STORAGE.FORCE_PATH_STYLE = true
      CONFIG.OBJECT_STORAGE.READ_FORCE_PATH_STYLE = true

      const [ writeClient, readClient ] = await Promise.all([ getClient(), getReadClient() ])

      expect(readClient).not.to.equal(writeClient)
    } finally {
      CONFIG.OBJECT_STORAGE.ENDPOINT = originalEndpoint
      CONFIG.OBJECT_STORAGE.READ_ENDPOINT = originalReadEndpoint
      CONFIG.OBJECT_STORAGE.FORCE_PATH_STYLE = originalForcePathStyle
      CONFIG.OBJECT_STORAGE.READ_FORCE_PATH_STYLE = originalReadForcePathStyle
    }
  })

  it('should use the configured read endpoint for public URLs while keeping the write endpoint for raw/internal URLs', async function () {
    const originalEndpoint = CONFIG.OBJECT_STORAGE.ENDPOINT
    const originalReadEndpoint = CONFIG.OBJECT_STORAGE.READ_ENDPOINT
    const originalReadForcePathStyle = CONFIG.OBJECT_STORAGE.READ_FORCE_PATH_STYLE
    const originalReplaceReadBucketName = CONFIG.OBJECT_STORAGE.REPLACE_READ_BUCKET_NAME
    const originalUsePresigned = CONFIG.OBJECT_STORAGE.USE_PRESIGNED_PUBLIC_URLS
    const originalForcePathStyle = CONFIG.OBJECT_STORAGE.FORCE_PATH_STYLE
    const originalBaseUrl = CONFIG.OBJECT_STORAGE.THUMBNAILS.BASE_URL

    try {
      CONFIG.OBJECT_STORAGE.ENDPOINT = 'sf-objectstorage.com'
      CONFIG.OBJECT_STORAGE.READ_ENDPOINT = 'sf.aml.ccwu.cc'
      CONFIG.OBJECT_STORAGE.USE_PRESIGNED_PUBLIC_URLS = false
      CONFIG.OBJECT_STORAGE.FORCE_PATH_STYLE = true
      CONFIG.OBJECT_STORAGE.THUMBNAILS.BASE_URL = ''

      expect(getEndpoint()).to.equal('https://sf-objectstorage.com')
      expect(getReadEndpoint()).to.equal('https://sf.aml.ccwu.cc')

      const rawUrl = buildObjectStorageRawUrl(CONFIG.OBJECT_STORAGE.THUMBNAILS, 'abc.jpg')

      expect(rawUrl).to.equal('https://sf-objectstorage.com/bucket-873-1217/thumbnails/abc.jpg')

      CONFIG.OBJECT_STORAGE.READ_FORCE_PATH_STYLE = true
      CONFIG.OBJECT_STORAGE.REPLACE_READ_BUCKET_NAME = undefined
      expect(
        await buildObjectStoragePublicFileUrl({
          bucket: CONFIG.OBJECT_STORAGE.THUMBNAILS,
          key: 'abc.jpg',
          fileType: 'thumbnails'
        })
      ).to.equal('https://sf.aml.ccwu.cc/bucket-873-1217/thumbnails/abc.jpg')

      CONFIG.OBJECT_STORAGE.REPLACE_READ_BUCKET_NAME = 'storage1'
      expect(
        await buildObjectStoragePublicFileUrl({
          bucket: CONFIG.OBJECT_STORAGE.THUMBNAILS,
          key: 'abc.jpg',
          fileType: 'thumbnails'
        })
      ).to.equal('https://sf.aml.ccwu.cc/storage1/thumbnails/abc.jpg')

      CONFIG.OBJECT_STORAGE.REPLACE_READ_BUCKET_NAME = ''
      expect(
        await buildObjectStoragePublicFileUrl({
          bucket: CONFIG.OBJECT_STORAGE.THUMBNAILS,
          key: 'abc.jpg',
          fileType: 'thumbnails'
        })
      ).to.equal('https://sf.aml.ccwu.cc/thumbnails/abc.jpg')

      CONFIG.OBJECT_STORAGE.READ_FORCE_PATH_STYLE = false
      CONFIG.OBJECT_STORAGE.REPLACE_READ_BUCKET_NAME = 'storage1'
      expect(
        await buildObjectStoragePublicFileUrl({
          bucket: CONFIG.OBJECT_STORAGE.THUMBNAILS,
          key: 'abc.jpg',
          fileType: 'thumbnails'
        })
      ).to.equal('https://storage1.sf.aml.ccwu.cc/thumbnails/abc.jpg')

      CONFIG.OBJECT_STORAGE.REPLACE_READ_BUCKET_NAME = ''
      expect(
        await buildObjectStoragePublicFileUrl({
          bucket: CONFIG.OBJECT_STORAGE.THUMBNAILS,
          key: 'abc.jpg',
          fileType: 'thumbnails'
        })
      ).to.equal('https://sf.aml.ccwu.cc/thumbnails/abc.jpg')
    } finally {
      CONFIG.OBJECT_STORAGE.ENDPOINT = originalEndpoint
      CONFIG.OBJECT_STORAGE.READ_ENDPOINT = originalReadEndpoint
      CONFIG.OBJECT_STORAGE.READ_FORCE_PATH_STYLE = originalReadForcePathStyle
      CONFIG.OBJECT_STORAGE.REPLACE_READ_BUCKET_NAME = originalReplaceReadBucketName
      CONFIG.OBJECT_STORAGE.USE_PRESIGNED_PUBLIC_URLS = originalUsePresigned
      CONFIG.OBJECT_STORAGE.FORCE_PATH_STYLE = originalForcePathStyle
      CONFIG.OBJECT_STORAGE.THUMBNAILS.BASE_URL = originalBaseUrl
    }
  })

  it('should sign non-path-style read URLs directly against the final replacement host/path', async function () {
    const originalEndpoint = CONFIG.OBJECT_STORAGE.ENDPOINT
    const originalReadEndpoint = CONFIG.OBJECT_STORAGE.READ_ENDPOINT
    const originalReadForcePathStyle = CONFIG.OBJECT_STORAGE.READ_FORCE_PATH_STYLE
    const originalReplaceReadBucketName = CONFIG.OBJECT_STORAGE.REPLACE_READ_BUCKET_NAME
    const originalUsePresigned = CONFIG.OBJECT_STORAGE.USE_PRESIGNED_PUBLIC_URLS
    const originalForcePathStyle = CONFIG.OBJECT_STORAGE.FORCE_PATH_STYLE
    const originalBaseUrl = CONFIG.OBJECT_STORAGE.WEB_VIDEOS.BASE_URL
    const originalAccessKeyId = CONFIG.OBJECT_STORAGE.CREDENTIALS.ACCESS_KEY_ID
    const originalSecretAccessKey = CONFIG.OBJECT_STORAGE.CREDENTIALS.SECRET_ACCESS_KEY

    try {
      CONFIG.OBJECT_STORAGE.ENDPOINT = 'sf-objectstorage.com'
      CONFIG.OBJECT_STORAGE.READ_ENDPOINT = 'sf.aml.ccwu.cc'
      CONFIG.OBJECT_STORAGE.READ_FORCE_PATH_STYLE = false
      CONFIG.OBJECT_STORAGE.USE_PRESIGNED_PUBLIC_URLS = true
      CONFIG.OBJECT_STORAGE.FORCE_PATH_STYLE = true
      CONFIG.OBJECT_STORAGE.WEB_VIDEOS.BASE_URL = ''
      CONFIG.OBJECT_STORAGE.CREDENTIALS.ACCESS_KEY_ID = 'test-access-key'
      CONFIG.OBJECT_STORAGE.CREDENTIALS.SECRET_ACCESS_KEY = 'test-secret-key'

      CONFIG.OBJECT_STORAGE.REPLACE_READ_BUCKET_NAME = 'storage1'
      const replacedHostUrl = await buildObjectStoragePublicFileUrl({
        bucket: CONFIG.OBJECT_STORAGE.WEB_VIDEOS,
        key: 'abc.mp4',
        fileType: 'web-videos'
      })

      expect(replacedHostUrl.split('?')[0]).to.equal('https://storage1.sf.aml.ccwu.cc/web-videos/abc.mp4')

      CONFIG.OBJECT_STORAGE.REPLACE_READ_BUCKET_NAME = ''
      const strippedBucketUrl = await buildObjectStoragePublicFileUrl({
        bucket: CONFIG.OBJECT_STORAGE.WEB_VIDEOS,
        key: 'abc.mp4',
        fileType: 'web-videos'
      })

      expect(strippedBucketUrl.split('?')[0]).to.equal('https://sf.aml.ccwu.cc/web-videos/abc.mp4')
    } finally {
      CONFIG.OBJECT_STORAGE.ENDPOINT = originalEndpoint
      CONFIG.OBJECT_STORAGE.READ_ENDPOINT = originalReadEndpoint
      CONFIG.OBJECT_STORAGE.READ_FORCE_PATH_STYLE = originalReadForcePathStyle
      CONFIG.OBJECT_STORAGE.REPLACE_READ_BUCKET_NAME = originalReplaceReadBucketName
      CONFIG.OBJECT_STORAGE.USE_PRESIGNED_PUBLIC_URLS = originalUsePresigned
      CONFIG.OBJECT_STORAGE.FORCE_PATH_STYLE = originalForcePathStyle
      CONFIG.OBJECT_STORAGE.WEB_VIDEOS.BASE_URL = originalBaseUrl
      CONFIG.OBJECT_STORAGE.CREDENTIALS.ACCESS_KEY_ID = originalAccessKeyId
      CONFIG.OBJECT_STORAGE.CREDENTIALS.SECRET_ACCESS_KEY = originalSecretAccessKey
    }
  })
})
