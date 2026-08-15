import type { S3Client } from '@aws-sdk/client-s3'
import { logger } from '@server/helpers/logger.js'
import { HttpProxyAgent, HttpsProxyAgent } from '@server/helpers/hpagent.js'
import { getProxy, isProxyEnabled } from '@server/helpers/proxy.js'
import { CONFIG } from '@server/initializers/config.js'
import http from 'http'
import https from 'https'
import { getObjectStorageClientPool, ObjectStorageClientPool } from './client-pool.js'
import { lTags } from './logger.js'
import { getReadForcePathStyle } from '../read-url.js'

const writeS3ClientPromises = new Map<string, Promise<S3Client>>()
const writeS3ClientsResolved = new Map<string, S3Client>()

let readS3ClientPromise: Promise<S3Client>
let readS3ClientEndpoint: string

const DEFAULT_OBJECT_STORAGE_CONNECTION_TIMEOUT_MS = 30_000
const DEFAULT_OBJECT_STORAGE_SOCKET_TIMEOUT_MS = 120_000

export function getClient () {
  const endpoint = getEndpoint()
  const pool = getObjectStorageClientPool()
  const cacheKey = `${pool}|${endpoint}`
  const cachedClient = writeS3ClientPromises.get(cacheKey)
  if (cachedClient !== undefined) return cachedClient

  const clientPromise = buildClient(endpoint, CONFIG.OBJECT_STORAGE.FORCE_PATH_STYLE, pool)
    .then(client => {
      writeS3ClientsResolved.set(cacheKey, client)
      return client
    })

  writeS3ClientPromises.set(cacheKey, clientPromise)
  return clientPromise
}

export function getReadClient () {
  const endpoint = getReadEndpoint()
  const forcePathStyle = getReadForcePathStyle()

  if (readS3ClientPromise !== undefined && readS3ClientEndpoint === `${endpoint}|${forcePathStyle}`) return readS3ClientPromise

  readS3ClientEndpoint = `${endpoint}|${forcePathStyle}`
  readS3ClientPromise = buildClient(endpoint, forcePathStyle, 'read')
    .then(client => client)

  return readS3ClientPromise
}

// Synchronous access to the default write client (only available after first getClient() call completes)
export function getClientSync (): S3Client | undefined {
  return writeS3ClientsResolved.get(`write|${getEndpoint()}`)
}

export function getEndpoint () {
  return normalizeEndpoint(CONFIG.OBJECT_STORAGE.ENDPOINT)
}

export function getReadEndpoint () {
  const endpointConfig = CONFIG.OBJECT_STORAGE.READ_ENDPOINT || CONFIG.OBJECT_STORAGE.ENDPOINT
  return normalizeEndpoint(endpointConfig)
}

function normalizeEndpoint (endpointConfig: string) {
  return endpointConfig.startsWith('http://') || endpointConfig.startsWith('https://')
    ? endpointConfig
    : 'https://' + endpointConfig
}

async function buildClient (endpoint: string, forcePathStyle = CONFIG.OBJECT_STORAGE.FORCE_PATH_STYLE, pool: ObjectStorageClientPool) {
  const OBJECT_STORAGE = CONFIG.OBJECT_STORAGE

  const { S3Client } = await import('@aws-sdk/client-s3')

  const requestHandler = await getProxyRequestHandler(pool)

  const client = new S3Client({
    endpoint,
    region: OBJECT_STORAGE.REGION,
    credentials: OBJECT_STORAGE.CREDENTIALS.ACCESS_KEY_ID
      ? {
        accessKeyId: OBJECT_STORAGE.CREDENTIALS.ACCESS_KEY_ID,
        secretAccessKey: OBJECT_STORAGE.CREDENTIALS.SECRET_ACCESS_KEY
      }
      : undefined,
    requestHandler,
    maxAttempts: CONFIG.OBJECT_STORAGE.MAX_REQUEST_ATTEMPTS,
    forcePathStyle,

    // Default behaviour has incompatibilities with some S3 providers: https://github.com/aws/aws-sdk-js-v3/issues/6810
    requestChecksumCalculation: 'WHEN_REQUIRED',
    responseChecksumValidation: 'WHEN_REQUIRED'
  })

  logger.info('Initialized %s S3 client %s with region %s.', pool, endpoint, OBJECT_STORAGE.REGION, lTags())

  return client
}

// ---------------------------------------------------------------------------
// Private
// ---------------------------------------------------------------------------

async function getProxyRequestHandler (pool: ObjectStorageClientPool) {
  const { NodeHttpHandler } = await import('@smithy/node-http-handler')
  return new NodeHttpHandler(buildObjectStorageNodeHttpHandlerOptions(pool))
}

export function getObjectStorageSocketTimeoutMs () {
  return CONFIG.OBJECT_STORAGE.PROXY.REQUEST_TIMEOUT_MS ?? DEFAULT_OBJECT_STORAGE_SOCKET_TIMEOUT_MS
}

export function getObjectStorageConnectionTimeoutMs (socketTimeoutMs = getObjectStorageSocketTimeoutMs()) {
  return Math.min(socketTimeoutMs, DEFAULT_OBJECT_STORAGE_CONNECTION_TIMEOUT_MS)
}

export function getObjectStorageMaxSockets (pool: ObjectStorageClientPool = 'write') {
  const maxSockets = Math.max(
    16,
    Math.min(64, Math.max(1, CONFIG.OBJECT_STORAGE.CONCURRENCY) * 3)
  )

  if (pool === 'move') return Math.max(8, Math.floor(maxSockets / 2))

  return maxSockets
}

export function buildObjectStorageNodeHttpHandlerOptions (pool: ObjectStorageClientPool = 'write') {
  const maxSockets = getObjectStorageMaxSockets(pool)
  const maxFreeSockets = Math.min(16, maxSockets)
  const socketTimeout = getObjectStorageSocketTimeoutMs()
  const connectionTimeout = getObjectStorageConnectionTimeoutMs(socketTimeout)

  if (isProxyEnabled()) {
    const proxy = getProxy()

    return {
      connectionTimeout,
      socketTimeout,
      throwOnRequestTimeout: true,
      httpAgent: new HttpProxyAgent({
        keepAlive: true,
        keepAliveMsecs: 1000,
        maxSockets,
        maxFreeSockets,
        scheduling: 'lifo',
        proxy
      }),
      httpsAgent: new HttpsProxyAgent({
        keepAlive: true,
        keepAliveMsecs: 1000,
        maxSockets,
        maxFreeSockets,
        scheduling: 'lifo',
        proxy
      })
    }
  }

  return {
    connectionTimeout,
    socketTimeout,
    throwOnRequestTimeout: true,
    httpAgent: new http.Agent({
      keepAlive: true,
      keepAliveMsecs: 1000,
      maxSockets,
      maxFreeSockets,
      scheduling: 'lifo'
    }),
    httpsAgent: new https.Agent({
      keepAlive: true,
      keepAliveMsecs: 1000,
      maxSockets,
      maxFreeSockets,
      scheduling: 'lifo'
    })
  }
}
