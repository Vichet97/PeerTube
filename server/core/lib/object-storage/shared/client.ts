import type { S3Client } from '@aws-sdk/client-s3'
import { logger } from '@server/helpers/logger.js'
import { HttpProxyAgent, HttpsProxyAgent } from '@server/helpers/hpagent.js'
import { getProxy, isProxyEnabled } from '@server/helpers/proxy.js'
import { CONFIG } from '@server/initializers/config.js'
import http from 'http'
import https from 'https'
import { lTags } from './logger.js'
import { getReadForcePathStyle } from '../read-url.js'

let writeS3ClientPromise: Promise<S3Client>
let writeS3ClientResolved: S3Client
let writeS3ClientEndpoint: string

let readS3ClientPromise: Promise<S3Client>
let readS3ClientEndpoint: string

const DEFAULT_OBJECT_STORAGE_CONNECTION_TIMEOUT_MS = 30_000
const DEFAULT_OBJECT_STORAGE_SOCKET_TIMEOUT_MS = 120_000

export function getClient () {
  const endpoint = getEndpoint()
  if (writeS3ClientPromise !== undefined && writeS3ClientEndpoint === endpoint) return writeS3ClientPromise

  writeS3ClientEndpoint = endpoint
  writeS3ClientPromise = buildClient(endpoint)
    .then(client => {
      writeS3ClientResolved = client
      return client
    })

  return writeS3ClientPromise
}

export function getReadClient () {
  const endpoint = getReadEndpoint()
  const forcePathStyle = getReadForcePathStyle()

  if (endpoint === getEndpoint() && forcePathStyle === CONFIG.OBJECT_STORAGE.FORCE_PATH_STYLE) return getClient()
  if (readS3ClientPromise !== undefined && readS3ClientEndpoint === `${endpoint}|${forcePathStyle}`) return readS3ClientPromise

  readS3ClientEndpoint = `${endpoint}|${forcePathStyle}`
  readS3ClientPromise = buildClient(endpoint, forcePathStyle)
    .then(client => client)

  return readS3ClientPromise
}

// Synchronous access to cached client (only available after first getClient() call completes)
export function getClientSync (): S3Client | undefined {
  return writeS3ClientResolved
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

async function buildClient (endpoint: string, forcePathStyle = CONFIG.OBJECT_STORAGE.FORCE_PATH_STYLE) {
  const OBJECT_STORAGE = CONFIG.OBJECT_STORAGE

  const { S3Client } = await import('@aws-sdk/client-s3')

  const requestHandler = await getProxyRequestHandler()

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

  logger.info('Initialized S3 client %s with region %s.', endpoint, OBJECT_STORAGE.REGION, lTags())

  return client
}

// ---------------------------------------------------------------------------
// Private
// ---------------------------------------------------------------------------

async function getProxyRequestHandler () {
  const { NodeHttpHandler } = await import('@smithy/node-http-handler')
  return new NodeHttpHandler(buildObjectStorageNodeHttpHandlerOptions())
}

export function getObjectStorageSocketTimeoutMs () {
  return CONFIG.OBJECT_STORAGE.PROXY.REQUEST_TIMEOUT_MS ?? DEFAULT_OBJECT_STORAGE_SOCKET_TIMEOUT_MS
}

export function getObjectStorageConnectionTimeoutMs (socketTimeoutMs = getObjectStorageSocketTimeoutMs()) {
  return Math.min(socketTimeoutMs, DEFAULT_OBJECT_STORAGE_CONNECTION_TIMEOUT_MS)
}

export function buildObjectStorageNodeHttpHandlerOptions () {
  const maxSockets = Math.max(
    16,
    Math.min(64, Math.max(1, CONFIG.OBJECT_STORAGE.CONCURRENCY) * 3)
  )
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
