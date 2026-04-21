import type { S3Client } from '@aws-sdk/client-s3'
import { logger } from '@server/helpers/logger.js'
import { getProxyAgent } from '@server/helpers/requests.js'
import { CONFIG } from '@server/initializers/config.js'
import { lTags } from './logger.js'

let s3ClientPromise: Promise<S3Client>
let s3ClientResolved: S3Client
export function getClient () {
  if (s3ClientPromise !== undefined) return s3ClientPromise

  s3ClientPromise = (async () => {
    const OBJECT_STORAGE = CONFIG.OBJECT_STORAGE

    const { S3Client } = await import('@aws-sdk/client-s3')

    const requestHandler = await getProxyRequestHandler()

    s3ClientResolved = new S3Client({
      endpoint: getEndpoint(),
      region: OBJECT_STORAGE.REGION,
      credentials: OBJECT_STORAGE.CREDENTIALS.ACCESS_KEY_ID
        ? {
          accessKeyId: OBJECT_STORAGE.CREDENTIALS.ACCESS_KEY_ID,
          secretAccessKey: OBJECT_STORAGE.CREDENTIALS.SECRET_ACCESS_KEY
        }
        : undefined,
      requestHandler,
      maxAttempts: CONFIG.OBJECT_STORAGE.MAX_REQUEST_ATTEMPTS,
      forcePathStyle: OBJECT_STORAGE.FORCE_PATH_STYLE,

      // Default behaviour has incompatibilities with some S3 providers: https://github.com/aws/aws-sdk-js-v3/issues/6810
      requestChecksumCalculation: 'WHEN_REQUIRED',
      responseChecksumValidation: 'WHEN_REQUIRED'
    })

    logger.info('Initialized S3 client %s with region %s.', getEndpoint(), OBJECT_STORAGE.REGION, lTags())

    return s3ClientResolved
  })()

  return s3ClientPromise
}

// Synchronous access to cached client (only available after first getClient() call completes)
export function getClientSync (): S3Client | undefined {
  return s3ClientResolved
}

let endpoint: string
export function getEndpoint () {
  if (endpoint) return endpoint

  const endpointConfig = CONFIG.OBJECT_STORAGE.ENDPOINT
  endpoint = endpointConfig.startsWith('http://') || endpointConfig.startsWith('https://')
    ? CONFIG.OBJECT_STORAGE.ENDPOINT
    : 'https://' + CONFIG.OBJECT_STORAGE.ENDPOINT

  return endpoint
}

// ---------------------------------------------------------------------------
// Private
// ---------------------------------------------------------------------------

async function getProxyRequestHandler () {
  const { NodeHttpHandler } = await import('@smithy/node-http-handler')

  // Get agents (either from proxy or default)
  const { agent } = getProxyAgent()

  // Use the existing agents from getProxyAgent
  // The timeout configuration is handled by the individual S3 request timeouts
  // in object-storage-helpers.ts (createObjectReadStream function)
  return new NodeHttpHandler({
    httpAgent: agent.http,
    httpsAgent: agent.https
  })
}
