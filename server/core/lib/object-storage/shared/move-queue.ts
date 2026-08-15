import { CONFIG } from '@server/initializers/config.js'
import PQueue from 'p-queue'
import { getObjectStorageMaxSockets } from './client.js'
import { withObjectStorageClientPool } from './client-pool.js'

let moveFileQueue: PQueue | undefined

export function getObjectStorageMoveQueueConcurrency () {
  const uploadPartQueueSize = Math.max(1, CONFIG.OBJECT_STORAGE.UPLOAD_PART_QUEUE_SIZE)
  const socketBound = Math.max(1, Math.floor(getObjectStorageMaxSockets('move') / uploadPartQueueSize))
  const configuredConcurrency = Math.max(1, CONFIG.OBJECT_STORAGE.UPLOAD_CONCURRENCY)

  return Math.min(configuredConcurrency, socketBound)
}

export function getObjectStorageMoveQueue (): PQueue {
  const concurrency = getObjectStorageMoveQueueConcurrency()

  if (!moveFileQueue) {
    moveFileQueue = new PQueue({ concurrency })
  } else if (moveFileQueue.concurrency !== concurrency) {
    moveFileQueue.concurrency = concurrency
  }

  return moveFileQueue
}

export function addObjectStorageMoveTasks<T> (tasks: (() => Promise<T>)[]) {
  return getObjectStorageMoveQueue().addAll(tasks.map(task => () => withObjectStorageClientPool('move', task)))
}
