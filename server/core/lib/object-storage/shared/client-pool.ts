import { AsyncLocalStorage } from 'node:async_hooks'

export type ObjectStorageClientPool = 'write' | 'move' | 'read'

const objectStorageClientPool = new AsyncLocalStorage<ObjectStorageClientPool>()

export function withObjectStorageClientPool<T> (pool: ObjectStorageClientPool, callback: () => T): T {
  return objectStorageClientPool.run(pool, callback)
}

export function getObjectStorageClientPool (): ObjectStorageClientPool {
  return objectStorageClientPool.getStore() ?? 'write'
}
