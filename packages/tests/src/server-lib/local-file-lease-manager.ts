/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { LocalFileLeaseManager } from '@peertube/peertube-server/core/lib/local-file-lease-manager.js'
import { Redis } from '@peertube/peertube-server/core/lib/redis.js'

describe('local-file-lease-manager', function () {
  it('should refresh and release an in-memory lease idempotently', async function () {
    const manager = LocalFileLeaseManager.Instance
    const lease = await manager.acquire({
      videoUUID: 'lease-video',
      leaseId: 'test:lease-refresh',
      ttlMs: 50
    })

    expect(lease).to.not.equal(undefined)
    if (!lease) throw new Error('Expected an acquired local-file lease.')

    expect(await manager.hasActiveLeases('lease-video')).to.be.true
    expect(await lease.refresh()).to.be.true

    await lease.release()
    await lease.release()

    expect(await manager.hasActiveLeases('lease-video')).to.be.false
  })

  it('should keep cleanup and acquisition mutually exclusive', async function () {
    const manager = LocalFileLeaseManager.Instance
    const cleanupLock = await manager.acquireCleanupLock('cleanup-video')

    expect(cleanupLock).to.not.equal(undefined)
    if (!cleanupLock) throw new Error('Expected an acquired cleanup lock.')

    expect(await cleanupLock.refresh()).to.be.true
    expect(await manager.acquire({
      videoUUID: 'cleanup-video',
      leaseId: 'test:blocked-by-cleanup',
      maxWaitMs: 0
    })).to.equal(undefined)

    await cleanupLock.release()
    expect(await cleanupLock.refresh()).to.be.false

    const lease = await manager.acquire({
      videoUUID: 'cleanup-video',
      leaseId: 'test:after-cleanup',
      maxWaitMs: 0
    })
    expect(lease).to.not.equal(undefined)
    if (!lease) throw new Error('Expected an acquired local-file lease.')

    await lease.release()
  })

  it('should retain a durable queue lease until its lifecycle releases it', async function () {
    const manager = LocalFileLeaseManager.Instance
    const lease = await manager.acquire({
      videoUUID: 'durable-lease-video',
      leaseId: 'test:durable-lease',
      ttlMs: 1,
      persistent: true
    })

    expect(lease).to.not.equal(undefined)
    if (!lease) throw new Error('Expected an acquired local-file lease.')

    await new Promise(resolve => setTimeout(resolve, 10))
    expect(await manager.hasActiveLeases('durable-lease-video')).to.be.true
    expect(await manager.getNextLeaseExpiry('durable-lease-video')).to.equal(undefined)

    lease.deactivate?.()
    expect(await lease.refresh()).to.be.false
    expect(await manager.hasActiveLeases('durable-lease-video')).to.be.true

    await lease.release()
  })

  it('should make reacquiring the same durable job lease idempotent', async function () {
    const manager = LocalFileLeaseManager.Instance
    const first = await manager.acquire({
      videoUUID: 'idempotent-lease-video',
      leaseId: 'test:idempotent-job-lease',
      persistent: true
    })
    const second = await manager.acquire({
      videoUUID: 'idempotent-lease-video',
      leaseId: 'test:idempotent-job-lease',
      persistent: true
    })

    expect(first).to.not.equal(undefined)
    expect(second).to.not.equal(undefined)

    await first?.release()
    await second?.release()
  })

  it('should reconcile only stale durable job leases and retain temporary local reads', async function () {
    const redis = Redis.Instance as any
    const originalGetClient = redis.getClient
    const originalGetPrefix = redis.getPrefix
    const temporaryLeaseId = 'test:temporary-local-read'
    const staleJobLeaseId = 'test:stale-job-lease'
    const removed: string[][] = []
    const now = Date.now()
    const persistentLeaseScoreFloor = 8_000_000_000_000_000

    redis.getPrefix = () => 'test:'
    redis.getClient = () => ({
      smembers: () => Promise.resolve([ 'reconcile-video' ]),
      zrange: () => Promise.resolve([
        temporaryLeaseId,
        String(now + 60_000),
        staleJobLeaseId,
        String(persistentLeaseScoreFloor + now - 1)
      ]),
       eval: (script: string, _keyCount: number, ...args: string[]) => {
         if (!script.includes('currentScore')) return Promise.resolve(0)

         removed.push([ args[2] ])
         return Promise.resolve(1)
       },
      zcard: () => Promise.resolve(1),
      del: () => Promise.resolve(1),
      srem: () => Promise.resolve(1)
    })

    try {
      await LocalFileLeaseManager.Instance.reconcile(new Map(), now)

      expect(removed).to.deep.equal([ [ staleJobLeaseId ] ])
    } finally {
      redis.getClient = originalGetClient
      redis.getPrefix = originalGetPrefix
    }
  })
})
