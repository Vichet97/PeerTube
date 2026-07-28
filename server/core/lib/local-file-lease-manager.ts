import { logger, loggerTagsFactory } from '@server/helpers/logger.js'
import { Redis } from '@server/lib/redis.js'
import { Redis as IORedis } from 'ioredis'
import { randomUUID } from 'node:crypto'

const lTags = loggerTagsFactory('local-file-lease')

// Queue-owned leases are durable and are reconciled against live BullMQ jobs at
// startup. Short-lived local-read leases are refreshed while their caller is
// alive, but must expire quickly after a crash or abandoned request so they do
// not retain object-storage replicas for a full day.
export const LOCAL_FILE_READ_LEASE_TTL_MS = 15 * 60 * 1000
export const LOCAL_FILE_READ_LEASE_HEARTBEAT_MS = Math.floor(LOCAL_FILE_READ_LEASE_TTL_MS / 3)
const PERSISTENT_LEASE_SCORE_FLOOR = 8_000_000_000_000_000
const CLEANUP_LOCK_TTL_MS = 30_000
const CLEANUP_LOCK_HEARTBEAT_MS = Math.floor(CLEANUP_LOCK_TTL_MS / 3)
const CLEANUP_LOCK_RETRY_DELAY_MS = 25
const CLEANUP_LOCK_MAX_WAIT_MS = 5_000
const PERSISTENT_LEASE_ACQUIRE_MAX_WAIT_MS = 60_000
const LEASE_RELEASE_CHANNEL_SUFFIX = 'local-file-lease-released'
const CLEANUP_LOCK_RELEASE_CHANNEL_SUFFIX = 'local-file-cleanup-lock-released'

const ACQUIRE_LEASE_SCRIPT = `
  local existingVideoUUID = redis.call('GET', KEYS[4])
  if existingVideoUUID and existingVideoUUID ~= ARGV[4] then return 0 end
  local existingLeaseScore = redis.call('ZSCORE', KEYS[1], ARGV[3])
  if not existingLeaseScore or tonumber(existingLeaseScore) <= tonumber(ARGV[1]) then
    if redis.call('EXISTS', KEYS[3]) == 1 then return 0 end
  end
  redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
  local leaseScore = ARGV[2]
  local currentScore = redis.call('ZSCORE', KEYS[1], ARGV[3])
  if ARGV[6] == '1' and currentScore and tonumber(currentScore) >= tonumber(leaseScore) then
    leaseScore = tonumber(currentScore) + 1
  end
  redis.call('ZADD', KEYS[1], leaseScore, ARGV[3])
  redis.call('SADD', KEYS[2], ARGV[4])
  if ARGV[6] == '1' then
    redis.call('SET', KEYS[4], ARGV[4])
    redis.call('PERSIST', KEYS[1])
  else
    redis.call('SET', KEYS[4], ARGV[4], 'PX', ARGV[5])
    local latest = redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES')
    local latestScore = tonumber(latest[2])
    if latestScore >= tonumber(ARGV[7]) then
      redis.call('PERSIST', KEYS[1])
    else
      redis.call('PEXPIRE', KEYS[1], math.max(1, latestScore - tonumber(ARGV[1])))
    end
  end
  return 1
`

const RELEASE_LEASE_SCRIPT = `
  redis.call('ZREM', KEYS[1], ARGV[1])
  redis.call('DEL', KEYS[3])
  if redis.call('ZCARD', KEYS[1]) == 0 then
    redis.call('DEL', KEYS[1])
    redis.call('SREM', KEYS[2], ARGV[2])
  else
    local latest = redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES')
    local latestScore = tonumber(latest[2])
    if latestScore >= tonumber(ARGV[4]) then
      redis.call('PERSIST', KEYS[1])
    else
      redis.call('PEXPIRE', KEYS[1], math.max(1, latestScore - tonumber(ARGV[3])))
    end
  end
  return 1
`

const REFRESH_CLEANUP_LOCK_SCRIPT = `
  if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
`

const RELEASE_CLEANUP_LOCK_SCRIPT = `
  if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
  redis.call('DEL', KEYS[1])
  return 1
`

const REMOVE_STALE_LEASE_SCRIPT = `
  local currentScore = redis.call('ZSCORE', KEYS[1], ARGV[1])
  if not currentScore or tonumber(currentScore) ~= tonumber(ARGV[2]) then return 0 end
  redis.call('ZREM', KEYS[1], ARGV[1])
  redis.call('DEL', KEYS[2])
  return 1
`

const CHECK_ACTIVE_LEASES_SCRIPT = `
  redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
  local activeLeaseCount = redis.call('ZCARD', KEYS[1])
  if activeLeaseCount == 0 then
    redis.call('DEL', KEYS[1])
    redis.call('SREM', KEYS[2], ARGV[2])
  end
  return activeLeaseCount
`

const CLEAR_EMPTY_VIDEO_LEASES_SCRIPT = `
  if redis.call('ZCARD', KEYS[1]) ~= 0 then return 0 end
  redis.call('DEL', KEYS[1])
  redis.call('SREM', KEYS[2], ARGV[1])
  return 1
`

type LeaseChangeListener = (videoUUID: string) => void | Promise<void>
type CleanupLockChangeListener = (lockId: string) => void | Promise<void>

export type LocalFileLease = {
  leaseId: string
  refresh: () => Promise<boolean>
  deactivate?: () => void
  release: () => Promise<void>
}

export type LocalFileCleanupLock = {
  refresh: () => Promise<boolean>
  release: () => Promise<void>
}

type JobWithLeaseOptions = {
  opts?: object
}

class LocalFileLeaseManager {
  private static instance: LocalFileLeaseManager

  private readonly inMemoryLeasesByVideoUUID = new Map<string, Set<string>>()
  private readonly inMemoryLeaseExpiriesByVideoUUID = new Map<string, Map<string, number>>()
  private readonly inMemoryVideoUUIDByLeaseId = new Map<string, string>()
  private readonly inMemoryCleanupLocks = new Set<string>()
  private readonly leaseChangeListeners = new Set<LeaseChangeListener>()
  private readonly cleanupLockChangeListeners = new Set<CleanupLockChangeListener>()
  private readonly leaseReleasePublisherId = randomUUID()
  private leaseReleaseSubscriber?: IORedis
  private leaseReleaseSubscriptionPromise?: Promise<void>

  private constructor () {}

  static get Instance () {
    return this.instance || (this.instance = new this())
  }

  createLeaseId (prefix: string) {
    return `${prefix}:${randomUUID()}`
  }

  async acquire (options: {
    videoUUID: string
    leaseId?: string
    maxWaitMs?: number
    ttlMs?: number
    persistent?: boolean
  }): Promise<LocalFileLease | undefined> {
    const {
      videoUUID,
      ttlMs = LOCAL_FILE_READ_LEASE_TTL_MS,
      persistent = false
    } = options
    const maxWaitMs = options.maxWaitMs ?? (
      persistent ? PERSISTENT_LEASE_ACQUIRE_MAX_WAIT_MS : CLEANUP_LOCK_MAX_WAIT_MS
    )
    const leaseId = options.leaseId ?? this.createLeaseId('local-file')
    const startedAt = Date.now()

    while (true) {
      const redisResult = await this.tryAcquireRedisLease({ videoUUID, leaseId, ttlMs, persistent })
      if (redisResult === true) {
        this.addInMemoryLease(videoUUID, leaseId, persistent ? Infinity : Date.now() + ttlMs)

        return this.buildLease({ videoUUID, leaseId, ttlMs, persistent })
      }

      if (redisResult === undefined && this.tryAcquireInMemoryLease(videoUUID, leaseId, ttlMs, persistent)) {
        return this.buildLease({ videoUUID, leaseId, ttlMs, persistent })
      }

      if (Date.now() - startedAt >= maxWaitMs) return undefined

      await wait(CLEANUP_LOCK_RETRY_DELAY_MS)
    }
  }

  async hasActiveLeases (videoUUID: string) {
    this.removeExpiredInMemoryLeases(videoUUID)

    const inMemoryLeases = this.inMemoryLeasesByVideoUUID.get(videoUUID)
    if (inMemoryLeases?.size) return true

    const client = this.getRedisClient()
    if (!client) return false

    try {
      const key = this.buildVideoLeaseKey(videoUUID)
      const activeLeaseCount = await client.eval(
        CHECK_ACTIVE_LEASES_SCRIPT,
        2,
        key,
        this.buildLeaseVideosKey(),
        Date.now(),
        videoUUID
      )

      return Number(activeLeaseCount) > 0
    } catch (err) {
      logger.warn('Cannot check local file leases; keeping local files conservatively.', { err, ...lTags(videoUUID) })

      return true
    }
  }

  async getNextLeaseExpiry (videoUUID: string) {
    this.removeExpiredInMemoryLeases(videoUUID)

    const inMemoryExpiries = this.inMemoryLeaseExpiriesByVideoUUID.get(videoUUID)
    const inMemoryExpiryValues = inMemoryExpiries
      ? [ ...inMemoryExpiries.values() ].filter(expiry => Number.isFinite(expiry))
      : []
    let nextExpiry = inMemoryExpiryValues.length !== 0
      ? Math.min(...inMemoryExpiryValues)
      : undefined

    const client = this.getRedisClient()
    if (!client) return Number.isFinite(nextExpiry) ? nextExpiry : undefined

    try {
      const result = await client.zrange(this.buildVideoLeaseKey(videoUUID), 0, 0, 'WITHSCORES')
      if (result.length >= 2) {
        const redisExpiry = Number(result[1])
        if (Number.isFinite(redisExpiry) && redisExpiry < PERSISTENT_LEASE_SCORE_FLOOR) {
          nextExpiry = nextExpiry === undefined ? redisExpiry : Math.min(nextExpiry, redisExpiry)
        }
      }
    } catch (err) {
      logger.warn('Cannot determine next local file lease expiry.', { err, ...lTags(videoUUID) })
    }

    return Number.isFinite(nextExpiry) ? nextExpiry : undefined
  }

  async getVideoUUID (leaseId: string) {
    const inMemoryVideoUUID = this.inMemoryVideoUUIDByLeaseId.get(leaseId)
    if (inMemoryVideoUUID) return inMemoryVideoUUID

    const client = this.getRedisClient()
    if (!client) return undefined

    try {
      return await client.get(this.buildLeaseIndexKey(leaseId)) || undefined
    } catch (err) {
      logger.warn('Cannot find local file lease owner.', { err, ...lTags() })

      return undefined
    }
  }

  async acquireCleanupLock (videoUUID: string, maxWaitMs = CLEANUP_LOCK_MAX_WAIT_MS): Promise<LocalFileCleanupLock | undefined> {
    const startedAt = Date.now()
    const owner = this.createLeaseId('cleanup')

    while (true) {
      const client = this.getRedisClient()
      if (!client) {
        if (!this.inMemoryCleanupLocks.has(videoUUID)) {
          this.inMemoryCleanupLocks.add(videoUUID)

          return this.buildCleanupLock(videoUUID)
        }
      } else {
        try {
          const result = await client.set(
            this.buildCleanupLockKey(videoUUID),
            owner,
            'PX',
            CLEANUP_LOCK_TTL_MS,
            'NX'
          )

          if (result === 'OK') return this.buildCleanupLock(videoUUID, owner)
        } catch (err) {
          logger.warn('Cannot acquire local file cleanup lock; skipping cleanup cycle.', { err, ...lTags(videoUUID) })

          return undefined
        }
      }

      if (Date.now() - startedAt >= maxWaitMs) return undefined

      await wait(CLEANUP_LOCK_RETRY_DELAY_MS)
    }
  }

  onLeaseReleased (listener: LeaseChangeListener) {
    this.leaseChangeListeners.add(listener)
    void this.ensureLeaseReleaseSubscriber()
      .catch(err => logger.warn('Cannot initialize local file lease release subscriber.', { err, ...lTags() }))

    return () => this.leaseChangeListeners.delete(listener)
  }

  onCleanupLockReleased (listener: CleanupLockChangeListener) {
    this.cleanupLockChangeListeners.add(listener)
    void this.ensureLeaseReleaseSubscriber()
      .catch(err => logger.warn('Cannot initialize local file cleanup lock release subscriber.', { err, ...lTags() }))

    return () => this.cleanupLockChangeListeners.delete(listener)
  }

  async reconcile (liveLeases: Map<string, string>, reconciliationStartedAt = Date.now()) {
    const client = this.getRedisClient()
    if (!client) return

    const liveLeaseIdsByVideoUUID = new Map<string, Set<string>>()
    for (const [ leaseId, videoUUID ] of liveLeases) {
      if (!liveLeaseIdsByVideoUUID.has(videoUUID)) liveLeaseIdsByVideoUUID.set(videoUUID, new Set())
      liveLeaseIdsByVideoUUID.get(videoUUID).add(leaseId)

      await this.tryAcquireRedisLease({
        videoUUID,
        leaseId,
        ttlMs: LOCAL_FILE_READ_LEASE_TTL_MS,
        persistent: true
      })
      this.addInMemoryLease(videoUUID, leaseId, Infinity)
    }

    try {
      const videos = await client.smembers(this.buildLeaseVideosKey())

      for (const videoUUID of videos) {
        const key = this.buildVideoLeaseKey(videoUUID)
        const membersWithScores = await client.zrange(key, 0, -1, 'WITHSCORES')
        const liveMembers = liveLeaseIdsByVideoUUID.get(videoUUID) ?? new Set<string>()
        const staleMembers: { leaseId: string, score: string }[] = []

        for (let index = 0; index < membersWithScores.length; index += 2) {
          const leaseId = membersWithScores[index]
          const score = Number(membersWithScores[index + 1])
          const isPersistentJobLease = Number.isFinite(score) && score >= PERSISTENT_LEASE_SCORE_FLOOR
          const acquiredAt = isPersistentJobLease ? score - PERSISTENT_LEASE_SCORE_FLOOR : undefined

          // Temporary local-read leases are not BullMQ jobs and must keep their
          // TTL protection. The timestamp fence avoids pruning a job acquired
          // while this reconciliation snapshot was being assembled.
          if (isPersistentJobLease && acquiredAt !== undefined && acquiredAt <= reconciliationStartedAt && !liveMembers.has(leaseId)) {
            staleMembers.push({ leaseId, score: membersWithScores[index + 1] })
          }
        }

        for (const staleMember of staleMembers) {
          const removed = await client.eval(
            REMOVE_STALE_LEASE_SCRIPT,
            2,
            key,
            this.buildLeaseIndexKey(staleMember.leaseId),
            staleMember.leaseId,
            staleMember.score
          )

          if (removed === 1) this.removeInMemoryLease(videoUUID, staleMember.leaseId)
        }

        const becameEmpty = await client.eval(
          CLEAR_EMPTY_VIDEO_LEASES_SCRIPT,
          2,
          key,
          this.buildLeaseVideosKey(),
          videoUUID
        )
        if (becameEmpty === 1) {
          this.emitLeaseReleased(videoUUID)
        }
      }
    } catch (err) {
      logger.warn('Cannot reconcile local file leases.', { err, ...lTags() })
    }
  }

  getLeaseId (job: JobWithLeaseOptions) {
    return (job.opts as { localFileLeaseId?: string } | undefined)?.localFileLeaseId
  }

  releaseLease (videoUUID: string, leaseId: string) {
    return this.release(videoUUID, leaseId)
  }

  async releaseLeaseById (leaseId: string, videoUUID?: string) {
    const resolvedVideoUUID = videoUUID ?? await this.getVideoUUID(leaseId)
    if (!resolvedVideoUUID) return false

    await this.release(resolvedVideoUUID, leaseId)

    return true
  }

  buildLegacyJobLeaseId (jobType: string, jobId: string | number) {
    return `job:${jobType}:${jobId}`
  }

  private async tryAcquireRedisLease (options: {
    videoUUID: string
    leaseId: string
    ttlMs: number
    persistent: boolean
  }): Promise<boolean | undefined> {
    const client = this.getRedisClient()
    if (!client) return undefined

    const { videoUUID, leaseId, ttlMs } = options

    try {
      const now = Date.now()
      const result = await client.eval(
        ACQUIRE_LEASE_SCRIPT,
        4,
        this.buildVideoLeaseKey(videoUUID),
        this.buildLeaseVideosKey(),
        this.buildCleanupLockKey(videoUUID),
        this.buildLeaseIndexKey(leaseId),
        now,
        options.persistent ? PERSISTENT_LEASE_SCORE_FLOOR + now : now + ttlMs,
        leaseId,
        videoUUID,
        ttlMs,
        options.persistent ? '1' : '0',
        PERSISTENT_LEASE_SCORE_FLOOR
      )

      return result === 1
    } catch (err) {
      logger.warn('Cannot acquire local file lease in Redis; retaining local files conservatively.', { err, ...lTags(videoUUID) })

      return false
    }
  }

  private buildLease (options: {
    videoUUID: string
    leaseId: string
    ttlMs: number
    persistent: boolean
  }): LocalFileLease {
    const { videoUUID, leaseId, ttlMs, persistent } = options
    let released = false
    let active = true

    return {
      leaseId,
      deactivate: () => {
        active = false
      },
      refresh: async () => {
        if (released || !active) return false

        const refreshed = await this.tryAcquireRedisLease({ videoUUID, leaseId, ttlMs, persistent })
        if (refreshed === true) {
          if (released) {
            await this.release(videoUUID, leaseId)
            return false
          }

          this.addInMemoryLease(videoUUID, leaseId, persistent ? Infinity : Date.now() + ttlMs)
          return true
        }

        if (refreshed === undefined && this.tryAcquireInMemoryLease(videoUUID, leaseId, ttlMs, persistent)) {
          if (released) {
            this.removeInMemoryLease(videoUUID, leaseId)
            return false
          }

          return true
        }

        return false
      },
      release: async () => {
        if (released) return
        released = true
        active = false

        await this.release(videoUUID, leaseId)
      }
    }
  }

  private async release (videoUUID: string, leaseId: string) {
    this.removeInMemoryLease(videoUUID, leaseId)

    const client = this.getRedisClient()
    if (client) {
      try {
        await client.eval(
          RELEASE_LEASE_SCRIPT,
          3,
          this.buildVideoLeaseKey(videoUUID),
          this.buildLeaseVideosKey(),
          this.buildLeaseIndexKey(leaseId),
          leaseId,
          videoUUID,
          Date.now(),
          PERSISTENT_LEASE_SCORE_FLOOR
        )
      } catch (err) {
        logger.warn('Cannot release local file lease from Redis.', { err, ...lTags(videoUUID) })
      }
    }

    this.emitLeaseReleased(videoUUID)
  }

  private buildCleanupLock (videoUUID: string, owner?: string): LocalFileCleanupLock {
    let released = false

    return {
      refresh: async () => {
        if (released) return false

        if (!owner) return this.inMemoryCleanupLocks.has(videoUUID)

        const client = this.getRedisClient()
        if (!client) return false

        try {
          const result = await client.eval(
            REFRESH_CLEANUP_LOCK_SCRIPT,
            1,
            this.buildCleanupLockKey(videoUUID),
            owner,
            CLEANUP_LOCK_TTL_MS
          )

          return result === 1
        } catch (err) {
          logger.warn('Cannot refresh local file cleanup lock.', { err, ...lTags(videoUUID) })

          return false
        }
      },
      release: async () => {
        if (released) return
        released = true

        if (!owner) {
          this.inMemoryCleanupLocks.delete(videoUUID)
          this.emitCleanupLockReleased(videoUUID)
          return
        }

        const client = this.getRedisClient()
        if (!client) return

        try {
          const released = await client.eval(
            RELEASE_CLEANUP_LOCK_SCRIPT,
            1,
            this.buildCleanupLockKey(videoUUID),
            owner
          )

          if (released === 1) this.emitCleanupLockReleased(videoUUID)
        } catch (err) {
          logger.warn('Cannot release local file cleanup lock.', { err, ...lTags(videoUUID) })
        }
      }
    }
  }

  private addInMemoryLease (videoUUID: string, leaseId: string, expiresAt: number) {
    if (!this.inMemoryLeasesByVideoUUID.has(videoUUID)) this.inMemoryLeasesByVideoUUID.set(videoUUID, new Set())
    this.inMemoryLeasesByVideoUUID.get(videoUUID).add(leaseId)

    if (!this.inMemoryLeaseExpiriesByVideoUUID.has(videoUUID)) this.inMemoryLeaseExpiriesByVideoUUID.set(videoUUID, new Map())
    this.inMemoryLeaseExpiriesByVideoUUID.get(videoUUID).set(leaseId, expiresAt)
    this.inMemoryVideoUUIDByLeaseId.set(leaseId, videoUUID)
  }

  private tryAcquireInMemoryLease (videoUUID: string, leaseId: string, ttlMs: number, persistent: boolean) {
    const alreadyActive = this.inMemoryLeasesByVideoUUID.get(videoUUID)?.has(leaseId) === true
    if (this.inMemoryCleanupLocks.has(videoUUID) && !alreadyActive) return false

    this.addInMemoryLease(videoUUID, leaseId, persistent ? Infinity : Date.now() + ttlMs)

    return true
  }

  private removeInMemoryLease (videoUUID: string, leaseId: string) {
    const leases = this.inMemoryLeasesByVideoUUID.get(videoUUID)
    if (!leases) return

    leases.delete(leaseId)
    if (leases.size === 0) this.inMemoryLeasesByVideoUUID.delete(videoUUID)

    const expiries = this.inMemoryLeaseExpiriesByVideoUUID.get(videoUUID)
    expiries?.delete(leaseId)
    if (expiries?.size === 0) this.inMemoryLeaseExpiriesByVideoUUID.delete(videoUUID)
    this.inMemoryVideoUUIDByLeaseId.delete(leaseId)
  }

  private removeExpiredInMemoryLeases (videoUUID: string) {
    const expiries = this.inMemoryLeaseExpiriesByVideoUUID.get(videoUUID)
    if (!expiries) return

    const now = Date.now()
    for (const [ leaseId, expiresAt ] of expiries) {
      if (expiresAt <= now) this.removeInMemoryLease(videoUUID, leaseId)
    }
  }

  private emitLeaseReleased (videoUUID: string) {
    this.notifyLeaseReleased(videoUUID)
    if (this.leaseChangeListeners.size !== 0) {
      void this.ensureLeaseReleaseSubscriber()
        .catch(err => logger.warn('Cannot initialize local file lease release subscriber.', { err, ...lTags() }))
    }
    void this.publishLeaseReleased(videoUUID)
  }

  private emitCleanupLockReleased (lockId: string) {
    this.notifyCleanupLockReleased(lockId)
    if (this.cleanupLockChangeListeners.size !== 0) {
      void this.ensureLeaseReleaseSubscriber()
        .catch(err => logger.warn('Cannot initialize local file cleanup lock release subscriber.', { err, ...lTags() }))
    }
    void this.publishCleanupLockReleased(lockId)
  }

  private async publishLeaseReleased (videoUUID: string) {
    const client = this.getRedisClient()
    if (!client) return

    try {
      await client.publish(this.buildLeaseReleaseChannel(), JSON.stringify({
        videoUUID,
        publisherId: this.leaseReleasePublisherId
      }))
    } catch (err) {
      logger.warn('Cannot publish local file lease release.', { err, ...lTags(videoUUID) })
    }
  }

  private async publishCleanupLockReleased (lockId: string) {
    const client = this.getRedisClient()
    if (!client) return

    try {
      await client.publish(this.buildCleanupLockReleaseChannel(), JSON.stringify({
        lockId,
        publisherId: this.leaseReleasePublisherId
      }))
    } catch (err) {
      logger.warn('Cannot publish local file cleanup lock release.', { err, ...lTags() })
    }
  }

  private async ensureLeaseReleaseSubscriber () {
    if (this.leaseReleaseSubscriber !== undefined || this.leaseReleaseSubscriptionPromise !== undefined) return

    const client = this.getRedisClient()
    if (!client) return

    const subscriber = client.duplicate()
    const leaseChannel = this.buildLeaseReleaseChannel()
    const cleanupLockChannel = this.buildCleanupLockReleaseChannel()

    subscriber.on('error', err => logger.warn('Local file lease release subscriber failed.', { err, ...lTags() }))
    subscriber.on('end', () => {
      if (this.leaseReleaseSubscriber !== subscriber) return

      this.leaseReleaseSubscriber = undefined
    })
    subscriber.on('message', (receivedChannel, message) => {
      try {
        const event = JSON.parse(message) as { videoUUID?: unknown, lockId?: unknown, publisherId?: unknown }
        if (event.publisherId === this.leaseReleasePublisherId) return

        if (receivedChannel === leaseChannel && typeof event.videoUUID === 'string' && event.videoUUID.length !== 0) {
          this.notifyLeaseReleased(event.videoUUID)
        }

        if (receivedChannel === cleanupLockChannel && typeof event.lockId === 'string' && event.lockId.length !== 0) {
          this.notifyCleanupLockReleased(event.lockId)
        }
      } catch (err) {
        logger.warn('Cannot process local file lease or cleanup lock release event.', { err, ...lTags() })
      }
    })

    this.leaseReleaseSubscriptionPromise = subscriber.subscribe(leaseChannel, cleanupLockChannel)
      .then(() => {
        this.leaseReleaseSubscriber = subscriber
      })
      .catch(err => {
        subscriber.disconnect()
        logger.warn('Cannot subscribe to local file lease release events.', { err, ...lTags() })
      })
      .finally(() => {
        this.leaseReleaseSubscriptionPromise = undefined
      })

    await this.leaseReleaseSubscriptionPromise
  }

  private notifyLeaseReleased (videoUUID: string) {
    for (const listener of this.leaseChangeListeners) {
      Promise.resolve(listener(videoUUID))
        .catch(err => logger.warn('Cannot notify local file lease release.', { err, ...lTags(videoUUID) }))
    }
  }

  private notifyCleanupLockReleased (lockId: string) {
    for (const listener of this.cleanupLockChangeListeners) {
      Promise.resolve(listener(lockId))
        .catch(err => logger.warn('Cannot notify local file cleanup lock release.', { err, ...lTags() }))
    }
  }

  private getRedisClient () {
    return Redis.Instance.getClient()
  }

  private buildVideoLeaseKey (videoUUID: string) {
    return `${Redis.Instance.getPrefix()}local-file-leases:${videoUUID}`
  }

  private buildLeaseVideosKey () {
    return `${Redis.Instance.getPrefix()}local-file-lease-videos`
  }

  private buildCleanupLockKey (videoUUID: string) {
    return `${Redis.Instance.getPrefix()}local-file-cleanup-lock:${videoUUID}`
  }

  private buildLeaseIndexKey (leaseId: string) {
    return `${Redis.Instance.getPrefix()}local-file-lease-index:${leaseId}`
  }

  private buildLeaseReleaseChannel () {
    return `${Redis.Instance.getPrefix()}${LEASE_RELEASE_CHANNEL_SUFFIX}`
  }

  private buildCleanupLockReleaseChannel () {
    return `${Redis.Instance.getPrefix()}${CLEANUP_LOCK_RELEASE_CHANNEL_SUFFIX}`
  }
}

function wait (milliseconds: number) {
  return new Promise(resolve => setTimeout(resolve, milliseconds))
}

export {
  CLEANUP_LOCK_HEARTBEAT_MS,
  CLEANUP_LOCK_MAX_WAIT_MS,
  LocalFileLeaseManager
}
