import { CONFIG } from '@server/initializers/config.js'
import { Redis } from '@server/lib/redis.js'
import { FSWatcher, watch } from 'fs'
import { lstat, readdir } from 'fs/promises'
import { randomUUID } from 'node:crypto'
import { dirname, isAbsolute, join, relative, resolve } from 'path'

const GIGABYTE = 1024 ** 3
const CAPACITY_SNAPSHOT_MAX_AGE_MS = 5 * 60 * 1000
const CAPACITY_REDIS_KEY_SUFFIX = 'local-storage-import-capacity-snapshots'

export type LocalStorageImportCapacity = {
  usageBytes: number
  limitBytes: number
  resumeUsageBytes: number
}

type LocalStorageCapacityAvailableListener = () => void | Promise<void>

const localStorageCapacityAvailableListeners = new Set<LocalStorageCapacityAvailableListener>()
// Store the direct file total for each directory, not every media file. A 1 TB
// HLS store can contain millions of segments, while its directory count stays
// manageable. Recursive scans still produce an exact aggregate total.
const trackedDirectoryBytes = new Map<string, number>()
const directoryWatchers = new Map<string, FSWatcher>()
let trackedUsageBytes = 0
let trackerRoots: string[] = []
const recursiveWatchers = new Map<string, FSWatcher>()
let trackerStartPromise: Promise<void> | undefined
let trackerRecoveryPromise: Promise<void> | undefined
let trackerStarted = false
let trackingFailed = false
let watcherWork = Promise.resolve()
let initialWatcherChangedPaths: Set<string> | undefined
const capacitySnapshotOwnerId = randomUUID()

export async function startLocalStorageImportCapacityTracking () {
  if (trackerStarted && !trackingFailed) return trackerStartPromise

  trackerStarted = true
  trackingFailed = false
  trackerStartPromise = (async () => {
    try {
      trackerRoots = getLocalStorageRoots()

      try {
        // Start native recursive watchers before scanning. On supported
        // Node 20+ deployments this closes the scan-to-watch race without a
        // second full walk of a potentially multi-terabyte HLS tree. Older
        // platforms fall back to per-directory watchers after the scan.
        initialWatcherChangedPaths = new Set()
        const usesRecursiveWatchers = watchStorageTrees(trackerRoots, [])

        let tree = await collectStorageTrees(trackerRoots)
        replaceTrackedDirectories(tree.directories)
        watchStorageTrees(trackerRoots, tree.directories.keys())

        // The per-directory fallback cannot watch a directory before it has
        // been discovered by the first scan. Take one more snapshot after
        // those watchers exist so Linux/unsupported recursive-watch platforms
        // cannot lose mutations made during the initial walk.
        if (!usesRecursiveWatchers) {
          tree = await collectStorageTrees(trackerRoots)
          replaceTrackedDirectories(tree.directories)
          watchStorageTrees(trackerRoots, tree.directories.keys())
        }

        const changedPaths = initialWatcherChangedPaths
        initialWatcherChangedPaths = undefined
        for (const changedPath of changedPaths ?? []) void enqueueTrackedPathChange(changedPath)
        await watcherWork
      } catch {
        failClosedTracking()
      }
    } catch {
      // Storage capacity is a safety gate: failure to initialise it must not
      // prevent PeerTube from starting, but it must prevent new remote imports.
      // The next admission/config event attempts a fresh event-driven snapshot.
      trackerRoots = []
      failClosedTracking()
    }
  })()

  return trackerStartPromise
}

export async function getLocalStorageImportCapacity (): Promise<LocalStorageImportCapacity> {
  await startLocalStorageImportCapacityTracking()
  await watcherWork

  await publishLocalStorageImportCapacitySnapshot()

  const limitBytes = Math.max(1, CONFIG.IMPORT.VIDEOS.LOCAL_STORAGE_LIMIT_GB) * GIGABYTE

  return {
    usageBytes: trackedUsageBytes,
    limitBytes,
    resumeUsageBytes: getResumeUsageBytes()
  }
}

export function shouldDeferVideoImportForLocalStorage (capacity: LocalStorageImportCapacity, options: {
  hasDeferredImports: boolean
}) {
  // Once an import has been held, require the configured headroom before
  // releasing more work. This prevents the next import from immediately
  // refilling the bytes that just drained.
  if (options.hasDeferredImports) return capacity.usageBytes > capacity.resumeUsageBytes

  return capacity.usageBytes >= capacity.limitBytes
}

export function onLocalStorageImportCapacityAvailable (listener: LocalStorageCapacityAvailableListener) {
  localStorageCapacityAvailableListeners.add(listener)

  return () => localStorageCapacityAvailableListeners.delete(listener)
}

// The normal object-storage cleanup path calls this after unlinking local
// media. It makes queued-import admission react synchronously; fs.watch still
// covers every other process/manual file mutation.
export function notifyLocalStorageImportPathRemoved (path: string) {
  return notifyLocalStorageImportPathChanged(path)
}

export function notifyLocalStorageImportPathChanged (path: string) {
  if (trackerRoots.length === 0) return Promise.resolve()

  if (trackingFailed) {
    return recoverLocalStorageImportCapacityTracking()
  }

  return enqueueTrackedPathChange(resolve(path))
}

export function stopLocalStorageImportCapacityTracking () {
  closeAllDirectoryWatchers()
  trackerStartPromise = undefined
  trackerRecoveryPromise = undefined
  trackerStarted = false
  trackingFailed = false
  trackerRoots = []
  trackedUsageBytes = 0
  watcherWork = Promise.resolve()
  initialWatcherChangedPaths = undefined
  trackedDirectoryBytes.clear()
  void removeLocalStorageImportCapacitySnapshot()
}

function enqueueTrackedPathChange (path: string) {
  if (initialWatcherChangedPaths) {
    initialWatcherChangedPaths.add(path)
    return Promise.resolve()
  }

  watcherWork = watcherWork
    .then(() => reconcileChangedPath(path))
    .catch(() => failClosedTracking())

  return watcherWork
}

async function reconcileChangedPath (path: string) {
  if (!isInsideTrackedRoots(path)) return

  const previousUsageBytes = trackedUsageBytes

  try {
    const stats = await lstat(path)

    if (stats.isDirectory()) {
      await replaceTrackedDirectoryTree(path)
    } else if (stats.isFile()) {
      await replaceTrackedDirectoryDirectBytes(dirname(path))
    }
  } catch (err) {
    if ((err as { code?: string })?.code !== 'ENOENT') throw err

    removeTrackedDirectory(path)
    await replaceTrackedDirectoryDirectBytes(dirname(path))
  }

  emitCapacityAvailableIfNeeded(previousUsageBytes)
}

async function replaceTrackedDirectoryTree (path: string) {
  const tree = await collectStorageTree(path)
  removeTrackedDirectory(path)

  for (const [ nextPath, size ] of tree.directories) trackedDirectoryBytes.set(nextPath, size)
  const addedBytes = sumDirectoryBytes(tree.directories)

  // removeTrackedDirectory already subtracted previousBytes from the aggregate.
  trackedUsageBytes = Math.max(0, trackedUsageBytes + addedBytes)
  watchStorageDirectories(tree.directories.keys())
}

async function replaceTrackedDirectoryDirectBytes (path: string) {
  if (!isInsideTrackedRoots(path)) return

  const nextBytes = await collectDirectoryDirectBytes(path)
  const previousBytes = trackedDirectoryBytes.get(path) ?? 0

  trackedDirectoryBytes.set(path, nextBytes)
  applyUsageDelta(nextBytes - previousBytes)
}

function replaceTrackedDirectories (directories: Map<string, number>) {
  trackedDirectoryBytes.clear()
  for (const [ path, size ] of directories) trackedDirectoryBytes.set(path, size)
  trackedUsageBytes = sumDirectoryBytes(directories)
}

function applyUsageDelta (delta: number) {
  if (!delta) return

  trackedUsageBytes = Math.max(0, trackedUsageBytes + delta)
}

function removeTrackedDirectory (path: string) {
  let releasedBytes = 0

  for (const [ trackedPath, size ] of trackedDirectoryBytes) {
    if (!isSamePathOrDescendant(path, trackedPath)) continue

    trackedDirectoryBytes.delete(trackedPath)
    releasedBytes += size
  }

  trackedUsageBytes = Math.max(0, trackedUsageBytes - releasedBytes)
  closeDirectoryWatchersAtOrBelow(path)
  return releasedBytes
}

function emitCapacityAvailableIfNeeded (previousUsageBytes: number) {
  const resumeUsageBytes = getResumeUsageBytes()
  if (previousUsageBytes > resumeUsageBytes && trackedUsageBytes <= resumeUsageBytes) {
    for (const listener of localStorageCapacityAvailableListeners) {
      Promise.resolve(listener()).catch(() => {})
    }
  }
}

function getResumeUsageBytes () {
  const limitBytes = Math.max(1, CONFIG.IMPORT.VIDEOS.LOCAL_STORAGE_LIMIT_GB) * GIGABYTE
  const freeSpaceForImportBytes = Math.max(0, CONFIG.IMPORT.VIDEOS.LOCAL_STORAGE_FREE_SPACE_FOR_IMPORT_GB) * GIGABYTE

  return Math.max(0, limitBytes - freeSpaceForImportBytes)
}

function getChangedPath (watchedDirectory: string, filename: string | Buffer | null) {
  if (filename === null) return watchedDirectory

  const name = filename.toString()
  return resolve(watchedDirectory, isAbsolute(name) ? relative(watchedDirectory, name) : name)
}

function isInsideTrackedRoots (path: string) {
  return trackerRoots.some(root => isSamePathOrDescendant(root, path))
}

function isSamePathOrDescendant (parentPath: string, candidatePath: string) {
  const relativePath = relative(parentPath, candidatePath)
  return relativePath === '' || (!relativePath.startsWith('..') && !isAbsolute(relativePath))
}

function getLocalStorageRoots () {
  // The normal layout has all media under the streaming-playlists parent, so
  // that one root covers the entire local storage folder. Include configured
  // import paths outside that parent as separate roots: remote downloads begin
  // in TMP_DIR before they become web-video/HLS media.
  const candidates = [
    dirname(CONFIG.STORAGE.STREAMING_PLAYLISTS_DIR),
    CONFIG.STORAGE.TMP_DIR,
    CONFIG.STORAGE.TMP_PERSISTENT_DIR,
    CONFIG.STORAGE.WEB_VIDEOS_DIR,
    CONFIG.STORAGE.ORIGINAL_VIDEO_FILES_DIR,
    CONFIG.STORAGE.STREAMING_PLAYLISTS_DIR
  ].map(path => resolve(path))

  return [ ...new Set(candidates) ]
    .filter(path => !candidates.some(otherPath => otherPath !== path && isSamePathOrDescendant(otherPath, path)))
}

function watchStorageDirectories (directories: Iterable<string>) {
  for (const directory of directories) {
    if (directoryWatchers.has(directory)) continue

    const watcher = watch(directory, (_eventType, filename) => {
      const changedPath = getChangedPath(directory, filename)
      void enqueueTrackedPathChange(changedPath)
    })

    watcher.on('error', () => {
      // A watcher for a directory that was deleted can become invalid. The
      // parent watcher reconciles the deletion; otherwise fail closed rather
      // than silently admitting imports with stale usage information.
      directoryWatchers.delete(directory)

      if (!trackerRoots.includes(directory) && !isInsideTrackedRoots(directory)) return

      if (trackerRoots.includes(directory)) {
        failClosedTracking()
        return
      }

      watcherWork = watcherWork
        .then(async () => {
          try {
            const stats = await lstat(directory)
            if (!stats.isDirectory()) {
              removeTrackedDirectory(directory)
              return
            }

            watchStorageDirectories([ directory ])
          } catch (err) {
            if ((err as { code?: string })?.code === 'ENOENT') {
              removeTrackedDirectory(directory)
              return
            }

            failClosedTracking()
          }
        })
        .catch(() => failClosedTracking())
    })

    directoryWatchers.set(directory, watcher)
  }
}

export async function getSharedLocalStorageImportCapacity (): Promise<LocalStorageImportCapacity> {
  const localCapacity = await getLocalStorageImportCapacity()
  const client = Redis.Instance.isConnected() ? Redis.Instance.getClient() : undefined
  if (!client) return localCapacity

  try {
    const snapshots = await client.hgetall(buildCapacityRedisKey())
    let usageBytes = localCapacity.usageBytes
    const staleOwners: string[] = []
    const now = Date.now()

    for (const [ ownerId, rawSnapshot ] of Object.entries(snapshots)) {
      try {
        const snapshot = JSON.parse(rawSnapshot) as { usageBytes?: unknown, updatedAt?: unknown }
        if (
          typeof snapshot.usageBytes !== 'number' ||
          !Number.isFinite(snapshot.usageBytes) ||
          typeof snapshot.updatedAt !== 'number' ||
          now - snapshot.updatedAt > CAPACITY_SNAPSHOT_MAX_AGE_MS
        ) {
          staleOwners.push(ownerId)
          continue
        }

        usageBytes = Math.max(usageBytes, snapshot.usageBytes)
      } catch {
        staleOwners.push(ownerId)
      }
    }

    if (staleOwners.length !== 0) await client.hdel(buildCapacityRedisKey(), ...staleOwners)

    return { ...localCapacity, usageBytes }
  } catch (err) {
    // Shared snapshots prevent independent PM2 processes from each admitting
    // against their own stale watcher state. Do not silently fall back to the
    // local reading when Redis cannot provide that coordination.
    throw new Error('Cannot read shared local-storage import capacity.', { cause: err })
  }
}

function watchStorageTrees (roots: string[], directories: Iterable<string>) {
  const allDirectories = [ ...directories ]
  let usesRecursiveWatchers = true

  for (const root of roots) {
    if (recursiveWatchers.has(root)) continue

    const rootDirectories = allDirectories.filter(directory => isSamePathOrDescendant(root, directory))

    // A directory watcher may already be active after a recursive-watch
    // capability failure. In that case only add newly created directories.
    if (hasDirectoryWatcherUnder(root)) {
      watchStorageDirectories(rootDirectories)
      usesRecursiveWatchers = false
      continue
    }

    try {
      const watcher = watch(root, { recursive: true }, (_eventType, filename) => {
        const changedPath = getChangedPath(root, filename)
        void enqueueTrackedPathChange(changedPath)
      })

      watcher.on('error', () => {
        if (recursiveWatchers.get(root) === watcher) recursiveWatchers.delete(root)
        // A recursive watcher can fail at runtime on some network or mounted
        // filesystems even when its initial creation succeeded. Re-snapshot and
        // downgrade to the per-directory event watcher rather than stranding
        // every held import until restart.
        watcherWork = watcherWork
          .then(() => {
            if (!trackerRoots.includes(root)) return

            return replaceTrackedDirectoryTree(root)
          })
          .catch(() => failClosedTracking())
      })

      recursiveWatchers.set(root, watcher)
    } catch {
      watchStorageDirectories(rootDirectories)
      usesRecursiveWatchers = false
    }
  }

  return usesRecursiveWatchers
}

function hasDirectoryWatcherUnder (root: string) {
  for (const directory of directoryWatchers.keys()) {
    if (isSamePathOrDescendant(root, directory)) return true
  }

  return false
}

function closeDirectoryWatchersAtOrBelow (path: string) {
  for (const [ watchedDirectory, watcher ] of directoryWatchers) {
    if (!isSamePathOrDescendant(path, watchedDirectory)) continue

    directoryWatchers.delete(watchedDirectory)
    watcher.close()
  }
}

function closeAllDirectoryWatchers () {
  for (const watcher of recursiveWatchers.values()) watcher.close()
  recursiveWatchers.clear()

  for (const watcher of directoryWatchers.values()) watcher.close()
  directoryWatchers.clear()
}

function failClosedTracking () {
  trackedUsageBytes = Number.MAX_SAFE_INTEGER
  closeAllDirectoryWatchers()
  initialWatcherChangedPaths = undefined
  trackingFailed = true
  trackerStarted = false
  trackerStartPromise = undefined
}

async function recoverLocalStorageImportCapacityTracking () {
  if (trackerRecoveryPromise !== undefined) return trackerRecoveryPromise

  const previousUsageBytes = trackedUsageBytes
  trackerRecoveryPromise = (async () => {
    await startLocalStorageImportCapacityTracking()

    if (!trackingFailed) emitCapacityAvailableIfNeeded(previousUsageBytes)
  })().finally(() => {
    trackerRecoveryPromise = undefined
  })

  return trackerRecoveryPromise
}

async function publishLocalStorageImportCapacitySnapshot () {
  const client = Redis.Instance.isConnected() ? Redis.Instance.getClient() : undefined
  if (!client) return

  await client.hset(buildCapacityRedisKey(), capacitySnapshotOwnerId, JSON.stringify({
    usageBytes: trackedUsageBytes,
    updatedAt: Date.now()
  }))
}

async function removeLocalStorageImportCapacitySnapshot () {
  const client = Redis.Instance.isConnected() ? Redis.Instance.getClient() : undefined
  if (!client) return

  await client.hdel(buildCapacityRedisKey(), capacitySnapshotOwnerId)
}

function buildCapacityRedisKey () {
  return Redis.Instance.getPrefix() + CAPACITY_REDIS_KEY_SUFFIX
}

type StorageTree = {
  directories: Map<string, number>
}

async function collectStorageTree (path: string): Promise<StorageTree> {
  const tree: StorageTree = {
    directories: new Map<string, number>()
  }

  await collectStorageTreeInto(path, tree)
  return tree
}

async function collectStorageTrees (paths: string[]) {
  const tree: StorageTree = {
    directories: new Map<string, number>()
  }

  for (const path of paths) {
    const nextTree = await collectStorageTree(path)
    for (const [ directory, size ] of nextTree.directories) tree.directories.set(directory, size)
  }

  return tree
}

async function collectStorageTreeInto (path: string, tree: StorageTree): Promise<void> {
  let entries: Awaited<ReturnType<typeof readdir>>

  try {
    entries = await readdir(path, { withFileTypes: true })
  } catch (err) {
    if ((err as { code?: string })?.code === 'ENOENT') return
    throw err
  }

  let directBytes = 0

  for (const entry of entries) {
    const entryPath = join(path, entry.name)

    if (entry.isDirectory()) {
      await collectStorageTreeInto(entryPath, tree)
      continue
    }

    if (!entry.isFile()) continue
    directBytes += getAllocatedSize(await lstat(entryPath))
  }

  tree.directories.set(resolve(path), directBytes)
}

function getAllocatedSize (stats: { size: number }) {
  // Logical file size is portable across the supported storage backends and is
  // conservative for the normal, non-sparse media files PeerTube produces.
  return stats.size
}

async function collectDirectoryDirectBytes (path: string) {
  let entries: Awaited<ReturnType<typeof readdir>>

  try {
    entries = await readdir(path, { withFileTypes: true })
  } catch (err) {
    if ((err as { code?: string })?.code === 'ENOENT') return 0
    throw err
  }

  let total = 0
  for (const entry of entries) {
    if (!entry.isFile()) continue
    total += getAllocatedSize(await lstat(join(path, entry.name)))
  }

  return total
}

function sumDirectoryBytes (directories: Map<string, number>) {
  let total = 0
  for (const size of directories.values()) total += size
  return total
}
