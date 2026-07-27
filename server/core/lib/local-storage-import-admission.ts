import { CONFIG } from '@server/initializers/config.js'
import { FSWatcher, watch } from 'fs'
import { lstat, readdir } from 'fs/promises'
import { dirname, isAbsolute, join, relative, resolve } from 'path'

const GIGABYTE = 1024 ** 3

export type LocalStorageImportCapacity = {
  usageBytes: number
  limitBytes: number
  resumeUsageBytes: number
}

type LocalStorageCapacityAvailableListener = () => void | Promise<void>

const localStorageCapacityAvailableListeners = new Set<LocalStorageCapacityAvailableListener>()
const trackedFileBytes = new Map<string, number>()
const directoryWatchers = new Map<string, FSWatcher>()
let trackedUsageBytes = 0
let trackerRoots: string[] = []
const recursiveWatchers = new Map<string, FSWatcher>()
let trackerStartPromise: Promise<void> | undefined
let trackerStarted = false
let trackingFailed = false
let watcherWork = Promise.resolve()

export async function startLocalStorageImportCapacityTracking () {
  if (trackerStarted && !trackingFailed) return trackerStartPromise

  trackerStarted = true
  trackingFailed = false
  trackerStartPromise = (async () => {
    try {
      trackerRoots = getLocalStorageRoots()
      let tree = await collectStorageTrees(trackerRoots)

      replaceTrackedFiles(tree.files)

      try {
        // Node 20+ supports recursive fs.watch on PeerTube's supported
        // platforms. Fall back to one watcher per directory only when that
        // capability is unavailable, so a large HLS tree does not consume one
        // file descriptor per playlist under normal deployments.
        watchStorageTrees(trackerRoots, tree.directories)

        // A storage mutation can occur after the initial scan but before every
        // directory watcher is installed. Take one event-driven startup
        // snapshot after registration, then apply any watcher work queued while
        // that snapshot was being collected.
        tree = await collectStorageTrees(trackerRoots)
        replaceTrackedFiles(tree.files)
        watchStorageTrees(trackerRoots, tree.directories)
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
  if (trackerRoots.length === 0) return

  const previousUsageBytes = trackedUsageBytes
  const releasedBytes = removeTrackedPath(resolve(path))
  if (releasedBytes !== 0) emitCapacityAvailableIfNeeded(previousUsageBytes)
}

export function stopLocalStorageImportCapacityTracking () {
  closeAllDirectoryWatchers()
  trackerStartPromise = undefined
  trackerStarted = false
  trackingFailed = false
  trackerRoots = []
  trackedUsageBytes = 0
  watcherWork = Promise.resolve()
  trackedFileBytes.clear()
}

async function reconcileChangedPath (path: string) {
  if (!isInsideTrackedRoots(path)) return

  const previousUsageBytes = trackedUsageBytes

  try {
    const stats = await lstat(path)

    if (stats.isDirectory()) {
      await replaceTrackedDirectory(path)
    } else if (stats.isFile()) {
      const previousBytes = trackedFileBytes.get(path) ?? 0
      const nextBytes = getAllocatedSize(stats)
      trackedFileBytes.set(path, nextBytes)
      applyUsageDelta(nextBytes - previousBytes)
    }
  } catch (err) {
    if ((err as { code?: string })?.code !== 'ENOENT') return

    removeTrackedPath(path)
  }

  emitCapacityAvailableIfNeeded(previousUsageBytes)
}

async function replaceTrackedDirectory (path: string) {
  const tree = await collectStorageTree(path)
  const previousBytes = removeTrackedPath(path)

  for (const [ nextPath, size ] of tree.files) trackedFileBytes.set(nextPath, size)
  const addedBytes = sumFileBytes(tree.files)

  trackedUsageBytes = Math.max(0, trackedUsageBytes + addedBytes - previousBytes)
  watchStorageDirectories(tree.directories)
}

function replaceTrackedFiles (files: Map<string, number>) {
  trackedFileBytes.clear()
  for (const [ path, size ] of files) trackedFileBytes.set(path, size)
  trackedUsageBytes = sumFileBytes(files)
}

function applyUsageDelta (delta: number) {
  if (!delta) return

  trackedUsageBytes = Math.max(0, trackedUsageBytes + delta)
}

function removeTrackedPath (path: string) {
  let releasedBytes = 0

  for (const [ trackedPath, size ] of trackedFileBytes) {
    if (!isSamePathOrDescendant(path, trackedPath)) continue

    trackedFileBytes.delete(trackedPath)
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
      watcherWork = watcherWork
        .then(() => reconcileChangedPath(changedPath))
        .catch(() => failClosedTracking())
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
              removeTrackedPath(directory)
              return
            }

            watchStorageDirectories([ directory ])
          } catch (err) {
            if ((err as { code?: string })?.code === 'ENOENT') {
              removeTrackedPath(directory)
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

function watchStorageTrees (roots: string[], directories: Iterable<string>) {
  const allDirectories = [ ...directories ]

  for (const root of roots) {
    if (recursiveWatchers.has(root)) continue

    const rootDirectories = allDirectories.filter(directory => isSamePathOrDescendant(root, directory))

    // A directory watcher may already be active after a recursive-watch
    // capability failure. In that case only add newly created directories.
    if (hasDirectoryWatcherUnder(root)) {
      watchStorageDirectories(rootDirectories)
      continue
    }

    try {
      const watcher = watch(root, { recursive: true }, (_eventType, filename) => {
        const changedPath = getChangedPath(root, filename)
        watcherWork = watcherWork
          .then(() => reconcileChangedPath(changedPath))
          .catch(() => failClosedTracking())
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

            return replaceTrackedDirectory(root)
          })
          .catch(() => failClosedTracking())
      })

      recursiveWatchers.set(root, watcher)
    } catch {
      watchStorageDirectories(rootDirectories)
    }
  }
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
  trackingFailed = true
  trackerStarted = false
  trackerStartPromise = undefined
}

type StorageTree = {
  files: Map<string, number>
  directories: Set<string>
}

async function collectStorageTree (path: string): Promise<StorageTree> {
  const tree: StorageTree = {
    files: new Map<string, number>(),
    directories: new Set<string>()
  }

  await collectStorageTreeInto(path, tree)
  return tree
}

async function collectStorageTrees (paths: string[]) {
  const tree: StorageTree = {
    files: new Map<string, number>(),
    directories: new Set<string>()
  }

  for (const path of paths) {
    const nextTree = await collectStorageTree(path)
    for (const [ filePath, size ] of nextTree.files) tree.files.set(filePath, size)
    for (const directory of nextTree.directories) tree.directories.add(directory)
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

  tree.directories.add(resolve(path))

  for (const entry of entries) {
    const entryPath = join(path, entry.name)

    if (entry.isDirectory()) {
      await collectStorageTreeInto(entryPath, tree)
      continue
    }

    if (!entry.isFile()) continue
    tree.files.set(resolve(entryPath), getAllocatedSize(await lstat(entryPath)))
  }
}

function getAllocatedSize (stats: { size: number }) {
  // Logical file size is portable across the supported storage backends and is
  // conservative for the normal, non-sparse media files PeerTube produces.
  return stats.size
}

function sumFileBytes (files: Map<string, number>) {
  let total = 0
  for (const size of files.values()) total += size
  return total
}
