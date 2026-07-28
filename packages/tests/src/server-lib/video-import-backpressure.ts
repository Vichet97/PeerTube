/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { mkdtemp, mkdir, rm, writeFile } from 'fs/promises'
import { tmpdir } from 'os'
import { join } from 'path'
import { CONFIG } from '@server/initializers/config.js'
import {
  buildVideoImportLocalStorageCapacityJobId,
  isDeferredVideoImportJobId,
  isVideoImportBackpressureJobId,
  isVideoImportLocalStorageCapacityJobId
} from '@server/lib/job-queue/handlers/video-import-processability.js'
import {
  LocalStorageImportCapacity,
  getLocalStorageImportCapacity,
  notifyLocalStorageImportPathChanged,
  notifyLocalStorageImportPathRemoved,
  shouldDeferVideoImportForLocalStorage,
  stopLocalStorageImportCapacityTracking
} from '@server/lib/local-storage-import-admission.js'

const GB = 1024 ** 3

function capacity (usageGB: number): LocalStorageImportCapacity {
  return {
    usageBytes: usageGB * GB,
    limitBytes: 1024 * GB,
    resumeUsageBytes: 1022 * GB
  }
}

describe('video-import local storage admission', function () {
  it('should accept imports below the configured local storage limit', function () {
    expect(shouldDeferVideoImportForLocalStorage(capacity(1023.99), { hasDeferredImports: false })).to.be.false
  })

  it('should park imports once local storage reaches the configured limit', function () {
    expect(shouldDeferVideoImportForLocalStorage(capacity(1024), { hasDeferredImports: false })).to.be.true
  })

  it('should keep delayed imports parked until the configured headroom is free', function () {
    expect(shouldDeferVideoImportForLocalStorage(capacity(1022.01), { hasDeferredImports: true })).to.be.true
    expect(shouldDeferVideoImportForLocalStorage(capacity(1022), { hasDeferredImports: true })).to.be.false
  })

  it('should use a distinct delayed-job identity for local-storage admission', function () {
    const jobId = buildVideoImportLocalStorageCapacityJobId(42, 1_234_567)

    expect(jobId).to.equal('video-import-local-storage-capacity-42-1234567')
    expect(isVideoImportLocalStorageCapacityJobId(jobId)).to.be.true
    expect(isDeferredVideoImportJobId(jobId)).to.be.true
    expect(isVideoImportBackpressureJobId(jobId)).to.be.false
  })

  it('should track directory totals without retaining an entry for every media file', async function () {
    const root = await mkdtemp(join(tmpdir(), 'peertube-storage-admission-'))
    const streamingPlaylists = join(root, 'streaming-playlists')
    const tmp = join(root, 'tmp')
    const webVideos = join(root, 'web-videos')
    const originalVideos = join(root, 'original-video-files')
    const storage = CONFIG.STORAGE
    const originalPaths = {
      streamingPlaylists: storage.STREAMING_PLAYLISTS_DIR,
      tmp: storage.TMP_DIR,
      tmpPersistent: storage.TMP_PERSISTENT_DIR,
      webVideos: storage.WEB_VIDEOS_DIR,
      originalVideos: storage.ORIGINAL_VIDEO_FILES_DIR
    }

    try {
      await Promise.all([ mkdir(streamingPlaylists), mkdir(tmp), mkdir(webVideos), mkdir(originalVideos) ])
      await writeFile(join(root, 'existing.bin'), Buffer.alloc(10))

      storage.STREAMING_PLAYLISTS_DIR = streamingPlaylists
      storage.TMP_DIR = tmp
      storage.TMP_PERSISTENT_DIR = join(root, 'tmp-persistent')
      storage.WEB_VIDEOS_DIR = webVideos
      storage.ORIGINAL_VIDEO_FILES_DIR = originalVideos

      expect((await getLocalStorageImportCapacity()).usageBytes).to.equal(10)

      const addedPath = join(webVideos, 'import.mp4')
      await writeFile(addedPath, Buffer.alloc(20))
      await notifyLocalStorageImportPathChanged(addedPath)
      expect((await getLocalStorageImportCapacity()).usageBytes).to.equal(30)

      await rm(addedPath)
      await notifyLocalStorageImportPathRemoved(addedPath)
      expect((await getLocalStorageImportCapacity()).usageBytes).to.equal(10)
    } finally {
      stopLocalStorageImportCapacityTracking()
      storage.STREAMING_PLAYLISTS_DIR = originalPaths.streamingPlaylists
      storage.TMP_DIR = originalPaths.tmp
      storage.TMP_PERSISTENT_DIR = originalPaths.tmpPersistent
      storage.WEB_VIDEOS_DIR = originalPaths.webVideos
      storage.ORIGINAL_VIDEO_FILES_DIR = originalPaths.originalVideos
      await rm(root, { recursive: true, force: true })
    }
  })
})
