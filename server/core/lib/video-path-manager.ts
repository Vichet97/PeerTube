import { FileStorage, FileStorageType } from '@peertube/peertube-models'
import { buildUUID } from '@peertube/peertube-node-utils'
import { Awaitable } from '@peertube/peertube-typescript-utils'
import { logger, loggerTagsFactory } from '@server/helpers/logger.js'
import { extractVideo } from '@server/helpers/video.js'
import { CONFIG } from '@server/initializers/config.js'
import { DIRECTORIES } from '@server/initializers/constants.js'
import {
  MStreamingPlaylistVideo,
  MVideo,
  MVideoFile,
  MVideoFileStreamingPlaylistVideo,
  MVideoFileVideo,
  MVideoPrivacy,
  MVideoWithFile
} from '@server/types/models/index.js'
import { MVideoSource } from '@server/types/models/video/video-source.js'
import { Mutex } from 'async-mutex'
import { pathExists, remove } from 'fs-extra/esm'
import { extname, join } from 'path'
import { makeHLSFileAvailable, makeOriginalFileAvailable, makeWebVideoFileAvailable } from './object-storage/index.js'
import { getHLSDirectory, getHLSResolutionPlaylistFilename } from './paths.js'
import { isVideoInPrivateDirectory } from './video-privacy.js'
import { LocalFileLease, LocalFileLeaseManager } from './local-file-lease-manager.js'

type MakeAvailableCB<T> = (path: string) => Awaitable<T>
type MakeAvailableMultipleCB<T> = (paths: string[]) => Awaitable<T>
type MakeAvailableCreateMethod = {
  method: () => Awaitable<string>
  clean: boolean
  leaseVideoUUID?: string
  fallbackMethod?: () => Awaitable<string>
}
type FilesUnlockedListener = (videoUUID: string) => void | Promise<void>

const lTags = loggerTagsFactory('video-path-manager')
const LOCAL_FILE_READ_LEASE_HEARTBEAT_MS = 60 * 60 * 1000

class VideoPathManager {
  private static instance: VideoPathManager

  // Key is a video UUID
  private readonly videoFileMutexStore = new Map<string, Mutex>()
  private readonly filesUnlockedListeners = new Set<FilesUnlockedListener>()

  private constructor () {}

  getFSHLSOutputPath (video: MVideoPrivacy, filename?: string) {
    const base = getHLSDirectory(video)
    if (!filename) return base

    return join(base, filename)
  }

  getFSVideoFileOutputPath (videoOrPlaylist: MVideo | MStreamingPlaylistVideo, videoFile: MVideoFile) {
    const video = extractVideo(videoOrPlaylist)

    if (videoFile.isHLS()) {
      return join(getHLSDirectory(video), videoFile.filename)
    }

    if (isVideoInPrivateDirectory(video.privacy)) {
      return join(DIRECTORIES.WEB_VIDEOS.PRIVATE, videoFile.filename)
    }

    return join(DIRECTORIES.WEB_VIDEOS.PUBLIC, videoFile.filename)
  }

  getFSOriginalVideoFilePath (filename: string) {
    return join(DIRECTORIES.ORIGINAL_VIDEOS, filename)
  }

  // ---------------------------------------------------------------------------

  async makeAvailableVideoFiles<T> (videoFiles: (MVideoFileVideo | MVideoFileStreamingPlaylistVideo)[], cb: MakeAvailableMultipleCB<T>) {
    const createMethods: MakeAvailableCreateMethod[] = []

    for (const videoFile of videoFiles) {
      const video = extractVideo(videoFile.getVideoOrStreamingPlaylist())
      const localPath = this.getFSVideoFileOutputPath(videoFile.getVideoOrStreamingPlaylist(), videoFile)
      const fallbackMethod = () => {
        const destination = this.buildTMPDestination(videoFile.filename)

        if (videoFile.isHLS()) {
          const playlist = (videoFile as MVideoFileStreamingPlaylistVideo).VideoStreamingPlaylist

          return makeHLSFileAvailable(playlist.Video, videoFile.filename, destination)
        }

        return makeWebVideoFileAvailable(videoFile.filename, destination)
      }

      if (await this.shouldUseLocalPath(localPath, videoFile.storage)) {
        createMethods.push({
          method: () => localPath,
          clean: false,
          leaseVideoUUID: video.uuid,
          fallbackMethod
        })

        continue
      }

      createMethods.push({ method: fallbackMethod, clean: true })
    }

    return this.makeAvailableFactory({ createMethods, cbContext: cb })
  }

  async makeAvailableVideoFile<T> (videoFile: MVideoFileVideo | MVideoFileStreamingPlaylistVideo, cb: MakeAvailableCB<T>) {
    return this.makeAvailableVideoFiles([ videoFile ], paths => cb(paths[0]))
  }

  async makeAvailableVideoSource<T> (videoSource: MVideoSource, cb: MakeAvailableCB<T>, videoUUID?: string) {
    const localPath = this.getFSOriginalVideoFilePath(videoSource.keptOriginalFilename)
    const fallbackMethod = () => makeOriginalFileAvailable(
      videoSource.keptOriginalFilename,
      this.buildTMPDestination(videoSource.keptOriginalFilename)
    )

    if (await this.shouldUseLocalPath(localPath, videoSource.storage)) {
      return this.makeAvailableFactory({
        createMethods: [
          {
            method: () => localPath,
            clean: false,
            leaseVideoUUID: videoUUID,
            fallbackMethod
          }
        ],
        cbContext: paths => cb(paths[0])
      })
    }

    return this.makeAvailableFactory({
      createMethods: [
        {
          method: fallbackMethod,
          clean: true
        }
      ],
      cbContext: paths => cb(paths[0])
    })
  }

  async makeAvailableMaxQualityFiles<T> (
    video: MVideoWithFile,
    cb: (options: { videoPath: string, separatedAudioPath: string }) => Awaitable<T>
  ) {
    const { videoFile, separatedAudioFile } = video.getMaxQualityAudioAndVideoFiles()

    const files = [ videoFile ]
    if (separatedAudioFile) files.push(separatedAudioFile)

    return this.makeAvailableVideoFiles(files, ([ videoPath, separatedAudioPath ]) => {
      return cb({ videoPath, separatedAudioPath })
    })
  }

  // ---------------------------------------------------------------------------

  async makeAvailableResolutionPlaylistFile<T> (videoFile: MVideoFileStreamingPlaylistVideo, cb: MakeAvailableCB<T>) {
    const filename = getHLSResolutionPlaylistFilename(videoFile.filename)
    const localPath = join(getHLSDirectory(videoFile.getVideo()), filename)

    if (await this.shouldUseLocalPath(localPath, videoFile.storage)) {
      return this.makeAvailableFactory({
        createMethods: [
          {
            method: () => localPath,
            clean: false,
            leaseVideoUUID: extractVideo(videoFile.getVideoOrStreamingPlaylist()).uuid,
            fallbackMethod: () => makeHLSFileAvailable(
              videoFile.VideoStreamingPlaylist.Video,
              filename,
              this.buildTMPDestination(filename)
            )
          }
        ],
        cbContext: paths => cb(paths[0])
      })
    }

    return this.makeAvailableFactory({
      createMethods: [
        {
          method: () => makeHLSFileAvailable(videoFile.VideoStreamingPlaylist.Video, filename, this.buildTMPDestination(filename)),
          clean: true
        }
      ],
      cbContext: paths => cb(paths[0])
    })
  }

  async makeAvailablePlaylistFile<T> (playlist: MStreamingPlaylistVideo, filename: string, cb: MakeAvailableCB<T>) {
    const localPath = join(getHLSDirectory(playlist.Video), filename)

    if (await this.shouldUseLocalPath(localPath, playlist.storage)) {
      return this.makeAvailableFactory({
        createMethods: [
          {
            method: () => localPath,
            clean: false,
            leaseVideoUUID: playlist.Video.uuid,
            fallbackMethod: () => makeHLSFileAvailable(
              playlist.Video,
              filename,
              this.buildTMPDestination(filename)
            )
          }
        ],
        cbContext: paths => cb(paths[0])
      })
    }

    return this.makeAvailableFactory({
      createMethods: [
        {
          method: () => makeHLSFileAvailable(playlist.Video, filename, this.buildTMPDestination(filename)),
          clean: true
        }
      ],
      cbContext: paths => cb(paths[0])
    })
  }

  // ---------------------------------------------------------------------------

  async lockFiles (videoUUID: string) {
    if (!this.videoFileMutexStore.has(videoUUID)) {
      this.videoFileMutexStore.set(videoUUID, new Mutex())
    }

    const mutex = this.videoFileMutexStore.get(videoUUID)
    const releaser = await mutex.acquire()

    logger.debug('Locked files of %s.', videoUUID, lTags(videoUUID))

    let released = false

    return () => {
      if (released) return
      released = true

      releaser()
      this.notifyFilesUnlocked(videoUUID)
    }
  }

  unlockFiles (videoUUID: string) {
    const mutex = this.videoFileMutexStore.get(videoUUID)

    mutex.release()
    this.notifyFilesUnlocked(videoUUID)

    logger.debug('Released lockfiles of %s.', videoUUID, lTags(videoUUID))
  }

  onFilesUnlocked (listener: FilesUnlockedListener) {
    this.filesUnlockedListeners.add(listener)

    return () => this.filesUnlockedListeners.delete(listener)
  }

  hasLockedFiles (videoUUID: string) {
    return this.videoFileMutexStore.get(videoUUID)?.isLocked() === true
  }

  private notifyFilesUnlocked (videoUUID: string) {
    for (const listener of this.filesUnlockedListeners) {
      Promise.resolve(listener(videoUUID))
        .catch(err => logger.warn('Cannot notify local file unlock.', { err, ...lTags(videoUUID) }))
    }
  }

  private async makeAvailableFactory<T> (options: {
    createMethods: MakeAvailableCreateMethod[]
    cbContext: MakeAvailableMultipleCB<T>
  }) {
    const { cbContext, createMethods } = options

    let result: T

    const created: {
      destination: string
      clean: boolean
      lease?: LocalFileLease
      stopLeaseHeartbeat?: () => void
    }[] = []

    const cleanup = async () => {
      for (const createdFile of created) {
        const { destination, clean, lease, stopLeaseHeartbeat } = createdFile

        try {
          if (destination && clean) {
            // Skip if file doesn't exist (may have been already removed or never created)
            if (!await pathExists(destination)) {
              logger.debug('Skipping cleanup of non-existent file %s.', destination)
            } else {
              await remove(destination)
            }
          }
        } catch (err) {
          logger.error('Cannot remove ' + destination, { err })
        } finally {
          stopLeaseHeartbeat?.()
          await this.releaseLocalFileLease(lease)
        }
      }
    }

    try {
      for (const createMethod of createMethods) {
        const { method, clean, fallbackMethod, leaseVideoUUID } = createMethod
        let destination = await method()
        let lease: LocalFileLease | undefined
        let stopLeaseHeartbeat: (() => void) | undefined

        if (leaseVideoUUID) {
          lease = await LocalFileLeaseManager.Instance.acquire({
            videoUUID: leaseVideoUUID,
            leaseId: LocalFileLeaseManager.Instance.createLeaseId('local-read')
          })

          if (!lease && fallbackMethod) {
            destination = await fallbackMethod()
            lease = undefined
            created.push({ destination, clean: true })
            continue
          }

          if (!lease) throw new Error(`Cannot acquire local file lease for video ${leaseVideoUUID}`)
          stopLeaseHeartbeat = this.startLocalFileReadLeaseHeartbeat(lease, leaseVideoUUID)

          try {
            // Cleanup may have won the race just before the lease was acquired.
            // In that case use the normal object-storage staging fallback instead.
            if (!await pathExists(destination) && fallbackMethod) {
              stopLeaseHeartbeat()
              await lease.release()
              lease = undefined
              stopLeaseHeartbeat = undefined
              destination = await fallbackMethod()
              created.push({ destination, clean: true })
              continue
            }
          } catch (err) {
            stopLeaseHeartbeat?.()
            await lease?.release()

            throw err
          }
        }

        created.push({ destination, clean, lease, stopLeaseHeartbeat })
      }
    } catch (err) {
      await cleanup()

      throw err
    }

    try {
      result = await cbContext(created.map(c => c.destination))
    } catch (err) {
      await cleanup()

      throw err
    }

    await cleanup()

    return result
  }

  buildTMPDestination (filename: string) {
    return join(CONFIG.STORAGE.TMP_DIR, buildUUID() + extname(filename))
  }

  private startLocalFileReadLeaseHeartbeat (lease: LocalFileLease, videoUUID: string) {
    const timer = setInterval(() => {
      lease.refresh()
        .then(refreshed => {
          if (!refreshed) logger.warn('Cannot refresh local file read lease.', lTags(videoUUID))
        })
        .catch(err => logger.warn('Cannot refresh local file read lease.', { err, ...lTags(videoUUID) }))
    }, LOCAL_FILE_READ_LEASE_HEARTBEAT_MS)
    timer.unref?.()

    return () => {
      clearInterval(timer)
      lease.deactivate?.()
    }
  }

  private async releaseLocalFileLease (lease: LocalFileLease | undefined) {
    await lease?.release()
  }

  private async shouldUseLocalPath (path: string, storage: FileStorageType) {
    if (storage === FileStorage.FILE_SYSTEM) return true

    if (!await pathExists(path)) return false

    logger.debug('Using retained local file %s even though model storage is object storage.', path, lTags())

    return true
  }

  static get Instance () {
    return this.instance || (this.instance = new this())
  }
}

// ---------------------------------------------------------------------------

export {
  VideoPathManager
}
