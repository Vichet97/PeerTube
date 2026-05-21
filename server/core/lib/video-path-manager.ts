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

type MakeAvailableCB<T> = (path: string) => Awaitable<T>
type MakeAvailableMultipleCB<T> = (paths: string[]) => Awaitable<T>
type MakeAvailableCreateMethod = { method: () => Awaitable<string>, clean: boolean }

const lTags = loggerTagsFactory('video-path-manager')

class VideoPathManager {
  private static instance: VideoPathManager

  // Key is a video UUID
  private readonly videoFileMutexStore = new Map<string, Mutex>()

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
      const localPath = this.getFSVideoFileOutputPath(videoFile.getVideoOrStreamingPlaylist(), videoFile)

      if (await this.shouldUseLocalPath(localPath, videoFile.storage)) {
        createMethods.push({
          method: () => localPath,
          clean: false
        })

        continue
      }

      const destination = this.buildTMPDestination(videoFile.filename)

      if (videoFile.isHLS()) {
        const playlist = (videoFile as MVideoFileStreamingPlaylistVideo).VideoStreamingPlaylist

        createMethods.push({
          method: () => makeHLSFileAvailable(playlist.Video, videoFile.filename, destination),
          clean: true
        })
      } else {
        createMethods.push({
          method: () => makeWebVideoFileAvailable(videoFile.filename, destination),
          clean: true
        })
      }
    }

    return this.makeAvailableFactory({ createMethods, cbContext: cb })
  }

  async makeAvailableVideoFile<T> (videoFile: MVideoFileVideo | MVideoFileStreamingPlaylistVideo, cb: MakeAvailableCB<T>) {
    return this.makeAvailableVideoFiles([ videoFile ], paths => cb(paths[0]))
  }

  async makeAvailableVideoSource<T> (videoSource: MVideoSource, cb: MakeAvailableCB<T>) {
    const localPath = this.getFSOriginalVideoFilePath(videoSource.keptOriginalFilename)

    if (await this.shouldUseLocalPath(localPath, videoSource.storage)) {
      return this.makeAvailableFactory({
        createMethods: [
          {
            method: () => localPath,
            clean: false
          }
        ],
        cbContext: paths => cb(paths[0])
      })
    }

    return this.makeAvailableFactory({
      createMethods: [
        {
          method: () => makeOriginalFileAvailable(
            videoSource.keptOriginalFilename,
            this.buildTMPDestination(videoSource.keptOriginalFilename)
          ),
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
            clean: false
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
            clean: false
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

    return releaser
  }

  unlockFiles (videoUUID: string) {
    const mutex = this.videoFileMutexStore.get(videoUUID)

    mutex.release()

    logger.debug('Released lockfiles of %s.', videoUUID, lTags(videoUUID))
  }

  private async makeAvailableFactory<T> (options: {
    createMethods: MakeAvailableCreateMethod[]
    cbContext: MakeAvailableMultipleCB<T>
  }) {
    const { cbContext, createMethods } = options

    let result: T

    const created: { destination: string, clean: boolean }[] = []

    const cleanup = async () => {
      for (const { destination, clean } of created) {
        if (!destination || !clean) continue

        // Skip if file doesn't exist (may have been already removed or never created)
        if (!await pathExists(destination)) {
          logger.debug('Skipping cleanup of non-existent file %s.', destination)
          continue
        }

        try {
          await remove(destination)
        } catch (err) {
          logger.error('Cannot remove ' + destination, { err })
        }
      }
    }

    for (const { method, clean } of createMethods) {
      created.push({
        destination: await method(),
        clean
      })
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
