import { pick, timeoutPromise } from '@peertube/peertube-core-utils'
import { canCopyForHLS, getVideoStreamDuration, HLSFromTSTranscodeOptions, HLSTranscodeOptions } from '@peertube/peertube-ffmpeg'
import { retryTransactionWrapper } from '@server/helpers/database-utils.js'
import { logger } from '@server/helpers/logger.js'
import { sequelizeTypescript } from '@server/initializers/database.js'
import { createTorrentAndSetInfoHash } from '@server/lib/webtorrent.js'
import { MVideo } from '@server/types/models/index.js'
import { Job } from 'bullmq'
import { ensureDir, move, pathExists } from 'fs-extra/esm'
import { join } from 'path'
import { CONFIG } from '../../initializers/config.js'
import { VideoFileModel } from '../../models/video/video-file.js'
import { VideoStreamingPlaylistModel } from '../../models/video/video-streaming-playlist.js'
import { renameVideoFileInPlaylist, updateM3U8AndShaPlaylist } from '../hls.js'
import { generateHLSVideoFilename, getHLSResolutionPlaylistFilename } from '../paths.js'
import { createAllCaptionPlaylistsOnFSIfNeeded } from '../video-captions.js'
import { buildNewFile } from '../video-file.js'
import { VideoPathManager } from '../video-path-manager.js'
import { buildFFmpegVOD } from './shared/index.js'
import { buildGranularHLSPlaylistMoveJob, createPendingMoveJobs } from '../video-jobs.js'

const HLS_POST_TRANSCODING_STEP_TIMEOUT_MS = 10 * 60 * 1000

// Concat TS segments from a live video to a fragmented mp4 HLS playlist
export async function generateHlsPlaylistResolutionFromTS (options: {
  video: MVideo
  concatenatedTsFilePath: string
  resolution: number
  fps: number
  isAAC: boolean
  filesLockedInParent: boolean  // Lock already held by parent
}) {
  return generateHlsPlaylistCommon({
    type: 'hls-from-ts' as 'hls-from-ts',

    videoInputPath: options.concatenatedTsFilePath,

    ...pick(options, [ 'video', 'resolution', 'fps', 'filesLockedInParent', 'isAAC' ])
  })
}

// Generate an HLS playlist from an input file, and update the master playlist
export function generateHlsPlaylistResolution (options: {
  video: MVideo

  videoInputPath: string
  separatedAudioInputPath: string

  resolution: number
  fps: number
  filesLockedInParent: boolean  // Lock already held by parent
  separatedAudio: boolean
  job?: Job
}) {
  return generateHlsPlaylistCommon({
    type: 'hls' as 'hls',

    ...pick(options, [
      'videoInputPath',
      'separatedAudioInputPath',
      'video',
      'resolution',
      'fps',
      'filesLockedInParent',
      'separatedAudio',
      'job'
    ])
  })
}

export async function onHLSVideoFileTranscoding (options: {
  video: MVideo
  videoOutputPath: string
  m3u8OutputPath: string
  filesLockedInParent?: boolean // default false
}) {
  const { video, videoOutputPath, m3u8OutputPath, filesLockedInParent = false } = options

  // [LOGGER] HLS transcoding completed
  logger.info('[TRANSCODE_HLS] HLS transcoding completed for video %s', video.uuid, {
    videoOutputPath,
    m3u8OutputPath
  })

  // Create or update the playlist
  const { playlist, generated: playlistGenerated } = await runHLSPostTranscodingStep({
    videoUUID: video.uuid,
    step: 'load-or-generate-playlist',
    run: () => retryTransactionWrapper(() => {
      return sequelizeTypescript.transaction(async transaction => {
        return VideoStreamingPlaylistModel.loadOrGenerate(video, transaction)
      })
    })
  })

  // [LOGGER] Playlist loaded/created
  logger.info('[TRANSCODE_HLS] Playlist loaded/created for %s', video.uuid, {
    playlistId: playlist.id,
    playlistGenerated,
    playlistStorage: playlist.storage
  })

  logger.info('[TRANSCODE_HLS] About to build new file for %s', video.uuid)
  const newVideoFile = await runHLSPostTranscodingStep({
    videoUUID: video.uuid,
    step: 'build-new-file-metadata',
    run: () => buildNewFile({ mode: 'hls', path: videoOutputPath })
  })
  logger.info('[TRANSCODE_HLS] Built new video file: %s', newVideoFile.filename)
  newVideoFile.videoStreamingPlaylistId = playlist.id

  logger.info('[TRANSCODE_HLS] About to acquire file lock for %s (filesLockedInParent=%s)', video.uuid, filesLockedInParent)
  const mutexReleaser = !filesLockedInParent
    ? await VideoPathManager.Instance.lockFiles(video.uuid)
    : null
  logger.info('[TRANSCODE_HLS] File lock acquired for %s', video.uuid)

  try {
    await runHLSPostTranscodingStep({
      videoUUID: video.uuid,
      step: 'reload-video',
      run: () => video.reload()
    })

    const videoFilePath = VideoPathManager.Instance.getFSVideoFileOutputPath(playlist, newVideoFile)
    const hlsOutputPath = VideoPathManager.Instance.getFSHLSOutputPath(video)

    // [LOGGER] Checking destination paths
    logger.info('[TRANSCODE_HLS] Checking paths for video %s', video.uuid, {
      videoPrivacy: video.privacy,
      hlsOutputPath,
      videoFilePath,
      videoUUID: video.uuid
    })

    // Ensure destination directory exists
    await ensureDir(hlsOutputPath)
    logger.info('[TRANSCODE_HLS] Destination directory ensured: %s', hlsOutputPath)

    // Verify directory was created
    const destDirExists = await pathExists(hlsOutputPath)
    if (!destDirExists) {
      throw new Error(`Failed to create destination directory: ${hlsOutputPath}`)
    }
    logger.info('[TRANSCODE_HLS] Destination directory verified to exist: %s', hlsOutputPath)

    // Check source files exist
    const sourceM3u8Exists = await pathExists(m3u8OutputPath)
    const sourceVideoExists = await pathExists(videoOutputPath)
    logger.info('[TRANSCODE_HLS] Source files check: m3u8=%s (%s), video=%s (%s)', 
      sourceM3u8Exists, m3u8OutputPath, sourceVideoExists, videoOutputPath)
    
    if (!sourceM3u8Exists || !sourceVideoExists) {
      throw new Error(`Source files do not exist! m3u8=${sourceM3u8Exists}, video=${sourceVideoExists}`)
    }

    // [LOGGER] HLS output paths
    logger.info('[TRANSCODE_HLS] HLS output paths for %s', video.uuid, {
      hlsOutputPath,
      videoFilePath
    })

    // Move playlist file
    const resolutionPlaylistPath = VideoPathManager.Instance.getFSHLSOutputPath(
      video,
      getHLSResolutionPlaylistFilename(newVideoFile.filename)
    )
    logger.info('[TRANSCODE_HLS] Attempting to move playlist file')

    // Check if destination already exists
    const preMoveDestPlaylistExists = await pathExists(resolutionPlaylistPath)
    logger.info('[TRANSCODE_HLS] Destination playlist file exists before move: %s', preMoveDestPlaylistExists)

    try {
      await move(m3u8OutputPath, resolutionPlaylistPath, { overwrite: true })
      logger.info('[TRANSCODE_HLS] Playlist file moved successfully')
    } catch (err) {
      logger.error(
        '[TRANSCODE_HLS] FAILED to move playlist file from %s to %s: %s',
        m3u8OutputPath,
        resolutionPlaylistPath,
        err.message,
        { err }
      )
      throw err
    }
    // [LOGGER] Resolution playlist moved
    logger.info('[TRANSCODE_HLS] Resolution playlist moved to %s', resolutionPlaylistPath)

    // Move video file
    logger.info('[TRANSCODE_HLS] Attempting to move video file')

    // Check if destination already exists
    const preMoveDestVideoExists = await pathExists(videoFilePath)
    logger.info('[TRANSCODE_HLS] Destination video file exists before move: %s', preMoveDestVideoExists)

    try {
      await move(videoOutputPath, videoFilePath, { overwrite: true })
      logger.info('[TRANSCODE_HLS] Video file moved successfully')
    } catch (err) {
      logger.error('[TRANSCODE_HLS] FAILED to move video file from %s to %s: %s', videoOutputPath, videoFilePath, err.message, { err })
      throw err
    }
    // [LOGGER] Video file moved
    logger.info('[TRANSCODE_HLS] Video file moved to %s', videoFilePath)

    await renameVideoFileInPlaylist(resolutionPlaylistPath, newVideoFile.filename)

    // Update video duration if it was not set (in case of a live for example)
    if (!video.duration) {
      video.duration = await getVideoStreamDuration(videoFilePath)
      await video.save()
    }

    await runHLSPostTranscodingStep({
      videoUUID: video.uuid,
      step: 'create-torrent-and-infohash',
      run: () => createTorrentAndSetInfoHash(playlist, newVideoFile)
    })
    // [LOGGER] Torrent created
    logger.info('[TRANSCODE_HLS] Torrent created for video %s', video.uuid)

    const oldFile = await runHLSPostTranscodingStep({
      videoUUID: video.uuid,
      step: 'load-existing-hls-file',
      run: () => VideoFileModel.loadHLSFile({
        playlistId: playlist.id,
        fps: newVideoFile.fps,
        resolution: newVideoFile.resolution
      })
    })

    if (oldFile) {
      await runHLSPostTranscodingStep({
        videoUUID: video.uuid,
        step: 'remove-old-hls-file-from-playlist',
        run: () => video.removeStreamingPlaylistVideoFile(playlist, oldFile)
      })
      await runHLSPostTranscodingStep({
        videoUUID: video.uuid,
        step: 'destroy-old-hls-file',
        run: () => oldFile.destroy()
      })
      // [LOGGER] Old file removed
      logger.info('[TRANSCODE_HLS] Old HLS file removed for resolution %s', newVideoFile.resolution)
    }

    const savedVideoFile = await runHLSPostTranscodingStep({
      videoUUID: video.uuid,
      step: 'save-hls-video-file',
      run: () => VideoFileModel.customUpsert(newVideoFile, 'streaming-playlist', undefined)
    })
    // [LOGGER] Video file saved
    logger.info('[TRANSCODE_HLS] Video file saved with id %d', savedVideoFile.id)

    if (playlistGenerated) {
      await runHLSPostTranscodingStep({
        videoUUID: video.uuid,
        step: 'create-caption-playlists',
        run: () => createAllCaptionPlaylistsOnFSIfNeeded(video)
      })
    }

    await runHLSPostTranscodingStep({
      videoUUID: video.uuid,
      step: 'update-m3u8-and-sha-playlist',
      run: async () => {
        const updated = await updateM3U8AndShaPlaylist(video, playlist, { throwOnError: true })
        if (!updated) throw new Error(`HLS playlist ${playlist.id} was not updated for video ${video.uuid}`)
      }
    })
    // [LOGGER] M3U8 and SHA playlist updated
    logger.info('[TRANSCODE_HLS] M3U8 and SHA playlist updated for video %s', video.uuid)

    // Trigger granular move jobs for object storage
    if (CONFIG.OBJECT_STORAGE.ENABLED) {
      logger.info('[TRANSCODE_HLS] Object storage enabled, creating granular move jobs for %s', video.uuid)

      // Create HLS segment files move job
      const hlsMoveJob = await runHLSPostTranscodingStep({
        videoUUID: video.uuid,
        step: 'build-hls-object-storage-move-job',
        run: () => buildGranularHLSPlaylistMoveJob({
          videoUUID: video.uuid,
          playlistId: playlist.id,
          fileIds: [ savedVideoFile.id ],
          isNewVideo: false,
          previousVideoState: video.state
        })
      })

      if (hlsMoveJob) {
        await runHLSPostTranscodingStep({
          videoUUID: video.uuid,
          step: 'enqueue-hls-object-storage-move-job',
          run: () => createPendingMoveJobs({
            videoUUID: video.uuid,
            jobs: [ hlsMoveJob ]
          })
        })

        logger.info('[TRANSCODE_HLS] Created HLS segment move job for file %d of video %s', savedVideoFile.id, video.uuid)
      } else {
        logger.info('[TRANSCODE_HLS] Skipped HLS segment move job (already pending) for file %d of video %s', savedVideoFile.id, video.uuid)
      }
    } else {
      logger.info('[TRANSCODE_HLS] Object storage disabled, skipping move jobs for %s', video.uuid)
    }

    // [LOGGER] HLS transcoding complete
    logger.info('[TRANSCODE_HLS] HLS transcoding complete for video %s', video.uuid)

    // Verify files exist at destination after move
    const finalDestPlaylistExists = await pathExists(resolutionPlaylistPath)
    const finalDestVideoExists = await pathExists(videoFilePath)
    logger.info('[TRANSCODE_HLS] Verification - files at destination: playlist=%s, video=%s', finalDestPlaylistExists, finalDestVideoExists)

    // Check if source files still exist (they should have been moved)
    const srcPlaylistExists = await pathExists(m3u8OutputPath)
    const srcVideoExists = await pathExists(videoOutputPath)
    logger.info('[TRANSCODE_HLS] Source files still exist (should be moved): playlist=%s, video=%s', srcPlaylistExists, srcVideoExists)

    if (!finalDestPlaylistExists || !finalDestVideoExists) {
      logger.error(
        '[TRANSCODE_HLS] CRITICAL: Files NOT at destination after move! playlist=%s, video=%s',
        finalDestPlaylistExists,
        finalDestVideoExists
      )

      throw new Error(
        `HLS output files missing after move for video ${video.uuid}: ` +
          `playlist=${finalDestPlaylistExists}, video=${finalDestVideoExists}`
      )
    }

    return { resolutionPlaylistPath, videoFile: savedVideoFile }
  } finally {
    if (mutexReleaser) mutexReleaser()
  }
}

// ---------------------------------------------------------------------------
// Private
// ---------------------------------------------------------------------------

async function runHLSPostTranscodingStep <T> (options: {
  videoUUID: string
  step: string
  run: () => Promise<T>
  timeoutMs?: number
}): Promise<T> {
  const { videoUUID, step, run, timeoutMs = HLS_POST_TRANSCODING_STEP_TIMEOUT_MS } = options
  const startedAt = Date.now()

  logger.info('[TRANSCODE_HLS] Starting post-processing step %s for video %s', step, videoUUID)

  try {
    const promise = Promise.resolve().then(run)
    const result = await timeoutPromise(promise, timeoutMs) as T

    logger.info('[TRANSCODE_HLS] Finished post-processing step %s for video %s in %dms', step, videoUUID, Date.now() - startedAt)

    return result
  } catch (err) {
    const elapsedMs = Date.now() - startedAt
    const stepError = err instanceof Error
      ? err.message === 'Timeout'
        ? new Error(`HLS post-processing step ${step} timed out after ${timeoutMs}ms for video ${videoUUID}`, { cause: err })
        : err
      : new Error(`HLS post-processing step ${step} failed for video ${videoUUID}`, { cause: err })

    logger.error(
      '[TRANSCODE_HLS] Post-processing step %s failed for video %s after %dms',
      step,
      videoUUID,
      elapsedMs,
      { err: stepError, timeoutMs }
    )

    throw stepError
  }
}

async function generateHlsPlaylistCommon (options: {
  type: 'hls' | 'hls-from-ts'
  video: MVideo

  videoInputPath: string
  separatedAudioInputPath?: string

  resolution: number
  fps: number

  filesLockedInParent: boolean  // Lock already held by parent

  separatedAudio?: boolean

  isAAC?: boolean

  job?: Job
}) {
  const {
    type,
    video,
    videoInputPath,
    separatedAudioInputPath,
    resolution,
    fps,
    separatedAudio,
    isAAC,
    job,
    filesLockedInParent
  } = options

  const transcodeDirectory = CONFIG.STORAGE.TMP_DIR

  const videoTranscodedBasePath = join(transcodeDirectory, type)
  await ensureDir(videoTranscodedBasePath)
  logger.info('[TRANSCODE_HLS] Transcode directory ensured: %s', videoTranscodedBasePath)

  const videoFilename = generateHLSVideoFilename(resolution)
  const videoOutputPath = join(videoTranscodedBasePath, videoFilename)
  logger.info('[TRANSCODE_HLS] Video output path: %s', videoOutputPath)

  const resolutionPlaylistFilename = getHLSResolutionPlaylistFilename(videoFilename)
  const m3u8OutputPath = join(videoTranscodedBasePath, resolutionPlaylistFilename)
  logger.info('[TRANSCODE_HLS] M3U8 output path: %s', m3u8OutputPath)

  const transcodeOptions: HLSTranscodeOptions | HLSFromTSTranscodeOptions = {
    type,

    videoInputPath,
    separatedAudioInputPath,

    outputPath: m3u8OutputPath,

    resolution,
    fps,

    copyCodecs: !separatedAudioInputPath && await canCopyForHLS({ fps, resolution, path: videoInputPath }),

    separatedAudio,

    isAAC,

    filesLockedInParent,

    hlsPlaylist: {
      videoFilename
    }
  }

  logger.info('[TRANSCODE_HLS] About to start FFmpeg transcoding for %s', video.uuid, {
    outputPath: m3u8OutputPath,
    resolution,
    fps
  })

  await buildFFmpegVOD({ job, videoUUID: video.uuid }).transcode(transcodeOptions)

  logger.info('[TRANSCODE_HLS] FFmpeg transcoding done, now calling onHLSVideoFileTranscoding for %s', video.uuid)
  logger.info('[TRANSCODE_HLS] Input paths: videoOutputPath=%s, m3u8OutputPath=%s', videoOutputPath, m3u8OutputPath)

  try {
    await onHLSVideoFileTranscoding({
      video,
      videoOutputPath,
      m3u8OutputPath,
      filesLockedInParent
    })
  } catch (err) {
    logger.error('[TRANSCODE_HLS] Error in onHLSVideoFileTranscoding for %s: %s', video.uuid, err.message, { err })
    throw err
  }
}
