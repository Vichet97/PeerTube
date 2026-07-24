import { VideoFileStream } from '@peertube/peertube-models'
import { logger, loggerTagsFactory } from '@server/helpers/logger.js'
import { CONFIG } from '@server/initializers/config.js'
import { VideoSourceModel } from '@server/models/video/video-source.js'
import { MVideoFileStreamingPlaylistVideo, MVideoFileVideo, MVideoUUID } from '@server/types/models/index.js'
import { ensureDir, pathExists, remove } from 'fs-extra/esm'
import { dirname, join } from 'path'
import { execa } from 'execa'
import { removeOriginalFileObjectStorageByFilename, storeOriginalVideoFile } from './object-storage/index.js'
import { VideoPathManager } from './video-path-manager.js'

const lTags = loggerTagsFactory('transcription-audio-staging')

function getTranscriptionAudioFilename (videoUUID: string) {
  return join('transcription-audio', `${videoUUID}.wav`)
}

function getTranscriptionAudioLocalPath (videoUUID: string) {
  return join(CONFIG.STORAGE.TMP_DIR, getTranscriptionAudioFilename(videoUUID))
}

type VideoWithAudioCandidate = MVideoUUID & {
  id: number
  getMaxQualityFile: (stream: number) => MVideoFileVideo | MVideoFileStreamingPlaylistVideo | undefined
}

async function extractAudioForTranscription (inputPath: string, outputPath: string, videoUUID: string) {
  const ffmpegPath = process.env.FFMPEG_PATH || 'ffmpeg'
  const timeout = Math.max(CONFIG.VIDEO_TRANSCRIPTION.TIMEOUT, 60_000)

  await ensureDir(dirname(outputPath))

  await execa(ffmpegPath, [
    '-y',
    '-i',
    inputPath,
    '-vn',
    '-ac',
    '1',
    '-ar',
    '16000',
    outputPath
  ], { timeout })

  if (!await pathExists(outputPath)) {
    throw new Error(`Cannot find staged transcription audio output ${outputPath} for ${videoUUID}`)
  }
}

export async function prepareStagedTranscriptionAudio (video: VideoWithAudioCandidate) {
  const outputPath = getTranscriptionAudioLocalPath(video.uuid)

  await cleanupStagedTranscriptionAudio(video.uuid)

  const source = await VideoSourceModel.loadLatest(video.id)

  if (source?.keptOriginalFilename) {
    await VideoPathManager.Instance.makeAvailableVideoSource(source, async inputPath => {
      await extractAudioForTranscription(inputPath, outputPath, video.uuid)
    }, video.uuid)
  } else {
    const maxQualityFile = video.getMaxQualityFile(VideoFileStream.AUDIO) || video.getMaxQualityFile(VideoFileStream.VIDEO)
    if (!maxQualityFile) return undefined

    await VideoPathManager.Instance.makeAvailableVideoFile(maxQualityFile, async inputPath => {
      await extractAudioForTranscription(inputPath, outputPath, video.uuid)
    })
  }

  if (await pathExists(outputPath) !== true) {
    logger.info(`Could not prepare staged transcription audio for ${video.uuid}: no suitable source`, lTags(video.uuid))
    return undefined
  }

  if (CONFIG.OBJECT_STORAGE.ENABLED === true) {
    await storeOriginalVideoFile(outputPath, getTranscriptionAudioFilename(video.uuid))
  }

  return outputPath
}

export async function cleanupStagedTranscriptionAudio (videoUUID: string) {
  const localPath = getTranscriptionAudioLocalPath(videoUUID)
  await remove(localPath).catch(() => {})

  if (CONFIG.OBJECT_STORAGE.ENABLED === true) {
    await removeOriginalFileObjectStorageByFilename(getTranscriptionAudioFilename(videoUUID))
      .catch(() => {})
  }
}
