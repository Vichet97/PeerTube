import {
  buildPlatformBinaryPath,
  getBinPlatformFolder,
  resolvePlatformBinaryPathWithLegacyFallback
} from '@server/helpers/binaries/platform-binaries.js'
import { CONFIG } from '@server/initializers/config.js'
import { ensureDir, pathExists } from 'fs-extra/esm'
import { chmod, writeFile } from 'fs/promises'
import ffmpeg from 'fluent-ffmpeg'
import { OptionsOfBufferResponseBody } from 'got'
import { dirname } from 'path'
import { logger, loggerTagsFactory } from '../logger.js'
import { isBinaryResponse, unsafeSSRFGot } from '../requests.js'

const lTags = loggerTagsFactory('ffmpeg-cli')
const DEFAULT_RELEASE_URL = 'https://api.github.com/repos/descriptinc/ffmpeg-ffprobe-static/releases/latest'

function getAssetPlatformTag () {
  const platform = getBinPlatformFolder()
  if (platform === 'windows') return 'win32-x64'
  if (platform === 'linux-amd64') return 'linux-x64'
  if (platform === 'linux-arm64') return 'linux-arm64'

  return process.arch === 'arm64'
    ? 'darwin-arm64'
    : 'darwin-x64'
}

function getFFmpegBinaryName () {
  return process.platform === 'win32'
    ? 'ffmpeg.exe'
    : 'ffmpeg'
}

function getFFprobeBinaryName () {
  return process.platform === 'win32'
    ? 'ffprobe.exe'
    : 'ffprobe'
}

function getFFmpegBinaryPath () {
  return resolvePlatformBinaryPathWithLegacyFallback(CONFIG.STORAGE.BIN_DIR, getFFmpegBinaryName(), [ 'ffmpeg', 'ffmpeg.exe' ])
}

function getFFprobeBinaryPath () {
  return resolvePlatformBinaryPathWithLegacyFallback(CONFIG.STORAGE.BIN_DIR, getFFprobeBinaryName(), [ 'ffprobe', 'ffprobe.exe' ])
}

function getFFmpegDownloadPath () {
  return buildPlatformBinaryPath(CONFIG.STORAGE.BIN_DIR, getFFmpegBinaryName())
}

function getFFprobeDownloadPath () {
  return buildPlatformBinaryPath(CONFIG.STORAGE.BIN_DIR, getFFprobeBinaryName())
}

async function fetchReleaseAssets () {
  const gotOptions: OptionsOfBufferResponseBody = {
    context: { bodyKBLimit: 1_000_000 },
    responseType: 'buffer' as const
  }

  const url = process.env.FFMPEG_RELEASE_URL || DEFAULT_RELEASE_URL
  const response = await unsafeSSRFGot(url, gotOptions)
  if (isBinaryResponse(response)) {
    throw new Error(`Expected JSON response from ${url}, got binary`)
  }

  const json = JSON.parse(response.body.toString())
  const latest = Array.isArray(json)
    ? json.find((release: { prerelease?: boolean }) => release.prerelease === false) ?? json[0]
    : json

  if (!latest?.assets) throw new Error(`Cannot parse ffmpeg release assets from ${url}`)

  return latest.assets as { name: string, browser_download_url: string }[]
}

async function downloadBinary (assetName: string, targetPath: string) {
  const assets = await fetchReleaseAssets()
  const asset = assets.find(a => a.name === assetName)
  if (!asset) throw new Error(`Cannot find ffmpeg asset ${assetName}`)

  const gotOptions: OptionsOfBufferResponseBody = {
    context: { bodyKBLimit: 1_000_000 },
    responseType: 'buffer' as const
  }

  const gotResult = await unsafeSSRFGot(asset.browser_download_url, gotOptions)
  if (!isBinaryResponse(gotResult)) {
    throw new Error(`Expected binary response when downloading ${assetName}`)
  }

  await ensureDir(dirname(targetPath))
  await writeFile(targetPath, gotResult.body)
  if (process.platform !== 'win32') await chmod(targetPath, 0o744)
}

export async function ensureFFmpegBinaries () {
  const platformTag = getAssetPlatformTag()
  const ffmpegPath = getFFmpegBinaryPath()
  const ffprobePath = getFFprobeBinaryPath()

  if (!await pathExists(ffmpegPath)) {
    const downloadPath = getFFmpegDownloadPath()
    logger.info('Downloading ffmpeg binary for %s to %s', platformTag, downloadPath, lTags())
    await downloadBinary(`ffmpeg-${platformTag}`, downloadPath)
  }

  if (!await pathExists(ffprobePath)) {
    const downloadPath = getFFprobeDownloadPath()
    logger.info('Downloading ffprobe binary for %s to %s', platformTag, downloadPath, lTags())
    await downloadBinary(`ffprobe-${platformTag}`, downloadPath)
  }

  const resolvedFFmpegPath = getFFmpegBinaryPath()
  const resolvedFFprobePath = getFFprobeBinaryPath()

  ffmpeg.setFfmpegPath(resolvedFFmpegPath)
  ffmpeg.setFfprobePath(resolvedFFprobePath)
  process.env.FFMPEG_PATH = resolvedFFmpegPath
  process.env.FFPROBE_PATH = resolvedFFprobePath
}
