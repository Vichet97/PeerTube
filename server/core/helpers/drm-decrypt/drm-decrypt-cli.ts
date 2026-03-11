import { CONFIG } from '@server/initializers/config.js'
import { getBinPlatformFolder } from '@server/helpers/binaries/platform-binaries.js'
import { ensureDir, pathExists } from 'fs-extra/esm'
import { chmod, writeFile } from 'fs/promises'
import { OptionsOfBufferResponseBody } from 'got'
import { dirname } from 'path'
import { logger, loggerTagsFactory } from '../logger.js'
import { isBinaryResponse, unsafeSSRFGot } from '../requests.js'

const lTags = loggerTagsFactory('drm-decrypt')

function getDrmBinaryPath () {
  return CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION.BINARY_PATH
}

function getDrmAssetNameCandidates () {
  const releaseName = CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION.RELEASE.NAME
  if (releaseName !== 'mp4decrypt') return [ releaseName ]

  const platform = getBinPlatformFolder()
  const arch = process.arch
  const candidates = [ 'mp4decrypt' ]

  if (platform === 'windows') {
    candidates.unshift('mp4decrypt.exe')
    if (arch === 'arm64') {
      candidates.unshift('mp4decrypt-win-arm64.exe', 'mp4decrypt_win_arm64.exe')
    } else {
      candidates.unshift('mp4decrypt-win-x64.exe', 'mp4decrypt_win_x64.exe', 'mp4decrypt-win64.exe')
    }
  } else if (platform === 'linux-arm64') {
    candidates.unshift('mp4decrypt-linux-arm64', 'mp4decrypt_linux_arm64')
  } else if (platform === 'linux-amd64') {
    candidates.unshift('mp4decrypt-linux-x64', 'mp4decrypt_linux_x64')
  } else {
    candidates.unshift('mp4decrypt-macos', 'mp4decrypt_osx', 'mp4decrypt-darwin')
  }

  return candidates
}

export class DrmDecryptCLI {
  /**
   * Ensure the DRM decryption binary exists. If using release URL, download on first use.
   */
  static async safeGetBinaryPath (): Promise<string> {
    const releaseUrl = CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION.RELEASE.URL
    if (!releaseUrl) {
      return CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION.BINARY_PATH
    }

    const binaryPath = getDrmBinaryPath()
    if (!await pathExists(binaryPath)) {
      await ensureDir(dirname(binaryPath))
      await this.updateDrmBinary()
    }

    return binaryPath
  }

  static async updateDrmBinary () {
    const url = CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION.RELEASE.URL
    if (!url) throw new Error('DRM release URL is not configured')

    logger.info('Updating DRM decryption binary from %s.', url, lTags())

    const gotOptions: OptionsOfBufferResponseBody = {
      context: { bodyKBLimit: 100_000 },
      responseType: 'buffer' as 'buffer'
    }

    if (process.env.DRM_DECRYPT_DOWNLOAD_BEARER_TOKEN) {
      gotOptions.headers = {
        authorization: 'Bearer ' + process.env.DRM_DECRYPT_DOWNLOAD_BEARER_TOKEN
      }
    }

    try {
      let gotResult = await unsafeSSRFGot(url, gotOptions)

      if (!isBinaryResponse(gotResult)) {
        const json = JSON.parse(gotResult.body.toString())
        const latest = json.filter((release: { prerelease?: boolean }) => release.prerelease === false)[0]
        if (!latest) throw new Error('Cannot find latest release')

        const candidates = getDrmAssetNameCandidates()
        const releaseAsset = latest.assets.find((a: { name: string }) => candidates.includes(a.name)) ??
          latest.assets.find((a: { name: string }) => candidates.some(candidate => a.name.includes(candidate)))
        if (!releaseAsset) throw new Error(`Cannot find appropriate release with names: ${candidates.join(', ')}`)

        gotResult = await unsafeSSRFGot(releaseAsset.browser_download_url, gotOptions)
      }

      if (!isBinaryResponse(gotResult)) {
        throw new Error('Not a binary response')
      }

      const binaryPath = getDrmBinaryPath()
      await writeFile(binaryPath, gotResult.body)
      await chmod(binaryPath, 0o744)

      logger.info('DRM decryption binary updated %s.', binaryPath, lTags())
    } catch (err) {
      logger.error('Cannot update DRM decryption binary from %s.', url, { err, ...lTags() })
      throw err
    }
  }
}
