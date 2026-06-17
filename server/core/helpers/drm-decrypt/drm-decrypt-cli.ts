import { CONFIG } from '@server/initializers/config.js'
import { getBinPlatformFolder } from '@server/helpers/binaries/platform-binaries.js'
import { ensureDir, pathExists, remove } from 'fs-extra/esm'
import { chmod, readdir, readFile, writeFile } from 'fs/promises'
import { OptionsOfBufferResponseBody } from 'got'
import { basename, dirname, join } from 'path'
import { logger, loggerTagsFactory } from '../logger.js'
import { isBinaryResponse, unsafeSSRFGot } from '../requests.js'
import { unzip } from '../unzip.js'
import { execa } from 'execa'

const lTags = loggerTagsFactory('drm-decrypt')
const DEFAULT_BENTO4_DOWNLOADS_URL = 'https://www.bento4.com/downloads/'

function getDrmBinaryPath () {
  return CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION.BINARY_PATH
}

function getDrmReleaseUrl () {
  const configuredUrl = CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION.RELEASE.URL
  if (configuredUrl) return configuredUrl

  if (CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION.RELEASE.NAME !== 'mp4decrypt') return null
  if (!isManagedDrmBinaryPath(getDrmBinaryPath())) return null
  if (!getBento4ArchivePattern()) return null

  return DEFAULT_BENTO4_DOWNLOADS_URL
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

function getBento4ArchivePattern (platform = getBinPlatformFolder()) {
  if (platform === 'windows') return /https:\/\/www\.bok\.net\/Bento4\/binaries\/Bento4-SDK-[^"'\\s]+\.x86_64-microsoft-win32\.zip/i
  if (platform === 'linux-amd64') return /https:\/\/www\.bok\.net\/Bento4\/binaries\/Bento4-SDK-[^"'\\s]+\.x86_64-unknown-linux\.zip/i
  if (platform === 'macos') return /https:\/\/www\.bok\.net\/Bento4\/binaries\/Bento4-SDK-[^"'\\s]+\.universal-apple-macosx\.zip/i

  return null
}

function isManagedDrmBinaryPath (binaryPath: string) {
  const managedPrefix = CONFIG.STORAGE.BIN_DIR.replace(/\\/g, '/').replace(/\/+$/, '').toLowerCase()
  const normalizedPath = binaryPath.replace(/\\/g, '/').toLowerCase()
  return normalizedPath === managedPrefix || normalizedPath.startsWith(managedPrefix + '/')
}

function isArchiveAssetName (name: string) {
  return name.endsWith('.zip') || name.endsWith('.tar.gz') || name.endsWith('.tar.xz') || name.endsWith('.tar.bz2')
}

async function extractArchive (archivePath: string, destination: string) {
  if (archivePath.endsWith('.zip')) {
    await unzip({
      source: archivePath,
      destination,
      maxSize: 1024 * 1024 * 1024,
      maxFiles: 20_000
    })
    return
  }

  await execa('tar', [ '-xf', archivePath, '-C', destination ])
}

async function findBinaryInDirectory (directory: string, binaryNames: string[]): Promise<string | undefined> {
  const entries = await readdir(directory, { withFileTypes: true })

  for (const entry of entries) {
    const entryPath = join(directory, entry.name)

    if (entry.isDirectory()) {
      const nested = await findBinaryInDirectory(entryPath, binaryNames)
      if (nested) return nested
      continue
    }

    if (binaryNames.includes(entry.name)) return entryPath
  }

  return undefined
}

export function findBento4ArchiveUrlInHtml (html: string, platform = getBinPlatformFolder()) {
  const pattern = getBento4ArchivePattern(platform)
  if (!pattern) return null

  return html.match(pattern)?.[0] ?? null
}

export class DrmDecryptCLI {
  /**
   * Ensure the DRM decryption binary exists. If using release URL, download on first use.
   */
  static async safeGetBinaryPath (): Promise<string> {
    const releaseUrl = getDrmReleaseUrl()
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
    const url = getDrmReleaseUrl()
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
      let archiveAssetName: string | undefined

      if (!isBinaryResponse(gotResult)) {
        const textBody = gotResult.body.toString()

        try {
          const json = JSON.parse(textBody)
          const latest = json.filter((release: { prerelease?: boolean }) => release.prerelease === false)[0]
          if (!latest) throw new Error('Cannot find latest release')

          const candidates = getDrmAssetNameCandidates()
          const releaseAsset = latest.assets.find((a: { name: string }) => candidates.includes(a.name)) ??
            latest.assets.find((a: { name: string }) => candidates.some(candidate => a.name.includes(candidate)))
          if (!releaseAsset) throw new Error(`Cannot find appropriate release with names: ${candidates.join(', ')}`)

          archiveAssetName = releaseAsset.name
          gotResult = await unsafeSSRFGot(releaseAsset.browser_download_url, gotOptions)
        } catch (parseErr) {
          const archiveUrl = findBento4ArchiveUrlInHtml(textBody)
          if (!archiveUrl) {
            throw new Error('Cannot find Bento4 archive URL for the current platform', { cause: parseErr })
          }

          archiveAssetName = basename(archiveUrl)
          gotResult = await unsafeSSRFGot(archiveUrl, gotOptions)
        }
      }

      if (!isBinaryResponse(gotResult)) {
        throw new Error('Not a binary response')
      }

      const binaryPath = getDrmBinaryPath()
      if (archiveAssetName && isArchiveAssetName(archiveAssetName)) {
        const tempDirectory = join(CONFIG.STORAGE.TMP_PERSISTENT_DIR, 'drm-decrypt')
        const archivePath = join(tempDirectory, archiveAssetName)
        const extractDirectory = join(tempDirectory, archiveAssetName.replace(/(\.tar\.gz|\.tar\.xz|\.tar\.bz2|\.zip)$/i, ''))

        await ensureDir(extractDirectory)
        await writeFile(archivePath, gotResult.body)

        try {
          await extractArchive(archivePath, extractDirectory)

          const extractedBinaryPath = await findBinaryInDirectory(extractDirectory, getDrmAssetNameCandidates())
          if (!extractedBinaryPath) throw new Error('Cannot find DRM decryption binary in extracted archive')

          await ensureDir(dirname(binaryPath))
          await writeFile(binaryPath, await readFile(extractedBinaryPath))
          await chmod(binaryPath, 0o755)
        } finally {
          await remove(tempDirectory)
        }
      } else {
        await ensureDir(dirname(binaryPath))
        await writeFile(binaryPath, gotResult.body)
        await chmod(binaryPath, 0o755)
      }

      logger.info('DRM decryption binary updated %s.', binaryPath, lTags())
    } catch (err) {
      logger.error('Cannot update DRM decryption binary from %s.', url, { err, ...lTags() })
      throw err
    }
  }
}
