import { CONFIG } from '@server/initializers/config.js'
import { getBinPlatformFolder, getPlatformExecutableName } from '@server/helpers/binaries/platform-binaries.js'
import { execa } from 'execa'
import { ensureDir, pathExists, remove } from 'fs-extra/esm'
import { writeFile, readdir, chmod, readFile } from 'fs/promises'
import { OptionsOfBufferResponseBody } from 'got'
import { basename, dirname, join } from 'path'
import { logger, loggerTagsFactory } from '../logger.js'
import { isBinaryResponse, unsafeSSRFGot } from '../requests.js'
import { unzip } from '../unzip.js'

const lTags = loggerTagsFactory('n-m3u8dl-re-cli')

function getNm3u8BinaryPath () {
  return CONFIG.IMPORT.VIDEOS.HTTP.N_M3U8DL_RE.BINARY_PATH
}

function getLatestStableRelease (json: any[]) {
  return json.find(release => release.prerelease === false) ?? json[0]
}

function getAssetNeedle () {
  const platform = getBinPlatformFolder()

  if (platform === 'windows') {
    return process.arch === 'arm64'
      ? 'win-arm64'
      : 'win-x64'
  }

  if (platform === 'linux-arm64') return 'linux-arm64'
  if (platform === 'linux-amd64') return 'linux-x64'

  return process.arch === 'arm64'
    ? 'osx-arm64'
    : 'osx-x64'
}

function getArchiveNameSuffixes () {
  return process.platform === 'win32'
    ? [ '.zip' ]
    : [ '.tar.gz' ]
}

async function findBinaryInDirectory (directory: string, names: string[]): Promise<string | undefined> {
  const entries = await readdir(directory, { withFileTypes: true })

  for (const entry of entries) {
    const path = join(directory, entry.name)

    if (entry.isDirectory()) {
      const nested = await findBinaryInDirectory(path, names)
      if (nested) return nested
      continue
    }

    if (names.some(name => entry.name === name)) return path
  }

  return undefined
}

async function extractArchive (archivePath: string, destination: string) {
  if (archivePath.endsWith('.zip')) {
    await unzip({
      source: archivePath,
      destination,
      maxSize: 512 * 1024 * 1024,
      maxFiles: 10_000
    })
    return
  }

  await execa('tar', [ '-xzf', archivePath, '-C', destination ])
}

export class NM3U8DLRECLI {
  static async safeGetBinaryPath () {
    const binaryPath = getNm3u8BinaryPath()
    if (!await pathExists(binaryPath)) {
      await ensureDir(dirname(binaryPath))
      await this.updateBinary()
    }

    return binaryPath
  }

  static async updateBinary () {
    const url = CONFIG.IMPORT.VIDEOS.HTTP.N_M3U8DL_RE.RELEASE.URL
    if (!url) {
      throw new Error('N_m3u8DL-RE release URL is not configured')
    }

    logger.info('Updating N_m3u8DL-RE binary from %s.', url, lTags())

    const gotOptions: OptionsOfBufferResponseBody = {
      context: { bodyKBLimit: 100_000 },
      responseType: 'buffer' as 'buffer'
    }

    let gotResult = await unsafeSSRFGot(url, gotOptions)

    if (!isBinaryResponse(gotResult)) {
      const json = JSON.parse(gotResult.body.toString())
      const latest = getLatestStableRelease(json)
      if (!latest) throw new Error('Cannot find latest release for N_m3u8DL-RE')

      const needle = getAssetNeedle()
      const suffixes = getArchiveNameSuffixes()
      const releaseAsset = latest.assets.find((asset: { name: string }) => {
        return asset.name.includes(needle) && suffixes.some(suffix => asset.name.endsWith(suffix))
      })

      if (!releaseAsset) {
        throw new Error(`Cannot find N_m3u8DL-RE release asset for ${needle}`)
      }

      gotResult = await unsafeSSRFGot(releaseAsset.browser_download_url, gotOptions)

      if (!isBinaryResponse(gotResult)) throw new Error('Not a binary response')

      const tempDirectory = join(CONFIG.STORAGE.TMP_PERSISTENT_DIR, 'n-m3u8dl-re')
      const archivePath = join(tempDirectory, releaseAsset.name)
      const extractDirectory = join(tempDirectory, basename(releaseAsset.name, '.tar.gz'))
      await ensureDir(extractDirectory)
      await writeFile(archivePath, gotResult.body)

      try {
        await extractArchive(archivePath, extractDirectory)

        const expectedBinaryNames = [ getPlatformExecutableName('N_m3u8DL-RE'), 'N_m3u8DL-RE', 'N_m3u8DL-RE.exe' ]
        const extractedBinaryPath = await findBinaryInDirectory(extractDirectory, expectedBinaryNames)
        if (!extractedBinaryPath) throw new Error('Cannot find N_m3u8DL-RE binary in extracted archive')

        const binaryPath = getNm3u8BinaryPath()
        await ensureDir(dirname(binaryPath))
        await writeFile(binaryPath, await readFile(extractedBinaryPath))
        await chmod(binaryPath, 755)
      } finally {
        await remove(tempDirectory)
      }

      logger.info('N_m3u8DL-RE updated %s.', getNm3u8BinaryPath(), lTags())
      return
    }

    const binaryPath = getNm3u8BinaryPath()
    await ensureDir(dirname(binaryPath))
    await writeFile(binaryPath, gotResult.body)
    await chmod(binaryPath, 755)
    logger.info('N_m3u8DL-RE updated %s.', binaryPath, lTags())
  }
}
