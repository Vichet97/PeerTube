import { randomInt } from '@peertube/peertube-core-utils'
import { VideoResolution, VideoResolutionType } from '@peertube/peertube-models'
import { CONFIG } from '@server/initializers/config.js'
import {
  buildPlatformBinaryPath,
  getBinPlatformFolder,
  getPlatformExecutableName,
  resolvePlatformBinaryPathWithLegacyFallback
} from '@server/helpers/binaries/platform-binaries.js'
import { execa, Options as ExecaNodeOptions } from 'execa'
import { ensureDir, pathExists, remove } from 'fs-extra/esm'
import { chmod, readdir, readFile, writeFile } from 'fs/promises'
import { OptionsOfBufferResponseBody } from 'got'
import { basename, dirname, join } from 'path'
import { logger, loggerTagsFactory } from '../logger.js'
import { getProxy, isProxyEnabled } from '../proxy.js'
import { isBinaryResponse, unsafeSSRFGot } from '../requests.js'
import { unzip } from '../unzip.js'

type ProcessOptions = Pick<ExecaNodeOptions, 'cwd' | 'maxBuffer'>

const lTags = loggerTagsFactory('youtube-dl')

export function customHeadersToYoutubeDLArgs (customHeaders: Record<string, string> | undefined): string[] {
  if (!customHeaders || typeof customHeaders !== 'object') return []

  return Object.entries(customHeaders).flatMap(([ key, value ]) => {
    if (typeof key !== 'string' || typeof value !== 'string') return []
    return [ '--add-header', `${key}: ${value}` ]
  })
}

function getYoutubeDLAssetName () {
  const releaseName = CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE.NAME
  if (releaseName !== 'yt-dlp') return releaseName

  const platform = getBinPlatformFolder()
  if (platform === 'windows') return process.arch === 'arm64' ? 'yt-dlp_arm64.exe' : 'yt-dlp.exe'
  if (platform === 'linux-arm64') return 'yt-dlp_linux_aarch64'
  if (platform === 'linux-amd64') return 'yt-dlp_linux'

  return 'yt-dlp_macos'
}

function getYoutubeDLBinaryName () {
  const releaseName = CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE.NAME
  if (releaseName !== 'yt-dlp') return releaseName

  return process.platform === 'win32'
    ? 'yt-dlp.exe'
    : 'yt-dlp'
}

function getYoutubeDLBinaryPath () {
  const binaryName = getYoutubeDLBinaryName()

  return resolvePlatformBinaryPathWithLegacyFallback(CONFIG.STORAGE.BIN_DIR, binaryName, [
    CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE.NAME
  ])
}

function getYoutubeDLDownloadPath () {
  return buildPlatformBinaryPath(CONFIG.STORAGE.BIN_DIR, getYoutubeDLBinaryName())
}

function getAria2cManagedBinaryPath () {
  const binaryName = getPlatformExecutableName('aria2c')
  return resolvePlatformBinaryPathWithLegacyFallback(CONFIG.STORAGE.BIN_DIR, binaryName, [ 'aria2c', 'aria2c.exe' ])
}

function getAria2cAssetNeedles () {
  const platform = getBinPlatformFolder()
  if (platform === 'windows') return [ 'win-64bit' ]
  if (platform === 'linux-amd64') return [ 'linux-gnu-64bit', 'linux-musl-64bit' ]
  if (platform === 'linux-arm64') return [ 'linux-aarch64', 'linux-arm64' ]

  // macOS: aria2 official releases don't include macOS builds
  // Fall back to linux-amd64 (may work via Rosetta 2) or skip aria2
  if (platform === 'macos') return [ 'linux-amd64' ]

  return [ 'linux-gnu-64bit' ]
}

function isMacOSWithNoOfficialAria2Build () {
  return getBinPlatformFolder() === 'macos'
}

function getAria2cArchiveSuffixes () {
  return [ '.zip', '.tar.gz', '.tar.bz2', '.tar.xz' ]
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

async function updateAria2cBinary () {
  const releaseUrl = CONFIG.IMPORT.VIDEOS.HTTP.YT_DLP.ARIA2C.RELEASE.URL
  logger.info('Updating aria2c binary from %s.', releaseUrl, lTags())

  const gotOptions: OptionsOfBufferResponseBody = {
    context: { bodyKBLimit: 100_000 },
    responseType: 'buffer' as 'buffer'
  }

  let gotResult = await unsafeSSRFGot(releaseUrl, gotOptions)
  if (!isBinaryResponse(gotResult)) {
    const json = JSON.parse(gotResult.body.toString())
    const latest = json.find(release => release.prerelease === false) ?? json[0]
    if (!latest) throw new Error('Cannot find latest release for aria2')

    const needles = getAria2cAssetNeedles()
    const suffixes = getAria2cArchiveSuffixes()
    const releaseAsset = latest.assets.find((asset: { name: string }) => {
      return needles.some(needle => asset.name.includes(needle)) &&
        suffixes.some(suffix => asset.name.endsWith(suffix))
    })
    if (!releaseAsset) throw new Error(`Cannot find aria2 release asset for ${needles.join(', ')}`)

    gotResult = await unsafeSSRFGot(releaseAsset.browser_download_url, gotOptions)
    if (!isBinaryResponse(gotResult)) throw new Error('Not a binary response')

    const tempDirectory = join(CONFIG.STORAGE.TMP_PERSISTENT_DIR, 'aria2c')
    const archivePath = join(tempDirectory, releaseAsset.name)
    const extractDirectory = join(tempDirectory, basename(releaseAsset.name, '.tar.gz'))

    await ensureDir(extractDirectory)
    await writeFile(archivePath, gotResult.body)

    try {
      await extractArchive(archivePath, extractDirectory)

      const expectedBinaryNames = [ getPlatformExecutableName('aria2c'), 'aria2c', 'aria2c.exe' ]
      const extractedBinaryPath = await findBinaryInDirectory(extractDirectory, expectedBinaryNames)
      if (!extractedBinaryPath) throw new Error('Cannot find aria2c binary in extracted archive')

      const managedBinaryPath = getAria2cManagedBinaryPath()
      await ensureDir(dirname(managedBinaryPath))
      await writeFile(managedBinaryPath, await readFile(extractedBinaryPath))
      await chmod(managedBinaryPath, 0o755)
    } finally {
      await remove(tempDirectory)
    }

    logger.info('aria2c updated %s.', getAria2cManagedBinaryPath(), lTags())
    return
  }

  const managedBinaryPath = getAria2cManagedBinaryPath()
  await ensureDir(dirname(managedBinaryPath))
  await writeFile(managedBinaryPath, gotResult.body)
  await chmod(managedBinaryPath, 0o755)
  logger.info('aria2c updated %s.', managedBinaryPath, lTags())
}

export class YoutubeDLCLI {
  static async safeGet () {
    const youtubeDLBinaryPath = getYoutubeDLBinaryPath()

    if (!await pathExists(youtubeDLBinaryPath)) {
      await ensureDir(dirname(youtubeDLBinaryPath))

      await this.updateYoutubeDLBinary()
    }

    return new YoutubeDLCLI()
  }

  static async updateYoutubeDLBinary () {
    const url = CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE.URL

    logger.info('Updating youtubeDL binary from %s.', url, lTags())

    const gotOptions: OptionsOfBufferResponseBody = {
      context: { bodyKBLimit: 100_000 },
      responseType: 'buffer' as 'buffer'
    }

    if (process.env.YOUTUBE_DL_DOWNLOAD_BEARER_TOKEN) {
      gotOptions.headers = {
        authorization: 'Bearer ' + process.env.YOUTUBE_DL_DOWNLOAD_BEARER_TOKEN
      }
    }

    try {
      let gotResult = await unsafeSSRFGot(url, gotOptions)

      if (!isBinaryResponse(gotResult)) {
        const json = JSON.parse(gotResult.body.toString())
        const latest = json.filter(release => release.prerelease === false)[0]
        if (!latest) throw new Error('Cannot find latest release')

        const releaseAssetName = getYoutubeDLAssetName()
        const releaseAsset = latest.assets.find(a => a.name === releaseAssetName)
        if (!releaseAsset) throw new Error(`Cannot find appropriate release with name ${releaseAssetName} in release assets`)

        gotResult = await unsafeSSRFGot(releaseAsset.browser_download_url, gotOptions)
      }

      if (!isBinaryResponse(gotResult)) {
        throw new Error('Not a binary response')
      }

      const youtubeDLBinaryPath = getYoutubeDLDownloadPath()
      await ensureDir(dirname(youtubeDLBinaryPath))
      await writeFile(youtubeDLBinaryPath, gotResult.body)
      await chmod(youtubeDLBinaryPath, 0o755)

      logger.info('youtube-dl updated %s.', youtubeDLBinaryPath, lTags())
    } catch (err) {
      logger.error('Cannot update youtube-dl from %s.', url, { err, ...lTags() })
    }
  }

  static getYoutubeDLVideoFormat (enabledResolutions: VideoResolutionType[], useBestFormat: boolean) {
    /**
     * list of format selectors in order or preference
     * see https://github.com/ytdl-org/youtube-dl#format-selection
     *
     * case #1 asks for a mp4 using h264 (avc1) and the exact resolution in the hope
     * of being able to do a "quick-transcode"
     * case #2 is the first fallback. No "quick-transcode" means we can get anything else (like vp9)
     * case #3 is the resolution-degraded equivalent of #1, and already a pretty safe fallback
     *
     * in any case we avoid AV1, see https://github.com/Chocobozzz/PeerTube/issues/3499
     */

    let result: string[] = []

    if (!useBestFormat) {
      const resolution = enabledResolutions.length === 0
        ? VideoResolution.H_720P
        : Math.max(...enabledResolutions)

      result = [
        `bestvideo[vcodec^=avc1][height=${resolution}]+bestaudio[ext=m4a]`, // case #1
        `bestvideo[vcodec!*=av01][vcodec!*=vp9.2][height=${resolution}]+bestaudio`, // case #2
        `bestvideo[vcodec^=avc1][height<=${resolution}]+bestaudio[ext=m4a]` // case #
      ]
    }

    return result.concat([
      'bestvideo[vcodec!*=av01][vcodec!*=vp9.2]+bestaudio',
      'best[vcodec!*=av01][vcodec!*=vp9.2]', // case fallback for known formats
      'bestvideo[ext=mp4]+bestaudio[ext=m4a]',
      'best' // Ultimate fallback
    ]).join('/')
  }

  private constructor () {
  }

  download (options: {
    url: string
    format: string
    output: string
    processOptions: ProcessOptions
    timeout?: number
    additionalYoutubeDLArgs?: string[]
    onProgress?: (percent: number) => void
  }) {
    let args = options.additionalYoutubeDLArgs || []
    args = args.concat([ '--merge-output-format', 'mp4', '-f', options.format, '-o', options.output ])

    if (options.onProgress && CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE.NAME === 'yt-dlp') {
      args = [ '--newline' ].concat(args)
      return this.runWithProgress({
        url: options.url,
        processOptions: options.processOptions,
        timeout: options.timeout,
        args,
        onProgress: options.onProgress
      })
    }

    return this.run({
      url: options.url,
      processOptions: options.processOptions,
      timeout: options.timeout,
      args
    })
  }

  async getInfo (options: {
    url: string
    format: string
    processOptions: ProcessOptions
    timeout?: number
    additionalYoutubeDLArgs?: string[]
  }) {
    const { url, format, additionalYoutubeDLArgs = [], processOptions, timeout } = options

    const completeArgs = additionalYoutubeDLArgs.concat([ '--dump-json', '-f', format ])

    const data = await this.run({ url, args: completeArgs, processOptions, timeout })
    if (!data) return undefined

    const info = data.map(d => JSON.parse(d))

    return info.length === 1
      ? info[0]
      : info
  }

  async getListInfo (options: {
    url: string
    latestVideosCount?: number
    processOptions: ProcessOptions
  }): Promise<{ upload_date: string, webpage_url: string }[]> {
    const additionalYoutubeDLArgs = [ '--skip-download', '--playlist-reverse' ]

    if (CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE.NAME === 'yt-dlp') {
      // Optimize listing videos only when using yt-dlp because it is bugged with youtube-dl when fetching a channel
      additionalYoutubeDLArgs.push('--flat-playlist')
    }

    if (options.latestVideosCount !== undefined) {
      additionalYoutubeDLArgs.push('--playlist-end', options.latestVideosCount.toString())
    }

    const result = await this.getInfo({
      url: options.url,
      format: YoutubeDLCLI.getYoutubeDLVideoFormat([], false),
      processOptions: options.processOptions,
      additionalYoutubeDLArgs
    })

    if (!result) return result
    if (!Array.isArray(result)) return [ result ]

    return result
  }

  async getSubs (options: {
    url: string
    format: 'vtt'
    processOptions: ProcessOptions
  }) {
    const { url, format, processOptions } = options

    const args = [ '--skip-download', '--all-subs', `--sub-format=${format}` ]

    const data = await this.run({ url, args, processOptions })
    const files: string[] = []

    const skipString = '[info] Writing video subtitles to: '

    for (let i = 0, len = data.length; i < len; i++) {
      const line = data[i]

      if (line.startsWith(skipString)) {
        files.push(line.slice(skipString.length))
      }
    }

    return files
  }

  private async run (options: {
    url: string
    args: string[]
    timeout?: number
    processOptions: ProcessOptions
  }) {
    const { url, args, timeout, processOptions } = options

    const completeArgs = await this.buildYoutubeDLArgs({ args, withAria2c: true })

    try {
      return await this.runCommand({ url, completeArgs, processOptions, timeout })
    } catch (err) {
      if (!this.shouldRetryWithoutAria2c({ err, completeArgs })) throw err

      return this.retryWithoutAria2c({
        url,
        args,
        runner: completeArgs => this.runCommand({ url, completeArgs, processOptions, timeout })
      })
    }
  }

  private async runCommand (options: {
    url: string
    completeArgs: string[]
    timeout?: number
    processOptions: ProcessOptions
  }) {
    const { url, completeArgs, timeout, processOptions } = options

    const subProcess = this.spawnYoutubeDLProcess({ url, completeArgs, processOptions })

    if (timeout) {
      setTimeout(() => subProcess.kill(), timeout)
    }

    const output = await subProcess

    logger.debug('Run youtube-dl command.', { command: output.command, ...lTags() })

    return output.stdout
      ? output.stdout.trim().split(/\r?\n/)
      : undefined
  }

  private async runWithProgress (options: {
    url: string
    args: string[]
    timeout?: number
    processOptions: ProcessOptions
    onProgress: (percent: number) => void
  }) {
    const { url, args, timeout, processOptions, onProgress } = options

    const completeArgs = await this.buildYoutubeDLArgs({ args, withAria2c: true })

    try {
      return await this.runCommandWithProgress({ url, completeArgs, processOptions, timeout, onProgress })
    } catch (err) {
      if (!this.shouldRetryWithoutAria2c({ err, completeArgs })) throw err

      return this.retryWithoutAria2c({
        url,
        args,
        runner: completeArgs => this.runCommandWithProgress({ url, completeArgs, processOptions, timeout, onProgress })
      })
    }
  }

  private async runCommandWithProgress (options: {
    url: string
    completeArgs: string[]
    timeout?: number
    processOptions: ProcessOptions
    onProgress: (percent: number) => void
  }) {
    const { url, completeArgs, timeout, processOptions, onProgress } = options

    const subProcess = this.spawnYoutubeDLProcess({ url, completeArgs, processOptions })

    if (timeout) {
      setTimeout(() => subProcess.kill(), timeout)
    }

    const progressRegexes = [
      /\[download\]\s+(\d+(?:\.\d+)?)%/,
      /\((\d+(?:\.\d+)?)%\)/,
      /\b(\d+(?:\.\d+)?)%\b/
    ]
    let lastReportedPercent = -1

    const parseProgressChunk = (chunk: Buffer) => {
      const lines = chunk.toString().split(/\r?\n/)
      for (const line of lines) {
        for (const progressRegex of progressRegexes) {
          const match = line.match(progressRegex)
          if (!match) continue

          const percent = Math.min(100, Math.floor(parseFloat(match[1])))
          if (percent > lastReportedPercent && percent <= 100) {
            lastReportedPercent = percent
            onProgress(percent)
          }

          break
        }
      }
    }

    // yt-dlp outputs progress to stdout by default; some versions/configs use stderr
    const stdoutStream = subProcess.stdout ?? subProcess.stdio?.[1]
    const stderrStream = subProcess.stderr ?? subProcess.stdio?.[2]
    if (stdoutStream && typeof stdoutStream.on === 'function') {
      stdoutStream.on('data', parseProgressChunk)
    }
    if (stderrStream && typeof stderrStream.on === 'function') {
      stderrStream.on('data', parseProgressChunk)
    }

    const output = await subProcess

    logger.debug('Run youtube-dl command with progress.', { command: output.command, ...lTags() })

    return output.stdout
      ? output.stdout.trim().split(/\r?\n/)
      : undefined
  }

  private async buildYoutubeDLArgs (options: {
    args: string[]
    withAria2c: boolean
  }) {
    const { args, withAria2c } = options

    let completeArgs = this.wrapWithJSRuntimeOptions(args)
    completeArgs = this.wrapWithProxyOptions(completeArgs)
    completeArgs = this.wrapWithIPOptions(completeArgs)
    completeArgs = this.wrapWithFFmpegOptions(completeArgs)
    if (withAria2c) completeArgs = await this.wrapWithAria2cOptions(completeArgs)
    completeArgs = this.wrapWithPerformanceOptions(completeArgs)

    return completeArgs
  }

  private spawnYoutubeDLProcess (options: {
    url: string
    completeArgs: string[]
    processOptions: ProcessOptions
  }) {
    const { url, completeArgs, processOptions } = options

    const youtubeDLBinaryPath = getYoutubeDLBinaryPath()
    const subProcessBinary = this.getSubProcessBinary(youtubeDLBinaryPath)
    const subProcessArgs = [ ...completeArgs, url ]
    if (subProcessBinary !== youtubeDLBinaryPath) subProcessArgs.unshift(youtubeDLBinaryPath)

    return execa(subProcessBinary, subProcessArgs, processOptions)
  }

  private async retryWithoutAria2c (options: {
    url: string
    args: string[]
    runner: (completeArgs: string[]) => Promise<string[] | undefined>
  }) {
    const { url, args, runner } = options

    logger.warn(
      'aria2c failed on protocol-relative HLS fragment URLs for %s. ' +
        'Retrying yt-dlp without aria2c so fragments can be normalized by yt-dlp.',
      url,
      lTags()
    )

    const fallbackArgs = await this.buildYoutubeDLArgs({
      args: this.stripDownloaderOptions(args),
      withAria2c: false
    })

    return runner(fallbackArgs)
  }

  private stripDownloaderOptions (args: string[]) {
    const result: string[] = []

    for (let i = 0; i < args.length; i++) {
      const arg = args[i]

      if (arg === '--downloader' || arg === '--downloader-args') {
        i++
        continue
      }

      if (arg.startsWith('--downloader=') || arg.startsWith('--downloader-args=')) continue

      result.push(arg)
    }

    return result
  }

  private shouldRetryWithoutAria2c (options: {
    err: unknown
    completeArgs: string[]
  }) {
    const { err, completeArgs } = options

    if (!this.hasAria2cDownloader(completeArgs)) return false

    return this.isAria2cProtocolRelativeUrlError(err)
  }

  private hasAria2cDownloader (args: string[]) {
    const downloaderIndex = args.indexOf('--downloader')
    if (downloaderIndex !== -1) {
      return args[downloaderIndex + 1]?.includes('aria2c') === true
    }

    return args.some(arg => arg.startsWith('--downloader=') && arg.includes('aria2c'))
  }

  private isAria2cProtocolRelativeUrlError (err: unknown) {
    const errorString = this.stringifyError(err)

    return errorString.includes('Unrecognized URI or unsupported protocol: //') &&
      (
        errorString.includes('Unable to open fragment') ||
        errorString.includes('aria2c exited with code')
      )
  }

  private stringifyError (err: unknown) {
    if (!err) return ''

    const stringValues: string[] = []

    if (typeof err === 'string') {
      stringValues.push(err)
    } else if (typeof err === 'object') {
      const errorObject = err as Record<string, unknown>

      for (const key of [ 'message', 'shortMessage', 'originalMessage', 'stdout', 'stderr', 'stack' ]) {
        const value = errorObject[key]
        if (typeof value === 'string') stringValues.push(value)
      }
    }

    return stringValues.join('\n')
  }

  private wrapWithJSRuntimeOptions (args: string[]) {
    if (CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE.NAME === 'yt-dlp') {
      return [ '--js-runtimes', 'node:' + process.execPath ].concat(args)
    }

    return args
  }

  private wrapWithProxyOptions (args: string[]) {
    const config = CONFIG.IMPORT.VIDEOS.HTTP.PROXIES
    const configProxyEnabled = Array.isArray(config) && config.length !== 0

    if (configProxyEnabled || isProxyEnabled()) {
      const proxy = configProxyEnabled
        ? config[randomInt(0, config.length)]
        : getProxy()

      logger.debug('Using proxy %s for YoutubeDL', proxy, lTags())

      return [ '--proxy', proxy ].concat(args)
    }

    return args
  }

  private wrapWithIPOptions (args: string[]) {
    if (CONFIG.IMPORT.VIDEOS.HTTP.FORCE_IPV4) {
      logger.debug('Force ipv4 for YoutubeDL')

      return [ '--force-ipv4' ].concat(args)
    }

    return args
  }

  private wrapWithFFmpegOptions (args: string[]) {
    if (process.env.FFMPEG_PATH) {
      logger.debug('Using ffmpeg location %s for YoutubeDL', process.env.FFMPEG_PATH, lTags())

      return [ '--ffmpeg-location', process.env.FFMPEG_PATH ].concat(args)
    }

    return args
  }

  private wrapWithPerformanceOptions (args: string[]) {
    if (CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE.NAME !== 'yt-dlp') return args

    const concurrentFragments = CONFIG.IMPORT.VIDEOS.HTTP.YT_DLP.CONCURRENT_FRAGMENTS

    if (!Number.isFinite(concurrentFragments) || concurrentFragments <= 1) return args

    return [ '--concurrent-fragments', String(concurrentFragments) ].concat(args)
  }

  private async wrapWithAria2cOptions (args: string[]) {
    if (CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE.NAME !== 'yt-dlp') return args
    if (!CONFIG.IMPORT.VIDEOS.HTTP.YT_DLP.ARIA2C.ENABLED) return args

    const preferredBinaryPath = CONFIG.IMPORT.VIDEOS.HTTP.YT_DLP.ARIA2C.BINARY_PATH || 'aria2c'
    const binaryPath = await this.safeGetAria2cBinaryPath(preferredBinaryPath)
    if (!binaryPath) {
      logger.warn(
        'aria2c is enabled but unavailable (including auto-download). Falling back to yt-dlp default downloader.',
        lTags()
      )
      return args
    }

    const split = CONFIG.IMPORT.VIDEOS.HTTP.YT_DLP.ARIA2C.SPLIT
    const minSplitSize = CONFIG.IMPORT.VIDEOS.HTTP.YT_DLP.ARIA2C.MIN_SPLIT_SIZE

    if (!Number.isFinite(split) || split <= 0) return args

    const downloaderArgs = `aria2c:-x ${split} -s ${split} -k ${minSplitSize}`

    return [ '--downloader', binaryPath, '--downloader-args', downloaderArgs ].concat(args)
  }

  private async safeGetAria2cBinaryPath (preferredBinaryPath: string): Promise<string | null> {
    if (preferredBinaryPath && preferredBinaryPath !== 'aria2c') return preferredBinaryPath

    // Skip aria2 on macOS since there are no official macOS builds
    // Fall back to yt-dlp default downloader
    if (isMacOSWithNoOfficialAria2Build()) {
      logger.info('[ARIA2] Skipping aria2 on macOS (no official build available), using yt-dlp default downloader.', lTags())
      return null
    }

    try {
      await execa(preferredBinaryPath, [ '--version' ], { timeout: 3000 })
      return preferredBinaryPath
    } catch {
      const managedBinaryPath = getAria2cManagedBinaryPath()
      if (await pathExists(managedBinaryPath)) return managedBinaryPath

      try {
        await updateAria2cBinary()
      } catch (err) {
        logger.warn(
          'Cannot auto-download aria2c binary. Falling back to yt-dlp default downloader.',
          { err, ...lTags() }
        )
        return null
      }

      if (await pathExists(managedBinaryPath)) return managedBinaryPath

      logger.warn(
        'aria2c auto-download finished but binary is still missing at %s. Falling back to yt-dlp default downloader.',
        managedBinaryPath,
        lTags()
      )
      return null
    }
  }

  private getSubProcessBinary (youtubeDLBinaryPath: string) {
    const pythonPath = CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE.PYTHON_PATH
    if (!pythonPath) return youtubeDLBinaryPath

    // Standalone yt-dlp binaries are native executables and should not be launched via python.
    if (CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE.NAME === 'yt-dlp') return youtubeDLBinaryPath

    return pythonPath
  }
}
