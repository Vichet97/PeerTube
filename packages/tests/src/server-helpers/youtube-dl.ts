/* eslint-disable @typescript-eslint/no-unused-expressions,@typescript-eslint/require-await */

import { expect } from 'chai'
import { mkdtemp, writeFile } from 'fs/promises'
import { remove } from 'fs-extra/esm'
import { tmpdir } from 'os'
import { join } from 'path'
import { YoutubeDLCLI } from '@peertube/peertube-server/core/helpers/youtube-dl/youtube-dl-cli.js'
import { YoutubeDLWrapper } from '@peertube/peertube-server/core/helpers/youtube-dl/youtube-dl-wrapper.js'
import { CONFIG } from '@peertube/peertube-server/core/initializers/config.js'

describe('YoutubeDLCLI', function () {

  describe('wrapWithJSRuntimeOptions', function () {
    let cli: any

    before(function () {
      cli = Object.create(YoutubeDLCLI.prototype)
    })

    it('Should include process.execPath in --js-runtimes when using yt-dlp', function () {
      const originalDescriptor = Object.getOwnPropertyDescriptor(CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE, 'NAME')

      Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE, 'NAME', {
        get: () => 'yt-dlp',
        configurable: true
      })

      try {
        const inputArgs = [ '--dump-json', '-f', 'best' ]
        const result: string[] = cli.wrapWithJSRuntimeOptions(inputArgs)

        expect(result[0]).to.equal('--js-runtimes')
        expect(result[1]).to.equal('node:' + process.execPath)
        expect(result[1]).to.match(/^node:/)
        expect(result.slice(2)).to.deep.equal(inputArgs)
      } finally {
        Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE, 'NAME', originalDescriptor)
      }
    })

    it('Should not modify args when not using yt-dlp', function () {
      const originalDescriptor = Object.getOwnPropertyDescriptor(CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE, 'NAME')

      Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE, 'NAME', {
        get: () => 'youtube-dl',
        configurable: true
      })

      try {
        const inputArgs = [ '--dump-json', '-f', 'best' ]
        const result: string[] = cli.wrapWithJSRuntimeOptions(inputArgs)

        expect(result).to.deep.equal(inputArgs)
      } finally {
        Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE, 'NAME', originalDescriptor)
      }
    })

    it('Should prepend runtime options before existing args', function () {
      const originalDescriptor = Object.getOwnPropertyDescriptor(CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE, 'NAME')

      Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE, 'NAME', {
        get: () => 'yt-dlp',
        configurable: true
      })

      try {
        const inputArgs = [ '--skip-download' ]
        const result: string[] = cli.wrapWithJSRuntimeOptions(inputArgs)

        expect(result).to.have.lengthOf(3)
        expect(result[0]).to.equal('--js-runtimes')
        expect(result[1]).to.equal('node:' + process.execPath)
        expect(result[2]).to.equal('--skip-download')
      } finally {
        Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE, 'NAME', originalDescriptor)
      }
    })

    it('Should handle empty args array', function () {
      const originalDescriptor = Object.getOwnPropertyDescriptor(CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE, 'NAME')

      Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE, 'NAME', {
        get: () => 'yt-dlp',
        configurable: true
      })

      try {
        const result: string[] = cli.wrapWithJSRuntimeOptions([])

        expect(result).to.have.lengthOf(2)
        expect(result[0]).to.equal('--js-runtimes')
        expect(result[1]).to.equal('node:' + process.execPath)
      } finally {
        Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE, 'NAME', originalDescriptor)
      }
    })
  })

  describe('aria2c protocol-relative HLS failure detection', function () {
    let cli: any

    before(function () {
      cli = Object.create(YoutubeDLCLI.prototype)
    })

    it('Should detect aria2c failures on protocol-relative HLS fragment URLs', function () {
      const err = {
        stdout: '[ERROR] Unrecognized URI or unsupported protocol: //hls15.stream6.store/segment/video/1080/fragment.png',
        stderr: 'ERROR: Unable to open fragment 0\nERROR: aria2c exited with code -1'
      }

      expect(cli.isAria2cProtocolRelativeUrlError(err)).to.be.true
    })

    it('Should not detect unrelated aria2c failures as protocol-relative URL errors', function () {
      const err = {
        stdout: '[ERROR] Download aborted.',
        stderr: 'ERROR: aria2c exited with code -1'
      }

      expect(cli.isAria2cProtocolRelativeUrlError(err)).to.be.false
    })

    it('Should only retry when aria2c was the configured downloader', function () {
      const err = {
        stdout: '[ERROR] Unrecognized URI or unsupported protocol: //hls15.stream6.store/segment/video/1080/fragment.png',
        stderr: 'ERROR: Unable to open fragment 0'
      }

      expect(cli.shouldRetryWithoutAria2c({
        err,
        completeArgs: [ '--downloader', 'aria2c', '--downloader-args', 'aria2c:-x 8 -s 8 -k 1M' ]
      })).to.be.true

      expect(cli.shouldRetryWithoutAria2c({
        err,
        completeArgs: [ '--concurrent-fragments', '10' ]
      })).to.be.false
    })

    it('Should preserve custom headers when retrying without aria2c', async function () {
      const originalDescriptor = Object.getOwnPropertyDescriptor(CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE, 'NAME')

      Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE, 'NAME', {
        get: () => 'yt-dlp',
        configurable: true
      })

      try {
        const inputArgs = [
          '--newline',
          '--downloader',
          'aria2c',
          '--downloader-args',
          'aria2c:-x 8 -s 8 -k 1M',
          '--add-header',
          'referer: https://kisskh.co/',
          '--add-header',
          'User-Agent: Mozilla/5.0',
          '--merge-output-format',
          'mp4'
        ]

        const result: string[] | undefined = await cli.retryWithoutAria2c({
          url: 'https://hls15.cdnvideo11.shop/master/example.m3u8',
          args: inputArgs,
          runner: async (completeArgs: string[]) => completeArgs
        })

        expect(result).to.include.members([
          '--add-header',
          'referer: https://kisskh.co/',
          'User-Agent: Mozilla/5.0'
        ])
        expect(result).to.not.include('--downloader')
        expect(result).to.not.include('--downloader-args')
        expect(result).to.not.include('aria2c')
        expect(result).to.not.include('aria2c:-x 8 -s 8 -k 1M')
      } finally {
        Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE, 'NAME', originalDescriptor)
      }
    })
  })

  describe('native live HLS fallback to ffmpeg downloader', function () {
    let cli: any

    before(function () {
      cli = Object.create(YoutubeDLCLI.prototype)
    })

    it('Should detect native live HLS downloader failures that should retry with ffmpeg', function () {
      const err = {
        stdout: 'WARNING: Live HLS streams are not supported by the native downloader.',
        stderr: 'please add "--downloader ffmpeg --hls-use-mpegts" to your command'
      }

      expect(cli.shouldRetryWithFFmpegDownloader({
        err,
        completeArgs: [ '--concurrent-fragments', '10' ]
      })).to.be.true
    })

    it('Should add ffmpeg downloader flags and keep custom headers when retrying native live HLS failures', async function () {
      const originalDescriptor = Object.getOwnPropertyDescriptor(CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE, 'NAME')

      Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE, 'NAME', {
        get: () => 'yt-dlp',
        configurable: true
      })

      try {
        const inputArgs = [
          '--newline',
          '--concurrent-fragments',
          '10',
          '--add-header',
          'referer: https://kisskh.co/',
          '--add-header',
          'User-Agent: Mozilla/5.0',
          '--merge-output-format',
          'mp4'
        ]

        const result: string[] | undefined = await cli.retryWithFFmpegDownloader({
          url: 'https://hls15.cdnvideo11.shop/master/example.m3u8',
          args: inputArgs,
          runner: async (completeArgs: string[]) => completeArgs
        })

        expect(result).to.include.members([
          '--downloader',
          'ffmpeg',
          '--hls-use-mpegts',
          '--add-header',
          'referer: https://kisskh.co/',
          'User-Agent: Mozilla/5.0'
        ])
        expect(result).to.not.include('--concurrent-fragments')
      } finally {
        Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.YOUTUBE_DL_RELEASE, 'NAME', originalDescriptor)
      }
    })
  })

  describe('YoutubeDLWrapper', function () {
    it('Should force an explicit .mp4 output path for yt-dlp downloads', async function () {
      const originalTmpDirDescriptor = Object.getOwnPropertyDescriptor(CONFIG.STORAGE, 'TMP_DIR')
      const originalSafeGet = YoutubeDLCLI.safeGet

      const tmpDirectory = await mkdtemp(join(tmpdir(), 'peertube-ytdlp-wrapper-'))
      let capturedOutput: string | undefined

      Object.defineProperty(CONFIG.STORAGE, 'TMP_DIR', {
        value: tmpDirectory,
        configurable: true
      })

      YoutubeDLCLI.safeGet = (async () => ({
        download: async (options: { output: string }) => {
          capturedOutput = options.output
          await writeFile(options.output, 'test')
        }
      })) as unknown as typeof YoutubeDLCLI.safeGet

      try {
        const wrapper = new YoutubeDLWrapper('https://example.com/master.m3u8', [ 720 ], false)
        const result = await wrapper.downloadVideo('.mp4', 30_000)

        expect(capturedOutput).to.match(/-import\.mp4$/)
        expect(result).to.equal(capturedOutput)
      } finally {
        YoutubeDLCLI.safeGet = originalSafeGet

        if (originalTmpDirDescriptor) {
          Object.defineProperty(CONFIG.STORAGE, 'TMP_DIR', originalTmpDirDescriptor)
        }

        await remove(tmpDirectory).catch(() => {})
      }
    })
  })
})
