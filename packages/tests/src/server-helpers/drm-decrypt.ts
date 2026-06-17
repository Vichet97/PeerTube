/* eslint-disable @typescript-eslint/no-unused-expressions */

import { expect } from 'chai'
import { findBento4ArchiveUrlInHtml } from '@peertube/peertube-server/core/helpers/drm-decrypt/drm-decrypt-cli.js'

describe('DrmDecryptCLI', function () {
  describe('findBento4ArchiveUrlInHtml', function () {
    const html = `
      <html><body>
        <a href="https://www.bok.net/Bento4/binaries/Bento4-SDK-1-6-0-641.x86_64-unknown-linux.zip">Linux</a>
        <a href="https://www.bok.net/Bento4/binaries/Bento4-SDK-1-6-0-641.x86_64-microsoft-win32.zip">Windows</a>
        <a href="https://www.bok.net/Bento4/binaries/Bento4-SDK-1-6-0-641.universal-apple-macosx.zip">macOS</a>
      </body></html>
    `

    it('Should find the Linux archive URL', function () {
      expect(findBento4ArchiveUrlInHtml(html, 'linux-amd64'))
        .to.equal('https://www.bok.net/Bento4/binaries/Bento4-SDK-1-6-0-641.x86_64-unknown-linux.zip')
    })

    it('Should find the Windows archive URL', function () {
      expect(findBento4ArchiveUrlInHtml(html, 'windows'))
        .to.equal('https://www.bok.net/Bento4/binaries/Bento4-SDK-1-6-0-641.x86_64-microsoft-win32.zip')
    })

    it('Should return null on unsupported platforms', function () {
      expect(findBento4ArchiveUrlInHtml(html, 'linux-arm64'))
        .to.equal(null)
    })
  })
})
