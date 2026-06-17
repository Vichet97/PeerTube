/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { CONFIG } from '@peertube/peertube-server/core/initializers/config.js'
import { shouldRunDrmDecryptionForImport } from '@server/lib/job-queue/handlers/video-import.js'

describe('video-import DRM gating', function () {
  it('should skip DRM decryption for ordinary imports without DRM metadata', function () {
    const originalEnabled = Object.getOwnPropertyDescriptor(CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION, 'ENABLED')
    const originalArgs = Object.getOwnPropertyDescriptor(CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION, 'ARGS')

    Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION, 'ENABLED', {
      value: true,
      configurable: true
    })
    Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION, 'ARGS', {
      value: [ '--key', '{clearkeys}', '{input}', '{output}' ],
      configurable: true
    })

    try {
      expect(shouldRunDrmDecryptionForImport({
        type: 'youtube-dl',
        generateTranscription: false,
        videoImportId: 1
      })).to.be.false
    } finally {
      if (originalEnabled) Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION, 'ENABLED', originalEnabled)
      if (originalArgs) Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION, 'ARGS', originalArgs)
    }
  })

  it('should skip DRM decryption when args are not configured', function () {
    const originalEnabled = Object.getOwnPropertyDescriptor(CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION, 'ENABLED')
    const originalArgs = Object.getOwnPropertyDescriptor(CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION, 'ARGS')

    Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION, 'ENABLED', {
      value: true,
      configurable: true
    })
    Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION, 'ARGS', {
      value: [],
      configurable: true
    })

    try {
      expect(shouldRunDrmDecryptionForImport({
        type: 'youtube-dl',
        generateTranscription: false,
        videoImportId: 1,
        drmType: 'widevine'
      })).to.be.false
    } finally {
      if (originalEnabled) Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION, 'ENABLED', originalEnabled)
      if (originalArgs) Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION, 'ARGS', originalArgs)
    }
  })

  it('should run DRM decryption only for DRM-marked youtube-dl imports', function () {
    const originalEnabled = Object.getOwnPropertyDescriptor(CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION, 'ENABLED')
    const originalArgs = Object.getOwnPropertyDescriptor(CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION, 'ARGS')

    Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION, 'ENABLED', {
      value: true,
      configurable: true
    })
    Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION, 'ARGS', {
      value: [ '--key', '{clearkeys}', '{input}', '{output}' ],
      configurable: true
    })

    try {
      expect(shouldRunDrmDecryptionForImport({
        type: 'youtube-dl',
        generateTranscription: false,
        videoImportId: 1,
        clearkeys: '{"kid":"key"}'
      })).to.be.true

      expect(shouldRunDrmDecryptionForImport({
        type: 'torrent-file',
        generateTranscription: false,
        videoImportId: 1,
        clearkeys: '{"kid":"key"}'
      })).to.be.false
    } finally {
      if (originalEnabled) Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION, 'ENABLED', originalEnabled)
      if (originalArgs) Object.defineProperty(CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION, 'ARGS', originalArgs)
    }
  })
})
