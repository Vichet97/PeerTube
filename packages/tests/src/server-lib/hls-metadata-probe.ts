/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { resolveHLSPlaylistFileProbeInput } from '@server/lib/hls.js'
import { VideoPathManager } from '@server/lib/video-path-manager.js'

describe('hls metadata probe', function () {
  it('should reuse stored metadata and avoid fetching the file when probe data already exists', async function () {
    const originalMakeAvailableVideoFile = VideoPathManager.Instance.makeAvailableVideoFile
    let makeAvailableCalls = 0

    VideoPathManager.Instance.makeAvailableVideoFile = (() => {
      makeAvailableCalls++
      return Promise.reject(new Error('should not fetch from storage'))
    }) as typeof VideoPathManager.Instance.makeAvailableVideoFile

    try {
      const file = {
        filename: 'video-file.mp4',
        metadata: {
          format: {},
          chapters: [],
          streams: [
            { codec_type: 'video', codec_tag_string: 'avc1', profile: 'High', level: 31, width: 1280, height: 720 },
            { codec_type: 'audio', codec_name: 'aac' }
          ]
        },
        withVideoOrPlaylist () {
          return this
        }
      } as any

      const result = await resolveHLSPlaylistFileProbeInput(file, {} as any)

      expect(result.path).to.equal('video-file.mp4')
      expect(result.fetchedFromStorage).to.be.false
      expect(result.probe.streams).to.have.length(2)
      expect(makeAvailableCalls).to.equal(0)
    } finally {
      VideoPathManager.Instance.makeAvailableVideoFile = originalMakeAvailableVideoFile
    }
  })
})
