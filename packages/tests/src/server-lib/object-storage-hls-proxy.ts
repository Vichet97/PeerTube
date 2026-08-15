/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import {
  clearHLSPlaylistResponseCache,
  getCachedHLSPlaylistResponse,
  HLSPlaylistResponseCache,
  HLSPlaylistResponseTiming,
  transformM3U8ToProxy
} from '@server/lib/object-storage/presigned-redirect.js'

describe('object-storage HLS proxy', function () {
  it('should sign each repeated direct media object only once', async function () {
    const signedKeys: string[] = []
    const playlist = [
      '#EXTM3U',
      '#EXT-X-MAP:URI="720-fragmented.mp4"',
      '#EXTINF:4.000,',
      '720-fragmented.mp4',
      '#EXT-X-BYTERANGE:1000@0',
      '#EXTINF:4.000,',
      '720-fragmented.mp4',
      '480.m3u8'
    ].join('\n')

    const result = await transformM3U8ToProxy({
      masterPlaylistKey: 'hls/video-uuid/720.m3u8',
      masterPlaylistContent: playlist,
      signDirectFile: key => {
        signedKeys.push(key)

        return Promise.resolve(`https://storage.example/${key}`)
      }
    })

    expect(signedKeys).to.deep.equal([ 'hls/video-uuid/720-fragmented.mp4' ])
    expect(result).to.include('URI="https://storage.example/hls/video-uuid/720-fragmented.mp4"')
    expect(result).to.include('https://storage.example/hls/video-uuid/720-fragmented.mp4')
    expect(result).to.match(/480\.m3u8\?expires=/)
  })

  it('should coalesce concurrent builds and evict cached responses when cleared', async function () {
    const cache = new HLSPlaylistResponseCache({ maxSize: 1024, ttl: 60_000 })
    let builds = 0

    const build = async () => {
      builds++
      await new Promise(resolve => setTimeout(resolve, 10))

      return '#EXTM3U'
    }

    const [ first, second, third ] = await Promise.all([
      cache.getOrCreate('hls/video-uuid/720.m3u8', build),
      cache.getOrCreate('hls/video-uuid/720.m3u8', build),
      cache.getOrCreate('hls/video-uuid/720.m3u8', build)
    ])

    expect([ first, second, third ]).to.deep.equal([ '#EXTM3U', '#EXTM3U', '#EXTM3U' ])
    expect(builds).to.equal(1)

    await cache.getOrCreate('hls/video-uuid/720.m3u8', build)
    expect(builds).to.equal(1)

    cache.clear()
    await cache.getOrCreate('hls/video-uuid/720.m3u8', build)
    expect(builds).to.equal(2)
  })

  it('should report cache, fetch, and transform timings for HLS playlist responses', async function () {
    clearHLSPlaylistResponseCache()

    const firstTimings: HLSPlaylistResponseTiming[] = []
    const first = await getCachedHLSPlaylistResponse({
      playlistKey: 'hls/timing-video/master.m3u8',
      getContent: () => Promise.resolve(Buffer.from('#EXTM3U')),
      onTiming: timing => firstTimings.push(timing)
    })

    const cachedTimings: HLSPlaylistResponseTiming[] = []
    const cached = await getCachedHLSPlaylistResponse({
      playlistKey: 'hls/timing-video/master.m3u8',
      getContent: () => Promise.resolve(Buffer.from('#EXTM3U')),
      onTiming: timing => cachedTimings.push(timing)
    })

    expect(first).to.equal('#EXTM3U')
    expect(cached).to.equal('#EXTM3U')
    expect(firstTimings.some(timing => timing.cacheState === 'build')).to.be.true
    expect(firstTimings.some(timing => timing.objectStorageFetchMs !== undefined)).to.be.true
    expect(firstTimings.some(timing => timing.transformMs !== undefined)).to.be.true
    expect(cachedTimings).to.deep.equal([ { cacheState: 'hit' } ])
  })

  it('should bound concurrent builds for distinct cold playlists', async function () {
    const cache = new HLSPlaylistResponseCache({ maxSize: 1024, ttl: 60_000, concurrency: 2 })
    let activeBuilds = 0
    let peakBuilds = 0
    let releaseBuilds = () => undefined
    const gate = new Promise<void>(resolve => { releaseBuilds = resolve })

    const build = async () => {
      activeBuilds++
      peakBuilds = Math.max(peakBuilds, activeBuilds)
      await gate
      activeBuilds--

      return '#EXTM3U'
    }

    const pending = [
      cache.getOrCreate('hls/video-a/master.m3u8', build),
      cache.getOrCreate('hls/video-b/master.m3u8', build),
      cache.getOrCreate('hls/video-c/master.m3u8', build)
    ]

    await new Promise(resolve => setTimeout(resolve, 0))
    expect(peakBuilds).to.equal(2)

    releaseBuilds()
    await Promise.all(pending)
    expect(peakBuilds).to.equal(2)
  })
})
