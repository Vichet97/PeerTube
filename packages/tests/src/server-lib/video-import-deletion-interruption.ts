/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import {
  handleDeletedVideoImportInterruption,
  isDeletedVideoImportInterruptionError
} from '@server/lib/job-queue/handlers/video-import-deletion-interruption.js'

describe('video-import deletion interruption', function () {
  it('should detect reload/deleted-video interruption errors', function () {
    expect(isDeletedVideoImportInterruptionError(
      new Error('Instance could not be reloaded because it does not exist anymore (find call returned null)')
    )).to.be.true

    expect(isDeletedVideoImportInterruptionError(
      new Error('Video linked to import 42 does not exist anymore.')
    )).to.be.true

    expect(isDeletedVideoImportInterruptionError(
      new Error('plain failure')
    )).to.be.false
  })

  it('should cleanup artifacts and destroy the orphaned import when the video disappeared', async function () {
    const removedPaths: string[] = []
    let destroyed = false

    const handled = await handleDeletedVideoImportInterruption({
      err: new Error('Instance could not be reloaded because it does not exist anymore (find call returned null)'),
      tempVideoPath: '/tmp/import.mp4',
      movedVideoDestPath: '/storage/web-videos/video.mp4',
      torrentPath: '/storage/torrents/video.torrent'
    }, {
      loadImport: () => Promise.resolve({
        Video: undefined,
        destroy: () => {
          destroyed = true
          return Promise.resolve()
        }
      }),
      removePath: path => {
        removedPaths.push(path)
        return Promise.resolve()
      }
    })

    expect(handled).to.be.true
    expect(removedPaths).to.deep.equal([
      '/tmp/import.mp4',
      '/storage/web-videos/video.mp4',
      '/storage/torrents/video.torrent'
    ])
    expect(destroyed).to.be.true
  })

  it('should not swallow unrelated errors or still-linked imports', async function () {
    const handledUnrelated = await handleDeletedVideoImportInterruption({
      err: new Error('plain failure'),
      tempVideoPath: '/tmp/import.mp4'
    }, {
      loadImport: () => Promise.resolve({ Video: undefined }),
      removePath: () => Promise.resolve(undefined)
    })

    const handledStillLinked = await handleDeletedVideoImportInterruption({
      err: new Error('Instance could not be reloaded because it does not exist anymore (find call returned null)'),
      tempVideoPath: '/tmp/import.mp4'
    }, {
      loadImport: () => Promise.resolve({ Video: { id: 1 } }),
      removePath: () => Promise.resolve(undefined)
    })

    expect(handledUnrelated).to.be.false
    expect(handledStillLinked).to.be.false
  })
})
