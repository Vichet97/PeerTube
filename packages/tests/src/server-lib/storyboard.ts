/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { storeStoryboardInObjectStorageWithDeps } from '@server/lib/storyboard.js'

describe('storyboard object storage persistence', function () {
  it('should throw when storyboard is still not ready in object storage after upload', async function () {
    let stored = false

    let error: Error | undefined

    try {
      await storeStoryboardInObjectStorageWithDeps({
        inputPath: '/tmp/storyboard.jpg',
        filename: 'storyboard.jpg',
        lTags: { tags: [ 'storyboard' ] }
      }, {
        storeStoryboard: () => {
          stored = true
          return Promise.resolve(undefined)
        },
        checkObjectStorageReadiness: () => Promise.resolve(false as any)
      })
    } catch (err) {
      error = err as Error
    }

    expect(stored).to.be.true
    expect(error).to.be.instanceOf(Error)
    expect(error?.message).to.include('did not become ready in object storage')
  })
})
