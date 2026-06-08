/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { VideoState } from '@peertube/peertube-models'
import {
  publishVideoAfterFirstTranscodingBatchIfNeededWithDeps
} from '@server/lib/job-queue/handlers/video-transcoding-publish.js'

describe('video-transcoding publish after first batch', function () {
  it('should retry retryable database commit failures when publishing after the first playable batch', async function () {
    let transactionAttempts = 0
    let setNewStateCalls = 0

    const loadVideo = () => {
      return Promise.resolve({
        uuid: 'video-uuid',
        state: VideoState.TO_TRANSCODE,
        waitTranscoding: true,
        VideoFiles: [ { id: 1 } ],
        VideoStreamingPlaylists: [],
        getHLSPlaylist: () => undefined,
        setNewState: function (newState: number) {
          setNewStateCalls += 1
          this.state = newState
          return Promise.resolve()
        }
      } as any)
    }

    const runTransaction = async (fn: (transaction: any) => Promise<any>) => {
      transactionAttempts += 1

      const result = await fn({ id: transactionAttempts })

      if (transactionAttempts === 1) {
        const err = new Error(
          'could not serialize access due to read/write dependencies among transactions'
        ) as Error & {
          parent: {
            code: string
            message: string
          }
        }

        err.parent = {
          code: '40001',
          message: 'could not serialize access due to read/write dependencies among transactions'
        }

        throw err
      }

      return result
    }

    await publishVideoAfterFirstTranscodingBatchIfNeededWithDeps({
      videoUUID: 'video-uuid',
      isNewVideo: true
    }, {
      loadVideo,
      runTransaction
    })

    expect(transactionAttempts).to.equal(2)
    expect(setNewStateCalls).to.equal(2)
  })
})
