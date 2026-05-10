/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { VideoState } from '@peertube/peertube-models'
import { maybeTransitionAfterObjectStorageMove } from '@peertube/peertube-server/core/lib/move-storage/move-to-object-storage.js'
import { VideoModel } from '@peertube/peertube-server/core/models/video/video.js'

describe('move-to-object-storage', function () {
  it('should skip stale move completion when the video is back in TO_TRANSCODE', async function () {
    const originalLoadFull = VideoModel.loadFull

    VideoModel.loadFull = (() => Promise.resolve({ state: VideoState.TO_TRANSCODE } as any)) as typeof VideoModel.loadFull

    try {
      await maybeTransitionAfterObjectStorageMove({
        videoUUID: 'video-uuid',
        moveVideoState: {
          isNewVideo: true,
          previousVideoState: VideoState.TO_TRANSCODE
        },
        reason: 'test'
      })
    } finally {
      VideoModel.loadFull = originalLoadFull
    }

    expect(true).to.be.true
  })
})
