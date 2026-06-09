/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { shouldRemoveDeletedVideoJob } from '@server/lib/job-queue/deleted-video-job-matchers.js'

describe('deleted-video-job-matchers', function () {
  it('should remove delayed video-import jobs by videoImportId when the linked video is deleted', function () {
    expect(shouldRemoveDeletedVideoJob('video-import', {
      videoImportId: 120075
    }, {
      videoUUID: 'video-uuid',
      videoId: 42,
      videoImportId: 120075
    })).to.be.true

    expect(shouldRemoveDeletedVideoJob('video-import', {
      videoImportId: 120076
    }, {
      videoUUID: 'video-uuid',
      videoId: 42,
      videoImportId: 120075
    })).to.be.false
  })

  it('should remove granular move jobs by videoUUID when the linked video is deleted', function () {
    expect(shouldRemoveDeletedVideoJob('move-hls-playlist-to-object-storage', {
      videoUUID: 'video-uuid'
    }, {
      videoUUID: 'video-uuid',
      videoId: 42
    })).to.be.true

    expect(shouldRemoveDeletedVideoJob('move-caption-to-object-storage', {
      videoUUID: 'other-video'
    }, {
      videoUUID: 'video-uuid',
      videoId: 42
    })).to.be.false
  })
})
