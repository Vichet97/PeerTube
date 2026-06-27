/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { reassignDeleteWebVideoFilesToLastHLSJob } from '@server/lib/transcoding/shared/job-builders/transcoding-cleanup-order.js'

describe('transcoding job builder cleanup ordering', function () {
  it('should only delete temporary web video files on the final HLS job', function () {
    const children = [
      [
        { type: 'new-resolution-to-hls', resolution: 1080, deleteWebVideoFiles: true, transcodingPriority: 'required' as const },
        { type: 'new-resolution-to-hls', resolution: 0, deleteWebVideoFiles: true, transcodingPriority: 'required' as const }
      ],
      [
        { type: 'new-resolution-to-hls', resolution: 720, deleteWebVideoFiles: false, transcodingPriority: 'optional' as const }
      ],
      [
        { type: 'new-resolution-to-hls', resolution: 480, deleteWebVideoFiles: false, transcodingPriority: 'optional' as const }
      ]
    ]

    reassignDeleteWebVideoFilesToLastHLSJob(children)

    expect(children[0][0].deleteWebVideoFiles).to.be.false
    expect(children[0][1].deleteWebVideoFiles).to.be.true
    expect(children[1][0].deleteWebVideoFiles).to.be.false
    expect(children[2][0].deleteWebVideoFiles).to.be.false
  })

  it('should leave non-HLS payloads unchanged', function () {
    const children = [
      [
        { type: 'new-resolution-to-web-video', deleteWebVideoFiles: true }
      ]
    ]

    reassignDeleteWebVideoFilesToLastHLSJob(children)

    expect(children[0][0].deleteWebVideoFiles).to.be.true
  })

  it('should fall back to the last HLS payload if no required HLS payload exists', function () {
    const children = [
      [
        { type: 'new-resolution-to-hls', resolution: 720, deleteWebVideoFiles: false, transcodingPriority: 'optional' as const }
      ],
      [
        { type: 'new-resolution-to-hls', resolution: 480, deleteWebVideoFiles: false, transcodingPriority: 'optional' as const }
      ]
    ]

    reassignDeleteWebVideoFilesToLastHLSJob(children)

    expect(children[0][0].deleteWebVideoFiles).to.be.false
    expect(children[1][0].deleteWebVideoFiles).to.be.true
  })
})
