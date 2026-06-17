/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { JobQueue } from '@peertube/peertube-server/core/lib/job-queue/index.js'

describe('job-queue local file consumer scans', function () {
  it('should tolerate undefined jobs while scanning pending local file consumers', async function () {
    const queues = (JobQueue.Instance as any).queues as Record<string, any>
    const originalQueue = queues['video-transcoding']

    queues['video-transcoding'] = {
      getJobs: () => Promise.resolve([
        undefined,
        { data: { videoUUID: 'other-video' } },
        { data: { videoUUID: 'target-video' } }
      ])
    }

    try {
      const result = await JobQueue.Instance.hasPendingOrActiveJob('video-transcoding' as any, 'target-video')
      expect(result).to.be.true
    } finally {
      queues['video-transcoding'] = originalQueue
    }
  })
})
