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

  it('should share the first transcoding progress rebuild across concurrent requests', async function () {
    const instance = JobQueue.Instance as any
    const queues = instance.queues as Record<string, any>
    const originalTranscodingQueue = queues['video-transcoding']
    const originalBuilderQueue = queues['transcoding-job-builder']
    const originalCache = instance.transcodingProgressCache
    const originalRefreshPromise = instance.transcodingProgressRefreshPromise

    let transcodeCalls = 0
    let builderCalls = 0
    let resolveTranscode: (jobs: any[]) => void = () => {}
    let resolveBuilder: (jobs: any[]) => void = () => {}

    const transcodeJobs = new Promise<any[]>(resolve => { resolveTranscode = resolve })
    const builderJobs = new Promise<any[]>(resolve => { resolveBuilder = resolve })

    queues['video-transcoding'] = {
      getJobs: () => {
        transcodeCalls++
        return transcodeJobs
      }
    }
    queues['transcoding-job-builder'] = {
      getJobs: () => {
        builderCalls++
        return builderJobs
      }
    }
    instance.transcodingProgressCache = undefined
    instance.transcodingProgressRefreshPromise = undefined

    try {
      const first = JobQueue.Instance.getTranscodingProgressForVideo('target-video')
      const second = JobQueue.Instance.getTranscodingProgressForVideo('target-video')

      await Promise.resolve()

      expect(transcodeCalls).to.equal(1)
      expect(builderCalls).to.equal(1)

      resolveTranscode([ { data: { videoUUID: 'target-video' }, progress: 64 } ])
      resolveBuilder([])

      expect(await Promise.all([ first, second ])).to.deep.equal([ 64, 64 ])
    } finally {
      queues['video-transcoding'] = originalTranscodingQueue
      queues['transcoding-job-builder'] = originalBuilderQueue
      instance.transcodingProgressCache = originalCache
      instance.transcodingProgressRefreshPromise = originalRefreshPromise
    }
  })

  it('should list pending local-file consumers with one combined fetch per queue', async function () {
    const queues = (JobQueue.Instance as any).queues as Record<string, any>
    const originalQueue = queues['video-transcoding']
    let calls = 0
    let requestedStates: string[] = []

    queues['video-transcoding'] = {
      getJobs: (states: string[]) => {
        calls++
        requestedStates = states

        return Promise.resolve([
          undefined,
          { data: { videoUUID: 'video-1' } },
          { data: { videoUUID: 'video-2' } }
        ])
      }
    }

    try {
      const result = await JobQueue.Instance.listVideoUUIDsWithPendingLocalFileConsumerJobs()

      expect(calls).to.equal(1)
      expect(requestedStates).to.deep.equal([ 'waiting', 'delayed', 'prioritized', 'waiting-children', 'active' ])
      expect([ ...result ].sort()).to.deep.equal([ 'video-1', 'video-2' ])
    } finally {
      queues['video-transcoding'] = originalQueue
    }
  })
})
