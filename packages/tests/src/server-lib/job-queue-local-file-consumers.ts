/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { CONFIG } from '@peertube/peertube-server/core/initializers/config.js'
import { JobQueue } from '@peertube/peertube-server/core/lib/job-queue/index.js'
import { LocalFileLeaseManager } from '@peertube/peertube-server/core/lib/local-file-lease-manager.js'
import { VideoModel } from '@peertube/peertube-server/core/models/video/video.js'

describe('job-queue local file consumer scans', function () {
  it('should use configured object-storage concurrency for granular move workers', function () {
    const objectStorageConfig = CONFIG.OBJECT_STORAGE as { CONCURRENCY: number }
    const originalConcurrency = objectStorageConfig.CONCURRENCY
    objectStorageConfig.CONCURRENCY = 13

    try {
      const getJobConcurrency = (JobQueue.Instance as any).getJobConcurrency.bind(JobQueue.Instance) as (jobType: string) => number

      for (const jobType of [
        'move-to-object-storage',
        'move-video-file-to-object-storage',
        'move-hls-playlist-to-object-storage',
        'move-thumbnail-to-object-storage',
        'move-caption-to-object-storage'
      ]) {
        expect(getJobConcurrency(jobType)).to.equal(13)
      }
    } finally {
      objectStorageConfig.CONCURRENCY = originalConcurrency
    }
  })

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

  it('should attach a local-file lease to an ID-based torrent job', async function () {
    const instance = JobQueue.Instance as any
    const originalQueue = instance.queues['manage-video-torrent']
    const originalLoad = VideoModel.load
    const originalAcquire = LocalFileLeaseManager.Instance.acquire
    let addedOptions: any
    let acquiredVideoUUID: string | undefined

    instance.queues['manage-video-torrent'] = {
      add: (_name: string, _data: unknown, options: unknown) => {
        addedOptions = options
        return Promise.resolve({ id: 'job-id' })
      }
    }
    VideoModel.load = (() => Promise.resolve({ uuid: 'torrent-video' } as any)) as typeof VideoModel.load
    LocalFileLeaseManager.Instance.acquire = ((options: any) => {
      acquiredVideoUUID = options.videoUUID
      expect(options.persistent).to.be.true
      return Promise.resolve({ leaseId: options.leaseId, refresh: () => Promise.resolve(true), release: () => Promise.resolve() })
    }) as typeof LocalFileLeaseManager.Instance.acquire

    try {
      await JobQueue.Instance.createJob({
        type: 'manage-video-torrent',
        payload: { action: 'create', videoId: 1, videoFileId: 2 }
      })

      expect(acquiredVideoUUID).to.equal('torrent-video')
      expect(addedOptions.localFileLeaseId).to.match(/^job:manage-video-torrent:/)
      expect(addedOptions.localFileLeaseVideoUUID).to.equal('torrent-video')
    } finally {
      instance.queues['manage-video-torrent'] = originalQueue
      VideoModel.load = originalLoad
      LocalFileLeaseManager.Instance.acquire = originalAcquire
    }
  })

  it('should reuse a custom-ID job without allocating another durable lease', async function () {
    const instance = JobQueue.Instance as any
    const originalQueue = instance.queues['move-hls-playlist-to-object-storage']
    const originalAcquire = LocalFileLeaseManager.Instance.acquire
    const existingJob = { id: 'existing-job' }
    let acquireCalls = 0
    let addCalls = 0

    instance.queues['move-hls-playlist-to-object-storage'] = {
      getJob: () => Promise.resolve(existingJob),
      add: () => {
        addCalls++
        return Promise.resolve({ id: 'unexpected-new-job' })
      }
    }
    LocalFileLeaseManager.Instance.acquire = (() => {
      acquireCalls++
      return Promise.resolve(undefined)
    }) as typeof LocalFileLeaseManager.Instance.acquire

    try {
      const result = await instance.addJobToQueue(instance.queues['move-hls-playlist-to-object-storage'], {
        type: 'move-hls-playlist-to-object-storage',
        payload: { videoUUID: 'video-uuid', playlistId: 1, fileIds: [ 1 ], isNewVideo: false },
        customJobId: 'same-job'
      })

      expect(result).to.equal(existingJob)
      expect(acquireCalls).to.equal(0)
      expect(addCalls).to.equal(0)
    } finally {
      instance.queues['move-hls-playlist-to-object-storage'] = originalQueue
      LocalFileLeaseManager.Instance.acquire = originalAcquire
    }
  })

  it('should release a removed job lease by its persisted lease ID without reloading the video', async function () {
    const originalRelease = LocalFileLeaseManager.Instance.releaseLeaseById
    let releasedLeaseId: string | undefined
    let releasedVideoUUID: string | undefined

    LocalFileLeaseManager.Instance.releaseLeaseById = ((leaseId: string, videoUUID?: string) => {
      releasedLeaseId = leaseId
      releasedVideoUUID = videoUUID
      return Promise.resolve(true)
    }) as typeof LocalFileLeaseManager.Instance.releaseLeaseById

    try {
      await JobQueue.Instance.releaseLocalFileLeaseForRemovedJob({
        id: 'job-id',
        queueName: 'video-transcoding',
        data: { videoUUID: 'video-id' },
        opts: { localFileLeaseId: 'persisted-lease-id' }
      } as any, 'video-transcoding')

      expect(releasedLeaseId).to.equal('persisted-lease-id')
      expect(releasedVideoUUID).to.equal(undefined)
    } finally {
      LocalFileLeaseManager.Instance.releaseLeaseById = originalRelease
    }
  })
})
