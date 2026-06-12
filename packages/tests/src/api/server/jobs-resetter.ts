/* eslint-disable @typescript-eslint/no-unused-expressions,@typescript-eslint/require-await */

import { wait } from '@peertube/peertube-core-utils'
import { VideoImportState } from '@peertube/peertube-models'
import {
  cleanupTests,
  createSingleServer,
  makeGetRequest,
  makePostBodyRequest,
  PeerTubeServer
} from '@peertube/peertube-server-commands'
import { execFileSync } from 'child_process'
import { expect } from 'chai'

describe('Test jobs resetter', function () {
  let server: PeerTubeServer

  async function waitForServerAuthentication (server: PeerTubeServer) {
    let lastError: unknown

    for (let i = 0; i < 30; i++) {
      try {
        server.accessToken = await server.login.getAccessToken()
        return
      } catch (err) {
        lastError = err
        await wait(1000)
      }
    }

    if (lastError instanceof Error) throw lastError

    throw new Error(lastError ? String(lastError) : 'Cannot authenticate test server during startup retry.')
  }

  async function waitForVideo (id: string) {
    let lastError: unknown

    for (let i = 0; i < 60; i++) {
      try {
        return await server.videos.get({ id })
      } catch (err) {
        lastError = err
        await wait(250)
      }
    }

    if (lastError instanceof Error) throw lastError

    throw new Error(lastError ? String(lastError) : `Cannot load video ${id}.`)
  }

  function isRetryableLocalConnectError (err: unknown) {
    const message = err instanceof Error
      ? `${err.message}\n${err.cause instanceof Error ? err.cause.message : ''}`
      : String(err)

    return message.includes('EADDRINUSE 127.0.0.1:9001') ||
      message.includes('ENOBUFS 127.0.0.1:9001') ||
      message.includes('ECONNREFUSED 127.0.0.1:9001')
  }

  async function withLocalApiRetry<T> (description: string, fn: () => Promise<T>) {
    let lastError: unknown

    for (let i = 0; i < 20; i++) {
      try {
        return await fn()
      } catch (err) {
        lastError = err

        if (!isRetryableLocalConnectError(err)) throw err

        await wait(500)
      }
    }

    if (lastError instanceof Error) throw lastError

    throw new Error(`Timed out retrying ${description}: ${String(lastError)}`)
  }

  function seedImportRowsInDb (options: {
    userId: number
    successVideoId: number
    pendingVideoId: number
  }) {
    const seedScript = `
      import { VideoImportState } from '@peertube/peertube-models'
      import { initDatabaseModels, sequelizeTypescript } from './dist/core/initializers/database.js'
      import { VideoImportModel } from './dist/core/models/video/video-import.js'

      const input = JSON.parse(process.env.RESETTER_SEED_INPUT)

      await sequelizeTypescript.authenticate()
      await initDatabaseModels(true)

      const successImport = await VideoImportModel.create({
        targetUrl: 'https://example.invalid/resetter-success-history.mp4',
        state: VideoImportState.SUCCESS,
        attempts: 1,
        progress: 100,
        payload: {},
        userId: input.userId,
        videoId: input.successVideoId
      })

      const pendingImport = await VideoImportModel.create({
        targetUrl: 'https://example.invalid/resetter-stale-pending.mp4',
        state: VideoImportState.PENDING,
        attempts: 0,
        progress: null,
        payload: {},
        userId: input.userId,
        videoId: input.pendingVideoId
      })

      console.log('__RESETTER_JSON__' + JSON.stringify({
        successImportId: successImport.id,
        pendingImportId: pendingImport.id
      }))

      await sequelizeTypescript.close()
    `

    const output = execFileSync(process.execPath, [ '--input-type=module', '--eval', seedScript ], {
      cwd: process.cwd(),
      encoding: 'utf-8',
      env: {
        ...process.env,
        NODE_ENV: 'test',
        NODE_APP_INSTANCE: '1',
        NODE_CONFIG: JSON.stringify({
          object_storage: { enabled: false },
          redis: { hostname: '127.0.0.1', port: 6379 }
        }),
        NODE_DB_LOG: 'false',
        RESETTER_SEED_INPUT: JSON.stringify(options)
      }
    })

    const outputLines = output
      .split(/\r?\n/)
      .map(line => line.trim())
      .filter(line => line.length !== 0)

    const jsonLine = [ ...outputLines ].reverse().find(line => line.startsWith('__RESETTER_JSON__'))

    if (!jsonLine) {
      throw new Error(`Could not find resetter seed JSON in child output: ${output}`)
    }

    return JSON.parse(jsonLine.replace('__RESETTER_JSON__', '')) as {
      successImportId: number
      pendingImportId: number
    }
  }

  before(async function () {
    this.timeout(240000)

    server = await createSingleServer(1, {
      object_storage: {
        enabled: false
      },
      redis: {
        hostname: '127.0.0.1',
        port: 6379
      }
    }, {
      env: {
        PEERTUBE_TEST_DISABLE_JOB_WORKERS: 'true'
      }
    })

    await waitForServerAuthentication(server)
  })

  it('Should keep successful deleted-video import history but remove incomplete orphan imports during system reset', async function () {
    this.timeout(180000)

    const me = await withLocalApiRetry('loading current user info', () => server.users.getMyInfo({ token: server.accessToken }))

    const successCreated = await withLocalApiRetry('uploading success seed video', () => server.videos.upload({
      attributes: {
        name: 'resetter-success-video',
        fixture: 'video_short_0p.mp4',
        waitTranscoding: false
      },
      waitTorrentGeneration: false
    }))

    const pendingCreated = await withLocalApiRetry('uploading pending seed video', () => server.videos.upload({
      attributes: {
        name: 'resetter-pending-video',
        fixture: 'video_short_0p.mp4',
        waitTranscoding: false
      },
      waitTorrentGeneration: false
    }))

    const successVideo = await waitForVideo(successCreated.uuid)
    const pendingVideo = await waitForVideo(pendingCreated.uuid)

    const seeded = seedImportRowsInDb({
      userId: me.id,
      successVideoId: successVideo.id,
      pendingVideoId: pendingVideo.id
    })

    await withLocalApiRetry('deleting success seed video', () => server.videos.remove({ id: successVideo.id }))
    await withLocalApiRetry('deleting pending seed video', () => server.videos.remove({ id: pendingVideo.id }))
    await wait(1000)

    const startResetResponse = await withLocalApiRetry('starting resetter', () => makePostBodyRequest({
      url: server.url,
      path: '/api/v1/jobs/recheck-videos-status',
      token: server.accessToken,
      expectedStatus: 200
    }))

    expect(startResetResponse.body.state).to.equal('running')

    let statusBody: any = startResetResponse.body

    for (let i = 0; i < 240; i++) {
      const res = await withLocalApiRetry('polling resetter status', () => makeGetRequest({
        url: server.url,
        path: '/api/v1/jobs/recheck-videos-status',
        token: server.accessToken,
        expectedStatus: 200
      }))

      statusBody = res.body
      if (statusBody.state !== 'running') break

      await wait(500)
    }

    expect(statusBody.state, JSON.stringify(statusBody)).to.equal('completed')
    expect(statusBody.result).to.exist

    {
      const { total, data } = await withLocalApiRetry(
        'loading success import after reset',
        () => server.videoImports.listMyVideoImports({ id: seeded.successImportId })
      )
      expect(total).to.equal(1)
      expect(data).to.have.lengthOf(1)
      expect(data[0].state.id).to.equal(VideoImportState.SUCCESS)
      expect(data[0].video ?? null).to.be.null
    }

    {
      const { total, data } = await withLocalApiRetry(
        'loading pending import after reset',
        () => server.videoImports.listMyVideoImports({ id: seeded.pendingImportId })
      )
      expect(total).to.equal(0)
      expect(data).to.have.lengthOf(0)
    }
  })

  after(async function () {
    await cleanupTests([ server ])
  })
})
