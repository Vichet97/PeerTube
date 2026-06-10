/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import {
  runImportPreparationStep,
  VIDEO_IMPORT_PREPARATION_STEP_TIMEOUT_MS
} from '@server/lib/job-queue/handlers/video-import.js'

describe('video-import preparation step timeout', function () {
  it('should complete successful preparation steps', async function () {
    const result = await runImportPreparationStep({
      importId: 1,
      videoUUID: 'video-uuid',
      step: 'generate-thumbnails',
      run: () => Promise.resolve('ok'),
      timeoutMs: 50
    })

    expect(result).to.equal('ok')
  })

  it('should convert timed out preparation steps into explicit import step errors', async function () {
    let error: Error | undefined

    try {
      await runImportPreparationStep({
        importId: 42,
        videoUUID: 'video-uuid',
        step: 'generate-thumbnails',
        run: () => new Promise(resolve => setTimeout(resolve, 30)),
        timeoutMs: 5
      })
    } catch (err) {
      error = err as Error
    }

    expect(error).to.be.instanceOf(Error)
    expect(error.message).to.contain('Video import preparation step generate-thumbnails timed out')
  })

  it('should expose the default preparation timeout constant', function () {
    expect(VIDEO_IMPORT_PREPARATION_STEP_TIMEOUT_MS).to.equal(10 * 60 * 1000)
  })
})
