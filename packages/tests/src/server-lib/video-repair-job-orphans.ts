/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import { getUnprocessedOrphanedVideoRepairJobRefs } from '@server/controllers/api/video-repair-job-orphans.js'

describe('video-repair-job-orphans', function () {
  it('should treat videoImportId-only jobs as orphans when their imports no longer exist', function () {
    const importOnlyRef = { key: 'video-import:1' }
    const stillLinkedRef = { key: 'video-import:2' }

    const refs = getUnprocessedOrphanedVideoRepairJobRefs({
      index: {
        byVideoUUID: new Map(),
        byVideoId: new Map(),
        byVideoImportId: new Map([
          [ 120075, [ importOnlyRef ] ],
          [ 120076, [ stillLinkedRef ] ]
        ])
      },
      existingVideoUUIDs: new Set<string>(),
      existingVideoIds: new Set<number>(),
      existingVideoImportIds: new Set<number>([ 120076 ]),
      processedJobKeys: new Set<string>(),
      dedupeRefs: refsArg => refsArg,
      getRefKey: ref => ref.key
    })

    expect(refs).to.deep.equal([ importOnlyRef ])
  })

  it('should exclude already processed orphan refs after dedupe', function () {
    const ref = { key: 'video-import:1' }

    const refs = getUnprocessedOrphanedVideoRepairJobRefs({
      index: {
        byVideoUUID: new Map(),
        byVideoId: new Map(),
        byVideoImportId: new Map([ [ 120075, [ ref, ref ] ] ])
      },
      existingVideoUUIDs: new Set<string>(),
      existingVideoIds: new Set<number>(),
      existingVideoImportIds: new Set<number>(),
      processedJobKeys: new Set<string>([ 'video-import:1' ]),
      dedupeRefs: refsArg => [ ...new Set(refsArg) ],
      getRefKey: orphanRef => orphanRef.key
    })

    expect(refs).to.deep.equal([])
  })
})
