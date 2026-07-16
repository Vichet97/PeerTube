/* eslint-disable @typescript-eslint/no-unused-expressions */

import { expect } from 'chai'
import { getAllPrivacies } from '@peertube/peertube-core-utils'
import { VideoInclude, VideoPrivacy } from '@peertube/peertube-models'
import {
  BuildVideosListQueryOptions,
  VideosIdListQueryBuilder
} from '@server/models/video/sql/video/videos-id-list-query-builder.js'
import { Sequelize } from 'sequelize'

describe('VideosIdListQueryBuilder', function () {
  const sequelize = new Sequelize({ dialect: 'postgres', logging: false })

  function buildQuery (overrides: Partial<BuildVideosListQueryOptions> = {}) {
    const include = VideoInclude.BLACKLISTED | VideoInclude.NOT_PUBLISHED_STATE | VideoInclude.BLOCKED_OWNER
    const options = {
      serverAccountIdForBlock: 1,
      displayOnlyForFollower: null,
      count: 100,
      start: 0,
      sort: '-publishedAt',
      trendingDays: 7,
      include,
      accountId: 2,
      includeCollaborations: true,
      ...overrides
    } as BuildVideosListQueryOptions

    return new VideosIdListQueryBuilder(sequelize).getQuery(options)
  }

  it('should avoid joins and predicates that are redundant for the my videos list', function () {
    const result = buildQuery({ privacyOneOf: getAllPrivacies() })

    expect(result.query).to.contain('"video"."channelId" IN')
    expect(result.query).to.contain('UNION ALL')
    expect(result.query).to.not.contain('INNER JOIN "account"')
    expect(result.query).to.not.contain('"accountActor"')
    expect(result.query).to.not.contain('"video"."privacy" IN')
    expect(result.replacements.privacyOneOf).to.be.undefined
  })

  it('should preserve explicit privacy subsets', function () {
    const privacyOneOf = [ VideoPrivacy.PUBLIC, VideoPrivacy.PRIVATE ]
    const result = buildQuery({ privacyOneOf })

    expect(result.query).to.contain('"video"."privacy" IN (:privacyOneOf)')
    expect(result.replacements.privacyOneOf).to.deep.equal(privacyOneOf)
  })

  it('should filter direct ownership through the video channel without account joins', function () {
    const result = buildQuery({ includeCollaborations: false })

    expect(result.query).to.contain('"videoChannel"."accountId" = :accountId')
    expect(result.query).to.not.contain('INNER JOIN "account"')
    expect(result.query).to.not.contain('"videoChannelCollaborator"')
  })

  it('should join account actors directly when block filtering is required', function () {
    const result = buildQuery({ include: VideoInclude.BLACKLISTED | VideoInclude.NOT_PUBLISHED_STATE })

    expect(result.query).to.contain(
      'INNER JOIN "actor" "accountActor" ON "videoChannel"."accountId" = "accountActor"."accountId"'
    )
    expect(result.query).to.contain('"accountBlocklist"."targetAccountId" = "videoChannel"."accountId"')
    expect(result.query).to.not.contain('INNER JOIN "account"')
  })

})
