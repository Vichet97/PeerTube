import * as Sequelize from 'sequelize'

async function up (utils: {
  transaction: Sequelize.Transaction
  queryInterface: Sequelize.QueryInterface
  sequelize: Sequelize.Sequelize
}): Promise<void> {
  const actorTableDefinition = await utils.queryInterface.describeTable('actor')
  const accountTableDefinition = await utils.queryInterface.describeTable('account')
  const videoChannelTableDefinition = await utils.queryInterface.describeTable('videoChannel')

  let videoChannelJoin: string
  if (videoChannelTableDefinition['actorId']) {
    videoChannelJoin = 'LEFT JOIN "videoChannel" ON "videoChannel"."actorId" = actor.id'
  } else if (videoChannelTableDefinition['accountId'] && accountTableDefinition['actorId']) {
    videoChannelJoin = 'LEFT JOIN account AS "actorAccount" ON "actorAccount"."actorId" = actor.id LEFT JOIN "videoChannel" ON "videoChannel"."accountId" = "actorAccount"."id"'
  } else if (videoChannelTableDefinition['accountId'] && actorTableDefinition['accountId']) {
    videoChannelJoin = 'LEFT JOIN "videoChannel" ON "videoChannel"."accountId" = actor."accountId"'
  } else {
    videoChannelJoin = 'LEFT JOIN "videoChannel" ON 1 = 0'
  }

  let accountJoin: string
  if (accountTableDefinition['actorId']) {
    accountJoin = 'LEFT JOIN account ON account."actorId" = actor.id'
  } else if (actorTableDefinition['accountId']) {
    accountJoin = 'LEFT JOIN account ON account.id = actor."accountId"'
  } else {
    accountJoin = 'LEFT JOIN account ON 1 = 0'
  }

  const query = `
    WITH t AS (
      SELECT actor.id FROM actor
      ${videoChannelJoin}
      ${accountJoin}
      WHERE "videoChannel".id IS NULL and "account".id IS NULL
    ) DELETE FROM "actorFollow" WHERE "actorId" IN (SELECT t.id FROM t) OR "targetActorId" in (SELECT t.id FROM t)
  `

  await utils.sequelize.query(query)
}

function down (options) {
  throw new Error('Not implemented.')
}

export {
  up,
  down
}
