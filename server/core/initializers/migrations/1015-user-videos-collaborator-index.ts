import * as Sequelize from 'sequelize'

async function up (utils: {
  transaction: Sequelize.Transaction
  queryInterface: Sequelize.QueryInterface
  sequelize: Sequelize.Sequelize
}): Promise<void> {
  const query = `
    CREATE INDEX IF NOT EXISTS "video_channel_collaborator_account_channel_accepted"
    ON "videoChannelCollaborator" ("accountId", "channelId")
    WHERE "state" = 2;
  `

  await utils.sequelize.query(query, { transaction: utils.transaction })
}

function down () {
  throw new Error('Not implemented.')
}

export {
  down,
  up
}
