import * as Sequelize from 'sequelize'

async function up (utils: {
  transaction: Sequelize.Transaction
  queryInterface: Sequelize.QueryInterface
  sequelize: Sequelize.Sequelize
}): Promise<void> {
  {
    const query = `
      CREATE TABLE IF NOT EXISTS "userApiToken" (
        "id"   SERIAL,
        "userId" INTEGER NOT NULL REFERENCES "user" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
        "tokenHash" VARCHAR(64) NOT NULL,
        "name" VARCHAR(100) NOT NULL,
        "description" VARCHAR(500),
        "expiresAt" TIMESTAMP WITH TIME ZONE,
        "scopes" JSONB NOT NULL DEFAULT '[]',
        "createdAt" TIMESTAMP WITH TIME ZONE NOT NULL,
        "lastUsedAt" TIMESTAMP WITH TIME ZONE,
        PRIMARY KEY ("id")
      );
    `

    await utils.sequelize.query(query, { transaction: utils.transaction })
  }

  {
    const query = `
      CREATE UNIQUE INDEX "user_api_token_token_hash_unique" ON "userApiToken" ("tokenHash");
    `
    await utils.sequelize.query(query, { transaction: utils.transaction })
  }

  {
    const query = `
      CREATE INDEX "user_api_token_user_id" ON "userApiToken" ("userId");
    `
    await utils.sequelize.query(query, { transaction: utils.transaction })
  }
}

function down (options) {
  throw new Error('Not implemented.')
}

export {
  up,
  down
}
