import * as Sequelize from 'sequelize'

async function up (utils: {
  transaction: Sequelize.Transaction
  queryInterface: Sequelize.QueryInterface
  sequelize: Sequelize.Sequelize
}): Promise<void> {
  const tableDefinition = await utils.queryInterface.describeTable('user')

  if (!tableDefinition['lastLoginDate']) {
    const field = {
      type: Sequelize.DATE,
      allowNull: true
    }
    await utils.queryInterface.addColumn('user', 'lastLoginDate', field)
  }

}

function down (options) {
  throw new Error('Not implemented.')
}

export {
  up,
  down
}
