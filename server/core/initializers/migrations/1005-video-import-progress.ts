import * as Sequelize from 'sequelize'

async function up (utils: {
  transaction: Sequelize.Transaction
  queryInterface: Sequelize.QueryInterface
  sequelize: Sequelize.Sequelize
}): Promise<void> {
  const { queryInterface, transaction } = utils

  await queryInterface.addColumn('videoImport', 'progress', {
    type: Sequelize.INTEGER,
    allowNull: true
  }, { transaction })
}

function down () {
  throw new Error('Not implemented.')
}

export {
  down,
  up
}
