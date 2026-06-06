/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from 'chai'
import * as databaseUtils from '@server/helpers/database-utils.js'

describe('database-utils', function () {
  it('should retry generic sequelize database errors', function () {
    expect((databaseUtils as any).isRetryableTransactionError({ name: 'SequelizeDatabaseError' })).to.be.true
  })

  it('should retry postgres serialization and deadlock errors even when not wrapped as SequelizeDatabaseError', function () {
    expect((databaseUtils as any).isRetryableTransactionError({
      name: 'Error',
      parent: { code: '40001', message: 'could not serialize access due to read/write dependencies among transactions' }
    })).to.be.true

    expect((databaseUtils as any).isRetryableTransactionError({
      name: 'Error',
      original: { code: '40P01', message: 'deadlock detected' }
    })).to.be.true
  })

  it('should not retry unrelated errors', function () {
    expect((databaseUtils as any).isRetryableTransactionError(new Error('plain failure'))).to.be.false
  })
})
