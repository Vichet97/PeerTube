import { readdir } from 'fs/promises'
import { join } from 'path'
import { QueryTypes } from 'sequelize'
import { currentDir } from '@peertube/peertube-node-utils'
import { getNodeABIVersion } from '@server/helpers/version.js'
import { logger } from '../helpers/logger.js'
import { LAST_MIGRATION_VERSION } from './constants.js'
import { sequelizeTypescript } from './database.js'

async function migrate () {
  await cleanupDatabaseBeforeMigrationIfNeeded()

  const tables = await sequelizeTypescript.getQueryInterface().showAllTables()

  // No tables, we don't need to migrate anything
  // The installer will do that
  if (tables.length === 0) return

  let actualVersion: number | null = null

  const query = 'SELECT "migrationVersion" FROM "application"'
  const options = {
    type: QueryTypes.SELECT as QueryTypes.SELECT
  }

  const rows = await sequelizeTypescript.query<{ migrationVersion: number }>(query, options)
  if (rows?.[0]?.migrationVersion) {
    actualVersion = rows[0].migrationVersion
  }

  if (actualVersion === null) {
    await sequelizeTypescript.query(
      'INSERT INTO "application" ("migrationVersion", "nodeVersion", "nodeABIVersion") VALUES (0, :nodeVersion, :nodeABIVersion)',
      {
        replacements: {
          nodeVersion: process.version,
          nodeABIVersion: getNodeABIVersion()
        }
      }
    )
    actualVersion = 0
  }

  // No need migrations, abort
  if (actualVersion >= LAST_MIGRATION_VERSION) return

  // If there are a new migration scripts
  logger.info('Begin migrations.')

  const migrationScripts = await getMigrationScripts()

  for (const migrationScript of migrationScripts) {
    try {
      await executeMigration(actualVersion, migrationScript)
    } catch (err) {
      logger.error('Cannot execute migration %s.', migrationScript.version, { err })
      process.exit(-1)
    }
  }

  logger.info('Migrations finished. New migration version schema: %s', LAST_MIGRATION_VERSION)
}

// ---------------------------------------------------------------------------

export {
  migrate
}

// ---------------------------------------------------------------------------

async function getMigrationScripts () {
  const files = await readdir(join(currentDir(import.meta.url), 'migrations'))
  const filesToMigrate: {
    version: string
    script: string
  }[] = []

  files
    .filter(file => file.endsWith('.js'))
    .forEach(file => {
      // Filename is something like 'version-blabla.js'
      const version = file.split('-')[0]
      filesToMigrate.push({
        version,
        script: file
      })
    })

  return filesToMigrate
}

async function cleanupDatabaseBeforeMigrationIfNeeded () {
  if (process.env.PT_CLEAN_DATABASE_BEFORE_MIGRATION !== 'true') return

  logger.warn('PT_CLEAN_DATABASE_BEFORE_MIGRATION=true: dropping all public tables before migrations.')

  const tableRows = await sequelizeTypescript.query<{ tablename: string }>(
    `SELECT tablename FROM pg_tables WHERE schemaname = 'public'`,
    { type: QueryTypes.SELECT as QueryTypes.SELECT }
  )

  for (const row of tableRows) {
    const tableName = row.tablename.replace(/"/g, '""')
    await sequelizeTypescript.query(`DROP TABLE IF EXISTS "${tableName}" CASCADE`)
  }
}

async function executeMigration (actualVersion: number, entity: { version: string, script: string }) {
  const versionScript = parseInt(entity.version, 10)

  // Do not execute old migration scripts
  if (versionScript <= actualVersion) return undefined

  // Load the migration module and run it
  const migrationScriptName = entity.script
  logger.info('Executing %s migration script.', migrationScriptName)

  const migrationScript = await import(join(currentDir(import.meta.url), 'migrations', migrationScriptName))

  try {
    return await sequelizeTypescript.transaction(async t => {
      const options = {
        transaction: t,
        queryInterface: sequelizeTypescript.getQueryInterface(),
        sequelize: sequelizeTypescript
      }

      await migrationScript.up(options)

      // Update the new migration version
      await sequelizeTypescript.query('UPDATE "application" SET "migrationVersion" = ' + versionScript, { transaction: t })
    })
  } catch (err) {
    if (!isAlreadyExistsMigrationError(err)) throw err

    logger.warn('Ignoring migration %s failure because schema object already exists. Marking migration as applied.', migrationScriptName, { err })
    await sequelizeTypescript.query('UPDATE "application" SET "migrationVersion" = ' + versionScript)
    return undefined
  }
}

function isAlreadyExistsMigrationError (err: unknown) {
  const error = err as {
    message?: string
    parent?: {
      code?: string
      message?: string
    }
    original?: {
      code?: string
      message?: string
    }
  }

  const code = error.parent?.code || error.original?.code || ''
  if (code === '42701' || code === '42P07' || code === '42710') return true

  const messages = [ error.message, error.parent?.message, error.original?.message ]
    .filter((m): m is string => !!m)

  return messages.some(m => m.toLowerCase().includes('already exists'))
}
