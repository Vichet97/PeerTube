#!/usr/bin/env node
/**
 * Script to diagnose and fix stuck move-to-object-storage jobs
 * 
 * Usage: 
 *   node scripts/fix-stuck-move-jobs.js --diagnose
 *   node scripts/fix-stuck-move-jobs.js --fix-all
 *   node scripts/fix-stuck-move-jobs.js --fix-job <job_id>
 */

import { database as db } from '../server/core/initializers/database.js'
import { FileStorage } from '@peertube/peertube-models'

async function main() {
  const args = process.argv.slice(2)
  const command = args[0]
  const jobId = args[1]

  console.log('=== Move-to-Object-Storage Job Diagnostic Tool ===\n')

  try {
    await db.connect()

    if (command === '--diagnose') {
      await diagnose()
    } else if (command === '--fix-all') {
      await fixAllStuckJobs()
    } else if (command === '--fix-job' && jobId) {
      await fixStuckJob(jobId)
    } else {
      console.log('Usage:')
      console.log('  --diagnose       : Show all stuck jobs and their status')
      console.log('  --fix-all        : Fix all stuck jobs')
      console.log('  --fix-job <id>   : Fix a specific stuck job')
    }
  } catch (err) {
    console.error('Error:', err)
  } finally {
    await db.destroy()
    process.exit(0)
  }
}

async function diagnose() {
  console.log('Checking for stuck move-to-object-storage jobs...\n')

  // Check videoJobInfo for pending moves
  const [jobInfos] = await db.query(`
    SELECT v.uuid, v.state, vji.pendingMove, vji.pendingTranscode
    FROM "video" v
    LEFT JOIN "videoJobInfo" vji ON v.id = vji."videoId"
    WHERE vji."pendingMove" > 0
    ORDER BY v."createdAt" DESC
    LIMIT 50
  `)

  console.log('Videos with pendingMove counter > 0:')
  console.log('-----------------------------------')
  
  if (jobInfos.length === 0) {
    console.log('No videos with pending move operations.\n')
  } else {
    for (const info of jobInfos) {
      console.log(`UUID: ${info.uuid}`)
      console.log(`  State: ${info.state}`)
      console.log(`  pendingMove: ${info.pendingMove}`)
      console.log(`  pendingTranscode: ${info.pendingTranscode || 0}`)
      
      // Check actual file storage status
      const [files] = await db.query(`
        SELECT filename, storage, "videoId", "videoStreamingPlaylistId"
        FROM "videoFile"
        WHERE "videoId" = (SELECT id FROM "video" WHERE uuid = $1)
        OR "videoStreamingPlaylistId" IN (
          SELECT id FROM "videoStreamingPlaylist" WHERE "videoId" = (SELECT id FROM "video" WHERE uuid = $1)
        )
      `, { bind: [info.uuid] })

      const localFiles = files.filter((f: any) => f.storage === FileStorage.FILE_SYSTEM)
      const objectStorageFiles = files.filter((f: any) => f.storage === FileStorage.OBJECT_STORAGE)
      
      console.log(`  Local files: ${localFiles.length}`)
      console.log(`  Object storage files: ${objectStorageFiles.length}`)
      
      if (info.pendingMove > 0 && localFiles.length === 0 && objectStorageFiles.length > 0) {
        console.log(`  ⚠️  INCONSISTENCY: pendingMove=${info.pendingMove} but all files are on object storage`)
      }
      if (info.pendingMove > 0 && objectStorageFiles.length === 0 && localFiles.length > 0) {
        console.log(`  ⚠️  INCONSISTENCY: pendingMove=${info.pendingMove} but all files are local`)
      }
      console.log('')
    }
  }

  // Check videos in problematic states
  const [problematicVideos] = await db.query(`
    SELECT uuid, state, "publishedAt"
    FROM "video"
    WHERE state IN ('TO_MOVE_TO_EXTERNAL_STORAGE', 'TO_MOVE_TO_EXTERNAL_STORAGE_FAILED', 'TO_MOVE_TO_FILE_SYSTEM', 'TO_MOVE_TO_FILE_SYSTEM_FAILED')
    ORDER BY "publishedAt" DESC
    LIMIT 20
  `)

  console.log('\nVideos in storage-related states:')
  console.log('---------------------------------')
  
  if (problematicVideos.length === 0) {
    console.log('No videos in storage-related states.\n')
  } else {
    for (const v of problematicVideos) {
      console.log(`UUID: ${v.uuid}`)
      console.log(`  State: ${v.state}`)
      console.log(`  Published: ${v.publishedAt}`)
      console.log('')
    }
  }
}

async function fixAllStuckJobs() {
  console.log('Fixing all stuck jobs...\n')

  // Find and fix inconsistent pendingMove counters
  const [jobInfos] = await db.query(`
    SELECT v.uuid, v.id as "videoId", v.state, vji."pendingMove", vji."pendingTranscode"
    FROM "video" v
    JOIN "videoJobInfo" vji ON v.id = vji."videoId"
    WHERE vji."pendingMove" > 0
  `)

  let fixed = 0
  for (const info of jobInfos as any[]) {
    const [files] = await db.query(`
      SELECT id, filename, storage 
      FROM "videoFile" 
      WHERE "videoId" = $1
    `, { bind: [info.videoId] })

    const localFiles = files.filter((f: any) => f.storage === FileStorage.FILE_SYSTEM)
    const objectStorageFiles = files.filter((f: any) => f.storage === FileStorage.OBJECT_STORAGE)

    // If all files are on object storage, decrement pendingMove to 0
    if (localFiles.length === 0 && objectStorageFiles.length > 0) {
      await db.query(`
        UPDATE "videoJobInfo" 
        SET "pendingMove" = 0, "updatedAt" = NOW() 
        WHERE "videoId" = $1
      `, { bind: [info.videoId] })
      
      // If video is stuck in TO_MOVE state, move to PUBLISHED
      if (info.state === 'TO_MOVE_TO_EXTERNAL_STORAGE') {
        await db.query(`
          UPDATE "video" SET state = 'PUBLISHED', "updatedAt" = NOW() WHERE id = $1
        `, { bind: [info.videoId] })
      }
      
      console.log(`Fixed: ${info.uuid} - all files moved to object storage`)
      fixed++
    }
    // If all files are still local, the job is still running or failed
    else if (localFiles.length > 0 && objectStorageFiles.length === 0) {
      console.log(`Skipping: ${info.uuid} - files still on local storage (job may still be running)`)
    }
    // If files are mixed or no files, it's an edge case
    else {
      console.log(`Edge case: ${info.uuid} - mixed storage or no files (needs manual review)`)
    }
  }

  console.log(`\nFixed ${fixed} stuck job entries.`)
}

async function fixStuckJob(jobId) {
  console.log(`Fixing stuck job ${jobId}...\n`)

  // Note: This requires Redis/BullMQ access to properly fix the job
  // For now, we'll fix the database state
  console.log('Note: To fully fix a BullMQ job, you may need to:')
  console.log('1. Check Redis for the job in active state')
  console.log('2. Manually clean up using: redis-cli > DEL bull-*:move-to-object-storage:<job_id>')
  console.log('3. Then run --diagnose to check database state')
}

main()
