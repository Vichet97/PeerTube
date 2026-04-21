#!/usr/bin/env node
/**
 * Script to diagnose and fix a specific video's move-to-object-storage job
 * 
 * Usage: 
 *   node scripts/fix-specific-video-move.js <video_uuid>
 */

import { database as db } from '../server/core/initializers/database.js'
import { FileStorage } from '@peertube/peertube-models'
import { join } from 'path'

// Configuration
const STORAGE = {
  WEB_VIDEOS_DIR: 'storage/web-videos/',
  STREAMING_PLAYLISTS_DIR: 'storage/streaming-playlists/',
  ORIGINAL_VIDEO_FILES_DIR: 'storage/original-video-files/',
  THUMBNAILS_DIR: 'storage/thumbnails/',
  STORYBOARDS_DIR: 'storage/storyboards/',
  CAPTIONS_DIR: 'storage/captions/',
  TORRENTS_DIR: 'storage/torrents/'
}

async function main() {
  const videoUUID = process.argv[2]

  if (!videoUUID) {
    console.log('Usage: node scripts/fix-specific-video-move.js <video_uuid>')
    console.log('')
    console.log('Example: node scripts/fix-specific-video-move.js e16a7f2c-a1c4-4ac7-88ee-2a6106a98408')
    process.exit(1)
  }

  console.log('=== Diagnosing Move-to-Object-Storage for Video ===')
  console.log(`Video UUID: ${videoUUID}`)
  console.log('')

  try {
    await db.connect()

    // Get video info
    const [videos] = await db.query(`
      SELECT v.*, vji."pendingMove", vji."pendingTranscode", vji."pendingTranscription"
      FROM "video" v
      LEFT JOIN "videoJobInfo" vji ON v.id = vji."videoId"
      WHERE v.uuid = $1
    `, { bind: [videoUUID] })

    if (videos.length === 0) {
      console.log('ERROR: Video not found!')
      process.exit(1)
    }

    const video = videos[0]
    console.log('Video Info:')
    console.log(`  ID: ${video.id}`)
    console.log(`  UUID: ${video.uuid}`)
    console.log(`  State: ${video.state}`)
    console.log(`  pendingMove: ${video.pendingMove || 0}`)
    console.log(`  pendingTranscode: ${video.pendingTranscode || 0}`)
    console.log('')

    // Get video files
    const [files] = await db.query(`
      SELECT vf.*, 'web' as file_type, v.uuid as video_uuid
      FROM "videoFile" vf
      JOIN "video" v ON vf."videoId" = v.id
      WHERE v.uuid = $1
      
      UNION ALL
      
      SELECT vf.*, 'hls' as file_type, v.uuid as video_uuid
      FROM "videoFile" vf
      JOIN "videoStreamingPlaylist" vsp ON vf."videoStreamingPlaylistId" = vsp.id
      JOIN "video" v ON vsp."videoId" = v.id
      WHERE v.uuid = $1
    `, { bind: [videoUUID] })

    console.log('Video Files:')
    console.log('------------')
    
    if (files.length === 0) {
      console.log('No files found for this video.')
    } else {
      for (const file of files) {
        const storageLabel = file.storage === FileStorage.FILE_SYSTEM ? 'LOCAL' : 'OBJECT_STORAGE'
        console.log(`  ${file.filename}`)
        console.log(`    Type: ${file.file_type}`)
        console.log(`    Storage: ${storageLabel}`)
        console.log(`    Resolution: ${file.resolution}p`)
        console.log(`    Size: ${formatBytes(file.size)}`)
        console.log('')
      }
    }

    // Get thumbnails
    const [thumbnails] = await db.query(`
      SELECT t.*
      FROM "thumbnail" t
      WHERE t."videoId" = $1
    `, { bind: [video.id] })

    if (thumbnails.length > 0) {
      console.log('Thumbnails:')
      for (const thumb of thumbnails) {
        const storageLabel = thumb.storage === FileStorage.FILE_SYSTEM ? 'LOCAL' : 'OBJECT_STORAGE'
        console.log(`  ${thumb.filename}: ${storageLabel}`)
      }
      console.log('')
    }

    // Get captions
    const [captions] = await db.query(`
      SELECT vc.*
      FROM "videoCaption" vc
      WHERE vc."videoId" = $1
    `, { bind: [video.id] })

    if (captions.length > 0) {
      console.log('Captions:')
      for (const caption of captions) {
        const storageLabel = caption.storage === FileStorage.FILE_SYSTEM ? 'LOCAL' : 'OBJECT_STORAGE'
        console.log(`  ${caption.filename}: ${storageLabel}`)
      }
      console.log('')
    }

    // Get storyboards
    const [storyboards] = await db.query(`
      SELECT s.*
      FROM "storyboard" s
      WHERE s."videoId" = $1
    `, { bind: [video.id] })

    if (storyboards.length > 0) {
      console.log('Storyboards:')
      for (const storyboard of storyboards) {
        const storageLabel = storyboard.storage === FileStorage.FILE_SYSTEM ? 'LOCAL' : 'OBJECT_STORAGE'
        console.log(`  ${storyboard.filename}: ${storageLabel}`)
      }
      console.log('')
    }

    // Get video source
    const [sources] = await db.query(`
      SELECT vs.*
      FROM "videoSource" vs
      WHERE vs."videoId" = $1
    `, { bind: [video.id] })

    if (sources.length > 0) {
      const source = sources[0]
      const storageLabel = source.storage === FileStorage.FILE_SYSTEM ? 'LOCAL' : 'OBJECT_STORAGE'
      console.log('Video Source:')
      console.log(`  ${source.keptOriginalFilename}: ${storageLabel}`)
      console.log('')
    }

    // Summary
    console.log('=== Summary ===')
    const localFiles = files.filter((f: any) => f.storage === FileStorage.FILE_SYSTEM)
    const objectStorageFiles = files.filter((f: any) => f.storage === FileStorage.OBJECT_STORAGE)
    
    console.log(`Total files: ${files.length}`)
    console.log(`Local files: ${localFiles.length}`)
    console.log(`Object storage files: ${objectStorageFiles.length}`)
    console.log(`pendingMove counter: ${video.pendingMove || 0}`)
    
    if (localFiles.length === 0 && objectStorageFiles.length > 0 && (video.pendingMove || 0) > 0) {
      console.log('')
      console.log('⚠️  INCONSISTENCY DETECTED:')
      console.log('   All files are on object storage, but pendingMove counter is positive.')
      console.log('   This can be fixed by running:')
      console.log(`   UPDATE "videoJobInfo" SET "pendingMove" = 0 WHERE "videoId" = ${video.id};`)
      console.log('')
      console.log('   If video state is TO_MOVE_TO_EXTERNAL_STORAGE, it should also be changed to PUBLISHED.')
    }

    if (localFiles.length > 0 && objectStorageFiles.length === 0 && (video.pendingMove || 0) > 0) {
      console.log('')
      console.log('⚠️  JOB MAY BE RUNNING:')
      console.log('   Files are still on local storage, and pendingMove counter is positive.')
      console.log('   The job may be in progress or stuck.')
      console.log('')
      console.log('   To restart the move job:')
      console.log(`   Check the job queue for video ${videoUUID}`)
      console.log('')
    }

  } catch (err) {
    console.error('Error:', err)
  } finally {
    await db.destroy()
    process.exit(0)
  }
}

function formatBytes(bytes) {
  if (bytes === -1) return 'Unknown'
  if (bytes === 0) return '0 Bytes'
  const k = 1024
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

main()
