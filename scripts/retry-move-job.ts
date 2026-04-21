#!/usr/bin/env node
/**
 * Retry move-to-object-storage job for a specific video
 * 
 * Usage: node scripts/retry-move-job.js <video_uuid>
 */

import { JobQueue } from '../server/core/lib/job-queue/index.js'
import { VideoModel } from '../server/core/models/video/video.js'
import { VideoJobInfoModel } from '../server/core/models/video/video-job-info.js'
import { FileStorage, MoveStoragePayload } from '@peertube/peertube-models'

async function main() {
  const videoUUID = process.argv[2]
  
  if (!videoUUID) {
    console.log('Usage: node scripts/retry-move-job.js <video_uuid>')
    process.exit(1)
  }

  console.log(`Retrying move-to-object-storage job for video: ${videoUUID}`)

  // Initialize the job queue
  JobQueue.Instance.init()
  await JobQueue.Instance.start()

  // Load video to get current state
  const video = await VideoModel.load(videoUUID)
  if (!video) {
    console.error(`Video not found: ${videoUUID}`)
    process.exit(1)
  }

  console.log(`Video state: ${video.state}`)

  // Increment pendingMove counter (in case it was decremented)
  const pendingMove = await VideoJobInfoModel.createOrIncrease(video.uuid, 'pendingMove')
  console.log(`pendingMove counter: ${pendingMove}`)

  // Create move job payload
  const payload: MoveStoragePayload = {
    videoUUID: video.uuid,
    moveVideoState: {
      isNewVideo: false,
      previousVideoState: video.state
    }
  }

  console.log(`Creating move-to-object-storage job...`)

  // Create the job
  const job = await JobQueue.Instance.createJob({
    type: 'move-to-object-storage',
    payload
  })

  console.log(`Job created: ${job.id}`)
  console.log('Job queue initialized and job created.')

  // Give it a moment to start processing
  setTimeout(() => {
    console.log('Done. Check the job queue for progress.')
    process.exit(0)
  }, 2000)
}

main().catch(err => {
  console.error('Error:', err)
  process.exit(1)
})
