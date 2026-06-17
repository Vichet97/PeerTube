import { Job } from 'bullmq'
import { FileStorage, VideoChannelResetPayload, VideoPrivacyType } from '@peertube/peertube-models'
import { CONFIG } from '@server/initializers/config.js'
import { logger } from '@server/helpers/logger.js'
import { Redis } from '@server/lib/redis.js'
import {
  removeCaptionObjectStorage,
  removeHLSObjectStorage,
  removeOriginalFileObjectStorage,
  removeStoryboardObjectStorage,
  removeThumbnailObjectStorage,
  removeTorrentObjectStorage,
  removeWebVideoObjectStorage
} from '@server/lib/object-storage/index.js'
import { VideoChannelModel } from '@server/models/video/video-channel.js'
import { VideoCaptionModel } from '@server/models/video/video-caption.js'
import { VideoModel } from '@server/models/video/video.js'
import { VideoJobInfoModel } from '@server/models/video/video-job-info.js'
import { VideoSourceModel } from '@server/models/video/video-source.js'
import { StoryboardModel } from '@server/models/video/storyboard.js'
import { ThumbnailModel } from '@server/models/video/thumbnail.js'

export async function processVideoChannelReset (job: Job) {
  const payload = job.data as VideoChannelResetPayload

  logger.info('Processing video channel reset in job %s.', job.id)

  const videoChannel = await VideoChannelModel.loadAndPopulateAccount(payload.videoChannelId)

  if (!videoChannel) {
    throw new Error(`Video channel ${payload.videoChannelId} not found`)
  }

  logger.info(`Starting reset of channel "${videoChannel.name}"`)

  const videos = await VideoModel.findAll({
    attributes: [ 'id', 'uuid', 'name' ],
    where: {
      channelId: payload.videoChannelId
    }
  })

  logger.info(`Found ${videos.length} videos to delete in channel "${videoChannel.name}"`)

  let deletedCount = 0
  const errors: string[] = []

  for (const video of videos) {
    try {
      await VideoJobInfoModel.abortAllTasks(video.uuid, 'pendingTranscode')
      await VideoJobInfoModel.abortAllTasks(video.uuid, 'pendingMove')
      await VideoJobInfoModel.abortAllTasks(video.uuid, 'pendingTranscription')

      // Load video with all related data for object storage cleanup
      const videoFull = await VideoModel.loadWithFiles(video.uuid)

      // Clean up object storage files
      await cleanupVideoObjectStorage(videoFull)

      await Redis.Instance.setVideoDeletionFlag(video.uuid)

      try {
        await VideoModel.destroy({
          where: {
            id: video.id
          }
        })
      } catch (err) {
        await Redis.Instance.clearVideoDeletionFlag(video.uuid)
        throw err
      }

      deletedCount++
      logger.debug(`Deleted video "${video.name}" (uuid: ${video.uuid})`)
    } catch (err) {
      const msg = `Failed to delete video "${video.name}" (uuid: ${video.uuid}): ${err}`
      logger.error(msg)
      errors.push(msg)
    }
  }

  // Reset channel statistics
  // Note: totalViews and videosTTL are computed/virtual fields, not stored columns
  // await videoChannel.set('totalViews', 0)
  // await videoChannel.set('videosTTL', null)
  await videoChannel.save()

  const summary = {
    channelId: videoChannel.id,
    channelName: videoChannel.name,
    videosFound: videos.length,
    videosDeleted: deletedCount,
    errors: errors.length,
    errorMessages: errors
  }

  logger.info(`Video channel reset completed: ${deletedCount}/${videos.length} videos deleted from "${videoChannel.name}"`)

  return summary
}

async function cleanupVideoObjectStorage (video: MVideoFullForCleanup) {
  const cleanupErrors: string[] = []

  // Clean up Web Video files
  for (const file of (video.VideoFiles || [])) {
    if (CONFIG.OBJECT_STORAGE.ENABLED && file.storage === FileStorage.OBJECT_STORAGE) {
      try {
        await removeWebVideoObjectStorage(file)
        logger.debug(`Removed web video file ${file.filename} from object storage`)
      } catch (err) {
        const msg = `Failed to remove web video file ${file.filename} from object storage: ${err}`
        logger.error(msg)
        cleanupErrors.push(msg)
      }
    }

    // Clean up torrent files
    if (CONFIG.OBJECT_STORAGE.ENABLED && file.torrentFilename) {
      try {
        await removeTorrentObjectStorage(file.torrentFilename)
        logger.debug(`Removed torrent file ${file.torrentFilename} from object storage`)
      } catch (err) {
        const msg = `Failed to remove torrent file ${file.torrentFilename} from object storage: ${err}`
        logger.error(msg)
        cleanupErrors.push(msg)
      }
    }
  }

  // Clean up HLS playlists
  for (const playlist of (video.VideoStreamingPlaylists || [])) {
    if (CONFIG.OBJECT_STORAGE.ENABLED && playlist.storage === FileStorage.OBJECT_STORAGE) {
      try {
        await removeHLSObjectStorage(video)
        logger.debug(`Removed HLS playlist for video ${video.uuid} from object storage`)
      } catch (err) {
        const msg = `Failed to remove HLS playlist from object storage: ${err}`
        logger.error(msg)
        cleanupErrors.push(msg)
      }
    }

    // Clean up HLS video files torrents
    for (const file of (playlist.VideoFiles || [])) {
      if (CONFIG.OBJECT_STORAGE.ENABLED && file.torrentFilename) {
        try {
          await removeTorrentObjectStorage(file.torrentFilename)
          logger.debug(`Removed HLS torrent file ${file.torrentFilename} from object storage`)
        } catch (err) {
          const msg = `Failed to remove HLS torrent file ${file.torrentFilename} from object storage: ${err}`
          logger.error(msg)
          cleanupErrors.push(msg)
        }
      }
    }
  }

  // Clean up original video file
  const source = await VideoSourceModel.loadLatest(video.id)
  if (CONFIG.OBJECT_STORAGE.ENABLED && source?.keptOriginalFilename && source.storage === FileStorage.OBJECT_STORAGE) {
    try {
      await removeOriginalFileObjectStorage(source)
      logger.debug(`Removed original file ${source.keptOriginalFilename} from object storage`)
    } catch (err) {
      const msg = `Failed to remove original file from object storage: ${err}`
      logger.error(msg)
      cleanupErrors.push(msg)
    }
  }

  // Clean up captions
  const captions = await VideoCaptionModel.listVideoCaptions(video.id)
  for (const caption of captions) {
    if (CONFIG.OBJECT_STORAGE.ENABLED && caption.storage === FileStorage.OBJECT_STORAGE) {
      try {
        await removeCaptionObjectStorage(caption)
        logger.debug(`Removed caption ${caption.filename} from object storage`)
      } catch (err) {
        const msg = `Failed to remove caption ${caption.filename} from object storage: ${err}`
        logger.error(msg)
        cleanupErrors.push(msg)
      }
    }
  }

  // Clean up thumbnails
  const thumbnails = await ThumbnailModel.findAll({ where: { videoId: video.id } })
  for (const thumbnail of thumbnails) {
    if (CONFIG.OBJECT_STORAGE.ENABLED && thumbnail.storage === FileStorage.OBJECT_STORAGE) {
      try {
        await removeThumbnailObjectStorage(thumbnail)
        logger.debug(`Removed thumbnail ${thumbnail.filename} from object storage`)
      } catch (err) {
        const msg = `Failed to remove thumbnail ${thumbnail.filename} from object storage: ${err}`
        logger.error(msg)
        cleanupErrors.push(msg)
      }
    }
  }

  // Clean up storyboards
  const storyboards = await StoryboardModel.findAll({ where: { videoId: video.id } })
  for (const storyboard of storyboards) {
    if (CONFIG.OBJECT_STORAGE.ENABLED && storyboard.storage === FileStorage.OBJECT_STORAGE) {
      try {
        await removeStoryboardObjectStorage(storyboard)
        logger.debug(`Removed storyboard ${storyboard.filename} from object storage`)
      } catch (err) {
        const msg = `Failed to remove storyboard ${storyboard.filename} from object storage: ${err}`
        logger.error(msg)
        cleanupErrors.push(msg)
      }
    }
  }

  if (cleanupErrors.length > 0) {
    logger.warn(`Object storage cleanup had ${cleanupErrors.length} errors for video ${video.uuid}`, { errors: cleanupErrors })
  }
}

type MVideoFullForCleanup = {
  id: number
  uuid: string
  name: string
  privacy: VideoPrivacyType
  hasPrivateStaticPath: () => boolean
  VideoFiles?: any[]
  VideoStreamingPlaylists?: any[]
}
