import { FileStorage, type FileStorageType } from '@peertube/peertube-models'
import { ffprobePromise, getVideoStreamDimensionsInfo } from '@peertube/peertube-ffmpeg'
import { retryTransactionWrapper } from '@server/helpers/database-utils.js'
import { LoggerTags, logger } from '@server/helpers/logger.js'
import { deleteFileAndCatch } from '@server/helpers/utils.js'
import { checkObjectStorageReadiness, isTransientObjectStorageError, storeStoryboard } from '@server/lib/object-storage/index.js'
import { removeStoryboardObjectStorageByFilename } from '@server/lib/object-storage/videos.js'
import { CONFIG } from '@server/initializers/config.js'
import { STORYBOARD } from '@server/initializers/constants.js'
import { sequelizeTypescript } from '@server/initializers/database.js'
import { StoryboardModel } from '@server/models/video/storyboard.js'
import { VideoModel } from '@server/models/video/video.js'
import { MVideo } from '@server/types/models/index.js'
import { federateVideoIfNeeded } from './activitypub/videos/federate.js'

export async function buildSpriteSize (videoPath: string) {
  const probe = await ffprobePromise(videoPath)
  const videoStreamInfo = await getVideoStreamDimensionsInfo(videoPath, probe)

  if (videoStreamInfo.isPortraitMode) {
    return {
      spriteHeight: STORYBOARD.SPRITE_MAX_SIZE,
      spriteWidth: Math.round(STORYBOARD.SPRITE_MAX_SIZE * videoStreamInfo.ratio)
    }
  }

  return {
    spriteWidth: STORYBOARD.SPRITE_MAX_SIZE,
    spriteHeight: Math.round(STORYBOARD.SPRITE_MAX_SIZE / videoStreamInfo.ratio)
  }
}

export function buildTotalSprites (video: MVideo) {
  if (video.duration < 3) return { spriteDuration: undefined, totalSprites: 0 }

  const maxSprites = Math.min(Math.ceil(video.duration), STORYBOARD.SPRITES_MAX_EDGE_COUNT * STORYBOARD.SPRITES_MAX_EDGE_COUNT)

  const spriteDuration = Math.ceil(video.duration / maxSprites)
  const totalSprites = Math.ceil(video.duration / spriteDuration)

  // We can generate a single line so we don't need a prime number
  if (totalSprites <= STORYBOARD.SPRITES_MAX_EDGE_COUNT) return { spriteDuration, totalSprites }

  return { spriteDuration, totalSprites }
}

export function findGridSize (options: {
  toFind: number
  maxEdgeCount: number
}) {
  const { toFind, maxEdgeCount } = options

  for (let i = 1; i <= maxEdgeCount; i++) {
    for (let j = i; j <= maxEdgeCount; j++) {
      if (toFind <= i * j) return { width: j, height: i }
    }
  }

  throw new Error(`Could not find grid size (to find: ${toFind}, max edge count: ${maxEdgeCount}`)
}

export async function insertStoryboardInDatabase (options: {
  videoUUID: string
  lTags: LoggerTags
  filename: string
  destination: string
  imageSize: { width: number, height: number }
  spriteHeight: number
  spriteWidth: number
  spriteDuration: number
  storage?: FileStorageType
  federate: boolean
}) {
  const {
    videoUUID,
    lTags,
    imageSize,
    spriteHeight,
    spriteWidth,
    spriteDuration,
    destination,
    filename,
    federate,
    storage = FileStorage.FILE_SYSTEM
  } = options

  await retryTransactionWrapper(() => {
    return sequelizeTypescript.transaction(async transaction => {
      const video = await VideoModel.loadFull(videoUUID, transaction)
      if (!video) {
        logger.info(`Video ${videoUUID} does not exist anymore, skipping storyboard generation.`, lTags)
        deleteFileAndCatch(destination)
        if (storage === FileStorage.OBJECT_STORAGE) {
          removeStoryboardObjectStorageByFilename(filename)
            .catch(err => {
              logger.warn(
                'Cannot remove orphaned storyboard %s from object storage (video was deleted).',
                filename,
                { err, ...lTags }
              )
            })
        }
        return
      }

      const existing = await StoryboardModel.loadByVideo(video.id, transaction)
      if (existing) await existing.destroy({ transaction })

      await StoryboardModel.create({
        filename,
        totalHeight: imageSize.height,
        totalWidth: imageSize.width,
        spriteHeight,
        spriteWidth,
        spriteDuration,
        storage,
        videoId: video.id,
        cached: false
      }, { transaction })

      if (federate) {
        await federateVideoIfNeeded(video, false, transaction)
      }
    })
  })
}

export async function storeStoryboardInObjectStorageWithDeps (options: {
  inputPath: string
  filename: string
  lTags: LoggerTags
}, deps: {
  storeStoryboard: (inputPath: string, filename: string) => Promise<void>
  checkObjectStorageReadiness: (options: {
    key: string
    bucketInfo: typeof CONFIG.OBJECT_STORAGE.STORYBOARDS
    maxRetries?: number
    retryIntervalMs?: number
  }) => Promise<boolean>
}) {
  const { inputPath, filename, lTags } = options
  const { storeStoryboard, checkObjectStorageReadiness } = deps

  await retryTransientStoryboardObjectStorageStep({
    description: `uploading storyboard ${filename}`,
    lTags,
    run: () => storeStoryboard(inputPath, filename)
  })

  const isReady = await checkObjectStorageReadiness(
    {
      key: filename,
      bucketInfo: CONFIG.OBJECT_STORAGE.STORYBOARDS,
      maxRetries: 30,
      retryIntervalMs: 10000
    } as Parameters<typeof checkObjectStorageReadiness>[0]
  )

  if (!isReady) {
    throw new Error(`Storyboard ${filename} did not become ready in object storage after readiness checks.`)
  }
}

export function storeStoryboardInObjectStorage (options: {
  inputPath: string
  filename: string
  lTags: LoggerTags
}) {
  return storeStoryboardInObjectStorageWithDeps(options, {
    storeStoryboard,
    checkObjectStorageReadiness
  })
}

async function retryTransientStoryboardObjectStorageStep<T> (options: {
  description: string
  lTags: LoggerTags
  run: () => Promise<T>
  maxAttempts?: number
  delayMs?: number
}) {
  const { description, lTags, run, maxAttempts = 4, delayMs = 2000 } = options

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await run()
    } catch (err) {
      if (attempt >= maxAttempts || !isTransientObjectStorageError(err)) throw err

      logger.warn(
        'Transient object storage error while %s, retrying in %dms (attempt %d/%d).',
        description,
        delayMs,
        attempt,
        maxAttempts,
        { err, ...lTags }
      )

      await new Promise(resolve => setTimeout(resolve, delayMs))
    }
  }

  throw new Error(`Storyboard object storage step ${description} failed without a final error.`)
}
