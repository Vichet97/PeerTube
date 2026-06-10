import { FileStorage } from '@peertube/peertube-models'
import { retryTransactionWrapper } from '@server/helpers/database-utils.js'
import { logger, LoggerTags, loggerTagsFactory } from '@server/helpers/logger.js'
import { sequelizeTypescript } from '@server/initializers/database.js'
import { federateVideoIfNeeded } from '@server/lib/activitypub/videos/federate.js'
import { VideoPathManager } from '@server/lib/video-path-manager.js'
import { VideoCaptionModel } from '@server/models/video/video-caption.js'
import { VideoStreamingPlaylistModel } from '@server/models/video/video-streaming-playlist.js'
import { VideoModel } from '@server/models/video/video.js'
import { MStreamingPlaylistVideoUUID, MVideoCaption } from '@server/types/models/index.js'

export function pickCaptionsForMoveBatch (options: {
  captions: MVideoCaption[]
  representativeCaptionId: number
  includeAllVideoCaptions?: boolean
}) {
  const { captions, representativeCaptionId, includeAllVideoCaptions = false } = options

  if (!includeAllVideoCaptions) {
    return captions.filter(caption => caption.id === representativeCaptionId)
  }

  const captionsNeedingProcessing = captions.filter(caption => {
    return caption.storage === FileStorage.FILE_SYSTEM || caption.m3u8Filename == null
  })
  if (captionsNeedingProcessing.length !== 0) return captionsNeedingProcessing

  return []
}

export async function moveCaptionToStorage (options: {
  captionId: number
  videoUUID?: string
  includeAllVideoCaptions?: boolean
  loggerTags: LoggerTags['tags']

  moveCaptionFiles: (captions: MVideoCaption[], hls: MStreamingPlaylistVideoUUID) => Promise<void>
}) {
  const {
    loggerTags,
    captionId,
    videoUUID,
    includeAllVideoCaptions = false,
    moveCaptionFiles
  } = options

  const lTagsBase = loggerTagsFactory(...loggerTags)

  const representativeCaption = await VideoCaptionModel.loadWithVideo(captionId)
  const video = representativeCaption?.Video || (videoUUID ? await VideoModel.load(videoUUID) : undefined)

  if (!video) {
    logger.info(`Can't process caption ${captionId}, linked video does not exist anymore.`, lTagsBase())
    return
  }

  const captions = await VideoCaptionModel.listVideoCaptions(video.id)
  const captionsToProcess = pickCaptionsForMoveBatch({
    captions,
    representativeCaptionId: captionId,
    includeAllVideoCaptions
  })

  if (captionsToProcess.length === 0) {
    logger.info(`No caption files need processing for video ${video.uuid}.`, lTagsBase())
    return
  }

  const fileMutexReleaser = await VideoPathManager.Instance.lockFiles(video.uuid)

  const hls = await VideoStreamingPlaylistModel.loadHLSByVideoWithVideo(video.id)

  try {
    await moveCaptionFiles(captionsToProcess, hls)

    await retryTransactionWrapper(() => {
      return sequelizeTypescript.transaction(async t => {
        const videoFull = await VideoModel.loadFull(video.id, t)

        await federateVideoIfNeeded(videoFull, false, t)
      })
    })
  } finally {
    fileMutexReleaser()
  }
}
