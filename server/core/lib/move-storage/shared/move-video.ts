import { FileStorage, FileStorageType } from '@peertube/peertube-models'
import { LoggerTags, logger, loggerTagsFactory } from '@server/helpers/logger.js'
import { CONFIG } from '@server/initializers/config.js'
import { getHLSDirectory } from '@server/lib/paths.js'
import { VideoPathManager } from '@server/lib/video-path-manager.js'
import { VideoCaptionModel } from '@server/models/video/video-caption.js'
import { VideoJobInfoModel } from '@server/models/video/video-job-info.js'
import { VideoSourceModel } from '@server/models/video/video-source.js'
import { VideoModel } from '@server/models/video/video.js'
import {
  MStreamingPlaylistVideoUUID,
  MVideo,
  MVideoCaption,
  MVideoWithAllFiles,
  MThumbnail,
  MStoryboard
} from '@server/types/models/index.js'
import { MVideoSource } from '@server/types/models/video/video-source.js'
import { StoryboardModel } from '@server/models/video/storyboard.js'
import { ThumbnailModel } from '@server/models/video/thumbnail.js'
import { pathExists } from 'fs-extra/esm'
import { join } from 'path'

export async function moveVideoToStorage (options: {
  videoUUID: string
  loggerTags: LoggerTags['tags']

  targetStorage: FileStorageType

  moveWebVideoFiles: (video: MVideoWithAllFiles) => Promise<void>
  moveHLSFiles: (video: MVideoWithAllFiles, options?: {
    onInitialCutoverReady?: (options: { playlistId: number, fileIds: number[] }) => Promise<void>
  }) => Promise<boolean> // Returns true if initial HLS cutover was deferred to a follow-up job
  moveVideoSourceFile: (source: MVideoSource) => Promise<void>
  moveCaptionFiles: (captions: MVideoCaption[], hls: MStreamingPlaylistVideoUUID) => Promise<void>
  moveThumbnailFiles?: (thumbnails: MThumbnail[]) => Promise<void>
  moveStoryboardFiles?: (storyboards: MStoryboard[]) => Promise<void>
  moveTorrentFiles?: (video: MVideoWithAllFiles) => Promise<void>
  onInitialHLSCutoverReady?: (options: { playlistId: number, fileIds: number[] }) => Promise<void>
  onProgress?: (percent: number) => void
}) {
  const {
    loggerTags,
    videoUUID,
    moveVideoSourceFile,
    moveHLSFiles,
    moveWebVideoFiles,
    moveCaptionFiles,
    moveThumbnailFiles,
    moveStoryboardFiles,
    moveTorrentFiles,
    targetStorage
  } = options

  const lTagsBase = loggerTagsFactory(...loggerTags)

  const fileMutexReleaser = await VideoPathManager.Instance.lockFiles(videoUUID)

  const video = await VideoModel.loadWithFiles(videoUUID)
  // No video, maybe deleted?
  if (!video) {
    logger.info(`Can't move video ${videoUUID}, video does not exist.`, lTagsBase(videoUUID))
    fileMutexReleaser()
    return false
  }

  const lTags = lTagsBase(video.uuid, video.url)

  try {
    // Early exit if nothing to move - avoid expensive operations when files are already on target storage
    const hasResourcesToMove = await hasVideoResourcesToBeMoved(video, targetStorage)
    if (!hasResourcesToMove) {
      logger.info(`Video ${video.uuid} already on target storage, skipping move.`, lTags)

      const pendingMove = await VideoJobInfoModel.decrease(video.uuid, 'pendingMove')
      logger.info(`Decreased pendingMove counter for ${video.uuid}. Remaining: ${pendingMove}.`, lTags)

      fileMutexReleaser()
      return false
    }

    const { source, captions, hls, webFiles, thumbnails, storyboards } = await filterVideoResourcesToBeMoved(video, targetStorage)
    const hasTorrentResources = moveTorrentFiles
      ? await hasTorrentResourcesToBeMoved(video, targetStorage)
      : false

    const stages = [
      captions.length !== 0,
      !!source,
      webFiles.length !== 0,
      !!hls,
      thumbnails.length !== 0 && !!moveThumbnailFiles,
      storyboards.length !== 0 && !!moveStoryboardFiles,
      hasTorrentResources
    ]

    const totalStages = stages.filter(Boolean).length
    let completedStages = 0

    const updateStageProgress = () => {
      if (!options.onProgress || totalStages === 0) return

      const percent = 5 + Math.round((completedStages / totalStages) * 90)
      options.onProgress(Math.min(percent, 95))
    }

    if (options.onProgress) {
      options.onProgress(totalStages === 0 ? 100 : 5)
    }

    if (captions.length !== 0) {
      logger.debug(`Moving ${captions.length} captions of ${video.uuid}.`, lTags)

      const hls = video.getHLSPlaylist()
      await moveCaptionFiles(captions, hls)
      completedStages++
      updateStageProgress()
    }

    if (source) {
      logger.debug(`Moving video source ${source.keptOriginalFilename} file of video ${video.uuid}`, lTags)

      await moveVideoSourceFile(source)
      completedStages++
      updateStageProgress()
    }

    if (webFiles.length !== 0) {
      logger.debug(`Moving ${webFiles.length} web video files for video ${video.uuid}.`, lTags)

      await moveWebVideoFiles(video)
      completedStages++
      updateStageProgress()
    }

    let hlsCutoverDeferred = false
    if (hls) {
      logger.debug(`Moving HLS playlist of ${video.uuid}.`, lTags)

      hlsCutoverDeferred = await moveHLSFiles(video, {
        onInitialCutoverReady: options.onInitialHLSCutoverReady
      })
      completedStages++
      updateStageProgress()
    }

    if (thumbnails.length !== 0 && moveThumbnailFiles) {
      logger.debug(`Moving ${thumbnails.length} thumbnails of ${video.uuid}.`, lTags)

      await moveThumbnailFiles(thumbnails)
      completedStages++
      updateStageProgress()
    }

    if (storyboards.length !== 0 && moveStoryboardFiles) {
      logger.debug(`Moving ${storyboards.length} storyboards of ${video.uuid}.`, lTags)

      await moveStoryboardFiles(storyboards)
      completedStages++
      updateStageProgress()
    }

    if (moveTorrentFiles && hasTorrentResources) {
      logger.debug(`Moving torrent files of ${video.uuid}.`, lTags)

      await moveTorrentFiles(video)
      completedStages++
      updateStageProgress()
    }

    // Only decrement pendingMove if HLS cutover was NOT deferred to a follow-up job.
    // If hlsCutoverDeferred is true, the follow-up job (finalizeInitialHLSCutover) will handle it.
    if (!hlsCutoverDeferred) {
      const pendingMove = await VideoJobInfoModel.decrease(video.uuid, 'pendingMove')
      logger.info(`Moved video ${video.uuid}. Remaining pending move: ${pendingMove}.`, lTags)
    } else {
      logger.info(`Video ${video.uuid} has deferred HLS cutover to follow-up job.`, lTags)
    }
  } finally { // Error handling is managed by the job queue
    fileMutexReleaser()
  }

  return false
}

export async function onMoveVideoToStorageFailure (options: {
  videoUUID: string
  err: any
  loggerTags: LoggerTags['tags']
  moveToFailedState: (video: MVideoWithAllFiles) => Promise<void>
}) {
  const { videoUUID, err, loggerTags, moveToFailedState } = options

  const video = await VideoModel.loadWithFiles(videoUUID)
  if (!video) return

  logger.error(`Cannot move video ${video.url} storage.`, { err, tags: loggerTags })

  await moveToFailedState(video)
  await VideoJobInfoModel.abortAllTasks(video.uuid, 'pendingMove')
}

export async function filterVideoResourcesToBeMoved (videoArg: MVideo, targetStorage: FileStorageType) {
  const video = await VideoModel.loadFull(videoArg.id)
  const captions = await VideoCaptionModel.listVideoCaptions(video.id)
  const source = await VideoSourceModel.loadLatest(video.id)
  const thumbnails = await ThumbnailModel.findAll({ where: { videoId: video.id } })
  const storyboards = await StoryboardModel.findAll({ where: { videoId: video.id } })

  const hls = video.getHLSPlaylist()

  const moveHLS = hls && (hls.storage !== targetStorage || hls.VideoFiles.some(f => f.storage !== targetStorage))

  // Only include HLS if the master playlist file exists on disk.
  // HLS transcoding may still be in progress (e.g. Web Video finished first), so DB playlistFilename can be stale.
  let hlsReady = false
  if (moveHLS && hls.storage === FileStorage.FILE_SYSTEM) {
    const masterPlaylistPath = join(getHLSDirectory(video), hls.playlistFilename)
    hlsReady = await pathExists(masterPlaylistPath)
  } else if (moveHLS) {
    hlsReady = true
  }

  return {
    source: source?.keptOriginalFilename && source.storage !== targetStorage
      ? source
      : undefined,

    hls: moveHLS && hlsReady
      ? hls
      : undefined,

    webFiles: video.VideoFiles.filter(f => f.storage !== targetStorage),
    captions: captions.filter(c => {
      if (c.storage !== targetStorage) return true

      if (hls) {
        if (targetStorage === FileStorage.OBJECT_STORAGE) return !c.m3u8Filename || !c.m3u8Url
        else if (targetStorage === FileStorage.FILE_SYSTEM) return !c.m3u8Filename || c.m3u8Url
      }

      return false
    }),

    thumbnails: thumbnails.filter(t => t.storage !== targetStorage),
    storyboards: storyboards.filter(s => s.storage !== targetStorage)
  }
}

export async function hasVideoResourcesToBeMoved (video: MVideo, targetStorage: FileStorageType) {
  const { captions, hls, source, webFiles, thumbnails, storyboards } = await filterVideoResourcesToBeMoved(video, targetStorage)
  const hasTorrentResources = await hasTorrentResourcesToBeMoved(video, targetStorage)

  return (
    captions.length !== 0 ||
    !!hls ||
    !!source ||
    webFiles.length !== 0 ||
    thumbnails.length !== 0 ||
    storyboards.length !== 0 ||
    hasTorrentResources
  )
}

async function hasTorrentResourcesToBeMoved (video: MVideo, targetStorage: FileStorageType) {
  if (targetStorage !== FileStorage.OBJECT_STORAGE) return false
  if (!video.id) return false

  const videoWithFiles = await VideoModel.loadWithFiles(video.id)
  if (!videoWithFiles) return false

  const torrentFilenames = new Set<string>()

  for (const file of videoWithFiles.VideoFiles) {
    if (file.torrentFilename) torrentFilenames.add(file.torrentFilename)
  }

  for (const playlist of videoWithFiles.VideoStreamingPlaylists || []) {
    for (const file of playlist.VideoFiles) {
      if (file.torrentFilename) torrentFilenames.add(file.torrentFilename)
    }
  }

  for (const torrentFilename of torrentFilenames) {
    const torrentPath = join(CONFIG.STORAGE.TORRENTS_DIR, torrentFilename)
    if (await pathExists(torrentPath)) return true
  }

  return false
}
