import { getResolutionLabel } from '@peertube/peertube-core-utils'
import {
  Video,
  VideoAdditionalAttributes,
  VideoDetails,
  VideoFile,
  VideoInclude,
  VideosCommonQueryAfterSanitize,
  VideoStreamingPlaylist
} from '@peertube/peertube-models'
import { uuidToShort } from '@peertube/peertube-node-utils'
import { tracer } from '@server/lib/opentelemetry/tracing.js'
import { getHLSResolutionPlaylistFilename } from '@server/lib/paths.js'
import { getLocalVideoFileMetadataUrl } from '@server/lib/video-urls.js'
import { VideoViewsManager } from '@server/lib/views/video-views-manager.js'
import { generateMagnetUri } from '@server/lib/webtorrent.js'
import { isArray } from '../../../helpers/custom-validators/misc.js'
import {
  VIDEO_CATEGORIES,
  VIDEO_COMMENTS_POLICY,
  VIDEO_EMBED_PRIVACY_POLICIES,
  VIDEO_LANGUAGES,
  VIDEO_LICENCES,
  VIDEO_PRIVACIES,
  VIDEO_STATES
} from '../../../initializers/constants.js'
import { MServer, MStreamingPlaylistRedundanciesOpt, MVideoFormattable, MVideoFormattableDetails } from '../../../types/models/index.js'
import { MVideoFile } from '../../../types/models/video/video-file.js'
import { sortByResolutionDesc } from './shared/index.js'

export type VideoFormattingJSONOptions = {
  completeDescription?: boolean

  additionalAttributes?: {
    state?: boolean
    waitTranscoding?: boolean
    scheduledUpdate?: boolean
    blacklistInfo?: boolean
    files?: boolean
    source?: boolean
    blockedOwner?: boolean
    automaticTags?: boolean
    liveSchedules?: boolean
  }
}

export function guessAdditionalAttributesFromQuery (
  query: Pick<VideosCommonQueryAfterSanitize, 'include' | 'includeScheduledLive'>
): VideoFormattingJSONOptions {
  return {
    additionalAttributes: {
      state: query.includeScheduledLive || !!(query.include & VideoInclude.NOT_PUBLISHED_STATE),
      waitTranscoding: !!(query.include & VideoInclude.NOT_PUBLISHED_STATE),
      scheduledUpdate: !!(query.include & VideoInclude.NOT_PUBLISHED_STATE),
      blacklistInfo: !!(query.include & VideoInclude.BLACKLISTED),
      files: !!(query.include & VideoInclude.FILES),
      source: !!(query.include & VideoInclude.SOURCE),
      blockedOwner: !!(query.include & VideoInclude.BLOCKED_OWNER),
      automaticTags: !!(query.include & VideoInclude.AUTOMATIC_TAGS),
      liveSchedules: query.includeScheduledLive
    }
  }
}

// ---------------------------------------------------------------------------

export async function videoModelToFormattedJSON (
  video: MVideoFormattable,
  options: VideoFormattingJSONOptions = {}
): Promise<Video> {
  const span = tracer.startSpan('peertube.VideoModel.toFormattedJSON')

  const userHistory = isArray(video.UserVideoHistories)
    ? video.UserVideoHistories[0]
    : undefined

  const thumbnails = (video.Thumbnails || [])
    .map(t => Object.assign(t, { Video: video }))

  const thumbnailResults = await Promise.all(thumbnails.map(t => t.toFormattedJSON()))

  const videoObject: Video = {
    id: video.id,
    uuid: video.uuid,
    shortUUID: uuidToShort(video.uuid),

    url: video.url,

    name: video.name,
    category: {
      id: video.category,
      label: getCategoryLabel(video.category)
    },
    licence: {
      id: video.licence,
      label: getLicenceLabel(video.licence)
    },
    language: {
      id: video.language,
      label: getLanguageLabel(video.language)
    },
    privacy: {
      id: video.privacy,
      label: getPrivacyLabel(video.privacy)
    },

    nsfw: video.nsfw,
    nsfwFlags: video.nsfwFlags,
    nsfwSummary: video.nsfwSummary,

    truncatedDescription: video.getTruncatedDescription(),
    description: options?.completeDescription === true
      ? video.description
      : video.getTruncatedDescription(),

    isLocal: video.isLocal(),
    duration: video.duration,

    aspectRatio: video.aspectRatio,

    views: video.views,
    viewers: VideoViewsManager.Instance.getTotalViewersOf(video),

    likes: video.likes,
    dislikes: video.dislikes,

    thumbnailPath: video.getSmallestThumbnailStaticPath('16:9'),
    previewPath: video.getBestThumbnailStaticPath('16:9'),

    thumbnails: thumbnailResults,

    embedPath: video.getEmbedStaticPath(),
    createdAt: video.createdAt,
    updatedAt: video.updatedAt,
    publishedAt: video.publishedAt,
    originallyPublishedAt: video.originallyPublishedAt,

    isLive: video.isLive,

    account: video.VideoChannel.Account.toFormattedSummaryJSON(),
    channel: video.VideoChannel.toFormattedSummaryJSON(),

    userHistory: userHistory
      ? { currentTime: userHistory.currentTime }
      : undefined,

    comments: video.comments,

    // Can be added by external plugins
    pluginData: (video as any).pluginData,

    ...await buildAdditionalAttributes(video, options)
  }

  span.end()

  return videoObject
}

export async function videoModelToFormattedDetailsJSON (video: MVideoFormattableDetails): Promise<VideoDetails> {
  const span = tracer.startSpan('peertube.VideoModel.toFormattedDetailsJSON')

  const videoJSON = await video.toFormattedJSON({
    completeDescription: true,
    additionalAttributes: {
      liveSchedules: true,
      scheduledUpdate: true,
      blacklistInfo: true,
      files: true
    }
  }) as Video & Required<Pick<Video, 'files' | 'streamingPlaylists' | 'scheduledUpdate' | 'blacklisted' | 'blacklistedReason'>>

  const tags = video.Tags
    ? video.Tags.map(t => t.name)
    : []

  const detailsJSON = {
    ...videoJSON,

    support: video.support,
    channel: video.VideoChannel.toFormattedJSON(),
    account: video.VideoChannel.Account.toFormattedJSON(),
    tags,

    commentsPolicy: {
      id: video.commentsPolicy,
      label: VIDEO_COMMENTS_POLICY[video.commentsPolicy]
    },

    downloadEnabled: video.downloadEnabled,
    waitTranscoding: video.waitTranscoding,

    inputFileUpdatedAt: video.inputFileUpdatedAt,

    state: {
      id: video.state,
      label: getStateLabel(video.state)
    },

    trackerUrls: video.getTrackerUrls(),

    embedPrivacyPolicy: {
      id: video.embedPrivacyPolicy,
      label: VIDEO_EMBED_PRIVACY_POLICIES[video.embedPrivacyPolicy]
    }
  }

  span.end()

  return detailsJSON
}

export async function streamingPlaylistsModelToFormattedJSON (
  video: MVideoFormattable,
  playlists: MStreamingPlaylistRedundanciesOpt[]
): Promise<VideoStreamingPlaylist[]> {
  if (isArray(playlists) === false) return []

  const results = await Promise.all(playlists.map(async playlist => ({
    id: playlist.id,
    type: playlist.type,

    playlistUrl: await playlist.getMasterPlaylistUrl(video),
    segmentsSha256Url: await playlist.getSha256SegmentsUrl(video),

    redundancies: isArray(playlist.RedundancyVideos)
      ? playlist.RedundancyVideos.map(r => ({ baseUrl: r.fileUrl }))
      : [],

    files: await videoFilesModelToFormattedJSON(video, playlist.VideoFiles, { includePlaylistUrl: true })
  })))

  return results
}

// ---------------------------------------------------------------------------

export async function videoFilesModelToFormattedJSON (
  video: MVideoFormattable,
  videoFiles: MVideoFile[],
  options?: {
    includePlaylistUrl?: true
    includeMagnet?: boolean
  }
): Promise<(VideoFile & { playlistUrl: string })[]>

export async function videoFilesModelToFormattedJSON (
  video: MVideoFormattable,
  videoFiles: MVideoFile[],
  options: {
    includePlaylistUrl?: boolean // default false
    includeMagnet?: boolean // default true
  } = {}
): Promise<VideoFile[]> {
  const { includePlaylistUrl = false, includeMagnet = true } = options

  if (isArray(videoFiles) === false) return []

  const trackerUrls = includeMagnet
    ? video.getTrackerUrls()
    : []

  const results = await Promise.all(
    videoFiles
      .filter(f => !f.isLive())
      .sort(sortByResolutionDesc)
      .map(async videoFile => {
        const fileUrl = await videoFile.getFileUrl(video)

        return {
          id: videoFile.id,

          resolution: {
            id: videoFile.resolution,

            label: getResolutionLabel({
              resolution: videoFile.resolution,
              height: videoFile.height,
              width: videoFile.width
            })
          },

          width: videoFile.width,
          height: videoFile.height,

          magnetUri: includeMagnet && videoFile.hasTorrent()
            ? await generateMagnetUri(video, videoFile, trackerUrls)
            : undefined,

          size: videoFile.size,
          fps: videoFile.fps,

          torrentUrl: videoFile.getTorrentUrl(),
          torrentDownloadUrl: videoFile.getTorrentDownloadUrl(),

          fileUrl,
          fileDownloadUrl: await videoFile.getFileDownloadUrl(video),

          metadataUrl: videoFile.metadataUrl ?? getLocalVideoFileMetadataUrl(video, videoFile),

          hasAudio: videoFile.hasAudio(),
          hasVideo: videoFile.hasVideo(),

          playlistUrl: includePlaylistUrl === true
            ? getHLSResolutionPlaylistFilename(fileUrl)
            : undefined,

          storage: video.remote
            ? null
            : videoFile.storage
        }
      })
  )

  return results
}

// ---------------------------------------------------------------------------

export function getCategoryLabel (id: number) {
  return VIDEO_CATEGORIES[id] || 'Unknown'
}

export function getLicenceLabel (id: number) {
  return VIDEO_LICENCES[id] || 'Unknown'
}

export function getLanguageLabel (id: string) {
  return VIDEO_LANGUAGES[id] || 'Unknown'
}

export function getPrivacyLabel (id: number) {
  return VIDEO_PRIVACIES[id] || 'Unknown'
}

export function getStateLabel (id: number) {
  return VIDEO_STATES[id] || 'Unknown'
}

// ---------------------------------------------------------------------------
// Private
// ---------------------------------------------------------------------------

async function buildAdditionalAttributes (video: MVideoFormattable, options: VideoFormattingJSONOptions) {
  const add = options.additionalAttributes

  const result: Partial<VideoAdditionalAttributes> = {}

  if (add?.state === true) {
    result.state = {
      id: video.state,
      label: getStateLabel(video.state)
    }
  }

  if (add?.waitTranscoding === true) {
    result.waitTranscoding = video.waitTranscoding
  }

  if (add?.scheduledUpdate === true && video.ScheduleVideoUpdate) {
    result.scheduledUpdate = {
      updateAt: video.ScheduleVideoUpdate.updateAt,
      privacy: video.ScheduleVideoUpdate.privacy || undefined
    }
  }

  if (add?.blacklistInfo === true) {
    result.blacklisted = !!video.VideoBlacklist
    result.blacklistedReason = video.VideoBlacklist
      ? video.VideoBlacklist.reason
      : null
  }

  if (add?.blockedOwner === true) {
    result.blockedOwner = video.VideoChannel.Account.isBlocked()

    const server = video.VideoChannel.Account.Actor.Server as MServer
    result.blockedServer = !!(server?.isBlocked())
  }

  if (add?.files === true) {
    result.streamingPlaylists = await streamingPlaylistsModelToFormattedJSON(video, video.VideoStreamingPlaylists)
    result.files = await videoFilesModelToFormattedJSON(video, video.VideoFiles)
  }

  if (add?.source === true) {
    result.videoSource = video.VideoSource?.toFormattedJSON() || null
  }

  if (add?.automaticTags === true) {
    result.automaticTags = (video.VideoAutomaticTags || []).map(t => t.AutomaticTag.name)
  }

  if (add?.liveSchedules === true) {
    result.liveSchedules = (video.VideoLive?.LiveSchedules || []).map(s => s.toFormattedJSON())
  }

  return result
}
