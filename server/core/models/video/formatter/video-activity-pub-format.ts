import {
  ActivityHashTagObject,
  ActivityIconObject,
  ActivityPlaylistUrlObject,
  ActivityPubStoryboard,
  ActivitySensitiveTagObject,
  ActivityTagObject,
  ActivityTrackerUrlObject,
  ActivityUrlObject,
  nsfwFlagsToString,
  VideoCommentPolicy,
  VideoEmbedPrivacyPolicy,
  VideoObject
} from '@peertube/peertube-models'
import { getAPPublicValue } from '@server/helpers/activity-pub-utils.js'
import { isArray } from '@server/helpers/custom-validators/misc.js'
import { getActivityStreamDuration } from '@server/lib/activitypub/activity.js'
import { getLocalVideoFileMetadataUrl } from '@server/lib/video-urls.js'
import { generateMagnetUri } from '@server/lib/webtorrent.js'
import { WEBSERVER } from '../../../initializers/constants.js'
import {
  getLocalVideoChaptersActivityPubUrl,
  getLocalVideoCommentsActivityPubUrl,
  getLocalVideoDislikesActivityPubUrl,
  getLocalVideoLikesActivityPubUrl,
  getLocalVideoPlayerSettingsActivityPubUrl,
  getLocalVideoSharesActivityPubUrl
} from '../../../lib/activitypub/url.js'
import { MStreamingPlaylistFiles, MUserId, MVideo, MVideoAP, MVideoFile } from '../../../types/models/index.js'
import { sortByResolutionDesc } from './shared/index.js'
import { getCategoryLabel, getLanguageLabel, getLicenceLabel } from './video-api-format.js'

export async function videoModelToActivityPubObject (video: MVideoAP): Promise<VideoObject> {
  const language = video.language
    ? { identifier: video.language, name: getLanguageLabel(video.language) }
    : undefined

  const category = video.category
    ? { identifier: video.category + '', name: getCategoryLabel(video.category) }
    : undefined

  const licence = video.licence
    ? { identifier: video.licence + '', name: getLicenceLabel(video.licence) }
    : undefined

  const [ videoFileUrls, streamingPlaylistUrls ] = await Promise.all([
    buildVideoFileUrls({ video, files: video.VideoFiles }),
    buildStreamingPlaylistUrls(video)
  ])

  const url: ActivityUrlObject[] = [
    // HTML url should be the first element in the array so Mastodon correctly displays the embed
    {
      type: 'Link',
      mediaType: 'text/html',
      href: WEBSERVER.URL + video.getWatchStaticPath()
    } as ActivityUrlObject,

    {
      type: 'Link',
      mediaType: 'text/html',
      href: video.url
    } as ActivityUrlObject,

    ...videoFileUrls,

    ...streamingPlaylistUrls,

    ...buildTrackerUrls(video)
  ]

  return {
    type: 'Video' as 'Video',
    id: video.url,
    name: video.name,
    duration: getActivityStreamDuration(video.duration),
    uuid: video.uuid,
    category,
    licence,
    language,
    views: video.views,

    sensitive: video.nsfw,
    summary: video.nsfwSummary,

    waitTranscoding: video.waitTranscoding,

    state: video.state,

    canReply: video.commentsPolicy === VideoCommentPolicy.ENABLED
      ? null
      : getAPPublicValue(), // Requires approval

    commentsPolicy: video.commentsPolicy,

    downloadEnabled: video.downloadEnabled,
    published: video.publishedAt.toISOString(),

    originallyPublishedAt: video.originallyPublishedAt
      ? video.originallyPublishedAt.toISOString()
      : null,

    schedules: (video.VideoLive?.LiveSchedules || []).map(s => ({
      startDate: s.startAt
    })),

    updated: video.updatedAt.toISOString(),

    uploadDate: video.inputFileUpdatedAt?.toISOString(),

    tag: buildTags(video),

    mediaType: 'text/markdown',
    content: video.description,
    support: video.support,

    subtitleLanguage: await buildSubtitleLanguage(video),

    icon: await buildIcon(video),

    preview: await buildPreviewAPAttribute(video),

    aspectRatio: video.aspectRatio,

    url,

    likes: getLocalVideoLikesActivityPubUrl(video),
    dislikes: getLocalVideoDislikesActivityPubUrl(video),
    shares: getLocalVideoSharesActivityPubUrl(video),
    comments: getLocalVideoCommentsActivityPubUrl(video),
    hasParts: getLocalVideoChaptersActivityPubUrl(video),
    playerSettings: getLocalVideoPlayerSettingsActivityPubUrl(video),

    embedUrl: video.embedPrivacyPolicy === VideoEmbedPrivacyPolicy.ALL_ALLOWED
      ? video.getEmbedStaticUrl()
      : null,

    attributedTo: process.env.FEP_1B12_ONLY !== 'true'
      ? [
        {
          type: 'Person',
          id: video.VideoChannel.Account.Actor.url
        },
        {
          type: 'Group',
          id: video.VideoChannel.Actor.url
        }
      ]
      : video.VideoChannel.Account.Actor.url,

    audience: video.VideoChannel.Actor.url,

    ...buildLiveAPAttributes(video)
  }
}

// ---------------------------------------------------------------------------
// Private
// ---------------------------------------------------------------------------

function buildLiveAPAttributes (video: MVideoAP) {
  if (!video.isLive) {
    return {
      isLiveBroadcast: false,
      liveSaveReplay: null,
      permanentLive: null,
      latencyMode: null
    }
  }

  return {
    isLiveBroadcast: true,
    liveSaveReplay: video.VideoLive.saveReplay,
    permanentLive: video.VideoLive.permanentLive,
    latencyMode: video.VideoLive.latencyMode
  }
}

async function buildPreviewAPAttribute (video: MVideoAP): Promise<ActivityPubStoryboard[]> {
  if (!video.Storyboard) return undefined

  const storyboard = video.Storyboard

  return [
    {
      type: 'Image',
      rel: [ 'storyboard' ],
      url: [
        {
          mediaType: 'image/jpeg',

          href: await storyboard.getLocalFileUrl(),

          width: storyboard.totalWidth,
          height: storyboard.totalHeight,

          tileWidth: storyboard.spriteWidth,
          tileHeight: storyboard.spriteHeight,
          tileDuration: getActivityStreamDuration(storyboard.spriteDuration)
        }
      ]
    }
  ]
}

async function buildVideoFileUrls (options: {
  video: MVideo
  files: MVideoFile[]
  user?: MUserId
}): Promise<ActivityUrlObject[]> {
  const { video, files } = options

  if (!isArray(files)) return []

  const urls: ActivityUrlObject[] = []

  const trackerUrls = video.getTrackerUrls()
  const sortedFiles = files
    .filter(f => !f.isLive())
    .sort(sortByResolutionDesc)

  for (const file of sortedFiles) {
    const fileAP = await file.toActivityPubObject(video)
    urls.push(fileAP)

    urls.push({
      type: 'Link',
      rel: [ 'metadata', fileAP.mediaType ],
      mediaType: 'application/json' as 'application/json',
      href: getLocalVideoFileMetadataUrl(video, file),
      height: file.height || file.resolution,
      width: file.width,
      fps: file.fps
    })

    if (file.hasTorrent()) {
      urls.push({
        type: 'Link',
        mediaType: 'application/x-bittorrent' as 'application/x-bittorrent',
        href: file.getTorrentUrl(),
        height: file.height || file.resolution,
        width: file.width,
        fps: file.fps
      })

      urls.push({
        type: 'Link',
        mediaType: 'application/x-bittorrent;x-scheme-handler/magnet' as 'application/x-bittorrent;x-scheme-handler/magnet',
        href: await generateMagnetUri(video, file, trackerUrls),
        height: file.height || file.resolution,
        width: file.width,
        fps: file.fps
      })
    }
  }

  return urls
}

// ---------------------------------------------------------------------------

async function buildStreamingPlaylistUrls (video: MVideoAP): Promise<ActivityPlaylistUrlObject[]> {
  if (!isArray(video.VideoStreamingPlaylists)) return []

  const results = await Promise.all(video.VideoStreamingPlaylists.map(async playlist => ({
    type: 'Link' as const,
    mediaType: 'application/x-mpegURL' as 'application/x-mpegURL',
    href: await playlist.getMasterPlaylistUrl(video),
    tag: await buildStreamingPlaylistTags(video, playlist)
  })))

  return results
}

async function buildStreamingPlaylistTags (video: MVideoAP, playlist: MStreamingPlaylistFiles) {
  const urls = await buildVideoFileUrls({ video, files: playlist.VideoFiles })

  return [
    ...playlist.p2pMediaLoaderInfohashes.map(i => ({ type: 'Infohash' as 'Infohash', name: i })),

    {
      type: 'Link',
      name: 'sha256',
      mediaType: 'application/json' as 'application/json',
      href: playlist.getSha256SegmentsUrl(video)
    },

    ...urls
  ] as ActivityTagObject[]
}

// ---------------------------------------------------------------------------

function buildTrackerUrls (video: MVideoAP): ActivityTrackerUrlObject[] {
  return video.getTrackerUrls()
    .map(trackerUrl => {
      const rel2 = trackerUrl.startsWith('http')
        ? 'http'
        : 'websocket'

      return {
        type: 'Link',
        name: `tracker-${rel2}`,
        rel: [ 'tracker', rel2 ],
        href: trackerUrl
      }
    })
}

// ---------------------------------------------------------------------------

function buildTags (video: MVideoAP): (ActivitySensitiveTagObject | ActivityHashTagObject)[] {
  const tags = isArray(video.Tags)
    ? video.Tags
    : []

  return [
    ...tags.map(t =>
      ({
        type: 'Hashtag' as 'Hashtag',
        name: t.name
      }) as ActivityHashTagObject
    ),

    ...nsfwFlagsToString(video.nsfwFlags).map(f =>
      ({
        type: 'SensitiveTag' as 'SensitiveTag',
        name: f
      }) as ActivitySensitiveTagObject
    )
  ]
}

async function buildIcon (video: MVideoAP): Promise<ActivityIconObject[]> {
  const thumbnailResults = await Promise.all(
    video.Thumbnails
      .filter(i => !!i)
      .map(i => i.toActivityPubObject())
  )
  return thumbnailResults
}

async function buildSubtitleLanguage (video: MVideoAP) {
  if (!isArray(video.VideoCaptions)) return []

  return Promise.all(video.VideoCaptions.map(caption => caption.toActivityPubObject(video)))
}
