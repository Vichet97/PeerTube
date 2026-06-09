import { JobType } from '@peertube/peertube-models'

export type DeletedVideoJobIdentifiers = {
  videoUUID: string
  videoId: number
  videoImportId?: number
}

type JobData = {
  videoUUID?: unknown
  videoId?: unknown
  videoImportId?: unknown
}

const DELETED_VIDEO_JOB_TYPES: JobType[] = [
  'video-import',
  'video-transcoding',
  'transcoding-job-builder',
  'move-to-object-storage',
  'move-to-file-system',
  'move-video-file-to-object-storage',
  'move-hls-playlist-to-object-storage',
  'move-thumbnail-to-object-storage',
  'move-caption-to-object-storage',
  'video-transcription',
  'generate-video-storyboard',
  'federate-video',
  'video-studio-edition',
  'manage-video-torrent'
]

export function listDeletedVideoJobTypes () {
  return [ ...DELETED_VIDEO_JOB_TYPES ]
}

export function shouldRemoveDeletedVideoJob (
  jobType: JobType,
  data: JobData,
  identifiers: DeletedVideoJobIdentifiers
) {
  const { videoUUID, videoId, videoImportId } = identifiers

  switch (jobType) {
    case 'video-import':
      return typeof videoImportId === 'number' && data?.videoImportId === videoImportId

    case 'video-transcoding':
    case 'transcoding-job-builder':
    case 'move-to-object-storage':
    case 'move-to-file-system':
    case 'move-video-file-to-object-storage':
    case 'move-hls-playlist-to-object-storage':
    case 'move-thumbnail-to-object-storage':
    case 'move-caption-to-object-storage':
    case 'video-transcription':
    case 'generate-video-storyboard':
    case 'federate-video':
    case 'video-studio-edition':
      return data?.videoUUID === videoUUID

    case 'manage-video-torrent':
      return data?.videoId === videoId

    default:
      return false
  }
}
