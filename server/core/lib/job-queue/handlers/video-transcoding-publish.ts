import { VideoState } from '@peertube/peertube-models'
import { isRetryableTransactionError } from '@server/helpers/retryable-transaction-error.js'

type PublishableVideoFileContainer = {
  length: number
}

type PublishableHLSPlaylist = {
  VideoFiles: PublishableVideoFileContainer
}

type PublishableVideo = {
  state: number
  waitTranscoding: boolean
  VideoFiles: PublishableVideoFileContainer
  VideoStreamingPlaylists: PublishableHLSPlaylist[]
  getHLSPlaylist: () => PublishableHLSPlaylist | undefined
  setNewState: (newState: number, isNewVideo: boolean, transaction: unknown) => Promise<void> | void
}

export type PublishAfterFirstBatchDeps<TVideo extends PublishableVideo, TTransaction = unknown> = {
  loadVideo: (id: number | string, transaction?: TTransaction) => Promise<TVideo | null | undefined>
  runTransaction: <T>(fn: (transaction: TTransaction) => Promise<T>) => Promise<T>
}

export async function publishVideoAfterFirstTranscodingBatchIfNeededWithDeps<
  TVideo extends PublishableVideo,
  TTransaction = unknown
> (options: {
  videoUUID: string
  isNewVideo: boolean
}, deps: PublishAfterFirstBatchDeps<TVideo, TTransaction>) {
  const { videoUUID, isNewVideo } = options

  const video = await deps.loadVideo(videoUUID)
  if (!video) return

  if (video.state !== VideoState.TO_TRANSCODE && video.state !== VideoState.TO_MOVE_TO_EXTERNAL_STORAGE) return

  const hasWebFiles = video.VideoFiles.length !== 0
  const hlsPlaylist = video.getHLSPlaylist()
  const hasHLSFiles = !!hlsPlaylist && hlsPlaylist.VideoFiles.length !== 0
  if (!hasWebFiles && !hasHLSFiles) return

  await retryPublishAfterFirstBatch(async () => {
    return deps.runTransaction(async transaction => {
      const videoInTx = await deps.loadVideo(videoUUID, transaction)
      if (!videoInTx) return

      if (videoInTx.state !== VideoState.TO_TRANSCODE && videoInTx.state !== VideoState.TO_MOVE_TO_EXTERNAL_STORAGE) return

      const hasWebFilesInTx = videoInTx.VideoFiles.length !== 0
      const hlsPlaylistInTx = videoInTx.getHLSPlaylist()
      const hasHLSFilesInTx = !!hlsPlaylistInTx && hlsPlaylistInTx.VideoFiles.length !== 0
      if (!hasWebFilesInTx && !hasHLSFilesInTx) return

      videoInTx.waitTranscoding = false
      await videoInTx.setNewState(VideoState.PUBLISHED, isNewVideo, transaction)
    })
  })
}

async function retryPublishAfterFirstBatch<T> (fn: () => Promise<T>) {
  let lastError: unknown

  for (let attempt = 1; attempt <= 5; attempt++) {
    try {
      return await fn()
    } catch (err) {
      lastError = err

      if (attempt === 5 || !isRetryableTransactionError(err)) {
        throw err
      }
    }
  }

  throw lastError instanceof Error
    ? lastError
    : new Error('Publishing after the first transcoding batch failed without an error object.')
}
