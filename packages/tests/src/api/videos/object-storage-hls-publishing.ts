/* eslint-disable @typescript-eslint/no-unused-expressions,@typescript-eslint/require-await */

import { FileStorage, HttpStatusCode, VideoDetails, VideoPrivacy, VideoResolution } from '@peertube/peertube-models'
import { areMockObjectStorageTestsDisabled } from '@peertube/peertube-node-utils'
import {
  cleanupTests,
  createMultipleServers,
  doubleFollow,
  makeRawRequest,
  ObjectStorageCommand,
  PeerTubeServer,
  setAccessTokensToServers,
  waitJobs
} from '@peertube/peertube-server-commands'
import { SQLCommand } from '@tests/shared/sql-command.js'
import { expect } from 'chai'
import { ensureDir, pathExists } from 'fs-extra/esm'
import { writeFile } from 'fs/promises'
import { basename, join } from 'path'

function getResolutionPlaylistFilename (fileUrl: string) {
  return basename(new URL(fileUrl).pathname).replace(/-fragmented\.mp4$/, '.m3u8')
}

function getHLSDirectoryPath (server: PeerTubeServer, video: VideoDetails) {
  const isPrivate = video.privacy.id === VideoPrivacy.PRIVATE || video.privacy.id === VideoPrivacy.INTERNAL

  return server.getDirectoryPath(join('streaming-playlists', 'hls', isPrivate ? 'private' : '', video.uuid))
}

async function restoreLocalHLSFiles (options: {
  server: PeerTubeServer
  video: VideoDetails
  file: VideoDetails['streamingPlaylists'][number]['files'][number]
}) {
  const { server, video, file } = options
  const token = video.privacy.id === VideoPrivacy.PRIVATE || video.privacy.id === VideoPrivacy.INTERNAL
    ? server.accessToken
    : undefined

  const directory = getHLSDirectoryPath(server, video)
  await ensureDir(directory)

  const segmentFilename = basename(new URL(file.fileUrl).pathname)
  const playlistFilename = getResolutionPlaylistFilename(file.fileUrl)
  const segmentPath = join(directory, segmentFilename)
  const playlistPath = join(directory, playlistFilename)

  if (await pathExists(segmentPath) && await pathExists(playlistPath)) {
    return
  }

  const playlistUrl = file.playlistUrl || new URL(playlistFilename, video.streamingPlaylists[0].playlistUrl).toString()

  const segmentResponse = await makeRawRequest({
    url: file.fileDownloadUrl,
    token,
    redirects: 1,
    responseType: 'arraybuffer',
    expectedStatus: HttpStatusCode.OK_200
  })
  await writeFile(segmentPath, segmentResponse.body)

  const playlistResponse = await makeRawRequest({
    url: playlistUrl,
    token,
    redirects: 1,
    expectedStatus: HttpStatusCode.OK_200
  })
  await writeFile(playlistPath, playlistResponse.text)
}

describe('Test object storage HLS publishing regression', function () {
  if (areMockObjectStorageTestsDisabled()) return

  let servers: PeerTubeServer[] = []
  let videoUUID: string

  const objectStorage = new ObjectStorageCommand()
  const objectStorageConfig = (() => {
    const config = objectStorage.getDefaultMockConfig()

    return {
      ...config,
      object_storage: {
        ...config.object_storage,
        force_path_style: false,
        use_presigned_public_urls: false,
        keep_local_file_after_move: 0,
        proxy: {
          ...config.object_storage.proxy,
          proxify_private_files: true
        }
      },
      video_transcription: {
        enabled: false
      },
      live: {
        enabled: false
      }
    }
  })()

  before(async function () {
    this.timeout(240000)

    servers = await createMultipleServers(2, objectStorageConfig)
    await setAccessTokensToServers(servers)
    await doubleFollow(servers[0], servers[1])

    await objectStorage.prepareDefaultMockBuckets()

    await servers[0].config.updateExistingConfig({
      newConfig: {
        transcoding: {
          enabled: true,
          webVideos: {
            enabled: false
          },
          hls: {
            enabled: true,
            splitAudioAndVideo: false
          },
          resolutions: {
            '0p': false,
            '144p': false,
            '240p': true,
            '360p': false,
            '480p': false,
            '720p': true,
            '1080p': false,
            '1440p': false,
            '2160p': false
          },
          alwaysTranscodeOriginalResolution: true,
          alwaysTranscodePodcastOptimizedAudio: false
        }
      }
    })

    const { uuid } = await servers[0].videos.quickUpload({ name: 'video' })
    videoUUID = uuid

    await servers[0].captions.add({ language: 'zh', videoId: uuid, fixture: 'subtitle-good1.vtt' })
    await servers[0].captions.add({ language: 'fr', videoId: uuid, fixture: 'subtitle-good1.vtt' })

    await waitJobs(servers)
  })

  it('publishes only object-storage renditions until a moved rendition is re-added', async function () {
    this.timeout(180000)

    const sqlCommand = new SQLCommand(servers[0])

    try {
      const videoBefore = await servers[0].videos.get({ id: videoUUID })
      const hlsBefore = videoBefore.streamingPlaylists[0]
      expect(hlsBefore.playlistUrl).to.contain(objectStorage.getMockPlaylistBaseUrl())

      const movedFile = [ ...hlsBefore.files ]
        .sort((a, b) => a.resolution.id - b.resolution.id)
        .find(file => file.resolution.id !== VideoResolution.H_720P)

      expect(movedFile).to.exist
      if (!movedFile) throw new Error('Missing secondary HLS file in regression fixture')

      await restoreLocalHLSFiles({ server: servers[0], video: videoBefore, file: movedFile })

      await sqlCommand.updateQuery(
        `UPDATE "videoFile" SET storage = :storage WHERE id = :fileId`,
        { storage: FileStorage.FILE_SYSTEM, fileId: movedFile.id }
      )
      await sqlCommand.updateQuery(
        `UPDATE "videoStreamingPlaylist" SET storage = :storage WHERE id = :playlistId`,
        { storage: FileStorage.OBJECT_STORAGE, playlistId: hlsBefore.id }
      )

      await servers[0].captions.delete({ videoId: videoUUID, language: 'zh' })
      await waitJobs(servers)

      const videoAfterDelete = await servers[0].videos.get({ id: videoUUID })
      const hlsAfterDelete = videoAfterDelete.streamingPlaylists[0]
      const masterAfterDelete = await servers[0].streamingPlaylists.get({ url: hlsAfterDelete.playlistUrl })
      const shaAfterDelete = await servers[0].streamingPlaylists.getSegmentSha256({
        url: hlsAfterDelete.segmentsSha256Url,
        withRetry: true
      })

      expect(hlsAfterDelete.files.map(file => file.resolution.id)).to.include(movedFile.resolution.id)
      expect(masterAfterDelete).to.not.include(getResolutionPlaylistFilename(movedFile.fileUrl))
      expect(Object.keys(shaAfterDelete)).to.not.include(basename(new URL(movedFile.fileUrl).pathname))

      const command = `npm run create-move-video-storage-job -- --to-object-storage -v ${videoUUID}`
      await servers[0].cli.execWithEnv(command, objectStorageConfig)
      await waitJobs(servers)

      const videoAfterMove = await servers[0].videos.get({ id: videoUUID })
      const hlsAfterMove = videoAfterMove.streamingPlaylists[0]
      const masterAfterMove = await servers[0].streamingPlaylists.get({ url: hlsAfterMove.playlistUrl })
      const shaAfterMove = await servers[0].streamingPlaylists.getSegmentSha256({
        url: hlsAfterMove.segmentsSha256Url,
        withRetry: true
      })

      expect(masterAfterMove).to.include(getResolutionPlaylistFilename(movedFile.fileUrl))
      expect(Object.keys(shaAfterMove)).to.include(basename(new URL(movedFile.fileUrl).pathname))
    } finally {
      await sqlCommand.cleanup()
    }
  })

  after(async function () {
    await objectStorage.cleanupMock()
    await cleanupTests(servers)
  })
})
