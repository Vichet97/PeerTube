import { VideoCreateUpdateCommon } from '../video-create-update-common.model.js'

export interface VideoImportCreate extends VideoCreateUpdateCommon {
  targetUrl?: string
  magnetUri?: string
  torrentfile?: Blob

  // Default is true if the feature is enabled by the instance admin
  generateTranscription?: boolean

  // Custom HTTP headers for yt-dlp (e.g. {"Authorization": "Bearer xxx", "Cookie": "session=abc"})
  customHeaders?: Record<string, string>

  // License server URL for DRM decryption (Widevine, FairPlay). Required when importing DRM-protected content.
  licenseServerUrl?: string

  // DRM type: widevine, fairplay, clearkey, or empty for auto-detect
  drmType?: string

  // Clearkeys for Clearkey DRM in JSON format (e.g. {"kid": "hexkey"} or [{"kid": "...", "k": "..."}])
  clearkeys?: string

  // Force using N_m3u8DL-RE (true) or yt-dlp (false) for this import.
  // Clearkeys always force N_m3u8DL-RE regardless of this value.
  useNm3u8dlRe?: boolean

  channelId: number // Required
}
