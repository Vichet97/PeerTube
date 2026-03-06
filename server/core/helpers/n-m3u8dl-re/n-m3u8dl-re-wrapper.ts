import { CONFIG } from '@server/initializers/config.js'
import { pathExists } from 'fs-extra/esm'
import { execa } from 'execa'
import { basename, dirname } from 'path'
import { ensureDir } from 'fs-extra/esm'
import { logger, loggerTagsFactory } from '../logger.js'
import { generateVideoImportTmpPath } from '../utils.js'

const lTags = loggerTagsFactory('n-m3u8dl-re')

/**
 * Parse clearkeys JSON to --key kid:key format for N_m3u8DL-RE.
 * Accepts: {"kid": "key"} or [{"kid": "xxx", "k": "yyy"}, ...]
 */
function formatClearkeysForNm3u8dlRe (clearkeysJson: string | null | undefined): string[] {
  if (!clearkeysJson?.trim()) return []

  try {
    const parsed = JSON.parse(clearkeysJson.trim())
    const pairs: Array<[string, string]> = []

    if (Array.isArray(parsed)) {
      for (const item of parsed) {
        if (item && typeof item === 'object' && item.kid && item.k) {
          pairs.push([ String(item.kid), String(item.k) ])
        }
      }
    } else if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      for (const [ kid, k ] of Object.entries(parsed)) {
        if (typeof k === 'string') pairs.push([ kid, k ])
      }
    }

    return pairs.map(([ kid, k ]) => `${kid}:${k}`)
  } catch {
    return []
  }
}

export async function downloadWithNm3u8dlRe (options: {
  url: string
  clearkeys: string | null | undefined
  customHeaders?: Record<string, string>
  timeout: number
  onProgress?: (percent: number) => void
}): Promise<string> {
  const { url, clearkeys, customHeaders, timeout, onProgress } = options

  const binaryPath = CONFIG.IMPORT.VIDEOS.HTTP.N_M3U8DL_RE.BINARY_PATH
  if (!await pathExists(binaryPath)) {
    throw new Error(
      `N_m3u8DL-RE not found at ${binaryPath}. Install it for DASH/M3U8+Clearkey imports, or set import.videos.http.n_m3u8dl_re.binary_path`
    )
  }

  const outputPath = generateVideoImportTmpPath(url, '.mp4')
  const workDir = dirname(outputPath)
  const saveName = basename(outputPath, '.mp4')
  await ensureDir(workDir)

  const keyPairs = formatClearkeysForNm3u8dlRe(clearkeys)
  if (keyPairs.length === 0) {
    throw new Error('Clearkeys required for N_m3u8DL-RE but none provided or invalid JSON')
  }

  const args = [
    url,
    '--auto-select',
    '-sv', 'for=best',
    '-sa', 'for=best',
    '--mux-after-done', 'format=mp4',
    '--save-name', saveName,
    '--save-dir', workDir,
    '--no-ansi-color'
  ]

  for (const keyArg of keyPairs) {
    args.push('--key', keyArg)
  }

  if (customHeaders && Object.keys(customHeaders).length > 0) {
    for (const [ k, v ] of Object.entries(customHeaders)) {
      args.push('--header', `${k}: ${v}`)
    }
  }

  logger.info('Downloading with N_m3u8DL-RE %s', url, lTags())

  const subprocess = execa(binaryPath, args, {
    timeout,
    cwd: workDir
  })

  if (onProgress) {
    let lastReportedPercent = -1
    const progressRegex = /(\d+(?:\.\d+)?)\s*%/
    const parseProgressChunk = (chunk: Buffer) => {
      const str = chunk.toString()
      const lines = str.split(/\r?\n|\r/)
      for (const line of lines) {
        const match = line.match(progressRegex)
        if (match) {
          const percent = Math.min(100, Math.floor(parseFloat(match[1])))
          if (percent > lastReportedPercent && percent <= 100) {
            lastReportedPercent = percent
            onProgress(percent)
          }
        }
      }
    }
    const stdoutStream = subprocess.stdout ?? subprocess.stdio?.[1]
    const stderrStream = subprocess.stderr ?? subprocess.stdio?.[2]
    if (stdoutStream && typeof stdoutStream.on === 'function') {
      stdoutStream.on('data', parseProgressChunk)
    }
    if (stderrStream && typeof stderrStream.on === 'function') {
      stderrStream.on('data', parseProgressChunk)
    }
  }

  const result = await subprocess

  if (result.exitCode !== 0) {
    throw new Error(
      `N_m3u8DL-RE failed (exit code ${result.exitCode}): ${result.stderr || result.stdout || 'No output'}`
    )
  }

  if (await pathExists(outputPath)) return outputPath

  throw new Error(`N_m3u8DL-RE did not produce output at ${outputPath}`)
}

export function isMpdOrM3u8Url (url: string): boolean {
  try {
    const u = new URL(url)
    const path = u.pathname.toLowerCase()
    return path.endsWith('.mpd') || path.endsWith('.m3u8') || path.includes('.mpd?') || path.includes('.m3u8?')
  } catch {
    return false
  }
}
