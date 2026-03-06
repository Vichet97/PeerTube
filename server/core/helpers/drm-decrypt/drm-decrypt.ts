import { CONFIG } from '@server/initializers/config.js'
import { execa } from 'execa'
import { pathExists } from 'fs-extra/esm'
import { dirname } from 'path'
import { ensureDir } from 'fs-extra/esm'
import { DrmDecryptCLI } from './drm-decrypt-cli.js'
import { logger, loggerTagsFactory } from '../logger.js'

const lTags = loggerTagsFactory('drm-decrypt')

export interface DrmDecryptOptions {
  inputPath: string
  outputPath: string
  licenseServerUrl?: string | null
  drmType?: string | null
  clearkeys?: string | null
}

/**
 * Format clearkeys JSON to mp4decrypt --key format.
 * Accepts: {"kid": "key"} or [{"kid": "xxx", "k": "yyy"}, ...]
 */
function formatClearkeysForMp4Decrypt (clearkeysJson: string | null | undefined): string {
  if (!clearkeysJson?.trim()) return ''

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

    return pairs.map(([ kid, k ]) => `--key ${kid}:${k}`).join(' ')
  } catch {
    return ''
  }
}

/**
 * Run the configured DRM decryption tool on an encrypted video file.
 * Replaces placeholders in args: {input}, {output}, {licenseServerUrl}, {clearkeys}
 * @returns The output path on success
 * @throws Error if the tool fails
 */
export async function runDrmDecryption (options: DrmDecryptOptions): Promise<string> {
  const { inputPath, outputPath, licenseServerUrl, clearkeys } = options

  if (!(await pathExists(inputPath))) {
    throw new Error(`DRM decrypt input file does not exist: ${inputPath}`)
  }

  const binaryPath = await DrmDecryptCLI.safeGetBinaryPath()
  const argsTemplate = CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION.ARGS

  const licenseUrl = licenseServerUrl ?? CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION.DEFAULT_LICENSE_SERVER_URL
  const clearkeysFormatted = formatClearkeysForMp4Decrypt(clearkeys)

  const args = argsTemplate.flatMap(arg => {
    const replaced = arg
      .replace(/{input}/g, inputPath)
      .replace(/{output}/g, outputPath)
      .replace(/{licenseServerUrl}/g, licenseUrl ?? '')
      .replace(/{clearkeys}/g, clearkeysFormatted)

    if (arg === '{clearkeys}' && clearkeysFormatted) {
      return clearkeysFormatted.split(/\s+/).filter(Boolean)
    }
    return [ replaced ]
  }).filter(arg => arg.length > 0)

  await ensureDir(dirname(outputPath))

  logger.info('Running DRM decryption tool.', { binaryPath, inputPath, outputPath, ...lTags() })

  const result = await execa(binaryPath, args, {
    timeout: 60 * 60 * 1000 // 1 hour
  })

  if (result.exitCode !== 0) {
    throw new Error(
      `DRM decryption failed (exit code ${result.exitCode}): ${result.stderr || result.stdout || 'No output'}`
    )
  }

  if (!(await pathExists(outputPath))) {
    throw new Error(`DRM decryption produced no output file: ${outputPath}`)
  }

  logger.info('DRM decryption completed.', { outputPath, ...lTags() })

  return outputPath
}
