import { logger, loggerTagsFactory } from '@server/helpers/logger.js'
import { DrmDecryptCLI } from '@server/helpers/drm-decrypt/drm-decrypt-cli.js'
import { CONFIG } from '@server/initializers/config.js'
import { SCHEDULER_INTERVALS_MS } from '../../initializers/constants.js'
import { AbstractScheduler } from './abstract-scheduler.js'

const lTags = loggerTagsFactory('schedulers', 'drm-decrypt')

export class DrmDecryptUpdateScheduler extends AbstractScheduler {
  private static instance: AbstractScheduler

  protected schedulerIntervalMs = SCHEDULER_INTERVALS_MS.DRM_DECRYPT_UPDATE

  private constructor () {
    super({ randomRunOnEnable: true })
  }

  protected internalExecute (): Promise<void> {
    if (!CONFIG.IMPORT.VIDEOS.HTTP.DRM_DECRYPTION.RELEASE.URL) {
      return Promise.resolve()
    }

    logger.info('Running DRM decryption binary update scheduler', lTags())

    return DrmDecryptCLI.updateDrmBinary()
  }

  static get Instance () {
    return this.instance || (this.instance = new this())
  }
}
