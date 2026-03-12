import { FileStorage, type FileStorageType, Storyboard } from '@peertube/peertube-models'
import { CONFIG } from '@server/initializers/config.js'
import { MStoryboard, MStoryboardVideo, MVideo } from '@server/types/models/index.js'
import { generateStoryboardObjectStorageKey } from '@server/lib/object-storage/keys.js'
import { buildObjectStoragePublicFileUrl, buildObjectStorageStoryboardPrivateFileUrl } from '@server/lib/object-storage/urls.js'
import { removeStoryboardObjectStorage } from '@server/lib/object-storage/videos.js'
import { remove } from 'fs-extra/esm'
import { join } from 'path'
import { Op, Transaction } from 'sequelize'
import { AfterDestroy, AllowNull, BelongsTo, Column, CreatedAt, DataType, Default, ForeignKey, Table, UpdatedAt } from 'sequelize-typescript'
import { logger } from '../../helpers/logger.js'
import { CONSTRAINTS_FIELDS, FILES_CACHE, LAZY_STATIC_PATHS, WEBSERVER } from '../../initializers/constants.js'
import { SequelizeModel } from '../shared/index.js'
import { VideoModel } from './video.js'

@Table({
  tableName: 'storyboard',
  indexes: [
    {
      fields: [ 'videoId' ],
      unique: true
    },
    {
      fields: [ 'filename' ],
      unique: true
    }
  ]
})
export class StoryboardModel extends SequelizeModel<StoryboardModel> {
  @AllowNull(false)
  @Column
  declare filename: string

  @AllowNull(false)
  @Column
  declare totalHeight: number

  @AllowNull(false)
  @Column
  declare totalWidth: number

  @AllowNull(false)
  @Column
  declare spriteHeight: number

  @AllowNull(false)
  @Column
  declare spriteWidth: number

  @AllowNull(false)
  @Column
  declare spriteDuration: number

  @AllowNull(true)
  @Column(DataType.STRING(CONSTRAINTS_FIELDS.COMMONS.URL.max))
  declare fileUrl: string

  @AllowNull(false)
  @Column
  declare cached: boolean

  @AllowNull(false)
  @Default(FileStorage.FILE_SYSTEM)
  @Column
  declare storage: FileStorageType

  @ForeignKey(() => VideoModel)
  @Column
  declare videoId: number

  @BelongsTo(() => VideoModel, {
    foreignKey: {
      allowNull: false
    },
    onDelete: 'CASCADE'
  })
  declare Video: Awaited<VideoModel>

  @CreatedAt
  declare createdAt: Date

  @UpdatedAt
  declare updatedAt: Date

  @AfterDestroy
  static removeInstanceFile (instance: StoryboardModel) {
    // Don't block the transaction
    instance.removeFile()
      .catch(err => logger.error('Cannot remove storyboard file %s.', instance.filename, { err }))
  }

  static loadByVideo (videoId: number, transaction?: Transaction): Promise<MStoryboard> {
    const query = {
      where: {
        videoId
      },
      transaction
    }

    return StoryboardModel.findOne(query)
  }

  static loadByFilename (filename: string): Promise<MStoryboard> {
    const query = {
      where: {
        filename
      },
      include: [
        {
          model: VideoModel.unscoped()
        }
      ]
    }

    return StoryboardModel.findOne(query)
  }

  // ---------------------------------------------------------------------------

  static async listStoryboardsOf (video: MVideo): Promise<MStoryboardVideo[]> {
    const query = {
      where: {
        videoId: video.id
      }
    }

    const storyboards = await StoryboardModel.findAll<MStoryboard>(query)

    return storyboards.map(s => Object.assign(s, { Video: video }))
  }

  static listRemoteCached () {
    return this.findAll<MStoryboard>({
      where: {
        cached: true,
        fileUrl: {
          [Op.ne]: null
        }
      }
    })
  }

  // ---------------------------------------------------------------------------

  getLocalFileUrl () {
    if (this.isLocal() && this.storage === FileStorage.OBJECT_STORAGE) {
      if (
        this.Video?.hasPrivateStaticPath() &&
        CONFIG.OBJECT_STORAGE.PROXY.PROXIFY_PRIVATE_FILES === true
      ) {
        return buildObjectStorageStoryboardPrivateFileUrl(this.Video, this.filename)
      }

      return buildObjectStoragePublicFileUrl({
        bucket: CONFIG.OBJECT_STORAGE.STORYBOARDS,
        key: generateStoryboardObjectStorageKey(this.filename)
      })
    }

    // Remote files are cached by our instance
    return WEBSERVER.URL + this.getFileStaticPath()
  }

  getFileStaticPath () {
    return LAZY_STATIC_PATHS.STORYBOARDS + this.filename
  }

  getFSPath () {
    return join(CONFIG.STORAGE.STORYBOARDS_DIR, this.filename)
  }

  getFSCachedPath () {
    return join(FILES_CACHE.STORYBOARDS.DIRECTORY, this.filename)
  }

  isLocal () {
    return !this.fileUrl
  }

  removeFile () {
    if (this.cached) {
      return remove(this.getFSCachedPath())
    }

    if (this.storage === FileStorage.OBJECT_STORAGE) {
      return removeStoryboardObjectStorage(this)
    }

    const path = this.getFSPath()
    logger.info('Removing storyboard file ' + path)
    return remove(path)
  }

  toFormattedJSON (this: MStoryboardVideo): Storyboard {
    const storyboardPath = this.isLocal() && this.storage === FileStorage.OBJECT_STORAGE
      ? this.getLocalFileUrl()
      : this.getFileStaticPath()

    return {
      fileUrl: this.getLocalFileUrl(),
      storyboardPath,

      totalHeight: this.totalHeight,
      totalWidth: this.totalWidth,

      spriteWidth: this.spriteWidth,
      spriteHeight: this.spriteHeight,

      spriteDuration: this.spriteDuration
    }
  }
}
