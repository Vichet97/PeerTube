import { forceNumber } from '@peertube/peertube-core-utils'
import { Op, QueryTypes, Transaction } from 'sequelize'
import { AllowNull, BelongsTo, Column, CreatedAt, Default, ForeignKey, IsInt, Table, Unique, UpdatedAt } from 'sequelize-typescript'
import { SequelizeModel } from '../shared/sequelize-type.js'
import { VideoModel } from './video.js'

export type VideoJobInfoColumnType = 'pendingMove' | 'pendingTranscode' | 'pendingTranscription'

@Table({
  tableName: 'videoJobInfo',
  indexes: [
    {
      fields: [ 'videoId' ],
      where: {
        videoId: {
          [Op.ne]: null
        }
      }
    }
  ]
})
export class VideoJobInfoModel extends SequelizeModel<VideoJobInfoModel> {
  @CreatedAt
  declare createdAt: Date

  @UpdatedAt
  declare updatedAt: Date

  @AllowNull(false)
  @Default(0)
  @IsInt
  @Column
  declare pendingMove: number

  @AllowNull(false)
  @Default(0)
  @IsInt
  @Column
  declare pendingTranscode: number

  @AllowNull(false)
  @Default(0)
  @IsInt
  @Column
  declare pendingTranscription: number

  @ForeignKey(() => VideoModel)
  @Unique
  @Column
  declare videoId: number

  @BelongsTo(() => VideoModel, {
    foreignKey: {
      allowNull: false
    },
    onDelete: 'cascade'
  })
  declare Video: Awaited<VideoModel>

  static load (videoId: number, transaction?: Transaction) {
    const where = {
      videoId
    }

    return VideoJobInfoModel.findOne({ where, transaction })
  }

  static loadByUUID (videoUUID: string, transaction?: Transaction) {
    return VideoJobInfoModel.findOne({
      include: [
        {
          model: VideoModel.unscoped(),
          required: true,
          where: { uuid: videoUUID }
        }
      ],
      transaction
    })
  }

  static async increaseOrCreate (videoUUID: string, column: VideoJobInfoColumnType, amountArg = 1): Promise<number> {
    const options = { type: QueryTypes.SELECT as QueryTypes.SELECT, bind: { videoUUID } }
    const amount = forceNumber(amountArg)

    const [ result ] = await VideoJobInfoModel.sequelize.query(
      `
    INSERT INTO "videoJobInfo" ("videoId", "${column}", "createdAt", "updatedAt")
    SELECT
      "video"."id" AS "videoId", ${amount}, NOW(), NOW()
    FROM
      "video"
    WHERE
      "video"."uuid" = $videoUUID
    ON CONFLICT ("videoId") DO UPDATE
    SET
      "${column}" = "videoJobInfo"."${column}" + ${amount},
      "updatedAt" = NOW()
    RETURNING
      "${column}"
    `,
      options
    )

    return result[column]
  }

  static async decrease (videoUUID: string, column: VideoJobInfoColumnType, amountArg = 1): Promise<number> {
    const options = { type: QueryTypes.SELECT as QueryTypes.SELECT, bind: { videoUUID } }
    const amount = Math.max(1, forceNumber(amountArg) || 1)

    const result = await VideoJobInfoModel.sequelize.query(
      `
    UPDATE
      "videoJobInfo"
    SET
      "${column}" = GREATEST("videoJobInfo"."${column}" - ${amount}, 0),
      "updatedAt" = NOW()
    FROM "video"
    WHERE
      "video"."id" = "videoJobInfo"."videoId" AND "video"."uuid" = $videoUUID
    RETURNING
      "${column}";
    `,
      options
    )

    if (result.length === 0) return 0

    return result[0][column]
  }

  static async abortAllTasks (videoUUID: string, column: VideoJobInfoColumnType): Promise<void> {
    const options = { type: QueryTypes.UPDATE as QueryTypes.UPDATE, bind: { videoUUID } }

    await VideoJobInfoModel.sequelize.query(
      `
    UPDATE
      "videoJobInfo"
    SET
      "${column}" = 0,
      "updatedAt" = NOW()
    FROM "video"
    WHERE
      "video"."id" = "videoJobInfo"."videoId" AND "video"."uuid" = $videoUUID
    `,
      options
    )
  }

  /**
   * Atomically replace pipeline counters only when they still match the
   * snapshot inspected by a reconciler. Queue state lives in Redis, so this
   * compare-and-set prevents a concurrently-created job from being erased by
   * a stale database repair.
   */
  static async replaceCountersIfUnchanged (options: {
    videoUUID: string
    expected: Record<VideoJobInfoColumnType, number>
    next: Record<VideoJobInfoColumnType, number>
  }): Promise<boolean> {
    const { videoUUID, expected, next } = options
    const result = await VideoJobInfoModel.sequelize.query(
      `
        UPDATE "videoJobInfo"
        SET
          "pendingMove" = $nextPendingMove,
          "pendingTranscode" = $nextPendingTranscode,
          "pendingTranscription" = $nextPendingTranscription,
          "updatedAt" = NOW()
        FROM "video"
        WHERE
          "video"."id" = "videoJobInfo"."videoId"
          AND "video"."uuid" = $videoUUID
          AND "videoJobInfo"."pendingMove" = $expectedPendingMove
          AND "videoJobInfo"."pendingTranscode" = $expectedPendingTranscode
          AND "videoJobInfo"."pendingTranscription" = $expectedPendingTranscription
        RETURNING "videoJobInfo"."videoId";
      `,
      {
        type: QueryTypes.SELECT,
        bind: {
          videoUUID,
          expectedPendingMove: expected.pendingMove,
          expectedPendingTranscode: expected.pendingTranscode,
          expectedPendingTranscription: expected.pendingTranscription,
          nextPendingMove: next.pendingMove,
          nextPendingTranscode: next.pendingTranscode,
          nextPendingTranscription: next.pendingTranscription
        }
      }
    )

    return result.length !== 0
  }
}
