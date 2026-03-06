import { UserRightType } from '@peertube/peertube-models'
import { sha256 } from '@peertube/peertube-node-utils'
import { randomBytes } from 'crypto'
import {
  AllowNull,
  BelongsTo,
  Column,
  CreatedAt,
  DataType,
  Default,
  ForeignKey,
  Table
} from 'sequelize-typescript'
import { AccountModel } from '../account/account.js'
import { ActorModel } from '../actor/actor.js'
import { SequelizeModel } from '../shared/index.js'
import { UserModel } from './user.js'

const API_TOKEN_PREFIX = 'peertube_api_'

@Table({
  tableName: 'userApiToken',
  timestamps: true,
  updatedAt: false,
  indexes: [
    {
      fields: [ 'tokenHash' ],
      unique: true
    },
    {
      fields: [ 'userId' ]
    }
  ]
})
export class UserApiTokenModel extends SequelizeModel<UserApiTokenModel> {
  @AllowNull(false)
  @ForeignKey(() => UserModel)
  @Column
  declare userId: number

  @BelongsTo(() => UserModel, {
    foreignKey: {
      allowNull: false
    },
    onDelete: 'cascade'
  })
  declare User: Awaited<UserModel>

  @AllowNull(false)
  @Column
  declare tokenHash: string

  @AllowNull(false)
  @Column
  declare name: string

  @AllowNull(true)
  @Column
  declare description: string

  @AllowNull(true)
  @Column
  declare expiresAt: Date

  @AllowNull(false)
  @Default([])
  @Column(DataType.JSONB)
  declare scopes: UserRightType[]

  @CreatedAt
  declare createdAt: Date

  @AllowNull(true)
  @Column
  declare lastUsedAt: Date

  static generateToken (): string {
    const randomPart = randomBytes(16).toString('hex')
    return API_TOKEN_PREFIX + randomPart
  }

  static hashToken (rawToken: string): string {
    return sha256(rawToken, 'hex')
  }

  static async getByToken (bearerToken: string) {
    if (!bearerToken || !bearerToken.startsWith(API_TOKEN_PREFIX)) return null

    const tokenHash = UserApiTokenModel.hashToken(bearerToken)
    return UserApiTokenModel.findOne({
      where: { tokenHash },
      include: [
        {
          model: UserModel.unscoped(),
          required: true,
          include: [
            {
              attributes: [ 'id' ],
              model: AccountModel.unscoped(),
              required: true,
              include: [
                {
                  attributes: [ 'id', 'url' ],
                  model: ActorModel.unscoped(),
                  required: true
                }
              ]
            }
          ]
        }
      ]
    })
  }

  static async updateLastUsed (id: number) {
    await UserApiTokenModel.update(
      { lastUsedAt: new Date() },
      { where: { id } }
    )
  }
}
