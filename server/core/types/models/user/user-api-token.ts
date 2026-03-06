import { PickWith } from '@peertube/peertube-typescript-utils'
import { UserApiTokenModel } from '@server/models/user/user-api-token.js'
import { MUserAccountUrl } from './user.js'

type Use<K extends keyof UserApiTokenModel, M> = PickWith<UserApiTokenModel, K, M>

// ############################################################################

export type MUserApiToken = Omit<UserApiTokenModel, 'User'>
export type MUserApiTokenUser = MUserApiToken & Use<'User', MUserAccountUrl>
