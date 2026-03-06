import { UserRightType } from './user-right.enum.js'

export interface UserApiTokenCreate {
  name: string
  description?: string
  expiresAt?: string | null
  scopes: UserRightType[]
}

export interface UserApiToken {
  id: number
  name: string
  description?: string | null
  expiresAt?: string | null
  scopes: UserRightType[]
  createdAt: Date | string
  lastUsedAt?: Date | string | null
  maskedToken: string
}
