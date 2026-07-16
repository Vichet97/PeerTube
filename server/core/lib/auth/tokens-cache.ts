import { LRUCache } from 'lru-cache'
import { MOAuthTokenUser } from '@server/types/models/index.js'
import { LRU_CACHE } from '../../initializers/constants.js'

export class TokensCache {

  private static instance: TokensCache

  private readonly accessTokenCache = new LRUCache<string, MOAuthTokenUser>({ max: LRU_CACHE.USER_TOKENS.MAX_SIZE })
  private readonly apiTokenCache = new LRUCache<string, MOAuthTokenUser>({
    max: LRU_CACHE.USER_TOKENS.MAX_SIZE,
    ttl: 10 * 1000
  })
  private readonly userHavingToken = new LRUCache<number, string>({ max: LRU_CACHE.USER_TOKENS.MAX_SIZE })

  private constructor () { }

  static get Instance () {
    return this.instance || (this.instance = new this())
  }

  hasToken (token: string) {
    return this.accessTokenCache.has(token)
  }

  getByToken (token: string) {
    return this.accessTokenCache.get(token)
  }

  setToken (token: MOAuthTokenUser) {
    this.accessTokenCache.set(token.accessToken, token)
    this.userHavingToken.set(token.userId, token.accessToken)
  }

  hasApiToken (token: string) {
    return this.apiTokenCache.has(token)
  }

  getApiToken (token: string) {
    return this.apiTokenCache.get(token)
  }

  setApiToken (token: MOAuthTokenUser) {
    this.apiTokenCache.set(token.accessToken, token, { ttl: this.getApiTokenTTL(token) })
  }

  deleteUserToken (userId: number) {
    this.clearCacheByUserId(userId)
  }

  clearCacheByUserId (userId: number) {
    const token = this.userHavingToken.get(userId)

    if (token !== undefined) {
      this.accessTokenCache.delete(token)
      this.userHavingToken.delete(userId)
    }
  }

  clearCacheByToken (token: string) {
    const tokenModel = this.accessTokenCache.get(token)

    if (tokenModel !== undefined) {
      this.userHavingToken.delete(tokenModel.userId)
      this.accessTokenCache.delete(token)
    }

    this.apiTokenCache.delete(token)
  }

  private getApiTokenTTL (token: MOAuthTokenUser) {
    const maxTTL = 10 * 1000
    const expiresAt = token.accessTokenExpiresAt?.getTime()

    if (!expiresAt) return maxTTL

    return Math.max(1, Math.min(maxTTL, expiresAt - Date.now()))
  }
}
