import express from 'express'
import RateLimit, { Options as RateLimitHandlerOptions } from 'express-rate-limit'
import { UserRole, UserRoleType } from '@peertube/peertube-models'
import { CONFIG } from '@server/initializers/config.js'
import { RunnerModel } from '@server/models/runner/runner.js'
import { optionalAuthenticate } from './auth.js'
import { logger } from '@server/helpers/logger.js'

const whitelistRoles = new Set<UserRoleType>([ UserRole.ADMINISTRATOR, UserRole.MODERATOR ])

export function buildRateLimiter (limiterOptions: {
  windowMs: number
  max: number
  skipFailedRequests?: boolean
  allowBypassWithApiToken?: boolean
}) {
  return RateLimit({
    windowMs: limiterOptions.windowMs,
    max: limiterOptions.max,
    skipFailedRequests: limiterOptions.skipFailedRequests,

    handler: (req, res, next, options) => {
      // Bypass rate limit for registered runners
      if (req.body?.runnerToken) {
        return RunnerModel.loadByToken(req.body.runnerToken)
          .then(runner => {
            if (runner) return next()

            return sendRateLimited(req, res, options)
          })
      }

      // Bypass rate limit for admins/moderators or API token holders (if configured)
      return optionalAuthenticate(req, res, () => {
        if (res.locals.authenticated === true) {
          if (whitelistRoles.has(res.locals.oauth.token.User.role)) {
            return next()
          }

          // bypass_with_token: true — only tokens from /my-account/api-tokens skip the limit
          if (limiterOptions.allowBypassWithApiToken === true && (res.locals.oauth.token as any).isApiToken === true) {
            return next()
          }
        }

        return sendRateLimited(req, res, options)
      })
    }
  })
}

export const apiRateLimiter = buildRateLimiter({
  windowMs: CONFIG.RATES_LIMIT.API.WINDOW_MS,
  max: CONFIG.RATES_LIMIT.API.MAX,
  allowBypassWithApiToken: CONFIG.RATES_LIMIT.API.BYPASS_WITH_TOKEN
})

export const activityPubRateLimiter = buildRateLimiter({
  windowMs: CONFIG.RATES_LIMIT.ACTIVITY_PUB.WINDOW_MS,
  max: CONFIG.RATES_LIMIT.ACTIVITY_PUB.MAX
})

// ---------------------------------------------------------------------------
// Private
// ---------------------------------------------------------------------------

function sendRateLimited (req: express.Request, res: express.Response, options: RateLimitHandlerOptions) {
  logger.debug('Rate limit exceeded for route ' + req.originalUrl)

  return res.status(options.statusCode).send(options.message)
}
