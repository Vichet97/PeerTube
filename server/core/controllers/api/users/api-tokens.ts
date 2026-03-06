import { UserApiToken, UserApiTokenCreate } from '@peertube/peertube-models'
import { asyncMiddleware, authenticate } from '@server/middlewares/index.js'
import { createUserApiTokenValidator, revokeUserApiTokenValidator } from '@server/middlewares/validators/users/api-tokens.js'
import { UserApiTokenModel } from '@server/models/user/user-api-token.js'
import express from 'express'

const apiTokensRouter = express.Router()

apiTokensRouter.get(
  '/api-tokens',
  authenticate,
  asyncMiddleware(listApiTokens)
)

apiTokensRouter.post(
  '/api-tokens',
  authenticate,
  createUserApiTokenValidator,
  asyncMiddleware(createApiToken)
)

apiTokensRouter.delete(
  '/api-tokens/:id',
  authenticate,
  revokeUserApiTokenValidator,
  asyncMiddleware(revokeApiToken)
)

export {
  apiTokensRouter
}

// ---------------------------------------------------------------------------

async function listApiTokens (req: express.Request, res: express.Response) {
  const userId = res.locals.oauth.token.user.id

  const tokens = await UserApiTokenModel.findAll({
    where: { userId },
    order: [ [ 'createdAt', 'DESC' ] ]
  })

  const data: UserApiToken[] = tokens.map(t => formatApiToken(t))

  return res.json({ data, total: data.length })
}

async function createApiToken (req: express.Request, res: express.Response) {
  const user = res.locals.oauth.token.user
  const body: UserApiTokenCreate = req.body

  const rawToken = UserApiTokenModel.generateToken()
  const tokenHash = UserApiTokenModel.hashToken(rawToken)

  let expiresAt: Date | null = null
  if (body.expiresAt) {
    expiresAt = new Date(body.expiresAt)
  }

  const scopes = body.scopes?.length ? body.scopes : []

  const token = await UserApiTokenModel.create({
    userId: user.id,
    tokenHash,
    name: body.name,
    description: body.description || null,
    expiresAt,
    scopes
  })

  const formatted: UserApiToken = formatApiToken(token)
  const response = {
    ...formatted,
    token: rawToken
  }

  return res.json(response)
}

async function revokeApiToken (req: express.Request, res: express.Response) {
  const apiToken = res.locals.apiToken

  await apiToken.destroy()

  return res.status(204).end()
}

function formatApiToken (token: UserApiTokenModel): UserApiToken {
  const suffix = token.tokenHash.slice(-4)
  const masked = `peertube_api_****...${suffix}`
  return {
    id: token.id,
    name: token.name,
    description: token.description,
    expiresAt: token.expiresAt ? token.expiresAt.toISOString() : null,
    scopes: token.scopes,
    createdAt: token.createdAt,
    lastUsedAt: token.lastUsedAt ? token.lastUsedAt.toISOString() : null,
    maskedToken: masked
  }
}
