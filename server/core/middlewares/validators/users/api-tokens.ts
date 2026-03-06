import { UserRight, UserRightType } from '@peertube/peertube-models'
import { isIdValid } from '@server/helpers/custom-validators/misc.js'
import express from 'express'
import { body, param } from 'express-validator'
import { areValidationErrors } from '../shared/utils.js'

const VALID_USER_RIGHTS = Object.values(UserRight).filter(v => typeof v === 'number') as UserRightType[]

export const createUserApiTokenValidator = [
  body('name')
    .trim()
    .isLength({ min: 1, max: 100 }).withMessage('Should have a name between 1 and 100 characters'),

  body('description')
    .optional()
    .trim()
    .isLength({ max: 500 }).withMessage('Should have a description of maximum 500 characters'),

  body('expiresAt')
    .optional({ values: 'null' })
    .custom((value) => {
      if (value === null || value === undefined) return true
      const date = new Date(value)
      if (isNaN(date.getTime())) return false
      if (date <= new Date()) return false
      return true
    }).withMessage('Should be a valid future ISO date or null'),

  body('scopes')
    .isArray().withMessage('Should be an array of scopes'),
  body('scopes.*')
    .isInt({ min: 0 }).withMessage('Each scope must be a valid UserRight value'),

  async (req: express.Request, res: express.Response, next: express.NextFunction) => {
    if (areValidationErrors(req, res)) return

    const user = res.locals.oauth.token.user
    const scopes: UserRightType[] = req.body.scopes || []

    for (const scope of scopes) {
      if (!VALID_USER_RIGHTS.includes(scope)) {
        return res.fail({
          status: 400,
          message: req.t('Invalid scope value')
        })
      }
      if (!user.hasRight(scope)) {
        return res.fail({
          status: 403,
          message: req.t('You can only grant scopes you possess')
        })
      }
    }

    return next()
  }
]

export const revokeUserApiTokenValidator = [
  param('id')
    .custom(isIdValid),

  async (req: express.Request, res: express.Response, next: express.NextFunction) => {
    if (areValidationErrors(req, res)) return

    const { UserApiTokenModel } = await import('@server/models/user/user-api-token.js')
    const token = await UserApiTokenModel.findByPk(req.params.id)

    if (!token || token.userId !== res.locals.oauth.token.user.id) {
      return res.fail({
        status: 404,
        message: req.t('API token not found')
      })
    }

    res.locals.apiToken = token

    return next()
  }
]
