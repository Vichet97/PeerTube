import express from 'express'
import { body, param, query } from 'express-validator'
import { isValidJobState, isValidJobType } from '../../helpers/custom-validators/jobs.js'
import { loggerTagsFactory } from '../../helpers/logger.js'
import { areValidationErrors } from './shared/index.js'

const lTags = loggerTagsFactory('validators', 'jobs')

const listJobsValidator = [
  param('state')
    .optional()
    .custom(isValidJobState),

  query('jobType')
    .optional()
    .custom(isValidJobType),

  query('search')
    .optional()
    .trim()
    .isLength({ min: 1, max: 255 }),

  (req: express.Request, res: express.Response, next: express.NextFunction) => {
    if (areValidationErrors(req, res, lTags())) return

    return next()
  }
]

const createMoveStorageJobsValidator = [
  body('storage')
    .isIn([ 'object-storage', 'file-system' ])
    .withMessage('Storage must be object-storage or file-system'),

  body('scope')
    .optional()
    .isIn([ 'all', 'disk-relief' ])
    .withMessage('Scope must be all or disk-relief'),

  (req: express.Request, res: express.Response, next: express.NextFunction) => {
    if (areValidationErrors(req, res, lTags())) return

    return next()
  }
]

const createRetryTranscodingJobsValidator = [
  (req: express.Request, res: express.Response, next: express.NextFunction) => {
    if (areValidationErrors(req, res, lTags())) return

    return next()
  }
]

// ---------------------------------------------------------------------------

export {
  createMoveStorageJobsValidator,
  createRetryTranscodingJobsValidator,
  listJobsValidator
}
