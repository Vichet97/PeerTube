export function isRetryableTransactionError (err: unknown) {
  if (!err || typeof err !== 'object') return false

  const error = err as {
    name?: string
    message?: string
    code?: string
    parent?: { code?: string, message?: string }
    original?: { code?: string, message?: string }
  }

  if (error.name === 'SequelizeDatabaseError') return true

  const code = error.parent?.code || error.original?.code || error.code
  if (code === '40001' || code === '40P01') return true

  const message = (
    error.parent?.message ||
    error.original?.message ||
    error.message ||
    ''
  ).toLowerCase()

  return message.includes('could not serialize access') ||
    message.includes('deadlock detected')
}
