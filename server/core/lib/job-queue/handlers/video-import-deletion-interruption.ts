type ImportLike = {
  Video?: unknown
  destroy?: () => Promise<unknown>
}

type HandleDeletedVideoImportOptions = {
  err: unknown
  tempVideoPath?: string
  movedVideoDestPath?: string
  torrentPath?: string
}

type HandleDeletedVideoImportDeps = {
  loadImport: () => Promise<ImportLike | null | undefined>
  removePath: (path: string) => Promise<unknown>
}

export function isDeletedVideoImportInterruptionError (err: unknown) {
  const message = err instanceof Error
    ? err.message
    : String(err || '')

  return message.includes('Instance could not be reloaded because it does not exist anymore') ||
    message.includes('Video linked to import') ||
    message.includes('video import or video linked to this import does not exist anymore')
}

export async function handleDeletedVideoImportInterruption (
  options: HandleDeletedVideoImportOptions,
  deps: HandleDeletedVideoImportDeps
) {
  if (!isDeletedVideoImportInterruptionError(options.err)) return false

  const currentImport = await deps.loadImport()
  if (currentImport?.Video) return false

  for (const path of [ options.tempVideoPath, options.movedVideoDestPath, options.torrentPath ]) {
    if (!path) continue

    try {
      await deps.removePath(path)
    } catch {
      // Ignore cleanup errors here. The caller is already handling a deletion race.
    }
  }

  try {
    await currentImport?.destroy?.()
  } catch {
    // Ignore import cleanup errors here. The row may already be gone.
  }

  return true
}
