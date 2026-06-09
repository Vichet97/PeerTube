type VideoRepairJobIndexLike<T> = {
  byVideoUUID: Map<string, T[]>
  byVideoId: Map<number, T[]>
  byVideoImportId: Map<number, T[]>
}

export function getUnprocessedOrphanedVideoRepairJobRefs<T> (options: {
  index: VideoRepairJobIndexLike<T>
  existingVideoUUIDs: Set<string>
  existingVideoIds: Set<number>
  existingVideoImportIds: Set<number>
  processedJobKeys: Set<string>
  dedupeRefs: (refs: T[]) => T[]
  getRefKey: (ref: T) => string
}) {
  const {
    index,
    existingVideoUUIDs,
    existingVideoIds,
    existingVideoImportIds,
    processedJobKeys,
    dedupeRefs,
    getRefKey
  } = options

  const orphanRefs: T[] = []

  for (const [ videoUUID, refs ] of index.byVideoUUID) {
    if (existingVideoUUIDs.has(videoUUID)) continue
    orphanRefs.push(...refs)
  }

  for (const [ videoId, refs ] of index.byVideoId) {
    if (existingVideoIds.has(videoId)) continue
    orphanRefs.push(...refs)
  }

  for (const [ videoImportId, refs ] of index.byVideoImportId) {
    if (existingVideoImportIds.has(videoImportId)) continue
    orphanRefs.push(...refs)
  }

  return dedupeRefs(orphanRefs)
    .filter(ref => !processedJobKeys.has(getRefKey(ref)))
}
