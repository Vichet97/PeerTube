type HLSCleanupPayload = {
  type?: string
  deleteWebVideoFiles?: boolean
  transcodingPriority?: 'required' | 'optional'
}

export function reassignDeleteWebVideoFilesToLastHLSJob (children: HLSCleanupPayload[][]) {
  let lastRequiredHLSPayload: HLSCleanupPayload | undefined
  let lastHLSPayload: HLSCleanupPayload | undefined

  for (const chain of children) {
    for (const payload of chain) {
      if (payload.type !== 'new-resolution-to-hls') continue

      payload.deleteWebVideoFiles = false
      if (payload.transcodingPriority === 'required') {
        lastRequiredHLSPayload = payload
      }
      lastHLSPayload = payload
    }
  }

  const cleanupPayload = lastRequiredHLSPayload ?? lastHLSPayload
  if (cleanupPayload) {
    cleanupPayload.deleteWebVideoFiles = true
  }
}
