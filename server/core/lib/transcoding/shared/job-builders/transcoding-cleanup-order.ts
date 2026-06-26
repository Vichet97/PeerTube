type HLSCleanupPayload = {
  type?: string
  deleteWebVideoFiles?: boolean
}

export function reassignDeleteWebVideoFilesToLastHLSJob (children: HLSCleanupPayload[][]) {
  let lastHLSPayload: HLSCleanupPayload | undefined

  for (const chain of children) {
    for (const payload of chain) {
      if (payload.type !== 'new-resolution-to-hls') continue

      payload.deleteWebVideoFiles = false
      lastHLSPayload = payload
    }
  }

  if (lastHLSPayload) {
    lastHLSPayload.deleteWebVideoFiles = true
  }
}
