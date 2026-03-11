import { existsSync } from 'fs'
import { join } from 'path'

export type BinPlatformFolder = 'windows' | 'linux-amd64' | 'linux-arm64' | 'macos'

export function getBinPlatformFolder (): BinPlatformFolder {
  if (process.platform === 'win32') return 'windows'
  if (process.platform === 'darwin') return 'macos'

  if (process.platform === 'linux') {
    if (process.arch === 'arm64') return 'linux-arm64'
    return 'linux-amd64'
  }

  // Keep linux-amd64 as fallback for unsupported targets.
  return 'linux-amd64'
}

export function getPlatformExecutableName (baseName: string) {
  if (process.platform !== 'win32') return baseName

  return baseName.endsWith('.exe')
    ? baseName
    : `${baseName}.exe`
}

export function buildPlatformBinaryPath (baseBinDir: string, binaryName: string) {
  return join(baseBinDir, getBinPlatformFolder(), binaryName)
}

export function resolvePlatformBinaryPathWithLegacyFallback (baseBinDir: string, binaryName: string, extraLegacyCandidates: string[] = []) {
  const platformPath = buildPlatformBinaryPath(baseBinDir, binaryName)
  if (existsSync(platformPath)) return platformPath

  const legacyCandidates = [ binaryName, ...extraLegacyCandidates ].map(name => join(baseBinDir, name))

  for (const path of legacyCandidates) {
    if (existsSync(path)) return path
  }

  return platformPath
}
