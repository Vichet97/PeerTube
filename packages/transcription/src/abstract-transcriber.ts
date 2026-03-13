import { SimpleLogger } from '@peertube/peertube-models'
import { buildSUUID } from '@peertube/peertube-node-utils'
import { $ } from 'execa'
import { existsSync } from 'node:fs'
import { PerformanceObserver } from 'node:perf_hooks'
import { join } from 'path'
import { TranscriptFile, TranscriptFormat } from './transcript-file.js'
import { TranscriptionEngine } from './transcription-engine.js'
import { TranscriptionModel } from './transcription-model.js'
import { TranscriptionRun } from './transcription-run.js'

export interface TranscribeArgs {
  mediaFilePath: string
  model: TranscriptionModel
  format: TranscriptFormat
  transcriptDirectory: string

  language?: string
  runId?: string
}

export abstract class AbstractTranscriber {
  engine: TranscriptionEngine

  protected binDirectory: string
  protected enginePath: string

  protected logger: SimpleLogger

  protected performanceObserver?: PerformanceObserver
  protected run?: TranscriptionRun

  constructor (options: {
    engine: TranscriptionEngine
    binDirectory?: string
    enginePath?: string

    logger: SimpleLogger
    performanceObserver?: PerformanceObserver
  }) {
    const { engine, logger, enginePath, binDirectory, performanceObserver } = options

    this.engine = engine
    this.enginePath = enginePath
    this.logger = logger
    this.binDirectory = binDirectory
    this.performanceObserver = performanceObserver
  }

  createRun (uuid: string = buildSUUID()) {
    this.run = new TranscriptionRun(this.logger, uuid)
  }

  startRun () {
    this.run.start()
  }

  stopRun () {
    this.run.stop()
    delete this.run
  }

  assertLanguageDetectionAvailable (language?: string) {
    if (!this.engine.languageDetection && !language) {
      throw new Error(`Language detection isn't available in ${this.engine.name}. A language must me provided explicitly.`)
    }
  }

  supports (model: TranscriptionModel) {
    return model.format === 'PyTorch'
  }

  protected getEngineBinary () {
    if (this.enginePath) return this.enginePath
    if (this.binDirectory) {
      const localBin = join(this.binDirectory, this.engine.command)
      if (existsSync(localBin)) return localBin
    }

    return this.engine.command
  }

  protected getExec (env?: { [id: string]: string }) {
    const logLevels = {
      command: 'debug',
      output: 'debug',
      ipc: 'debug',
      error: 'error',
      duration: 'debug'
    }

    return $({
      verbose: (_verboseLine, { message, ...verboseObject }) => {
        const level = logLevels[verboseObject.type]

        this.logger[level](message, verboseObject)
      },

      env
    })
  }

  protected getExecEnv () {
    return undefined
  }

  protected async runEngineCommand (args: string[]) {
    const $$ = this.getExec(this.getExecEnv())
    const primaryBinary = this.getEngineBinary()

    try {
      await $$`${primaryBinary} ${args}`
      return
    } catch (err) {
      if (!this.isMissingExecutableError(err)) throw err

      const fallback = this.getPythonModuleFallback()
      if (!fallback) throw err

      const reason = err instanceof Error ? err.message : 'unknown error'
      this.logger.warn(
        `Cannot execute transcription engine binary ${primaryBinary} (${reason}). ` +
        `Falling back to ${fallback.command} -m ${fallback.module}.`
      )

      await $$`${fallback.command} ${[ '-m', fallback.module, ...args ]}`
    }
  }

  private isMissingExecutableError (err: unknown) {
    if (!err || typeof err !== 'object') return false

    const code = (err as any).code
    if (code === 'ENOENT') return true

    const message = String((err as any).message || '')
    return message.includes('ENOENT')
  }

  private getPythonModuleFallback () {
    switch (this.engine.name) {
      case 'whisper-ctranslate2':
        return { command: 'python3', module: 'whisper_ctranslate2' }
      case 'openai-whisper':
        return { command: 'python3', module: 'whisper' }
      default:
        return undefined
    }
  }

  protected async installPythonPackage (directory: string, packageName: string, packageVersion: string) {
    const $$ = this.getExec()
    const packageSpec = `${packageName}==${packageVersion}`

    const installStrategies: [ string, string[] ][] = [
      [ 'pip3', [ 'install', '-U', '-t', directory, packageSpec ] ],
      [ 'python3', [ '-m', 'pip', 'install', '-U', '-t', directory, packageSpec ] ],
      [ 'pip', [ 'install', '-U', '-t', directory, packageSpec ] ]
    ]

    let lastError: unknown

    for (const [ command, args ] of installStrategies) {
      try {
        await $$`${command} ${args}`
        return
      } catch (err) {
        lastError = err
      }
    }

    throw lastError instanceof Error
      ? lastError
      : new Error(`Cannot install ${packageSpec}: no pip command worked.`)
  }

  abstract transcribe (options: TranscribeArgs): Promise<TranscriptFile>

  abstract install (path: string): Promise<void>
}
