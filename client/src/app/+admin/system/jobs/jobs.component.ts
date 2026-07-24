import { CommonModule } from '@angular/common'
import { Component, OnInit, inject, viewChild } from '@angular/core'
import { FormsModule } from '@angular/forms'
import { Notifier, RestPagination } from '@app/core'
import { SelectOptionsComponent } from '@app/shared/shared-forms/select/select-options.component'

import { Job, JobState, JobType } from '@peertube/peertube-models'
import { peertubeLocalStorage } from '@root-helpers/peertube-web-storage'
import { SortMeta } from 'primeng/api'
import { interval, Subscription } from 'rxjs'
import { switchMap, tap } from 'rxjs/operators'
import { SelectOptionsItem } from 'src/types'
import { JobStateClient } from '../../../../types/job-state-client.type'
import { JobTypeClient } from '../../../../types/job-type-client.type'
import { ButtonComponent } from '../../../shared/shared-main/buttons/button.component'
import { NumberFormatterPipe } from '../../../shared/shared-main/common/number-formatter.pipe'
import { TableColumnInfo, TableComponent, TableQueryParams } from '../../../shared/shared-tables/table.component'
import { AdvancedInputFilterComponent } from '../../../shared/shared-forms/advanced-input-filter.component'
import {
  GlobalQueueCleanupStatus,
  JobService,
  RetainedLocalFilesCleanupStatus,
  VideoMaintenanceCounts,
  VideoPipelineReconciliationStatus,
  VideoSystemResetStatus
} from './job.service'

type ColumnName = 'select' | 'id' | 'type' | 'priority' | 'state' | 'progress' | 'createdAt' | 'processed'

type QueryParams = TableQueryParams & {
  jobType: string
  jobState: string
  search?: string
}

const PROGRESS_JOB_TYPES = new Set<JobTypeClient>([
  'video-transcoding',
  'video-import',
  'move-to-object-storage',
  'move-video-file-to-object-storage',
  'move-hls-playlist-to-object-storage',
  'move-thumbnail-to-object-storage'
])

@Component({
  selector: 'my-jobs',
  templateUrl: './jobs.component.html',
  styleUrls: [ './jobs.component.scss' ],
  imports: [
    CommonModule,
    FormsModule,
    ButtonComponent,
    AdvancedInputFilterComponent,
    SelectOptionsComponent,
    TableComponent,
    NumberFormatterPipe
  ]
})
export class JobsComponent implements OnInit {
  private static LS_STATE = 'jobs-list-state'
  private static LS_TYPE = 'jobs-list-type'

  private jobsService = inject(JobService)
  private notifier = inject(Notifier)

  creatingMoveJobs = false
  creatingRetryTranscodingJobs = false
  creatingTranscriptionJobs = false
  creatingStoryboardJobs = false
  cancellingAllJobs = false
  clearingGlobalQueueBacklog = false
  cleaningRetainedLocalFiles = false
  reconcilingVideoPipeline = false

  selectedJobIds = new Set<number>()
  retryingJobIds = new Set<number>()
  recheckingVideosStatus = false
  private resetStatusPollingSub?: Subscription
  private globalQueueCleanupPollingSub?: Subscription
  private retainedLocalFilesCleanupPollingSub?: Subscription
  private videoPipelineReconciliationPollingSub?: Subscription

  jobsCount = 0
  videoMaintenanceCounts: VideoMaintenanceCounts = {
    localStorageVideos: 0,
    objectStorageVideos: 0,
    failedTranscodingVideos: 0,
    notYetTranscodedVideos: 0
  }

  readonly table = viewChild<TableComponent<Job, ColumnName>>('table')

  jobState: JobStateClient = 'all'
  jobStates: JobStateClient[] = [ 'all', 'active', 'completed', 'failed', 'cancelled', 'waiting', 'delayed' ]
  jobStateItems: SelectOptionsItem[] = this.jobStates.map(s => ({
    id: s,
    label: s,
    classes: this.getJobStateClasses(s)
  }))

  jobType: JobTypeClient = 'all'
  jobTypes: JobTypeClient[] = [
    'all',

    'activitypub-cleaner',
    'activitypub-follow',
    'activitypub-http-broadcast-parallel',
    'activitypub-http-broadcast',
    'activitypub-http-fetcher',
    'activitypub-http-unicast',
    'activitypub-refresher',
    'actor-keys',
    'after-video-channel-import',
    'create-user-export',
    'email',
    'federate-video',
    'generate-video-storyboard',
    'manage-video-torrent',
    'move-to-file-system',
    'move-to-object-storage',
    'move-video-file-to-object-storage',
    'move-hls-playlist-to-object-storage',
    'move-thumbnail-to-object-storage',
    'notify',
    'transcoding-job-builder',
    'video-channel-import',
    'video-channel-reset',
    'video-file-import',
    'video-import',
    'video-live-ending',
    'video-redundancy',
    'video-studio-edition',
    'video-transcoding',
    'video-transcription',
    'videos-views-stats'
  ]
  jobTypeItems: SelectOptionsItem[] = this.jobTypes.map(i => ({ id: i, label: i }))

  columns: TableColumnInfo<ColumnName>[] = [
    { id: 'select', class: 'job-select', label: '', sortable: false },
    { id: 'id', class: 'job-id', label: $localize`ID`, sortable: false },
    { id: 'type', class: 'job-type', label: $localize`Type`, sortable: false },
    { id: 'priority', class: 'job-priority', label: $localize`Priority`, labelSmall: $localize`(1 = highest priority)`, sortable: false },
    { id: 'state', class: 'job-state', label: $localize`State`, isDisplayed: () => this.jobState === 'all', sortable: false },
    { id: 'progress', class: 'job-progress', label: $localize`Progress`, isDisplayed: () => this.hasGlobalProgress(), sortable: false },
    { id: 'createdAt', class: 'job-date', label: $localize`Created`, sortable: true },
    { id: 'processed', label: $localize`Processed/Finished`, sortable: false }
  ]
  customUpdateUrl: typeof this._customUpdateUrl
  customParseQueryParams: typeof this._customParseQueryParams
  dataLoader: typeof this._dataLoader

  constructor () {
    this.customUpdateUrl = this._customUpdateUrl.bind(this)
    this.customParseQueryParams = this._customParseQueryParams.bind(this)
    this.dataLoader = this._dataLoader.bind(this)
  }

  ngOnInit () {
    this.loadJobStateAndType()
    this.loadVideoMaintenanceCounts()
    this.resumeResetStatusPollingIfNeeded()
    this.resumeGlobalQueueCleanupPollingIfNeeded()
    this.resumeRetainedLocalFilesCleanupPollingIfNeeded()
    this.resumeVideoPipelineReconciliationPollingIfNeeded()
  }

  getJobStateClasses (state: JobStateClient): string[] {
    switch (state) {
      case 'all':
        return []

      case 'active':
        return [ 'pt-badge', 'badge-blue' ]

      case 'completed':
        return [ 'pt-badge', 'badge-green' ]

      case 'delayed':
      case 'prioritized':
      case 'paused':
        return [ 'pt-badge', 'badge-brown' ]

      case 'cancelled':
        return [ 'pt-badge', 'badge-grey' ]

      case 'failed':
        return [ 'pt-badge', 'badge-red' ]

      case 'waiting':
      case 'waiting-children':
        return [ 'pt-badge', 'badge-yellow' ]
    }

    // Do not remove, to ensure all cases are handled by the switch
    return state
  }

  onJobStateOrTypeChanged () {
    this.table().onFilter()
    this.saveJobStateAndType()
  }

  hasGlobalProgress () {
    return this.jobType === 'all' || PROGRESS_JOB_TYPES.has(this.jobType)
  }

  hasProgress (job: Job) {
    return PROGRESS_JOB_TYPES.has(job.type)
  }

  getProgress (job: Job) {
    if (job.state === 'active') {
      const p = job.progress
      return (p != null ? p : 0) + '%'
    }
    return ''
  }

  getRandomJobTypeBadge (type: string) {
    return this.table().getRandomBadge('type', type)
  }

  private _customUpdateUrl (): Partial<QueryParams> {
    return {
      jobType: this.jobType,
      jobState: this.jobState
    }
  }

  private _customParseQueryParams (queryParams: QueryParams) {
    if (queryParams.jobType) {
      this.jobType = queryParams.jobType as JobTypeClient
    }

    if (queryParams.jobState) {
      this.jobState = queryParams.jobState as JobStateClient
    }
  }

  private _dataLoader (options: {
    pagination: RestPagination
    sort: SortMeta
    search: string
  }) {
    const { pagination, sort, search } = options

    let jobState = this.jobState as JobState
    if (this.jobState === 'all') jobState = null

    return this.jobsService.listJobs({
      jobState,
      jobType: this.jobType,
      search,
      pagination,
      sort
    }).pipe(
      tap(result => {
        this.jobsCount = result.total
      })
    )
  }

  private loadJobStateAndType () {
    const state = peertubeLocalStorage.getItem(JobsComponent.LS_STATE)
    if (state && state !== 'undefined') this.jobState = state as JobState

    const jobType = peertubeLocalStorage.getItem(JobsComponent.LS_TYPE)
    if (jobType && jobType !== 'undefined') this.jobType = jobType as JobType
  }

  private saveJobStateAndType () {
    peertubeLocalStorage.setItem(JobsComponent.LS_STATE, this.jobState)
    peertubeLocalStorage.setItem(JobsComponent.LS_TYPE, this.jobType)
  }

  createMoveStorageJobs (storage: 'object-storage' | 'file-system', scope: 'all' | 'disk-relief' = 'all') {
    if (this.creatingMoveJobs) return

    this.creatingMoveJobs = true
    const target = storage === 'object-storage' ? $localize`object storage` : $localize`file system`
    const scopeLabel = scope === 'disk-relief' ? $localize`local media only` : $localize`all resources`
    this.notifier.info($localize`Creating jobs to move videos to ${target} (${scopeLabel})...`)

    // Fire and forget - don't wait for the API response
    this.jobsService.createMoveStorageJobs(storage, scope).subscribe({
      next: ({ jobsCreated }) => {
        this.creatingMoveJobs = false
        this.notifier.success($localize`Created ${jobsCreated} job(s) to move videos to ${target} (${scopeLabel}).`)
        this.table().loadData()
        this.loadVideoMaintenanceCounts()
      },
      error: () => {
        this.creatingMoveJobs = false
        this.notifier.error($localize`Failed to create move storage jobs.`)
      }
    })
  }

  createRetryTranscodingJobs () {
    if (this.creatingRetryTranscodingJobs) return

    this.creatingRetryTranscodingJobs = true
    this.notifier.info($localize`Creating retry transcoding jobs...`)

    // Fire and forget - don't wait for the API response
    this.jobsService.createRetryTranscodingJobs().subscribe({
      next: ({ jobsCreated }) => {
        this.creatingRetryTranscodingJobs = false
        this.notifier.success($localize`Created ${jobsCreated} retry transcoding job(s).`)
        this.table().loadData()
        this.loadVideoMaintenanceCounts()
      },

      error: () => {
        this.creatingRetryTranscodingJobs = false
        this.notifier.error($localize`Failed to create retry transcoding jobs.`)
      }
    })
  }

  createTranscriptionJobs () {
    if (this.creatingTranscriptionJobs) return

    this.creatingTranscriptionJobs = true
    this.notifier.info($localize`Creating transcription jobs...`)

    // Fire and forget - don't wait for the API response
    this.jobsService.createTranscriptionJobs().subscribe({
      next: ({ jobsCreated }) => {
        this.creatingTranscriptionJobs = false
        this.notifier.success($localize`Created ${jobsCreated} transcription job(s).`)
        this.table().loadData()
      },

      error: () => {
        this.creatingTranscriptionJobs = false
        this.notifier.error($localize`Failed to create transcription jobs.`)
      }
    })
  }

  createStoryboardJobs () {
    if (this.creatingStoryboardJobs) return

    this.creatingStoryboardJobs = true
    this.notifier.info($localize`Creating storyboard jobs...`)

    // Fire and forget - don't wait for the API response
    this.jobsService.createStoryboardJobs().subscribe({
      next: ({ jobsCreated }) => {
        this.creatingStoryboardJobs = false
        this.notifier.success($localize`Created ${jobsCreated} storyboard job(s).`)
        this.table().loadData()
      },

      error: () => {
        this.creatingStoryboardJobs = false
        this.notifier.error($localize`Failed to create storyboard jobs.`)
      }
    })
  }

  cancelSelectedJobs () {
    if (this.selectedJobIds.size === 0) return

    const jobTypes = [ this.jobType === 'all' ? 'all' : this.jobType ]
    const jobIds = Array.from(this.selectedJobIds)

    this.jobsService.cancelJobs(jobTypes, jobIds).subscribe({
      next: ({ cancelledCount }) => {
        this.notifier.success($localize`Cancelled ${cancelledCount} job(s).`)
        this.selectedJobIds.clear()
        this.table().loadData()
      },

      error: () => {
        // Handle error
      }
    })
  }

  toggleJobSelection (jobId: number) {
    if (this.selectedJobIds.has(jobId)) {
      this.selectedJobIds.delete(jobId)
    } else {
      this.selectedJobIds.add(jobId)
    }
  }

  isJobSelected (jobId: number) {
    return this.selectedJobIds.has(jobId)
  }

  get selectedJobsCount () {
    return this.selectedJobIds.size
  }

  canCancelSelectedJobs () {
    // Only allow cancellation when viewing waiting or delayed states
    return this.jobState === 'waiting' || this.jobState === 'delayed' || this.jobState === 'all'
  }

  canCancelAllJobs () {
    // Only allow cancellation when viewing waiting or delayed states
    return this.jobState === 'waiting' || this.jobState === 'delayed' || this.jobState === 'all'
  }

  cancelAllJobs () {
    if (this.cancellingAllJobs) return
    if (!this.canCancelAllJobs()) return

    const jobTypes = [ this.jobType === 'all' ? 'all' : this.jobType ]

    this.cancellingAllJobs = true
    this.notifier.info($localize`Cancelling jobs...`)

    this.jobsService.cancelJobs(jobTypes).subscribe({
      next: ({ cancelledCount }) => {
        this.cancellingAllJobs = false
        this.notifier.success($localize`Cancelled ${cancelledCount} job(s).`)
        this.table().loadData()
      },
      error: () => {
        this.cancellingAllJobs = false
        this.notifier.error($localize`Failed to cancel jobs.`)
      }
    })
  }

  recheckVideosStatus () {
    if (this.recheckingVideosStatus) return

    this.recheckingVideosStatus = true
    this.notifier.info($localize`Starting video system resetter...`)

    this.jobsService.recheckVideosStatus().subscribe({
      next: status => {
        if (status.state === 'running') {
          this.notifier.success($localize`Video system resetter started in background.`)
          this.startResetStatusPolling()
          return
        }

        this.handleResetStatus(status)
      },
      error: () => {
        this.recheckingVideosStatus = false
        this.notifier.error($localize`Failed to run video system resetter.`)
      }
    })
  }

  reconcileVideoPipeline () {
    if (this.reconcilingVideoPipeline) return

    this.reconcilingVideoPipeline = true
    this.notifier.info($localize`Starting safe video pipeline reconciliation...`)

    this.jobsService.reconcileVideoPipeline().subscribe({
      next: status => {
        if (status.state === 'running') {
          this.notifier.success($localize`Video pipeline reconciliation started in background.`)
          this.startVideoPipelineReconciliationPolling()
          return
        }

        this.handleVideoPipelineReconciliationStatus(status)
      },
      error: () => {
        this.reconcilingVideoPipeline = false
        this.notifier.error($localize`Failed to start video pipeline reconciliation.`)
      }
    })
  }

  clearGlobalQueueBacklog () {
    if (this.clearingGlobalQueueBacklog) return

    this.clearingGlobalQueueBacklog = true
    this.notifier.info($localize`Clearing global BullMQ waiting/delayed backlog...`)

    this.jobsService.clearGlobalQueueBacklog().subscribe({
      next: status => {
        if (status.state === 'running') {
          this.notifier.success($localize`Global queue scrub started in background.`)
          this.startGlobalQueueCleanupPolling()
          return
        }

        this.handleGlobalQueueCleanupStatus(status)
      },
      error: () => {
        this.clearingGlobalQueueBacklog = false
        this.notifier.error($localize`Failed to clear global BullMQ waiting/delayed backlog.`)
      }
    })
  }

  cleanupRetainedLocalFiles () {
    if (this.cleaningRetainedLocalFiles) return

    this.cleaningRetainedLocalFiles = true
    this.notifier.info($localize`Starting retained local file cleanup in background...`)

    this.jobsService.cleanupRetainedLocalFiles().subscribe({
      next: status => {
        if (status.state === 'running') {
          this.notifier.success($localize`Retained local file cleanup started in background.`)
          this.startRetainedLocalFilesCleanupPolling()
          return
        }

        this.handleRetainedLocalFilesCleanupStatus(status)
      },
      error: () => {
        this.cleaningRetainedLocalFiles = false
        this.notifier.error($localize`Failed to start retained local file cleanup.`)
      }
    })
  }

  canRetry (job: Job) {
    return (job.state || '').toLowerCase() === 'failed'
  }

  isRetrying (jobId: number) {
    return this.retryingJobIds.has(jobId)
  }

  onRetryClick (event: MouseEvent, job: Job) {
    event.preventDefault()
    event.stopPropagation()

    this.retryJob(job)
  }

  retryJob (job: Job) {
    const jobId = Number(job.id)
    if (!this.canRetry(job))
      return
    if (this.retryingJobIds.has(jobId))
      return

    this.retryingJobIds.add(jobId)

    this.jobsService.retryJob(job.type, jobId)
      .subscribe({
        next: () => {
          // Remove the failed job after successful retry
          this.jobsService.removeJob(job.type, jobId).subscribe({
            next: () => {
              this.notifier.success($localize`Retry job created and failed job removed.`)
              this.table().loadData()
            },
            error: () => {
              // Even if remove fails, just reload data
              this.notifier.success($localize`Retry job created.`)
              this.table().loadData()
            }
          })
          this.retryingJobIds.delete(jobId)
        },
        error: err => {
          this.retryingJobIds.delete(jobId)
          this.notifier.error($localize`Failed to create retry job.`)
        }
      })
  }

  refreshData () {
    this.table().loadData()
    this.loadVideoMaintenanceCounts()
  }

  private resumeResetStatusPollingIfNeeded () {
    this.jobsService.getRecheckVideosStatus().subscribe({
      next: status => {
        if (status.state === 'running') {
          this.recheckingVideosStatus = true
          this.startResetStatusPolling()
        }
      },
      error: () => {
        // noop
      }
    })
  }

  private resumeGlobalQueueCleanupPollingIfNeeded () {
    this.jobsService.getGlobalQueueBacklogCleanupStatus().subscribe({
      next: status => {
        if (status.state === 'running') {
          this.clearingGlobalQueueBacklog = true
          this.startGlobalQueueCleanupPolling()
        }
      },
      error: () => {
        // noop
      }
    })
  }

  private resumeRetainedLocalFilesCleanupPollingIfNeeded () {
    this.jobsService.getCleanupRetainedLocalFilesStatus().subscribe({
      next: status => {
        if (status.state === 'running') {
          this.cleaningRetainedLocalFiles = true
          this.startRetainedLocalFilesCleanupPolling()
        }
      },
      error: () => {
        // noop
      }
    })
  }

  private resumeVideoPipelineReconciliationPollingIfNeeded () {
    this.jobsService.getVideoPipelineReconciliationStatus().subscribe({
      next: status => {
        if (status.state === 'running') {
          this.reconcilingVideoPipeline = true
          this.startVideoPipelineReconciliationPolling()
        }
      },
      error: () => {
        // noop
      }
    })
  }

  private startResetStatusPolling () {
    this.resetStatusPollingSub?.unsubscribe()

    this.resetStatusPollingSub = interval(2000)
      .pipe(
        switchMap(() => this.jobsService.getRecheckVideosStatus())
      )
      .subscribe({
        next: status => this.handleResetStatus(status),
        error: () => {
          this.resetStatusPollingSub?.unsubscribe()
          this.resetStatusPollingSub = undefined
          this.recheckingVideosStatus = false
          this.notifier.error($localize`Failed to poll video system resetter status.`)
        }
      })
  }

  private startGlobalQueueCleanupPolling () {
    this.globalQueueCleanupPollingSub?.unsubscribe()

    this.globalQueueCleanupPollingSub = interval(2000)
      .pipe(
        switchMap(() => this.jobsService.getGlobalQueueBacklogCleanupStatus())
      )
      .subscribe({
        next: status => this.handleGlobalQueueCleanupStatus(status),
        error: () => {
          this.globalQueueCleanupPollingSub?.unsubscribe()
          this.globalQueueCleanupPollingSub = undefined
          this.clearingGlobalQueueBacklog = false
          this.notifier.error($localize`Failed to poll global queue scrub status.`)
        }
      })
  }

  private startRetainedLocalFilesCleanupPolling () {
    this.retainedLocalFilesCleanupPollingSub?.unsubscribe()

    this.retainedLocalFilesCleanupPollingSub = interval(2000)
      .pipe(
        switchMap(() => this.jobsService.getCleanupRetainedLocalFilesStatus())
      )
      .subscribe({
        next: status => this.handleRetainedLocalFilesCleanupStatus(status),
        error: () => {
          this.retainedLocalFilesCleanupPollingSub?.unsubscribe()
          this.retainedLocalFilesCleanupPollingSub = undefined
          this.cleaningRetainedLocalFiles = false
          this.notifier.error($localize`Failed to poll retained local file cleanup status.`)
        }
      })
  }

  private startVideoPipelineReconciliationPolling () {
    this.videoPipelineReconciliationPollingSub?.unsubscribe()

    this.videoPipelineReconciliationPollingSub = interval(2000)
      .pipe(
        switchMap(() => this.jobsService.getVideoPipelineReconciliationStatus())
      )
      .subscribe({
        next: status => this.handleVideoPipelineReconciliationStatus(status),
        error: () => {
          this.videoPipelineReconciliationPollingSub?.unsubscribe()
          this.videoPipelineReconciliationPollingSub = undefined
          this.reconcilingVideoPipeline = false
          this.notifier.error($localize`Failed to poll video pipeline reconciliation status.`)
        }
      })
  }

  private handleResetStatus (status: VideoSystemResetStatus) {
    if (status.state === 'running' || status.state === 'idle') return

    this.resetStatusPollingSub?.unsubscribe()
    this.resetStatusPollingSub = undefined
    this.recheckingVideosStatus = false

    if (status.state === 'failed') {
      this.notifier.error($localize`Video system resetter failed: ${status.error || 'unknown error'}.`)
      return
    }

    const result = status.result
    if (!result) {
      this.notifier.success($localize`Video system resetter finished.`)
      this.table().loadData()
      this.loadVideoMaintenanceCounts()
      return
    }

    this.notifier.success(
      $localize`System reset complete: checked ${result.videosChecked} video(s), updated ${result.videosUpdated}, deleted ${result.videosDeleted}, ` +
      $localize`removed ${result.jobsRemoved} indexed job(s), failed to remove ${result.jobsRemoveFailed}, drained ${result.queueJobsDrained} queued job(s), cleaned ${result.queueJobsCleaned} queued state record(s), ` +
      $localize`reset ${result.countersReset} counter(s), deleted ${result.orphanDbRecordsDeleted} orphan DB record(s), deleted ${result.localFilesDeleted} orphan local file(s), ` +
      $localize`paused ${result.queuesPaused} queue(s), reset hold ${result.resetHoldEnabled ? 'enabled' : 'disabled'}.`
    )
    this.table().loadData()
    this.loadVideoMaintenanceCounts()
  }

  private handleGlobalQueueCleanupStatus (status: GlobalQueueCleanupStatus) {
    if (status.state === 'running' || status.state === 'idle') return

    this.globalQueueCleanupPollingSub?.unsubscribe()
    this.globalQueueCleanupPollingSub = undefined
    this.clearingGlobalQueueBacklog = false

    if (status.state === 'failed') {
      this.notifier.error($localize`Global queue scrub failed: ${status.error || 'unknown error'}.`)
      return
    }

    const result = status.result
    if (!result) {
      this.notifier.success($localize`Global queue scrub finished.`)
      this.table().loadData()
      return
    }

    this.notifier.success(
      $localize`Global queue scrub complete: paused ${result.queuesPaused} queue(s), drained ${result.queueJobsDrained} waiting/delayed job(s), cleaned ${result.queueJobsCleaned} waiting/delayed state record(s).`
    )
    this.table().loadData()
  }

  private handleRetainedLocalFilesCleanupStatus (status: RetainedLocalFilesCleanupStatus) {
    if (status.state === 'running' || status.state === 'idle') return

    this.retainedLocalFilesCleanupPollingSub?.unsubscribe()
    this.retainedLocalFilesCleanupPollingSub = undefined
    this.cleaningRetainedLocalFiles = false

    if (status.state === 'failed') {
      this.notifier.error($localize`Retained local file cleanup failed: ${status.error || 'unknown error'}.`)
      return
    }

    const result = status.result
    if (!result) {
      this.notifier.success($localize`Retained local file cleanup finished.`)
      this.loadVideoMaintenanceCounts()
      return
    }

    this.notifier.success(
      $localize`Retained local file cleanup complete: scheduled ${result.scheduled} retained local file(s), skipped ${result.skippedMissing} already-missing file(s).`
    )
    this.loadVideoMaintenanceCounts()
  }

  private handleVideoPipelineReconciliationStatus (status: VideoPipelineReconciliationStatus) {
    if (status.state === 'running' || status.state === 'idle') return

    this.videoPipelineReconciliationPollingSub?.unsubscribe()
    this.videoPipelineReconciliationPollingSub = undefined
    this.reconcilingVideoPipeline = false

    if (status.state === 'failed') {
      this.notifier.error($localize`Video pipeline reconciliation failed: ${status.error || 'unknown error'}.`)
      return
    }

    const result = status.result
    if (!result) {
      this.notifier.success($localize`Video pipeline reconciliation finished.`)
      this.refreshData()
      return
    }

    this.notifier.success(
      $localize`Video pipeline reconciliation complete: checked ${result.videosChecked} video(s), preserved ${result.videosWithActiveWork} with live work, ` +
      $localize`cleared ${result.countersCleared} stale counter(s), recreated ${result.jobsRecreated} job flow(s), published ${result.videosPublished} video(s), ` +
      $localize`marked ${result.videosFailed} video(s) and ${result.importsFailed} import(s) failed, and removed ${result.failedJobsRemoved} failed job(s).`
    )
    this.refreshData()
  }

  private loadVideoMaintenanceCounts () {
    this.jobsService.getVideoMaintenanceCounts()
      .subscribe({
        next: counts => {
          this.videoMaintenanceCounts = counts
        },
        error: () => {
          // noop
        }
      })
  }
}
