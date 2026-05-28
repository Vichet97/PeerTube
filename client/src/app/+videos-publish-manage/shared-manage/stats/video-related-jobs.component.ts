import { CommonModule } from '@angular/common'
import { Component, LOCALE_ID, OnDestroy, OnInit, inject, viewChild } from '@angular/core'
import { Notifier, RestPagination } from '@app/core'
import { GlobalIconComponent } from '@app/shared/shared-icons/global-icon.component'
import { ButtonComponent } from '@app/shared/shared-main/buttons/button.component'
import { Job, JobType } from '@peertube/peertube-models'
import { SortMeta } from 'primeng/api'
import { interval, Subscription } from 'rxjs'
import { map, tap } from 'rxjs/operators'
import { VideoEdit } from '../common/video-edit.model'
import { TableColumnInfo, TableComponent } from '../../../shared/shared-tables/table.component'
import { VideoManageController } from '../video-manage-controller.service'
import { VideoStatsService } from './video-stats.service'

type ColumnName = 'id' | 'type' | 'priority' | 'state' | 'progress' | 'createdAt' | 'processed'

const PROGRESS_JOB_TYPES = new Set<JobType>([
  'video-transcoding',
  'video-import',
  'move-to-object-storage',
  'move-video-file-to-object-storage',
  'move-hls-playlist-to-object-storage',
  'move-thumbnail-to-object-storage'
])

@Component({
  templateUrl: './video-related-jobs.component.html',
  styleUrls: [
    '../common/video-manage-page-common.scss',
    './video-related-jobs.component.scss'
  ],
  imports: [
    CommonModule,
    GlobalIconComponent,
    ButtonComponent,
    TableComponent
  ]
})
export class VideoRelatedJobsComponent implements OnInit, OnDestroy {
  private localeId = inject(LOCALE_ID)
  private notifier = inject(Notifier)
  private statsService = inject(VideoStatsService)
  private manageController = inject(VideoManageController)

  readonly table = viewChild<TableComponent<Job, ColumnName>>('table')

  videoEdit: VideoEdit
  retryingJobKeys = new Set<string>()
  private jobsPollingSub?: Subscription

  columns: TableColumnInfo<ColumnName>[] = [
    { id: 'id', class: 'job-id', label: $localize`ID`, sortable: false },
    { id: 'type', class: 'job-type', label: $localize`Type`, sortable: false },
    { id: 'priority', class: 'job-priority', label: $localize`Priority`, labelSmall: $localize`(1 = highest priority)`, sortable: false },
    { id: 'state', class: 'job-state', label: $localize`State`, sortable: false },
    { id: 'progress', class: 'job-progress', label: $localize`Progress`, isDisplayed: () => this.hasGlobalProgress(), sortable: false },
    { id: 'createdAt', class: 'job-date', label: $localize`Created`, sortable: true },
    { id: 'processed', label: $localize`Processed/Finished`, sortable: false }
  ]
  dataLoader: typeof this._dataLoader

  constructor () {
    this.dataLoader = this._dataLoader.bind(this)
  }

  ngOnInit () {
    this.videoEdit = this.manageController.getStore().videoEdit
  }

  ngOnDestroy () {
    this.stopJobsPolling()
  }

  getRandomJobTypeBadge (type: string) {
    return this.table().getRandomBadge('type', type)
  }

  getJobStateClasses (state: string): string[] {
    switch (state) {
      case 'active':
        return [ 'pt-badge', 'badge-blue' ]
      case 'completed':
        return [ 'pt-badge', 'badge-green' ]
      case 'failed':
        return [ 'pt-badge', 'badge-red' ]
      case 'cancelled':
        return [ 'pt-badge', 'badge-grey' ]
      case 'waiting':
      case 'waiting-children':
      case 'prioritized':
        return [ 'pt-badge', 'badge-yellow' ]
      case 'delayed':
      case 'paused':
        return [ 'pt-badge', 'badge-brown' ]
      default:
        return []
    }
  }

  hasGlobalProgress () {
    return true
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

  getJobTypeLabel (type: string): string {
    const known: Record<string, string> = {
      'transcoding-job-builder': $localize`Transcoding job builder`,
      'video-transcoding': $localize`Video transcoding`,
      'video-transcription': $localize`Video transcription`,
      'move-to-object-storage': $localize`Move to object storage`,
      'move-video-file-to-object-storage': $localize`Move video file to object storage`,
      'move-hls-playlist-to-object-storage': $localize`Move HLS playlist to object storage`,
      'move-thumbnail-to-object-storage': $localize`Move thumbnail to object storage`,
      'move-to-file-system': $localize`Move to file system`,
      'video-import': $localize`Video import`,
      'video-file-import': $localize`Video file import`,
      'video-live-ending': $localize`Video live ending`,
      'video-studio-edition': $localize`Video studio edition`,
      'generate-video-storyboard': $localize`Generate video storyboard`,
      'manage-video-torrent': $localize`Manage video torrent`,
      'federate-video': $localize`Federate video`,
      'notify': $localize`Notification`,
      'activitypub-http-broadcast': $localize`ActivityPub HTTP broadcast`,
      'activitypub-http-broadcast-parallel': $localize`ActivityPub HTTP broadcast (parallel)`,
      'activitypub-http-unicast': $localize`ActivityPub HTTP unicast`,
      'activitypub-http-fetcher': $localize`ActivityPub HTTP fetcher`,
      'activitypub-follow': $localize`ActivityPub follow`,
      'activitypub-cleaner': $localize`ActivityPub cleaner`,
      'activitypub-refresher': $localize`ActivityPub refresher`,
      'videos-views-stats': $localize`Video views stats`,
      'video-redundancy': $localize`Video redundancy`,
      'video-channel-import': $localize`Video channel import`,
      'after-video-channel-import': $localize`After video channel import`,
      'actor-keys': $localize`Actor keys`,
      'email': $localize`Email`,
      'create-user-export': $localize`Create user export`,
      'import-user-archive': $localize`Import user archive`
    }

    const custom = known[type]
    if (custom) return custom

    return type
      .split('-')
      .map(part => part ? part[0].toUpperCase() + part.slice(1) : part)
      .join(' ')
  }

  getJobDetailsJson (job: Job): string {
    const details: Record<string, unknown> = {
      data: job.data ?? null,
      error: job.error ?? null,
      progress: job.progress ?? null,
      priority: job.priority ?? null,
      parent: job.parent ?? null
    }

    return JSON.stringify(details, null, 2)
  }

  stringifyValue (value: unknown) {
    if (value == null) return ''
    if (typeof value === 'string') return value

    return JSON.stringify(value, null, 2)
  }

  formatJobDate (value: string | Date) {
    if (!value) return '-'

    const date = new Date(value)
    if (Number.isNaN(date.getTime())) return String(value)

    return date.toLocaleString(this.localeId)
  }

  canRetry (job: Job) {
    return (job.state || '').toLowerCase() === 'failed'
  }

  isRetrying (job: Job) {
    return this.retryingJobKeys.has(this.getJobKey(job))
  }

  onRetryClick (event: MouseEvent, job: Job) {
    event.preventDefault()
    event.stopPropagation()

    this.retryFailedJob(job)
  }

  private retryFailedJob (job: Job) {
    if (!this.canRetry(job)) return

    const key = this.getJobKey(job)
    if (this.retryingJobKeys.has(key)) return

    this.retryingJobKeys.add(key)
    const videoId = this.videoEdit.getVideoAttributes().uuid

    this.statsService.retryRelatedJob({
      videoId,
      jobType: job.type as JobType,
      jobId: String(job.id)
    })
      .subscribe({
        next: () => {
          this.retryingJobKeys.delete(key)
          this.notifier.success($localize`Retry job created.`)
          this.table().loadData()
        },
        error: err => {
          this.retryingJobKeys.delete(key)
          this.notifier.handleError(err)
        }
      })
  }

  private getJobKey (job: Job) {
    return `${job.type}:${job.id}`
  }

  private _dataLoader (options: {
    pagination: RestPagination
    sort: SortMeta
    search: string
  }) {
    const { pagination, sort } = options
    const videoId = this.videoEdit.getVideoAttributes().uuid

    return this.statsService.getRelatedJobs(videoId)
      .pipe(
        tap(result => this.syncJobsPolling(result.data)),
        map(result => {
          const sortedData = result.data.map(j => ({
            ...j,
            uniqId: `${j.id}-${j.type}`
          }))

          if (sort?.field === 'createdAt') {
            sortedData.sort((a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime())
            if (sort.order !== 1) sortedData.reverse()
          }

          const start = pagination.start ?? 0
          const count = pagination.count ?? sortedData.length

          return {
            total: sortedData.length,
            data: sortedData.slice(start, start + count)
          }
        })
      )
  }

  private syncJobsPolling (jobs: Job[]) {
    if (jobs.some(job => this.isJobStillRunning(job))) {
      this.startJobsPolling()
      return
    }

    this.stopJobsPolling()
  }

  private isJobStillRunning (job: Job) {
    return job.state === 'active' ||
      job.state === 'waiting' ||
      job.state === 'delayed' ||
      job.state === 'prioritized' ||
      job.state === 'waiting-children'
  }

  private startJobsPolling () {
    if (this.jobsPollingSub) return

    this.jobsPollingSub = interval(2000)
      .subscribe(() => {
        const table = this.table()
        if (!table || table.loading) return

        table.loadData({ skipLoader: true }).catch(() => {
          // noop, table notifier already handles the error
        })
      })
  }

  private stopJobsPolling () {
    this.jobsPollingSub?.unsubscribe()
    this.jobsPollingSub = undefined
  }
}
