import { CommonModule } from '@angular/common'
import { Component, OnInit, inject, viewChild } from '@angular/core'
import { FormsModule } from '@angular/forms'
import { Notifier, RestPagination } from '@app/core'
import { SelectOptionsComponent } from '@app/shared/shared-forms/select/select-options.component'

import { Job, JobState, JobType } from '@peertube/peertube-models'
import { peertubeLocalStorage } from '@root-helpers/peertube-web-storage'
import { SortMeta } from 'primeng/api'
import { tap } from 'rxjs/operators'
import { SelectOptionsItem } from 'src/types'
import { JobStateClient } from '../../../../types/job-state-client.type'
import { JobTypeClient } from '../../../../types/job-type-client.type'
import { ButtonComponent } from '../../../shared/shared-main/buttons/button.component'
import { NumberFormatterPipe } from '../../../shared/shared-main/common/number-formatter.pipe'
import { TableColumnInfo, TableComponent, TableQueryParams } from '../../../shared/shared-tables/table.component'
import { AdvancedInputFilterComponent } from '../../../shared/shared-forms/advanced-input-filter.component'
import { JobService, VideoMaintenanceCounts } from './job.service'

type ColumnName = 'id' | 'type' | 'priority' | 'state' | 'progress' | 'createdAt' | 'processed'

type QueryParams = TableQueryParams & {
  jobType: string
  jobState: string
  search?: string
}

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
    'notify',
    'transcoding-job-builder',
    'video-channel-import',
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
    return this.jobType === 'all' || this.jobType === 'video-transcoding' || this.jobType === 'video-import'
  }

  hasProgress (job: Job) {
    return job.type === 'video-transcoding' || job.type === 'video-import'
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

  createMoveStorageJobs (storage: 'object-storage' | 'file-system') {
    if (this.creatingMoveJobs) return

    this.creatingMoveJobs = true
    this.jobsService.createMoveStorageJobs(storage).subscribe({
      next: ({ jobsCreated }) => {
        this.creatingMoveJobs = false
        const target = storage === 'object-storage' ? $localize`object storage` : $localize`file system`
        this.notifier.success(
          $localize`Created ${jobsCreated} job(s) to move videos to ${target}.`
        )
        this.table().loadData()
        this.loadVideoMaintenanceCounts()
      },
      error: () => {
        this.creatingMoveJobs = false
      }
    })
  }

  createRetryTranscodingJobs () {
    if (this.creatingRetryTranscodingJobs) return

    this.creatingRetryTranscodingJobs = true
    this.jobsService.createRetryTranscodingJobs().subscribe({
      next: ({ jobsCreated }) => {
        this.creatingRetryTranscodingJobs = false
        this.notifier.success($localize`Created ${jobsCreated} retry transcoding job(s).`)
        this.table().loadData()
        this.loadVideoMaintenanceCounts()
      },

      error: () => {
        this.creatingRetryTranscodingJobs = false
      }
    })
  }

  refreshData () {
    this.table().loadData()
    this.loadVideoMaintenanceCounts()
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
