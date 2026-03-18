import { SortMeta } from 'primeng/api'
import { Observable } from 'rxjs'
import { catchError, map } from 'rxjs/operators'
import { HttpClient, HttpParams } from '@angular/common/http'
import { Injectable, inject } from '@angular/core'
import { RestExtractor, RestPagination, RestService } from '@app/core'
import { Job, ResultList } from '@peertube/peertube-models'
import { environment } from '../../../../environments/environment'
import { JobStateClient } from '../../../../types/job-state-client.type'
import { JobTypeClient } from '../../../../types/job-type-client.type'

export type VideoMaintenanceCounts = {
  localStorageVideos: number
  objectStorageVideos: number
  failedTranscodingVideos: number
  notYetTranscodedVideos: number
}

@Injectable()
export class JobService {
  private authHttp = inject(HttpClient)
  private restService = inject(RestService)
  private restExtractor = inject(RestExtractor)

  private static BASE_JOB_URL = environment.apiUrl + '/api/v1/jobs'

  createMoveStorageJobs (storage: 'object-storage' | 'file-system') {
    return this.authHttp.post<{ jobsCreated: number }>(
      JobService.BASE_JOB_URL + '/create-move-storage-jobs',
      { storage }
    ).pipe(
      catchError(err => this.restExtractor.handleError(err))
    )
  }

  createRetryTranscodingJobs () {
    return this.authHttp.post<{ jobsCreated: number }>(
      JobService.BASE_JOB_URL + '/create-retry-transcoding-jobs',
      {}
    ).pipe(
      catchError(err => this.restExtractor.handleError(err))
    )
  }

  createTranscriptionJobs () {
    return this.authHttp.post<{ jobsCreated: number }>(
      JobService.BASE_JOB_URL + '/create-transcription-jobs',
      {}
    ).pipe(
      catchError(err => this.restExtractor.handleError(err))
    )
  }

  createStoryboardJobs () {
    return this.authHttp.post<{ jobsCreated: number }>(
      JobService.BASE_JOB_URL + '/create-storyboard-jobs',
      {}
    ).pipe(
      catchError(err => this.restExtractor.handleError(err))
    )
  }

  cancelJobs (jobTypes: string[], jobIds?: number[]) {
    return this.authHttp.post<{ cancelledCount: number }>(
      JobService.BASE_JOB_URL + '/cancel-jobs',
      { jobTypes, jobIds }
    ).pipe(
      catchError(err => this.restExtractor.handleError(err))
    )
  }

  recheckVideosStatus (jobType?: string) {
    return this.authHttp.post<{ videosChecked: number, videosUpdated: number }>(
      JobService.BASE_JOB_URL + '/recheck-videos-status',
      { jobType }
    ).pipe(
      catchError(err => this.restExtractor.handleError(err))
    )
  }

  retryJob (jobType: string, jobId: number) {
    return this.authHttp.post<{ jobId: number }>(
      JobService.BASE_JOB_URL + '/retry-job',
      { jobType, jobId: String(jobId) }
    ).pipe(
      catchError(err => this.restExtractor.handleError(err))
    )
  }

  removeJob (jobType: string, jobId: number) {
    return this.authHttp.delete(
      `${JobService.BASE_JOB_URL}/${jobType}/${jobId}`
    ).pipe(
      catchError(err => this.restExtractor.handleError(err))
    )
  }

  getVideoMaintenanceCounts () {
    return this.authHttp.get<VideoMaintenanceCounts>(JobService.BASE_JOB_URL + '/video-maintenance-counts')
      .pipe(
        catchError(err => this.restExtractor.handleError(err))
      )
  }

  listJobs (options: {
    jobState?: JobStateClient
    jobType: JobTypeClient
    search?: string
    pagination: RestPagination
    sort: SortMeta
  }): Observable<ResultList<Job>> {
    const { jobState, jobType, search, pagination, sort } = options

    let params = new HttpParams()
    params = this.restService.addRestGetParams(params, pagination, sort)

    if (jobType !== 'all') params = params.append('jobType', jobType)
    if (search) params = params.append('search', search)

    return this.authHttp.get<ResultList<Job>>(JobService.BASE_JOB_URL + `/${jobState || ''}`, { params })
      .pipe(
        map(res => this.restExtractor.convertResultListDateToHuman(res, [ 'createdAt', 'processedOn', 'finishedOn' ], 'precise')),
        map(res => this.restExtractor.applyToResultListData(res, this.prettyPrintData.bind(this))),
        map(res => this.restExtractor.applyToResultListData(res, this.buildUniqId.bind(this))),
        catchError(err => this.restExtractor.handleError(err))
      )
  }

  private prettyPrintData (obj: Job) {
    const data = JSON.stringify(obj.data, null, 2)

    return Object.assign(obj, { data })
  }

  private buildUniqId (obj: Job) {
    return Object.assign(obj, { uniqId: `${obj.id}-${obj.type}` })
  }
}
