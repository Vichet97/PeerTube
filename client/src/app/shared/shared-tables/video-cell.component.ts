import { CommonModule } from '@angular/common'
import { booleanAttribute, Component, effect, inject, input, OnDestroy, OnInit, signal } from '@angular/core'
import { RouterLink } from '@angular/router'
import { AuthService, ScreenService } from '@app/core'
import { CollaboratorStateComponent } from '../shared-main/channel/collaborator-state.component'
import { VideoChannel } from '../shared-main/channel/video-channel.model'
import { Video } from '../shared-main/video/video.model'
import { VideoService } from '../shared-main/video/video.service'
import { VideoThumbnailComponent } from '../shared-thumbnail/video-thumbnail.component'
import { VideoState } from '@peertube/peertube-models'
import { interval, Subscription } from 'rxjs'
import { switchMap } from 'rxjs/operators'

const LIST_PROCESSING_PROGRESS_POLL_INTERVAL_MS = 10000

@Component({
  selector: 'my-video-cell',
  styleUrls: [ 'video-cell.component.scss' ],
  templateUrl: 'video-cell.component.html',
  standalone: true,
  imports: [
    CommonModule,
    RouterLink,
    VideoThumbnailComponent,
    CollaboratorStateComponent
  ]
})
export class VideoCellComponent implements OnInit, OnDestroy {
  private readonly screenService = inject(ScreenService)
  private readonly authService = inject(AuthService)
  private readonly videoService = inject(VideoService)

  readonly video = input.required<Video>()
  readonly processingProgress = signal<number | null>(null)

  private processingProgressSubscription: Subscription

  readonly size = input<'small' | 'normal'>('normal')
  readonly thumbnail = input(true, { transform: booleanAttribute })
  readonly title = input(true, { transform: booleanAttribute })
  readonly displayEditorInfo = input(false, { transform: booleanAttribute })

  ellipsis: boolean

  constructor () {
    effect(() => {
      const video = this.video()
      const shouldPoll = video?.isLocal && video?.state && (
        video.state.id === VideoState.TO_TRANSCODE ||
        video.state.id === VideoState.TO_IMPORT
      )

      this.stopProcessingProgressPolling()

      if (shouldPoll) {
        this.processingProgress.set(0)
        this.startProcessingProgressPolling()
      } else {
        this.processingProgress.set(null)
      }
    })
  }

  private startProcessingProgressPolling () {
    const video = this.video()
    if (!video) return

    this.processingProgressSubscription = interval(LIST_PROCESSING_PROGRESS_POLL_INTERVAL_MS).pipe(
      switchMap(() => this.videoService.getProcessingProgress({ videoId: video.uuid }))
    ).subscribe({
      next: result => {
        if (!result) {
          this.processingProgress.set(null)
          this.stopProcessingProgressPolling()
          return
        }

        if (result.active === false) {
          this.processingProgress.set(null)
          this.stopProcessingProgressPolling()
          return
        }

        this.processingProgress.set(result.progress)
      },
      error: () => {
        this.processingProgress.set(null)
        this.stopProcessingProgressPolling()
      }
    })
  }

  private stopProcessingProgressPolling () {
    this.processingProgressSubscription?.unsubscribe()
    this.processingProgressSubscription = null
  }

  get user () {
    return this.authService.getUser()
  }

  ngOnInit () {
    this.ellipsis = !this.screenService.isInMobileView()
  }

  ngOnDestroy () {
    this.stopProcessingProgressPolling()
  }

  getVideoUrl () {
    return Video.buildWatchUrl(this.video())
  }

  getChannelUrl () {
    return VideoChannel.buildPublicUrl(this.video().channel)
  }
}
