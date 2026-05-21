import { Component, effect, inject, input, OnDestroy, signal } from '@angular/core'
import { AuthUser } from '@app/core'
import { AlertComponent } from '@app/shared/shared-main/common/alert.component'
import { PTDatePipe } from '@app/shared/shared-main/common/date.pipe'
import { VideoDetails } from '@app/shared/shared-main/video/video-details.model'
import { VideoProcessingProgress, VideoService } from '@app/shared/shared-main/video/video.service'
import { VideoStateMessageService } from '@app/shared/shared-video/video-state-message.service'
import { UserRight, VideoPrivacy, VideoState } from '@peertube/peertube-models'
import { interval, Subscription } from 'rxjs'
import { switchMap } from 'rxjs/operators'
import { ProgressBarComponent } from '@app/shared/shared-main/common/progress-bar.component'

@Component({
  selector: 'my-video-alert',
  templateUrl: './video-alert.component.html',
  styles: `my-alert { text-align: center }`,
  imports: [ PTDatePipe, AlertComponent, ProgressBarComponent ]
})
export class VideoAlertComponent implements OnDestroy {
  readonly user = input<AuthUser>(undefined)
  readonly video = input<VideoDetails>(undefined)
  readonly videoPassword = input<string>(undefined)
  readonly noPlaylistVideoFound = input<boolean>(undefined)

  readonly processingProgress = signal<number | null>(null)

  private readonly videoStateMessage = inject(VideoStateMessageService)
  private readonly videoService = inject(VideoService)
  private processingProgressSubscription: Subscription

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

  ngOnDestroy () {
    this.stopProcessingProgressPolling()
  }

  private startProcessingProgressPolling () {
    const video = this.video()
    if (!video) return

    this.processingProgressSubscription = interval(2000).pipe(
      switchMap(() => this.videoService.getProcessingProgress({
        videoId: video.uuid,
        videoPassword: this.videoPassword()
      }))
    ).subscribe({
      next: (result: VideoProcessingProgress | null) => {
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

  shouldShowProcessingProgress () {
    const progress = this.processingProgress()
    return progress != null && progress < 100
  }

  canSeeMoreStateInfo () {
    return !!(this.user()?.hasRight(UserRight.UPDATE_ANY_VIDEO))
  }

  getAlertWarning () {
    const video = this.video()
    if (!video) return undefined

    return this.videoStateMessage.buildWarn({ videoId: video.id, state: video.state.id })
  }

  getAlertError () {
    const video = this.video()
    if (!video) return undefined

    return this.videoStateMessage.buildErr({
      videoId: video.id,
      blacklisted: video.blacklisted,
      blacklistedReason: video.blacklistedReason
    })
  }

  hasVideoScheduledPublication () {
    return this.video()?.scheduledUpdate !== undefined
  }

  isWaitingForLive () {
    return this.video()?.state.id === VideoState.WAITING_FOR_LIVE
  }

  isLiveEnded () {
    return this.video()?.state.id === VideoState.LIVE_ENDED
  }

  isVideoPasswordProtected () {
    return this.video()?.privacy.id === VideoPrivacy.PASSWORD_PROTECTED
  }

  scheduledLiveDate () {
    const liveSchedules = this.video()?.liveSchedules
    if (!liveSchedules || liveSchedules.length === 0) return undefined

    return liveSchedules[0].startAt
  }
}
