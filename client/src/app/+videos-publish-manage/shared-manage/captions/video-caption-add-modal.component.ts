import { Component, ElementRef, OnInit, inject, input, output, viewChild } from '@angular/core'
import { FormsModule, ReactiveFormsModule } from '@angular/forms'
import { Notifier, ServerService } from '@app/core'
import { VIDEO_CAPTION_FILE_VALIDATOR, VIDEO_CAPTION_LANGUAGE_VALIDATOR } from '@app/shared/form-validators/video-captions-validators'
import { FormReactive } from '@app/shared/shared-forms/form-reactive'
import { FormReactiveService } from '@app/shared/shared-forms/form-reactive.service'
import { SelectOptionsComponent } from '@app/shared/shared-forms/select/select-options.component'
import { VideoCaptionEdit } from '@app/+videos-publish-manage/shared-manage/common/video-caption-edit.model'
import { NgbModal, NgbModalRef } from '@ng-bootstrap/ng-bootstrap'
import { HTMLServerConfig, ConstantLabel } from '@peertube/peertube-models'
import { ReactiveFileComponent } from '../../../shared/shared-forms/reactive-file.component'
import { GlobalIconComponent } from '../../../shared/shared-icons/global-icon.component'
import { HelpComponent } from '@app/shared/shared-main/buttons/help.component'
import { VideoCaptionService } from '@app/shared/shared-main/video-caption/video-caption.service'

@Component({
  selector: 'my-video-caption-add-modal',
  styleUrls: [ './video-caption-add-modal.component.scss' ],
  templateUrl: './video-caption-add-modal.component.html',
  imports: [ FormsModule, ReactiveFormsModule, GlobalIconComponent, ReactiveFileComponent, SelectOptionsComponent, HelpComponent ]
})
export class VideoCaptionAddModalComponent extends FormReactive implements OnInit {
  protected formReactiveService = inject(FormReactiveService)
  private modalService = inject(NgbModal)
  private serverService = inject(ServerService)
  private videoCaptionService = inject(VideoCaptionService)
  private notifier = inject(Notifier)

  readonly existingCaptions = input<string[]>(undefined)
  readonly serverConfig = input<HTMLServerConfig>(undefined)
  readonly videoId = input<string>(undefined)

  readonly captionAdded = output<VideoCaptionEdit>()
  readonly captionImported = output<void>()

  readonly modal = viewChild<ElementRef>('modal')

  videoCaptionLanguages: ConstantLabel<string>[] = []
  importMode: 'file' | 'url' = 'file'
  targetUrl = ''
  customHeadersJson = ''
  customHeadersError = ''
  importLoading = false

  customHeadersPlaceholder = $localize`{"Authorization": "Bearer xxx", "Referer": "https://example.com"}`

  private openedModal: NgbModalRef

  get videoCaptionExtensions () {
    return this.serverConfig().videoCaption.file.extensions
  }

  get videoCaptionMaxSize () {
    return this.serverConfig().videoCaption.file.size.max
  }

  getReactiveFileButtonTooltip () {
    return `(extensions: ${this.videoCaptionExtensions.join(', ')})`
  }

  ngOnInit () {
    this.serverService.getVideoLanguages()
      .subscribe(languages => this.videoCaptionLanguages = languages)

    this.buildForm({
      language: VIDEO_CAPTION_LANGUAGE_VALIDATOR,
      captionfile: VIDEO_CAPTION_FILE_VALIDATOR
    })
  }

  setImportMode (mode: 'file' | 'url') {
    this.importMode = mode
    this.form.updateValueAndValidity()
  }

  isUrlModeValid () {
    if (!this.targetUrl?.trim()) return false
    if (!this.form.value['language']) return false
    if (this.customHeadersError) return false
    try {
      if (this.customHeadersJson.trim()) {
        const parsed = JSON.parse(this.customHeadersJson.trim())
        if (parsed && typeof parsed !== 'object') return false
      }
    } catch {
      return false
    }
    return true
  }

  show () {
    this.openedModal = this.modalService.open(this.modal(), { centered: true, keyboard: false })
  }

  hide () {
    this.openedModal.close()
    this.form.reset()
    this.importMode = 'file'
    this.targetUrl = ''
    this.customHeadersJson = ''
    this.customHeadersError = ''
  }

  isReplacingExistingCaption () {
    const languageId = this.form.value['language']

    return languageId && this.existingCaptions().includes(languageId)
  }

  addCaption () {
    if (this.importMode === 'url') {
      this.importCaptionFromUrl()
      return
    }

    const languageId = this.form.value['language']
    const languageObject = this.videoCaptionLanguages.find(l => l.id === languageId)

    this.captionAdded.emit({
      language: languageObject,
      captionfile: this.form.value['captionfile'],
      action: 'CREATE'
    })

    this.hide()
  }

  async importCaptionFromUrl () {
    if (!this.videoId() || this.importLoading) return

    let customHeaders: Record<string, string> | undefined
    if (this.customHeadersJson.trim()) {
      try {
        const parsed = JSON.parse(this.customHeadersJson.trim())
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
          customHeaders = {}
          for (const [ k, v ] of Object.entries(parsed)) {
            if (typeof k === 'string' && typeof v === 'string') {
              customHeaders[k] = v
            }
          }
        }
      } catch {
        this.customHeadersError = $localize`Invalid JSON format`
        return
      }
    }
    this.customHeadersError = ''

    this.importLoading = true
    this.videoCaptionService.addCaptionFromUrl(
      this.videoId(),
      this.form.value['language'],
      this.targetUrl.trim(),
      customHeaders
    ).subscribe({
      next: () => {
        this.importLoading = false
        this.notifier.success($localize`Caption imported successfully`)
        this.captionImported.emit()
        this.hide()
      },
      error: () => {
        this.importLoading = false
      }
    })
  }
}
