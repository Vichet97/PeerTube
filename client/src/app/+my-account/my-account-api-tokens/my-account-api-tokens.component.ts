import { CommonModule } from '@angular/common'
import { Component, OnInit, inject, signal, computed, viewChild } from '@angular/core'
import { FormsModule } from '@angular/forms'
import { AuthService, ConfirmService, Notifier } from '@app/core'
import { USER_RIGHT_LABELS } from '@peertube/peertube-core-utils'
import { UserRight, UserRightType, UserApiToken, UserApiTokenCreate } from '@peertube/peertube-models'
import { NgbModal, NgbModalRef } from '@ng-bootstrap/ng-bootstrap'
import { InputTextComponent } from '@app/shared/shared-forms/input-text.component'
import { GlobalIconComponent } from '@app/shared/shared-icons/global-icon.component'
import { ApiTokenService, UserApiTokenWithSecret } from '@app/shared/shared-users/api-token.service'

@Component({
  selector: 'my-account-api-tokens',
  templateUrl: './my-account-api-tokens.component.html',
  styleUrls: [ './my-account-api-tokens.component.scss' ],
  imports: [
    CommonModule,
    FormsModule,
    InputTextComponent,
    GlobalIconComponent
  ],
  providers: [ ApiTokenService ]
})
export class MyAccountApiTokensComponent implements OnInit {
  private apiTokenService = inject(ApiTokenService)
  private authService = inject(AuthService)
  private notifier = inject(Notifier)
  private confirmService = inject(ConfirmService)
  private modalService = inject(NgbModal)

  readonly tokenCreatedModal = viewChild('tokenCreatedModal')

  private tokenCreatedModalRef: NgbModalRef

  tokens = signal<UserApiToken[]>([])
  createdToken = signal<UserApiTokenWithSecret | null>(null)
  showCreateForm = signal(false)
  loading = signal(false)

  name = ''
  description = ''
  expiresAt: string | null = null
  noExpiration = true
  selectedScopes = signal<Set<UserRightType>>(new Set())

  availableRights = computed(() => {
    const user = this.authService.getUser()
    const labels: { value: UserRightType, label: string }[] = []
    const rights = Object.values(UserRight).filter((v): v is UserRightType => typeof v === 'number')
    for (const right of rights) {
      if (user?.hasRight(right)) {
        labels.push({ value: right, label: USER_RIGHT_LABELS[right] })
      }
    }
    return labels
  })

  ngOnInit () {
    this.listTokens()
  }

  listTokens () {
    this.loading.set(true)
    this.apiTokenService.list().subscribe({
      next: ({ data }) => {
        this.tokens.set(data)
        this.loading.set(false)
      },
      error: () => this.loading.set(false)
    })
  }

  toggleCreateForm () {
    this.showCreateForm.update(v => !v)
    if (!this.showCreateForm()) {
      this.resetForm()
    }
  }

  toggleScope (right: UserRightType) {
    this.selectedScopes.update(set => {
      const next = new Set(set)
      if (next.has(right)) {
        next.delete(right)
      } else {
        next.add(right)
      }
      return next
    })
  }

  toggleFullAccess () {
    if (this.selectedScopes().has(UserRight.ALL)) {
      this.selectedScopes.set(new Set())
    } else {
      this.selectedScopes.set(new Set([ UserRight.ALL ]))
    }
  }

  hasScope (right: UserRightType) {
    return this.selectedScopes().has(right)
  }

  async createToken () {
    if (!this.name.trim()) {
      this.notifier.error($localize`Name is required`)
      return
    }

    const scopes = Array.from(this.selectedScopes())
    if (scopes.length === 0) {
      this.notifier.error($localize`Select at least one scope`)
      return
    }

    const body: UserApiTokenCreate = {
      name: this.name.trim(),
      description: this.description.trim() || undefined,
      expiresAt: this.noExpiration ? null : (this.expiresAt || undefined),
      scopes
    }

    this.loading.set(true)
    this.apiTokenService.create(body).subscribe({
      next: (result) => {
        this.createdToken.set(result)
        this.resetForm()
        this.showCreateForm.set(false)
        this.listTokens()
        this.loading.set(false)
        this.tokenCreatedModalRef = this.modalService.open(this.tokenCreatedModal(), {
          centered: true,
          backdrop: 'static',
          size: 'md'
        })
        this.tokenCreatedModalRef.closed.subscribe(() => this.createdToken.set(null))
      },
      error: () => this.loading.set(false)
    })
  }

  closeCreatedTokenModal () {
    if (this.tokenCreatedModalRef) {
      this.tokenCreatedModalRef.close()
      this.tokenCreatedModalRef = null
    }
    this.createdToken.set(null)
  }

  async revokeToken (token: UserApiToken) {
    const res = await this.confirmService.confirm(
      $localize`Are you sure you want to revoke this API token? It will stop working immediately.`,
      $localize`Revoke API token`
    )
    if (!res) return

    this.apiTokenService.revoke(token.id).subscribe({
      next: () => {
        this.notifier.success($localize`API token revoked`)
        this.listTokens()
      },
      error: err => this.notifier.handleError(err)
    })
  }

  get today () {
    return new Date().toISOString().slice(0, 16)
  }

  get hasFullAccessOption () {
    return this.availableRights().some(r => r.value === UserRight.ALL)
  }

  neverExpirationLabel = $localize`Never`

  getScopeLabels (scopes: UserRightType[]) {
    if (scopes.includes(UserRight.ALL)) return $localize`Full access`
    return scopes.map(s => USER_RIGHT_LABELS[s] || s).join(', ')
  }

  private resetForm () {
    this.name = ''
    this.description = ''
    this.expiresAt = null
    this.noExpiration = true
    this.selectedScopes.set(new Set())
  }
}
