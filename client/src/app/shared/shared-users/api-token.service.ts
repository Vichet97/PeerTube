import { HttpClient } from '@angular/common/http'
import { Injectable, inject } from '@angular/core'
import { RestExtractor } from '@app/core'
import { UserApiToken, UserApiTokenCreate } from '@peertube/peertube-models'
import { Observable } from 'rxjs'
import { catchError } from 'rxjs/operators'
import { environment } from '../../../environments/environment'

export type UserApiTokenWithSecret = UserApiToken & { token?: string }

@Injectable()
export class ApiTokenService {
  private authHttp = inject(HttpClient)
  private restExtractor = inject(RestExtractor)

  private static BASE_ME_API_TOKENS_URL = environment.apiUrl + '/api/v1/users/me/api-tokens'

  list (): Observable<{ data: UserApiToken[], total: number }> {
    return this.authHttp.get<{ data: UserApiToken[], total: number }>(ApiTokenService.BASE_ME_API_TOKENS_URL)
      .pipe(catchError(err => this.restExtractor.handleError(err)))
  }

  create (body: UserApiTokenCreate): Observable<UserApiTokenWithSecret> {
    return this.authHttp.post<UserApiTokenWithSecret>(ApiTokenService.BASE_ME_API_TOKENS_URL, body)
      .pipe(catchError(err => this.restExtractor.handleError(err)))
  }

  revoke (id: number): Observable<void> {
    return this.authHttp.delete<void>(ApiTokenService.BASE_ME_API_TOKENS_URL + '/' + id)
      .pipe(catchError(err => this.restExtractor.handleError(err)))
  }
}
