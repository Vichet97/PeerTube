/**
 * Auth cookie used for server-side HTML generation (e.g. watch page reload).
 * The cookie is sent with every same-origin request, allowing the server to
 * authenticate the user when generating HTML for private/internal videos.
 */
const AUTH_COOKIE_NAME = 'peertube_auth'

export function setAuthCookie (accessToken: string) {
  if (typeof document === 'undefined') return

  const maxAge = 60 * 60 * 24 // 1 day
  const secure = window.location.protocol === 'https:'
  let cookie = `${AUTH_COOKIE_NAME}=${encodeURIComponent(accessToken)}; path=/; max-age=${maxAge}; SameSite=Lax`
  if (secure) cookie += '; Secure'

  document.cookie = cookie
}

export function clearAuthCookie () {
  if (typeof document === 'undefined') return

  document.cookie = `${AUTH_COOKIE_NAME}=; path=/; max-age=0`
}
