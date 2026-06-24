const trimTrailingSlash = (value: string) => value.replace(/\/$/, '')

const isLocalHostname = (hostname: string) =>
  hostname === 'localhost' || hostname === '127.0.0.1' || hostname === '::1'

export const getApiBaseUrl = () => {
  const configured = process.env.REACT_APP_API_BASE_URL?.trim()
  if (configured) {
    return trimTrailingSlash(configured)
  }

  if (typeof window !== 'undefined') {
    if (isLocalHostname(window.location.hostname)) {
      return 'http://127.0.0.1:8000'
    }

    return ''
  }

  return 'http://127.0.0.1:8000'
}
