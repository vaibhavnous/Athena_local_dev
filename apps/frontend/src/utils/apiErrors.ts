export function isTransientReadError(error: any) {
  return (
    error?.code === 'ECONNABORTED' ||
    Number(error?.status) === 503 ||
    /timeout/i.test(error?.message || '')
  )
}
