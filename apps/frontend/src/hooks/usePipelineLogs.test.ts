import { isCurrentLogRequest, nextLogRefreshFailure } from './usePipelineLogs'

test('rejects a completed log request after the active run changes', () => {
  expect(isCurrentLogRequest('old-run', 'new-run')).toBe(false)
  expect(isCurrentLogRequest('new-run', 'new-run')).toBe(true)
})

test('waits for two transient failures before showing a refresh warning', () => {
  const first = nextLogRefreshFailure({ code: 'ECONNABORTED', message: 'timeout of 10000ms exceeded' })
  const second = nextLogRefreshFailure({ status: 503, message: 'temporarily unavailable' }, first.failureCount)

  expect(first).toMatchObject({ failureCount: 1, error: null, warning: null })
  expect(second).toMatchObject({
    failureCount: 2,
    error: null,
    warning: 'Live refresh delayed — retrying.',
  })
})

test('keeps non-transient log errors visible immediately', () => {
  expect(nextLogRefreshFailure({ status: 500, message: 'database unavailable' })).toMatchObject({
    failureCount: 1,
    error: 'database unavailable',
    warning: null,
  })
})
