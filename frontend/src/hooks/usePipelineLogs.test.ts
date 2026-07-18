import { isCurrentLogRequest } from './usePipelineLogs'

test('rejects a completed log request after the active run changes', () => {
  expect(isCurrentLogRequest('old-run', 'new-run')).toBe(false)
  expect(isCurrentLogRequest('new-run', 'new-run')).toBe(true)
})
