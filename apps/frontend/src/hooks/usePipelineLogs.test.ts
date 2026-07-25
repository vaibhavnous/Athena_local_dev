import { isCurrentLogRequest, transientLogNotice } from './usePipelineLogs'

test('rejects a completed log request after the active run changes', () => {
  expect(isCurrentLogRequest('old-run', 'new-run')).toBe(false)
  expect(isCurrentLogRequest('new-run', 'new-run')).toBe(true)
})

test('classifies transient log fetch failures as notices', () => {
  expect(transientLogNotice({ status: 503 })).toContain('temporarily unavailable')
  expect(transientLogNotice({ code: 'ECONNABORTED' })).toContain('temporarily unavailable')
  expect(transientLogNotice({ status: 403 })).toBeNull()
})
