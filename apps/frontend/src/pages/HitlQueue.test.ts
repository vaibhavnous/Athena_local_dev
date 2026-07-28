jest.mock('react-router-dom', () => ({
  useNavigate: () => jest.fn(),
  useSearchParams: () => [new URLSearchParams(), jest.fn()],
}), { virtual: true })

import { hasGate2ReviewItems, hasRenderableReviewData } from './HitlQueue'

test('treats loaded SFTP feeds as ready while a background review refresh continues', () => {
  expect(hasGate2ReviewItems({
    candidate_feeds: [
      { vendor: 'Insurance', entity: 'claims' },
      { vendor: 'Insurance', entity: 'policies' },
    ],
  }, true)).toBe(true)
})

test('does not treat the Gate 2 marker alone as loaded feed content', () => {
  expect(hasGate2ReviewItems({ next_gate: 2, candidate_feeds: [] }, true)).toBe(false)
  expect(hasRenderableReviewData({ next_gate: 2, candidate_feeds: [] }, 2, true)).toBe(false)
})
