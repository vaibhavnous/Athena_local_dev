jest.mock('react-router-dom', () => ({
  useNavigate: () => jest.fn(),
  useSearchParams: () => [new URLSearchParams(), jest.fn()],
}), { virtual: true })

import { hasGate2ReviewItems, hasRenderableReviewData } from './HitlQueue'
import { semanticReviewValidationError } from '../utils/reviewReadiness'

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

test('treats persisted Gate 3 table reviews as renderable content', () => {
  expect(hasRenderableReviewData({ semantic_tables: [{ queue_id: 'run-1:3:item-1' }] }, 3)).toBe(true)
})

test('reports incomplete PII metadata before semantic approval', () => {
  expect(semanticReviewValidationError({
    table_name: 'claims',
    columns: [{
      column_name: 'S_AADHAAR_ATTACHED',
      suggested_display_name: 'Aadhaar Attached',
      business_description: 'Indicates whether Aadhaar documentation is attached to the claim.',
      semantic_type: 'FLAG',
      is_pii_candidate: true,
      pii_type: null,
    }],
  })).toBe('S_AADHAAR_ATTACHED: PII type is required when PII is selected.')
})
