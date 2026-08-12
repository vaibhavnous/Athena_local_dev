jest.mock('react-router-dom', () => ({
  useNavigate: () => jest.fn(),
  useSearchParams: () => [new URLSearchParams(), jest.fn()],
}), { virtual: true })

import { hasGate2ReviewItems, hasRenderableReviewData, hasSilverMergeKeys, mergeRefreshedReviewItems, usesLegacyFeedReview } from './HitlQueue'
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

test('uses shared Table Review for ADLS and keeps Feed Review legacy-only', () => {
  expect(usesLegacyFeedReview('adls_gen2')).toBe(false)
  expect(usesLegacyFeedReview('database')).toBe(false)
  expect(usesLegacyFeedReview('sftp')).toBe(true)
})

test('requires a selected key before approving a Silver merge-key item', () => {
  expect(hasSilverMergeKeys({ mergeKeys: [] })).toBe(false)
  expect(hasSilverMergeKeys({ mergeKeyCandidates: ['ClaimID'], mergeKeys: [] })).toBe(false)
  expect(hasSilverMergeKeys({ mergeKeys: ['ClaimID', 'UpdateNum'] })).toBe(true)
})

test('waits for ADLS merge-key resolution instead of rendering the empty pre-LLM artifact', () => {
  const pending = { silver_merge_key_review_artifact: { feeds: [{ table: 'claims', merge_keys: [] }] } }
  const resolved = { silver_merge_key_review_artifact: { feeds: [{ table: 'claims', merge_keys: ['ClaimID'] }] } }

  expect(hasRenderableReviewData(pending, 'silver_merge_key_review', true)).toBe(false)
  expect(hasRenderableReviewData(resolved, 'silver_merge_key_review', true)).toBe(true)
  expect(hasRenderableReviewData(pending, 'silver_merge_key_review', false)).toBe(true)
})

test('refreshes untouched merge keys without overwriting user edits', () => {
  const refreshed = [{ key: 'claims', type: 'MERGE_KEY', mergeKeys: ['ClaimID', 'UpdateNum'] }]

  expect(mergeRefreshedReviewItems(
    [{ key: 'claims', type: 'MERGE_KEY', mergeKeys: [] }],
    refreshed,
  )[0].mergeKeys).toEqual(['ClaimID', 'UpdateNum'])
  expect(mergeRefreshedReviewItems(
    [{ key: 'claims', type: 'MERGE_KEY', mergeKeys: ['ManualID'], edited: true }],
    refreshed,
  )[0].mergeKeys).toEqual(['ManualID'])
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
