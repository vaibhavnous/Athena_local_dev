export type ReviewKey = 1 | 2 | 3 | 4 | 5 | 'metadata_ddl_review' | 'silver_merge_key_review' | 'gold_review'

const SEMANTIC_TYPES = new Set([
  'MEASURE', 'DIMENSION', 'ID', 'SURROGATE_KEY', 'DATE',
  'AUDIT_TIMESTAMP', 'PII', 'FLAG', 'HIGH_CARD_TEXT', 'UNKNOWN',
])

export function semanticReviewValidationError(review: any): string | null {
  const tableName = String(review?.qualified_table_name || review?.table_name || 'Semantic table').trim()
  const columns = Array.isArray(review?.columns)
    ? review.columns.filter((column: any) => column?.column_name !== '__TABLE_SUMMARY__')
    : []
  if (columns.length === 0) return `${tableName}: at least one semantic column is required.`

  const displayNames = new Set<string>()
  for (const column of columns) {
    const columnName = String(column?.column_name || '').trim()
    const label = columnName || 'Unnamed column'
    const displayName = String(column?.suggested_display_name || '').trim()
    const description = String(column?.business_description || '').trim()
    const semanticType = String(column?.semantic_type || 'UNKNOWN').trim().toUpperCase()
    const piiType = String(column?.pii_type || '').trim()

    if (!columnName) return `${tableName}: every semantic column requires a column name.`
    if (!displayName || displayName.length > 256) {
      return `${label}: display name is required and must be at most 256 characters.`
    }
    const displayKey = displayName.toLowerCase()
    if (displayNames.has(displayKey)) return `${tableName}: duplicate display name: ${displayName}.`
    displayNames.add(displayKey)
    if (!description || description.length > 1000) {
      return `${label}: business description is required and must be at most 1000 characters.`
    }
    if (!SEMANTIC_TYPES.has(semanticType)) return `${label}: unsupported semantic type '${semanticType}'.`
    if ((semanticType === 'PII' || Boolean(column?.is_pii_candidate)) && (!piiType || piiType === '-')) {
      return `${label}: PII type is required when PII is selected.`
    }
  }
  return null
}

export function getFileReviewFeeds(review: any) {
  if (Array.isArray(review?.candidate_feeds) && review.candidate_feeds.length > 0) {
    return review.candidate_feeds
  }
  return review?.candidate_feed ? [review.candidate_feed] : []
}

export function hasGate2ReviewItems(review: any, isFileSource: boolean) {
  return isFileSource
    ? getFileReviewFeeds(review).length > 0
    : Boolean((review?.nominated_tables || []).length)
}

export function hasRenderableReviewData(review: any, reviewKey: ReviewKey, isFileSource = false) {
  if (!review) return false

  if (reviewKey === 1) {
    if (Array.isArray(review)) return review.length > 0
    return Boolean((review?.kpis || []).length)
  }
  if (reviewKey === 2) return hasGate2ReviewItems(review, isFileSource)
  if (reviewKey === 3) {
    return Boolean(
      (review?.semantic_tables || []).length ||
      (review?.enriched_columns || []).length ||
      (review?.enriched_joins || []).length ||
      (review?.feed_semantic_summary || []).length ||
      Object.keys(review?.enriched_metadata || {}).length ||
      Object.keys(review?.semantic_counts || {}).length ||
      (review?.pii_columns || []).length ||
      (review?.join_key_columns || []).length ||
      (review?.measure_columns || []).length
    )
  }
  if (reviewKey === 4) return Boolean((review?.bronze_review_artifact?.feeds || []).length)
  if (reviewKey === 'metadata_ddl_review') return Boolean(review?.metadata_ddl_review?.script_body)
  if (reviewKey === 'silver_merge_key_review') {
    const feeds = review?.silver_merge_key_review_artifact?.feeds || []
    if (!feeds.length) return false
    if (!isFileSource) return true
    return feeds.every((feed: any) => (
      (feed?.merge_keys || feed?.primary_keys || []).length > 0 ||
      Boolean(feed?.merge_key_resolution_error)
    ))
  }
  if (reviewKey === 'gold_review') return Boolean((review?.gold_review_artifact?.items || []).length)
  if (reviewKey === 5) return Boolean((review?.silver_review_artifact?.items || []).length)
  return false
}

export function activeReviewKey(run: any): ReviewKey | null {
  const namedReview = String(run?.next_review_key || '')
  if (namedReview === 'metadata_ddl_review' || namedReview === 'silver_merge_key_review' || namedReview === 'gold_review') {
    return namedReview
  }

  const gate = Number(run?.next_gate || 0)
  return gate >= 1 && gate <= 5 ? (gate as ReviewKey) : null
}
