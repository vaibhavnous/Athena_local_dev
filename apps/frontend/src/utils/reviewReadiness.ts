export type ReviewKey = 1 | 2 | 3 | 4 | 5 | 'silver_merge_key_review' | 'gold_review'

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
    const metadata = review?.enriched_metadata || {}
    return Boolean(
      (review?.enriched_columns || []).length ||
      (review?.enriched_joins || []).length ||
      (review?.feed_semantic_summary || []).length ||
      (review?.semantic_tables || []).length ||
      (review?.tables || []).length ||
      (metadata?.columns || []).length ||
      (metadata?.semantic_tables || []).length ||
      (metadata?.tables || []).length ||
      Object.keys(review?.semantic_counts || {}).length ||
      (review?.pii_columns || []).length ||
      (review?.join_key_columns || []).length ||
      (review?.measure_columns || []).length
    )
  }
  if (reviewKey === 4) return Boolean((review?.bronze_review_artifact?.feeds || []).length)
  if (reviewKey === 'silver_merge_key_review') {
    return Boolean((review?.silver_merge_key_review_artifact?.feeds || []).length)
  }
  if (reviewKey === 'gold_review') return Boolean((review?.gold_review_artifact?.items || []).length)
  if (reviewKey === 5) return Boolean((review?.silver_review_artifact?.items || []).length)
  return false
}

export function activeReviewKey(run: any): ReviewKey | null {
  const namedReview = String(run?.next_review_key || '')
  if (namedReview === 'silver_merge_key_review' || namedReview === 'gold_review') {
    return namedReview
  }

  const gate = Number(run?.next_gate || 0)
  return gate >= 1 && gate <= 5 ? (gate as ReviewKey) : null
}
