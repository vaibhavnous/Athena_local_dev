// @ts-nocheck
import React, { useEffect, useMemo, useState } from 'react'
import { motion } from 'framer-motion'
import { CheckCircle, CheckCircle2, Loader2, Send, Shield, Table2, Timer } from 'lucide-react'
import useAthenaStore from '../store/useAthenaStore'
import KpiReviewCard from '../components/hitl/KpiReviewCard'
import EditKpiModal from '../components/hitl/EditKpiModal'
import {
  approveKpi,
  getEnrichmentReviews,
  fetchKpiReviews,
  getRun,
  getPipelineKpis,
  getTableReviews,
  modifyKpi,
  rejectKpi,
  submitEnrichmentReview,
  submitTableReviews
} from '../api/athenaApi'

function HitlQueue() {
  const {
    runs,
    hitlQueues,
    addNotification,
    submitDecisions: storeSubmitDecisions,
    updateRun,
    setHitlQueue,
    setHitlSourceRunId
  } = useAthenaStore()

  const reviewRuns = useMemo(
    () =>
      runs.filter((run) => {
        const gate = Number(run?.next_gate || 0)
        return gate === 1 || gate === 2 || gate === 3
      }),
    [runs]
  )

  const [selectedRunId, setSelectedRunId] = useState(reviewRuns[0]?.id || null)
  const [statusFilter, setStatusFilter] = useState('All')
  const [localDecisions, setLocalDecisions] = useState({})
  const [editedKpis, setEditedKpis] = useState({})
  const [editingKpi, setEditingKpi] = useState(null)
  const [rejectionReasons, setRejectionReasons] = useState({})
  const [submitting, setSubmitting] = useState(false)
  const [hydrating, setHydrating] = useState(false)
  const [tableReview, setTableReview] = useState(null)
  const [selectedTables, setSelectedTables] = useState({})
  const [enrichmentReview, setEnrichmentReview] = useState(null)
  const [gate3Decision, setGate3Decision] = useState('APPROVED')

  const REVIEWER_ID = 'reviewer@nousinfo.com'
  const currentRun = runs.find((run) => run.id === selectedRunId)
  const gateToReview = Number(currentRun?.next_gate || 0)
  const isReviewableRun = gateToReview === 1 || gateToReview === 2 || gateToReview === 3
  const isGate2 = gateToReview === 2
  const isGate3 = gateToReview === 3
  const isSftpRun = currentRun?.source === 'sftp' || currentRun?.source === 'adls_gen2'
  const queue = useMemo(
    () => hitlQueues[selectedRunId] || (currentRun?.kpis || []),
    [currentRun?.kpis, hitlQueues, selectedRunId]
  )

  useEffect(() => {
    const selectedStillExists = selectedRunId && runs.some((run) => run.id === selectedRunId)
    const selectedNeedsReview = currentRun && isReviewableRun

    if (selectedStillExists && selectedNeedsReview) return

    const nextRun = reviewRuns[0] || null
    if (nextRun && nextRun.id !== selectedRunId) {
      setSelectedRunId(nextRun.id)
      setTableReview(null)
      setEnrichmentReview(null)
      setSelectedTables({})
      setLocalDecisions({})
    } else if (!nextRun && selectedRunId) {
      setSelectedRunId(null)
      setTableReview(null)
      setEnrichmentReview(null)
      setSelectedTables({})
      setLocalDecisions({})
    }
  }, [runs, reviewRuns, selectedRunId, currentRun, isReviewableRun])

  useEffect(() => {
    setTableReview(null)
    setEnrichmentReview(null)
    setSelectedTables({})
  }, [selectedRunId, currentRun?.source, gateToReview])

  useEffect(() => {
    if (!selectedRunId) return
    let cancelled = false

    const hydrate = async () => {
      setHydrating(true)
      try {
        if (isGate3) {
          const review = await getEnrichmentReviews(selectedRunId)
          if (cancelled) return
          setEnrichmentReview(review)
          setGate3Decision('APPROVED')
          updateRun(selectedRunId, {
            enriched_metadata: review.enriched_metadata || {},
            enriched_columns: review.enriched_columns || [],
            enriched_joins: review.enriched_joins || [],
            semantic_counts: review.semantic_counts || {},
            pii_columns: review.pii_columns || [],
            join_key_columns: review.join_key_columns || [],
            measure_columns: review.measure_columns || [],
            next_gate: review.next_gate,
            resume_message: review.resume_message,
            gate3_approved: review.gate3_approved
          })
          return
        }

        if (isGate2) {
          const review = await getTableReviews(selectedRunId)
          if (cancelled) return
          setTableReview(review)
          setSelectedTables((prev) => {
            const next = { ...prev }
            const items = isSftpRun ? getSftpFeeds(review) : (review.nominated_tables || [])
            for (const table of items) {
              const key = isSftpRun ? sftpFeedKey(table) : tableReviewKey(table)
              if (!(key in next)) next[key] = true
            }
            return next
          })
          updateRun(selectedRunId, {
            nominated_tables: review.nominated_tables || [],
            certified_tables: review.certified_tables || [],
            candidate_feed: review.candidate_feed || null,
            candidate_feeds: review.candidate_feeds || [],
            next_gate: review.next_gate,
            resume_message: review.resume_message
          })
          return
        }

        const reviewData = await fetchKpiReviews(selectedRunId)
        if (cancelled) return
        if (reviewData.kpis && reviewData.kpis.length > 0) {
          const mapped = reviewData.kpis.map(mapHitlRow)
          setHitlQueue(selectedRunId, mapped)
          setHitlSourceRunId(selectedRunId, selectedRunId)
          updateRun(selectedRunId, { kpis: mapped })
          return
        }

        const fallback = await getPipelineKpis(selectedRunId)
        if (cancelled) return
        setHitlQueue(selectedRunId, fallback.kpis || [])
        setHitlSourceRunId(selectedRunId, fallback.runId)
        updateRun(selectedRunId, { kpis: fallback.kpis || [], kpi_source_run_id: fallback.runId })
      } catch (error) {
        if (cancelled) return
        addNotification({
          type: 'error',
          title: isGate2 ? 'Gate 2 Load Failed' : 'KPI Load Failed',
          message: error.message || (isGate2 ? 'Unable to load table review data.' : 'Unable to load KPI review data.'),
          duration: 5000
        })
      } finally {
        if (!cancelled) setHydrating(false)
      }
    }

    hydrate()
    return () => {
      cancelled = true
    }
  }, [selectedRunId, isGate2, isGate3, setHitlQueue, setHitlSourceRunId, updateRun, addNotification, currentRun?.source])

  const filteredQueue = useMemo(() => {
    if (statusFilter === 'All') return queue
    return queue.filter((item) => {
      const decision = localDecisions[item.queue_id || item.id] || item.decision
      if (statusFilter === 'Pending') return !decision
      return decision === statusFilter
    })
  }, [queue, statusFilter, localDecisions])

  const kpiCounts = useMemo(() => ({
    total: queue.length,
    pending: queue.filter((item) => !localDecisions[item.queue_id || item.id] && !item.decision).length,
    approved: Object.values(localDecisions).filter((value) => value === 'APPROVED').length,
    edited: Object.values(localDecisions).filter((value) => value === 'EDITED').length,
    rejected: Object.values(localDecisions).filter((value) => value === 'REJECTED').length
  }), [queue, localDecisions])

  const selectedTableCount = (tableReview?.nominated_tables || []).filter((table) => selectedTables[tableReviewKey(table)]).length
  const availableSftpFeeds = getSftpFeeds(tableReview)
  const selectedFeedCount = availableSftpFeeds.filter((feed) => selectedTables[sftpFeedKey(feed)]).length
  const totalFeedCount = availableSftpFeeds.length
  const someDecided = Object.keys(localDecisions).length > 0

  const handleApprove = (kpiId) => {
    setLocalDecisions((prev) => ({ ...prev, [kpiId]: 'APPROVED' }))
  }

  const handleReject = (kpiId, reason) => {
    setRejectionReasons((prev) => ({ ...prev, [kpiId]: reason }))
    setLocalDecisions((prev) => ({ ...prev, [kpiId]: 'REJECTED' }))
  }

  const handleEdit = (kpi) => {
    setEditingKpi(kpi)
  }

  const handleSaveEdit = (kpiId, updates) => {
    setLocalDecisions((prev) => ({ ...prev, [kpiId]: 'EDITED' }))
    setEditedKpis((prev) => ({ ...prev, [kpiId]: updates }))
    setEditingKpi(null)
  }

  const handleClearDecision = (kpiId) => {
    setLocalDecisions((prev) => {
      const next = { ...prev }
      delete next[kpiId]
      return next
    })
  }

  const handleAutoApproveAll = () => {
    const next = {}
    queue.forEach((item) => {
      if (!item.decision) next[item.id] = 'APPROVED'
    })
    setLocalDecisions((prev) => ({ ...prev, ...next }))
  }

  const handleSelectAllTables = () => {
    const next = {}
    for (const table of tableReview?.nominated_tables || []) {
      next[tableReviewKey(table)] = true
    }
    setSelectedTables(next)
  }

  const handleSelectAllFeeds = () => {
    const next = {}
    for (const feed of availableSftpFeeds) {
      next[sftpFeedKey(feed)] = true
    }
    setSelectedTables(next)
  }

  const handleSubmit = async () => {
    if (isGate2) {
      if (isSftpRun) {
        const approvedFeeds = availableSftpFeeds.filter((feed) => selectedTables[sftpFeedKey(feed)])

        if (!approvedFeeds.length) {
          addNotification({ type: 'amber', title: 'No Feeds Selected', message: 'Select at least one discovered feed before submitting Gate 2.', duration: 3000 })
          return
        }
      } else {
        const approvedTables = (tableReview?.nominated_tables || [])
          .map((table) => tableReviewKey(table))
          .filter((key) => selectedTables[key])

        if (!approvedTables.length) {
          addNotification({ type: 'amber', title: 'No Tables Selected', message: 'Select at least one table before submitting Gate 2.', duration: 3000 })
          return
        }
      }

      setSubmitting(true)
      try {
        const approvedTables = isSftpRun
          ? ['sftp-feed-approved']
          : (tableReview?.nominated_tables || [])
              .map((table) => tableReviewKey(table))
              .filter((key) => selectedTables[key])
        await submitTableReviews(selectedRunId, approvedTables)
        const refreshed = await getRun(selectedRunId)
        updateRun(selectedRunId, refreshed)
        setTableReview(null)
        setSelectedTables({})
        addNotification({
          type: 'success',
          title: 'Gate 2 Submitted',
          message: isSftpRun
            ? 'Approved feeds were submitted for SFTP Gate 2.'
            : 'Approved tables were submitted. Metadata discovery and profiling are resuming.',
          duration: 5000
        })
      } catch (error) {
        addNotification({ type: 'error', title: 'Gate 2 Failed', message: error.message, duration: 5000 })
      } finally {
        setSubmitting(false)
      }
      return
    }

    if (isGate3) {
      setSubmitting(true)
      try {
        await submitEnrichmentReview(selectedRunId, gate3Decision === 'APPROVED')
        const refreshed = await getRun(selectedRunId)
        updateRun(selectedRunId, refreshed)
        setEnrichmentReview(null)
        addNotification({
          type: 'success',
          title: 'Gate 3 Submitted',
          message: 'Enrichment review was submitted. Script generation is resuming.',
          duration: 5000
        })
      } catch (error) {
        addNotification({ type: 'error', title: 'Gate 3 Failed', message: error.message, duration: 5000 })
      } finally {
        setSubmitting(false)
      }
      return
    }

    const decided = queue.filter((item) => localDecisions[item.queue_id || item.id] || item.decision)
    if (decided.length === 0) {
      addNotification({ type: 'amber', title: 'No Decisions', message: 'Make at least one decision before submitting.', duration: 3000 })
      return
    }

    setSubmitting(true)
    let saved = 0
    let failed = 0
    const hasQueueIds = queue.some((item) => item.queue_id)

    try {
      if (hasQueueIds) {
        await Promise.allSettled(
          queue.map(async (item) => {
            const key = item.queue_id || item.id
            const decision = localDecisions[key] || item.decision
            if (!decision || !item.queue_id) return

            try {
              if (decision === 'APPROVED') {
                await approveKpi(item.queue_id, REVIEWER_ID)
              } else if (decision === 'REJECTED') {
                await rejectKpi(item.queue_id, REVIEWER_ID, rejectionReasons[key] || 'Rejected by reviewer')
              } else if (decision === 'EDITED') {
                const edited = editedKpis[key]
                await modifyKpi(item.queue_id, REVIEWER_ID, { ...(item.kpi_detail || item), ...edited })
              }
              saved++
            } catch {
              failed++
            }
          })
        )
      }

      const decisions = queue.map((item) => {
        const key = item.queue_id || item.id
        const decision = localDecisions[key] || item.decision
        if (!decision) return null
        const edited = editedKpis[key]
        return {
          kpi_id: key,
          decision,
          reviewer: REVIEWER_ID,
          notes: edited?.notes || rejectionReasons[key] || '',
          edited_definition: edited?.definition || null
        }
      }).filter(Boolean)

      storeSubmitDecisions(selectedRunId, decisions)
      updateRun(selectedRunId, { status: 'RUNNING' })
      setLocalDecisions({})
      setEditedKpis({})
      setRejectionReasons({})

      if (!hasQueueIds) {
        addNotification({
          type: 'amber',
          title: 'Decisions Recorded Locally',
          message: 'KPIs were loaded from fallback data. Database update was skipped.',
          duration: 5000
        })
      } else if (failed === 0) {
        addNotification({
          type: 'success',
          title: 'Decisions Saved',
          message: `${saved} KPI decision${saved !== 1 ? 's' : ''} saved. Pipeline resuming.`,
          duration: 5000
        })
      } else {
        addNotification({
          type: 'amber',
          title: 'Partial Save',
          message: `${saved} saved, ${failed} failed to save.`,
          duration: 5000
        })
      }
    } catch (error) {
      addNotification({ type: 'error', title: 'Submit Failed', message: error.message, duration: 5000 })
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="flex flex-col h-full gap-4">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-lg font-bold text-white">
            {isGate3 ? 'Gate 3 - Enrichment Review' : isGate2 ? (isSftpRun ? 'Gate 2 - SFTP Feed Review' : 'Gate 2 - Table Review') : 'Gate 1 - KPI Review'}
          </h1>
          <p className="text-sm text-gray-500 mt-0.5">
            {isGate3
              ? (enrichmentReview?.resume_message || 'Review semantic enrichment before the pipeline continues')
              : isGate2
              ? (tableReview?.resume_message || (isSftpRun ? 'Review discovered SFTP feeds before the pipeline continues.' : 'Review and certify nominated tables before the pipeline continues'))
              : 'Review and approve extracted KPIs before the pipeline continues'}
          </p>
        </div>

        <div className="flex items-center gap-2">
          {reviewRuns.length > 0 && (
            <select
              value={selectedRunId || ''}
              onChange={(event) => setSelectedRunId(event.target.value)}
              className="input-field w-auto text-xs"
            >
              {reviewRuns.map((run) => (
                <option key={run.id} value={run.id}>
                  {run.id.slice(0, 14)} - {run.brd_filename} ({run.next_gate === 3 ? 'Gate 3' : run.next_gate === 2 ? 'Gate 2' : 'Gate 1'})
                </option>
              ))}
            </select>
          )}

          {!isGate2 && !isGate3 && (
            <select
              value={statusFilter}
              onChange={(event) => setStatusFilter(event.target.value)}
              className="input-field w-auto text-xs"
            >
              <option value="All">All</option>
              <option value="Pending">Pending</option>
              <option value="APPROVED">Approved</option>
              <option value="EDITED">Edited</option>
              <option value="REJECTED">Rejected</option>
            </select>
          )}
        </div>
      </div>

      <div className="flex gap-4 flex-1 min-h-0">
        <div className="flex-1 overflow-y-auto pr-1 space-y-4 pb-20">
          {selectedRunId && isReviewableRun ? (
            isGate3 ? (
            <div className="space-y-4">
              <div className="card p-5">
                <div className="flex items-center justify-between gap-3 mb-4">
                  <div>
                    <h3 className="text-base font-bold text-text-primary">Enrichment Summary</h3>
                    <p className="text-sm text-text-secondary mt-1">{enrichmentReview?.resume_message || 'Review enriched metadata and approve or reject it.'}</p>
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-3">
                  <StatTile label="Columns" value={(enrichmentReview?.enriched_columns || []).length} />
                  <StatTile label="Joins" value={(enrichmentReview?.enriched_joins || []).length} />
                  <StatTile label="PII Columns" value={(enrichmentReview?.pii_columns || []).length} />
                  <StatTile label="Join Keys" value={(enrichmentReview?.join_key_columns || []).length} />
                </div>
              </div>

              <div className="card p-5">
                <h3 className="text-base font-bold text-text-primary mb-3">Semantic Types</h3>
                <div className="flex flex-wrap gap-2">
                  {Object.entries(enrichmentReview?.semantic_counts || {}).map(([key, value]) => (
                    <span key={key} className="px-3 py-1 rounded-full text-xs font-medium bg-bg-border text-text-secondary border border-bg-border">
                      {key}: {value}
                    </span>
                  ))}
                </div>
              </div>

              <div className="card p-5">
                <h3 className="text-base font-bold text-text-primary mb-3">Decision</h3>
                <div className="flex gap-3">
                  <button
                    onClick={() => setGate3Decision('APPROVED')}
                    className={`flex-1 px-4 py-3 rounded-lg border text-sm font-semibold transition-colors ${
                      gate3Decision === 'APPROVED'
                        ? 'bg-accent-green/15 border-accent-green/30 text-accent-green'
                        : 'border-bg-border text-text-secondary hover:border-gray-600'
                    }`}
                  >
                    Approve Enrichment
                  </button>
                  <button
                    onClick={() => setGate3Decision('REJECTED')}
                    className={`flex-1 px-4 py-3 rounded-lg border text-sm font-semibold transition-colors ${
                      gate3Decision === 'REJECTED'
                        ? 'bg-accent-red/15 border-accent-red/30 text-accent-red'
                        : 'border-bg-border text-text-secondary hover:border-gray-600'
                    }`}
                  >
                    Reject Enrichment
                  </button>
                </div>
              </div>
            </div>
            ) : isGate2 ? (
            (isSftpRun
              ? (availableSftpFeeds.length === 0)
              : (tableReview?.nominated_tables || []).length === 0) ? (
              <div className="flex items-center justify-center h-40 text-gray-600 text-sm">
                {isSftpRun ? 'No discovered feeds found for this run.' : 'No nominated tables found for this run.'}
              </div>
            ) : (
              isSftpRun
                ? (availableSftpFeeds.map((feed) => {
                    const key = sftpFeedKey(feed)
                    return (
                      <label key={key} className="card p-5 flex items-start gap-3 cursor-pointer border border-bg-border hover:border-gray-600 transition-colors">
                        <input
                          type="checkbox"
                          checked={!!selectedTables[key]}
                          onChange={() => setSelectedTables((prev) => ({ ...prev, [key]: !prev[key] }))}
                          className="mt-1 accent-accent-blue"
                        />
                        <SftpFeedReviewBody feed={feed} />
                      </label>
                    )
                  }))
                : ((tableReview?.nominated_tables || []).map((table) => {
                    const key = tableReviewKey(table)
                    return (
                      <label key={key} className="card p-5 flex items-start gap-3 cursor-pointer border border-bg-border hover:border-gray-600 transition-colors">
                        <input
                          type="checkbox"
                          checked={!!selectedTables[key]}
                          onChange={() => setSelectedTables((prev) => ({ ...prev, [key]: !prev[key] }))}
                          className="mt-1 accent-accent-blue"
                        />
                        <div className="min-w-0 flex-1">
                          <div className="flex items-center gap-2 flex-wrap">
                            <Table2 size={15} className="text-text-tertiary" />
                            <h3 className="text-base font-bold text-text-primary break-all">{key}</h3>
                          </div>
                          <div className="flex items-center gap-2 text-xs text-text-tertiary flex-wrap mt-2">
                            <span>Confidence {Number(table.confidence_score || 0).toFixed(3)}</span>
                            <span className="opacity-40">-</span>
                            <span>Coverage {Number(table.coverage_ratio || 0).toFixed(3)}</span>
                            {(table.matched_keywords || []).length > 0 && (
                              <>
                                <span className="opacity-40">-</span>
                                <span>{(table.matched_keywords || []).join(', ')}</span>
                              </>
                            )}
                          </div>
                          {table.nomination_reason && (
                            <p className="text-sm text-text-secondary leading-relaxed mt-2">{table.nomination_reason}</p>
                          )}
                        </div>
                      </label>
                    )
                  }))
            )
            ) : filteredQueue.length === 0 ? (
            <div className="flex items-center justify-center h-40 text-gray-600 text-sm">
              {queue.length === 0 ? 'No KPIs in queue. Select a run with Gate 1 pending.' : 'No KPIs match the current filter.'}
            </div>
          ) : (
            filteredQueue.map((kpi) => (
              <KpiReviewCard
                key={kpi.queue_id || kpi.id}
                kpi={{ ...kpi, ...(editedKpis[kpi.queue_id || kpi.id] || {}) }}
                localDecision={localDecisions[kpi.queue_id || kpi.id]}
                rejectionReason={rejectionReasons[kpi.queue_id || kpi.id]}
                onApprove={(id) => id ? handleApprove(id) : handleClearDecision(kpi.queue_id || kpi.id)}
                onEdit={handleEdit}
                onReject={handleReject}
              />
            ))
            )
          ) : (
            <div className="flex items-center justify-center h-40 rounded-2xl border border-dashed border-bg-border bg-bg-card/40 text-center px-6">
              <div>
                <p className="text-sm font-semibold text-gray-300">No pending gate review</p>
                <p className="text-xs text-gray-500 mt-1">This page only shows runs paused at Gate 1, Gate 2, or Gate 3.</p>
              </div>
            </div>
          )}
        </div>

        <div className="w-72 flex-shrink-0 flex flex-col gap-3">
          <div className="card p-4">
            <h3 className="text-xs uppercase tracking-wider text-gray-500 mb-3">Review Progress</h3>
            <div className="space-y-2">
              {selectedRunId && isReviewableRun ? (
                isGate2 ? (
                <>
                  <CountRow label={isSftpRun ? 'Total Feeds' : 'Total Tables'} value={isSftpRun ? totalFeedCount : (tableReview?.nominated_tables || []).length} color="text-gray-300" />
                  <CountRow label="Selected" value={isSftpRun ? selectedFeedCount : selectedTableCount} color="text-accent-green" pulse={(isSftpRun ? selectedFeedCount : selectedTableCount) > 0} />
                  <CountRow label="Unselected" value={Math.max(0, (isSftpRun ? (totalFeedCount - selectedFeedCount) : ((tableReview?.nominated_tables || []).length - selectedTableCount)))} color="text-accent-amber" />
                </>
                ) : isGate3 ? (
                <>
                  <CountRow label="Columns" value={(enrichmentReview?.enriched_columns || []).length} color="text-gray-300" />
                  <CountRow label="Joins" value={(enrichmentReview?.enriched_joins || []).length} color="text-accent-blue" />
                  <CountRow label="Decision" value={gate3Decision === 'APPROVED' ? 'Approve' : 'Reject'} color={gate3Decision === 'APPROVED' ? 'text-accent-green' : 'text-accent-red'} pulse />
                </>
                ) : (
                <>
                  <CountRow label="Total" value={kpiCounts.total} color="text-gray-300" />
                  <CountRow label="Pending" value={kpiCounts.pending} color="text-accent-amber" pulse={kpiCounts.pending > 0} />
                  <CountRow label="Approved" value={kpiCounts.approved} color="text-accent-green" />
                  <CountRow label="Edited" value={kpiCounts.edited} color="text-accent-purple" />
                  <CountRow label="Rejected" value={kpiCounts.rejected} color="text-accent-red" />
                </>
                )
              ) : (
                <span className="text-gray-500 text-sm">No active gate review</span>
              )}
            </div>

            <div className="mt-3">
              <div className="h-1.5 bg-bg-border rounded-full overflow-hidden">
                <div
                  className="h-full bg-accent-green transition-all duration-500 rounded-full"
                  style={{
                    width: `${isGate3
                      ? 100
                      : isGate2
                      ? (isSftpRun
                          ? (totalFeedCount > 0 ? (selectedFeedCount / totalFeedCount) * 100 : 0)
                          : ((tableReview?.nominated_tables || []).length > 0 ? (selectedTableCount / (tableReview?.nominated_tables || []).length) * 100 : 0))
                      : (kpiCounts.total > 0 ? ((kpiCounts.approved + kpiCounts.edited + kpiCounts.rejected) / kpiCounts.total) * 100 : 0)}%`
                  }}
                />
              </div>
            </div>
          </div>

          <div className="card p-4">
            <div className="flex items-center gap-2 mb-2">
              <Timer size={14} className="text-accent-amber" />
              <span className="text-xs font-medium text-gray-300">Review State</span>
            </div>
            <p className="text-2xl font-mono font-bold text-accent-amber">{hydrating ? 'SYNC' : 'READY'}</p>
            <p className="text-[10px] text-gray-600 mt-1">Pipeline is paused</p>
          </div>

          <button
            onClick={isGate3 ? () => setGate3Decision('APPROVED') : isGate2 ? (isSftpRun ? handleSelectAllFeeds : handleSelectAllTables) : handleAutoApproveAll}
            className="flex items-center justify-center gap-2 px-4 py-3 bg-accent-green/10 hover:bg-accent-green/20 border border-accent-green/25 text-accent-green text-sm font-semibold rounded-xl transition-colors"
          >
            <CheckCircle size={15} />
            {isGate3 ? 'Set Approve' : isGate2 ? (isSftpRun ? 'Select All Feeds' : 'Select All Tables') : 'Auto-approve All'}
          </button>

          <div className="p-3 bg-bg-card border border-bg-border rounded-xl">
            <div className="flex items-start gap-2">
              <Shield size={12} className="text-gray-600 mt-0.5 flex-shrink-0" />
              <p className="text-[10px] text-gray-600 leading-relaxed">
                {isGate2
                  ? (isSftpRun
                    ? 'Gate 2 validates the discovered SFTP feeds. Review entity, source file, sample rows, columns, keys, and measures before approving the feed set.'
                    : 'Certified tables become the source set for metadata discovery, profiling, and enrichment.')
                  : isGate3
                  ? 'Approving Gate 3 starts bronze, silver, and gold code generation. Rejecting keeps the run paused for rework.'
                  : 'Approvals are final once submitted. Rejected KPIs will be excluded from the final export.'}
              </p>
            </div>
          </div>
        </div>
      </div>

      {((isGate2 ? (isSftpRun ? (totalFeedCount > 0) : (tableReview?.nominated_tables || []).length > 0) : isGate3 ? true : someDecided)) && (
        <motion.div
          initial={{ y: 80, opacity: 0 }}
          animate={{ y: 0, opacity: 1 }}
          exit={{ y: 80, opacity: 0 }}
          className="fixed bottom-0 left-0 right-0 z-30 flex items-center justify-between px-6 py-4 bg-bg-card border-t border-bg-border shadow-2xl"
          style={{ background: 'rgba(17,24,39,0.95)', backdropFilter: 'blur(10px)' }}
        >
          <div className="flex items-center gap-4 text-sm">
            {isGate3 ? (
              <>
                <span className={gate3Decision === 'APPROVED' ? 'text-accent-green font-semibold' : 'text-accent-red font-semibold'}>
                  {gate3Decision === 'APPROVED' ? 'Approve selected' : 'Reject selected'}
                </span>
                <span className="text-gray-500">{(enrichmentReview?.enriched_columns || []).length} enriched columns</span>
              </>
            ) : isGate2 ? (
              <>
                <span className="text-accent-green font-semibold">{isSftpRun ? selectedFeedCount : selectedTableCount} selected</span>
                <span className="text-gray-500">{Math.max(0, (isSftpRun ? (totalFeedCount - selectedFeedCount) : ((tableReview?.nominated_tables || []).length - selectedTableCount)))} unselected</span>
              </>
            ) : (
              <>
                {kpiCounts.approved > 0 && <span className="text-accent-green font-semibold">{kpiCounts.approved} approved</span>}
                {kpiCounts.edited > 0 && <span className="text-accent-purple font-semibold">{kpiCounts.edited} edited</span>}
                {kpiCounts.rejected > 0 && <span className="text-accent-red font-semibold">{kpiCounts.rejected} rejected</span>}
                {kpiCounts.pending > 0 && <span className="text-gray-500">{kpiCounts.pending} still pending</span>}
              </>
            )}
          </div>

          <button
            onClick={handleSubmit}
            disabled={hydrating || submitting || !selectedRunId || !isReviewableRun}
            className="flex items-center gap-2 px-6 py-2.5 bg-accent-blue hover:bg-blue-600 disabled:opacity-50 disabled:cursor-not-allowed text-white text-sm font-bold rounded-xl transition-colors shadow-lg"
          >
            {submitting ? (
              <>
                <Loader2 size={14} className="animate-spin" />
                Submitting...
              </>
            ) : (
              <>
                {isGate3 || isGate2 ? <CheckCircle2 size={14} /> : <Send size={14} />}
                {isGate3 ? 'Submit Gate 3 & Resume Pipeline ->' : isGate2 ? 'Submit Gate 2 & Resume Pipeline ->' : 'Submit All Decisions & Resume Pipeline ->'}
              </>
            )}
          </button>
        </motion.div>
      )}

      <EditKpiModal
        kpi={editingKpi}
        isOpen={!!editingKpi}
        onClose={() => setEditingKpi(null)}
        onSave={handleSaveEdit}
      />
    </div>
  )
}

function mapHitlRow(row) {
  return {
    id: row.queue_id,
    queue_id: row.queue_id,
    item_id: row.item_id,
    item_type: row.item_type || 'METADATA',
    kpi_detail: row.kpi_detail || {},
    modified_detail: row.modified_detail || null,
    gate_status: row.gate_status,
    decision: row.gate_status !== 'PENDING' ? row.gate_status : null,
    reviewer_id: row.reviewer_id,
    rejection_reason: row.rejection_reason,
    auto_approved: row.auto_approved === true || row.auto_approved === 'true',
    queued_at: row.queued_at,
    decided_at: row.decided_at,
    timeout_at: row.timeout_at
  }
}

function CountRow({ label, value, color, pulse }) {
  return (
    <div className="flex items-center justify-between">
      <div className="flex items-center gap-2">
        {pulse && <span className="w-1.5 h-1.5 rounded-full bg-accent-amber animate-pulse" />}
        <span className="text-xs text-gray-500">{label}</span>
      </div>
      <span className={`text-sm font-bold font-mono ${color}`}>{value}</span>
    </div>
  )
}

function StatTile({ label, value }) {
  return (
    <div className="rounded-lg border border-bg-border bg-bg-base px-3 py-3">
      <div className="text-xs text-text-tertiary">{label}</div>
      <div className="text-lg font-bold text-text-primary mt-1">{value}</div>
    </div>
  )
}

function tableReviewKey(table) {
  return [table.database_name, table.schema_name, table.table_name].filter(Boolean).join('.')
}

function sftpFeedKey(feed) {
  return [feed.vendor, feed.entity, feed.file_name || feed.feed_id].filter(Boolean).join('.')
}

function getSftpFeeds(review) {
  if (!review) return []
  if (Array.isArray(review.candidate_feeds) && review.candidate_feeds.length > 0) {
    return review.candidate_feeds
  }
  return review.candidate_feed ? [review.candidate_feed] : []
}

function SftpFeedReviewBody({ feed }) {
  const columns = Array.isArray(feed?.columns) ? feed.columns : []
  const primaryKeys = Array.isArray(feed?.primary_keys) ? feed.primary_keys : []
  const measures = Array.isArray(feed?.measures) ? feed.measures : []
  const entities = Array.isArray(feed?.entities) ? feed.entities : []

  return (
    <div className="min-w-0 flex-1">
      <div className="flex items-center gap-2 flex-wrap">
        <Table2 size={15} className="text-text-tertiary" />
        <h3 className="text-base font-bold text-text-primary break-all">
          {feed.vendor || 'Vendor'}.{feed.entity || feed.semantic_type || 'feed'}
        </h3>
        {feed.semantic_type && (
          <span className="px-2 py-0.5 rounded-full text-[10px] font-medium bg-accent-blue/10 text-accent-blue border border-accent-blue/20">
            {feed.semantic_type}
          </span>
        )}
      </div>

      <div className="grid grid-cols-2 gap-2 mt-3 text-xs text-text-tertiary">
        <div><span className="text-gray-500">File:</span> {feed.file_name || 'n/a'}</div>
        <div><span className="text-gray-500">Format:</span> {feed.format || 'unknown'}</div>
        <div><span className="text-gray-500">Rows:</span> {Number(feed.sample_row_count || 0)}</div>
        <div><span className="text-gray-500">Columns:</span> {columns.length}</div>
      </div>

      {entities.length > 0 && (
        <p className="text-xs text-text-secondary mt-2">
          Feed set entities: {entities.join(', ')}
        </p>
      )}

      {feed.file_path && (
        <p className="text-xs text-text-secondary mt-2 break-all">
          Path: {feed.file_path}
        </p>
      )}

      {primaryKeys.length > 0 && (
        <p className="text-xs text-text-secondary mt-2">
          Primary keys: {primaryKeys.join(', ')}
        </p>
      )}

      {measures.length > 0 && (
        <p className="text-xs text-text-secondary mt-1">
          Measures: {measures.join(', ')}
        </p>
      )}

      {columns.length > 0 && (
        <div className="flex flex-wrap gap-1.5 mt-3">
          {columns.slice(0, 8).map((column) => (
            <span key={column} className="px-2 py-0.5 rounded-full text-[10px] font-medium bg-bg-base border border-bg-border text-text-secondary">
              {column}
            </span>
          ))}
          {columns.length > 8 && (
            <span className="px-2 py-0.5 rounded-full text-[10px] font-medium bg-bg-base border border-bg-border text-text-secondary">
              +{columns.length - 8} more
            </span>
          )}
        </div>
      )}
    </div>
  )
}

export default HitlQueue
