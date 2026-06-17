// @ts-nocheck
import React, { useEffect, useMemo, useState } from 'react'
import { motion } from 'framer-motion'
import { useNavigate } from 'react-router-dom'
import { CheckCircle, CheckCircle2, Copy, Download, Loader2, Send, Shield, Table2, Timer } from 'lucide-react'
import useAthenaStore from '../store/useAthenaStore'
import KpiReviewCard from '../components/hitl/KpiReviewCard'
import EditKpiModal from '../components/hitl/EditKpiModal'
import {
  getBronzeReview,
  getEnrichmentReviews,
  fetchKpiReviews,
  getRun,
  getPipelineKpis,
  getSilverReview,
  getTableReviews,
  submitBronzeReview,
  submitDecisions as submitHitlDecisions,
  submitEnrichmentReview,
  submitSilverReview,
  submitTableReviews
} from '../api/athenaApi'
import { getGateDisplayName } from '../utils/pipelinePhases'

const ATHENA_LOGO_SRC = `${process.env.PUBLIC_URL}/Athena_logo.png`

const sleep = (ms) => new Promise((resolve) => window.setTimeout(resolve, ms))

async function waitForRunGate(runId, updateRun, targetGate, attempts = 20) {
  let latest = null
  for (let index = 0; index < attempts; index += 1) {
    latest = await getRun(runId)
    updateRun(runId, latest)
    if (Number(latest?.next_gate || 0) === targetGate) return latest
    if (String(latest?.status || '').toUpperCase() === 'FAILED') return latest
    await sleep(1500)
  }
  return latest
}

function HitlQueue() {
  const navigate = useNavigate()
  const {
    runs,
    activeRunId,
    hitlQueues,
    addNotification,
    submitDecisions: storeSubmitDecisions,
    addRun,
    updateRun,
    setHitlQueue,
    setHitlSourceRunId,
    setActiveRun
  } = useAthenaStore()

  const reviewRuns = useMemo(
    () =>
      runs.filter((run) => {
        const gate = Number(run?.next_gate || 0)
        return (
          gate >= 1 &&
          gate <= 5 &&
          String(run?.status || '').toUpperCase() !== 'PAUSED_FOR_STAGE_CONFIRMATION' &&
          !run?.stage_confirmation?.awaiting_confirmation
        )
      }),
    [runs]
  )

  const initialReviewRun =
    reviewRuns.find((run) => run.id === activeRunId) || null
  const [selectedRunId, setSelectedRunId] = useState(initialReviewRun?.id || null)
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
  const [bronzeReview, setBronzeReview] = useState(null)
  const [silverReview, setSilverReview] = useState(null)
  const [gate3Decision, setGate3Decision] = useState('APPROVED')
  const [gateDecision, setGateDecision] = useState('APPROVED')
  const [selectedRunDetail, setSelectedRunDetail] = useState(null)

  const REVIEWER_ID = 'reviewer@nousinfo.com'
  const currentRun = runs.find((run) => run.id === selectedRunId) || (selectedRunDetail?.id === selectedRunId ? selectedRunDetail : null)
  const gateToReview = Number(currentRun?.next_gate || 0)
  const isReviewableRun = gateToReview >= 1 && gateToReview <= 5
  const isGate2 = gateToReview === 2
  const isGate3 = gateToReview === 3
  const isGate4 = gateToReview === 4
  const isGate5 = gateToReview === 5
  const isSftpRun = currentRun?.source === 'sftp' || currentRun?.source === 'adls_gen2'
  const gate1Name = getGateDisplayName(1)
  const gate2Name = getGateDisplayName(2, currentRun?.source)
  const gate3Name = getGateDisplayName(3)
  const gate4Name = getGateDisplayName(4)
  const gate5Name = getGateDisplayName(5)
  const rawQueue = useMemo(
    () => hitlQueues[selectedRunId] || (currentRun?.kpis || []),
    [currentRun?.kpis, hitlQueues, selectedRunId]
  )
  const queue = useMemo(
    () => filterReviewQueue(rawQueue, selectedRunId, currentRun?.source),
    [rawQueue, selectedRunId, currentRun?.source]
  )

  useEffect(() => {
    if (selectedRunId || !activeRunId) return
    let cancelled = false

    const hydrateActiveRun = async () => {
      try {
        const detail = await getRun(activeRunId)
        if (cancelled || !detail?.id) return

        const gate = Number(detail?.next_gate || 0)
        if (gate < 1 || gate > 5) return

        const alreadyKnown = runs.some((run) => run.id === detail.id)
        if (alreadyKnown) updateRun(detail.id, detail)
        else addRun(detail)

        setSelectedRunDetail(detail)
        setSelectedRunId(detail.id)
      } catch (error) {
        if (!cancelled) {
          console.warn('[HitlQueue] Failed to hydrate active review run', error)
        }
      }
    }

    hydrateActiveRun()
    return () => {
      cancelled = true
    }
  }, [activeRunId, addRun, runs, selectedRunId, updateRun])

  useEffect(() => {
    if (!selectedRunId || currentRun) return
    let cancelled = false

    const hydrateSelectedRun = async () => {
      try {
        const detail = await getRun(selectedRunId)
        if (cancelled || !detail?.id) return

        const alreadyKnown = runs.some((run) => run.id === detail.id)
        if (alreadyKnown) updateRun(detail.id, detail)
        else addRun(detail)

        setSelectedRunDetail(detail)
      } catch (error) {
        if (!cancelled) {
          console.warn('[HitlQueue] Failed to hydrate selected review run', error)
        }
      }
    }

    hydrateSelectedRun()
    return () => {
      cancelled = true
    }
  }, [addRun, currentRun, runs, selectedRunId, updateRun])

  useEffect(() => {
    const activeReviewRun = reviewRuns.find((run) => run.id === activeRunId)
    if (activeReviewRun && activeReviewRun.id !== selectedRunId) {
      setSelectedRunId(activeReviewRun.id)
      setTableReview(null)
      setEnrichmentReview(null)
      setSelectedTables({})
      setLocalDecisions({})
      return
    }

    const selectedStillExists =
      selectedRunId &&
      (runs.some((run) => run.id === selectedRunId) || selectedRunDetail?.id === selectedRunId)
    const selectedNeedsReview = currentRun && isReviewableRun

    if (selectedStillExists && selectedNeedsReview) return

    if (selectedRunId) {
      setSelectedRunId(null)
      setTableReview(null)
      setEnrichmentReview(null)
      setSelectedTables({})
      setLocalDecisions({})
    }
  }, [runs, reviewRuns, selectedRunId, currentRun, isReviewableRun, activeRunId, selectedRunDetail?.id])

  useEffect(() => {
    setTableReview(null)
    setEnrichmentReview(null)
    setBronzeReview(null)
    setSilverReview(null)
    setSelectedTables({})
    setGateDecision('APPROVED')
  }, [selectedRunId, currentRun?.source, gateToReview])

  // eslint-disable-next-line react-hooks/exhaustive-deps
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
            feed_semantic_summary: review.feed_semantic_summary || [],
            next_gate: review.next_gate,
            resume_message: review.resume_message,
            gate3_approved: review.gate3_approved
          })
          return
        }

        if (isGate4) {
          const review = await getBronzeReview(selectedRunId)
          if (cancelled) return
          setBronzeReview(review)
          updateRun(selectedRunId, {
            next_gate: review.next_gate,
            resume_message: review.resume_message,
            bronze_review_artifact: review.bronze_review_artifact || {}
          })
          return
        }

        if (isGate5) {
          const review = await getSilverReview(selectedRunId)
          if (cancelled) return
          setSilverReview(review)
          updateRun(selectedRunId, {
            next_gate: review.next_gate,
            resume_message: review.resume_message,
            silver_review_artifact: review.silver_review_artifact || {}
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

        let expectedSource = currentRun?.source
        if (!expectedSource) {
          try {
            const detail = await getRun(selectedRunId)
            if (cancelled) return
            expectedSource = detail?.source || expectedSource
            if (detail?.id) updateRun(selectedRunId, detail)
          } catch {
            // KPI review can still be validated by run_id when source is absent from the run summary.
          }
        }

        const reviewData = await fetchKpiReviews(selectedRunId)
        if (cancelled) return
        if (!reviewPayloadMatchesRun(reviewData, selectedRunId, expectedSource)) {
          setHitlQueue(selectedRunId, [])
          updateRun(selectedRunId, { kpis: [] })
          addNotification({
            type: 'error',
            title: 'Review Source Mismatch',
            message: 'Blocked KPI review data because it does not match the selected run source.',
            duration: 5000
          })
          return
        }
        if (reviewData.kpis && reviewData.kpis.length > 0) {
          const mapped = filterReviewQueue(reviewData.kpis.map(mapHitlRow), selectedRunId, expectedSource)
          setHitlQueue(selectedRunId, mapped)
          setHitlSourceRunId(selectedRunId, selectedRunId)
          updateRun(selectedRunId, { kpis: mapped })
          return
        }

        const fallback = await getPipelineKpis(selectedRunId)
        if (cancelled) return
        if (!reviewPayloadMatchesRun(fallback, selectedRunId, expectedSource)) {
          setHitlQueue(selectedRunId, [])
          updateRun(selectedRunId, { kpis: [] })
          return
        }
        const fallbackKpis = filterReviewQueue((fallback.kpis || []).map(mapHitlRow), selectedRunId, expectedSource)
        setHitlQueue(selectedRunId, fallbackKpis)
        setHitlSourceRunId(selectedRunId, fallback.runId)
        updateRun(selectedRunId, { kpis: fallbackKpis, kpi_source_run_id: fallback.runId })
      } catch (error) {
        if (cancelled) return
        addNotification({
          type: 'error',
          title: isGate2 ? `${gate2Name} Load Failed` : isGate3 ? `${gate3Name} Load Failed` : isGate4 ? `${gate4Name} Load Failed` : isGate5 ? `${gate5Name} Load Failed` : `${gate1Name} Load Failed`,
          message: error.message || (isGate2 ? 'Unable to load table review data.' : isGate3 ? 'Unable to load enrichment review data.' : isGate4 ? 'Unable to load Bronze review data.' : isGate5 ? 'Unable to load Silver review data.' : 'Unable to load KPI review data.'),
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
  }, [selectedRunId, isGate2, isGate3, isGate4, isGate5, gate1Name, gate2Name, gate3Name, gate4Name, gate5Name, isSftpRun, setHitlQueue, setHitlSourceRunId, updateRun, addNotification, currentRun?.source])

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
  const bronzeReviewFeeds = bronzeReview?.bronze_review_artifact?.feeds || []
  const silverReviewItems = silverReview?.silver_review_artifact?.items || []
  const gateReviewReady = isGate4 ? bronzeReviewFeeds.length > 0 : isGate5 ? silverReviewItems.length > 0 : false
  const allKpisDecided = queue.length > 0 && queue.every((item) => localDecisions[item.queue_id || item.id] || item.decision)
  const canSubmitReview = isGate2
    ? (isSftpRun ? totalFeedCount > 0 : (tableReview?.nominated_tables || []).length > 0)
    : isGate3
    ? true
    : (isGate4 || isGate5)
    ? true
    : allKpisDecided

  const returnToMonitor = (runId) => {
    if (runId) setActiveRun(runId)
    navigate('/app/data-discovery')
  }

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
          addNotification({ type: 'amber', title: 'No Feeds Selected', message: `Select at least one discovered feed before submitting ${gate2Name}.`, duration: 3000 })
          return
        }
      } else {
        const approvedTables = (tableReview?.nominated_tables || [])
          .map((table) => tableReviewKey(table))
          .filter((key) => selectedTables[key])

        if (!approvedTables.length) {
          addNotification({ type: 'amber', title: 'No Tables Selected', message: `Select at least one table before submitting ${gate2Name}.`, duration: 3000 })
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
        updateRun(selectedRunId, { ...refreshed, status: refreshed?.status || 'RUNNING' })
        setTableReview(null)
        setSelectedTables({})
        addNotification({
          type: 'success',
          title: `${gate2Name} Submitted`,
          message: isSftpRun
            ? `Approved feeds were submitted for ${gate2Name}.`
            : 'Approved tables were submitted. Metadata discovery and profiling are resuming.',
          duration: 5000
        })
        returnToMonitor(selectedRunId)
      } catch (error) {
        addNotification({ type: 'error', title: `${gate2Name} Failed`, message: error.message, duration: 5000 })
      } finally {
        setSubmitting(false)
      }
      return
    }

    if (isGate3) {
      setSubmitting(true)
      try {
        await submitEnrichmentReview(selectedRunId, gate3Decision === 'APPROVED')
        const refreshed = gate3Decision === 'APPROVED'
          ? await waitForRunGate(selectedRunId, updateRun, 4)
          : await getRun(selectedRunId)
        updateRun(selectedRunId, refreshed)
        setEnrichmentReview(null)
        addNotification({
          type: 'success',
          title: `${gate3Name} Submitted`,
          message: gate3Decision === 'APPROVED' && Number(refreshed?.next_gate || 0) === 4
            ? `${gate3Name} approved. Bronze scripts are generated and ready for ${gate4Name}.`
            : 'Enrichment review was submitted. Pipeline is still processing.',
          duration: 5000
        })
        returnToMonitor(selectedRunId)
      } catch (error) {
        addNotification({ type: 'error', title: `${gate3Name} Failed`, message: error.message, duration: 5000 })
      } finally {
        setSubmitting(false)
      }
      return
    }

    if (isGate4) {
      setSubmitting(true)
      try {
        await submitBronzeReview(selectedRunId, gateDecision)
        const refreshed = gateDecision === 'APPROVED'
          ? await waitForRunGate(selectedRunId, updateRun, 5)
          : await getRun(selectedRunId)
        updateRun(selectedRunId, refreshed)
        setBronzeReview(null)
        addNotification({
          type: 'success',
          title: `${gate4Name} Submitted`,
          message: Number(refreshed?.next_gate || 0) === 5
            ? `Bronze approved. Silver scripts are generated and ready for ${gate5Name}.`
            : 'Bronze review was submitted. Pipeline is still processing.',
          duration: 5000
        })
        returnToMonitor(selectedRunId)
      } catch (error) {
        addNotification({ type: 'error', title: `${gate4Name} Failed`, message: error.message, duration: 5000 })
      } finally {
        setSubmitting(false)
      }
      return
    }

    if (isGate5) {
      setSubmitting(true)
      try {
        await submitSilverReview(selectedRunId, gateDecision)
        const refreshed = await getRun(selectedRunId)
        updateRun(selectedRunId, refreshed)
        setSilverReview(null)
        addNotification({
          type: 'success',
          title: `${gate5Name} Submitted`,
          message: 'Silver review was submitted. Pipeline is resuming.',
          duration: 5000
        })
        returnToMonitor(selectedRunId)
      } catch (error) {
        addNotification({ type: 'error', title: `${gate5Name} Failed`, message: error.message, duration: 5000 })
      } finally {
        setSubmitting(false)
      }
      return
    }

    const missingDecisions = queue.filter((item) => !localDecisions[item.queue_id || item.id] && !item.decision)
    if (missingDecisions.length > 0) {
      addNotification({
        type: 'amber',
        title: 'Review Incomplete',
        message: `Decide all KPIs before submitting. ${missingDecisions.length} still pending.`,
        duration: 4000
      })
      return
    }

    setSubmitting(true)
    const hasQueueIds = queue.some((item) => item.queue_id)

    try {
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

      if (hasQueueIds) {
        await submitHitlDecisions(selectedRunId, decisions)
      }
      storeSubmitDecisions(selectedRunId, decisions)
      const refreshed = hasQueueIds ? await getRun(selectedRunId) : null
      updateRun(selectedRunId, refreshed || { status: 'RUNNING' })
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
      } else {
        addNotification({
          type: 'success',
          title: 'Decisions Saved',
          message: `${decisions.length} KPI decision${decisions.length !== 1 ? 's' : ''} saved. Pipeline resuming.`,
          duration: 5000
        })
      }
      returnToMonitor(selectedRunId)
    } catch (error) {
      addNotification({ type: 'error', title: 'Submit Failed', message: error.message, duration: 5000 })
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="flex flex-col h-full gap-4">
      <div className="rounded-[24px] border border-[#1d2940] bg-[#0d1729] shadow-[0_20px_80px_rgba(0,0,0,0.25)]">
        <div className="flex items-center justify-between gap-4 px-6 py-5">
          <div className="flex min-w-0 items-center gap-4">
            <img src={ATHENA_LOGO_SRC} alt="Athena" className="h-12 w-12 flex-shrink-0 object-contain" />
            <div className="min-w-0">
              <h1 className="text-[18px] font-bold text-white">
                {isGate5 ? gate5Name : isGate4 ? gate4Name : isGate3 ? gate3Name : isGate2 ? gate2Name : gate1Name}
              </h1>
              <p className="mt-0.5 text-sm text-[#95a3bf]">
                {isGate5
                  ? (silverReview?.resume_message || 'Review generated Silver scripts before the pipeline continues.')
                  : isGate4
                  ? (bronzeReview?.resume_message || 'Review generated Bronze artifacts before the pipeline continues.')
                  : isGate3
                  ? (enrichmentReview?.resume_message || 'Review semantic enrichment before the pipeline continues.')
                  : isGate2
                  ? (tableReview?.resume_message || (isSftpRun ? 'Review discovered feeds before the pipeline continues.' : 'Review nominated tables before the pipeline continues.'))
                  : 'Review and approve extracted KPIs before the pipeline continues.'}
              </p>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <button
              onClick={isGate3 ? () => setGate3Decision('APPROVED') : (isGate4 || isGate5) ? () => setGateDecision('APPROVED') : isGate2 ? (isSftpRun ? handleSelectAllFeeds : handleSelectAllTables) : handleAutoApproveAll}
              className="inline-flex items-center gap-2 rounded-xl border border-[#294766] bg-[#1b2a3f] px-4 py-2 text-sm font-semibold text-[#c6d2e8] transition-colors hover:bg-[#223550]"
            >
              <CheckCircle size={15} className="text-[#19c37d]" />
              {isGate2 ? 'Auto-Select Pending' : 'Auto-Approve Pending'}
            </button>

          {reviewRuns.length > 0 && (
            <select
              value={selectedRunId || ''}
              onChange={(event) => setSelectedRunId(event.target.value)}
              className="h-10 rounded-xl border border-[#253044] bg-[#0a1220] px-3 text-xs text-[#c6d2e8] outline-none"
            >
              {reviewRuns.map((run) => (
                <option key={run.id} value={run.id}>
                  {run.id.slice(0, 14)} - {run.brd_filename} (Gate {run.next_gate})
                </option>
              ))}
            </select>
          )}

          {!isGate2 && !isGate3 && !isGate4 && !isGate5 && (
            <select
              value={statusFilter}
              onChange={(event) => setStatusFilter(event.target.value)}
              className="h-10 rounded-xl border border-[#253044] bg-[#0a1220] px-3 text-xs text-[#c6d2e8] outline-none"
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
      </div>

      <div className="flex gap-4 flex-1 min-h-0">
        <div className="flex-1 overflow-y-auto pr-1 space-y-4 pb-20">
          {selectedRunId && isReviewableRun ? (
            isGate5 ? (
            <div className="space-y-4">
              <div className="rounded-[24px] border border-[#1d2940] bg-[#0d1729] p-5">
                <h3 className="text-base font-bold text-text-primary mb-3">Silver Review</h3>
                {silverReviewItems.length === 0 && (
                  <div className="rounded-2xl border border-[#22304b] bg-[#0b1424] p-4 text-sm text-text-secondary">
                    Silver scripts are not loaded yet. Submit is still available if {gate5Name} is pending.
                  </div>
                )}
                {((silverReview?.silver_review_artifact?.items) || []).map((item, index) => (
                  <div key={`${item.entity || index}`} className="mb-3 rounded-[20px] border border-[#22304b] bg-[#0f1a2e] p-4 space-y-3">
                    <div className="flex items-center justify-between gap-3">
                      <div className="text-sm font-semibold text-text-primary">{item.entity || 'Silver Item'}</div>
                      <span className="rounded-full border border-emerald-500/20 bg-emerald-500/10 px-2.5 py-1 text-[10px] font-semibold text-emerald-400">Ready</span>
                    </div>
                    <ReviewBlock label="Bronze Source" value={item.bronze_source || '-'} />
                    <ReviewBlock label="Transformations" value={(item.transformations || []).join('\n') || '-'} />
                    <ReviewBlock label="Type Casts" value={JSON.stringify(item.type_casts || [], null, 2)} />
                    <ReviewBlock label="Dedup Logic" value={item.dedup_logic || '-'} />
                    <ReviewBlock label="DQ Rules" value={(item.dq_rules || []).join('\n') || '-'} />
                    <ReviewBlock label="PII Masking Rules" value={(item.pii_masking_rules || []).join('\n') || '-'} />
                    <ReviewBlock label="Merge Strategy" value={item.merge_strategy || '-'} />
                    <ReviewBlock label="Generated Silver Script" value={item.generated_silver_script || '-'} />
                  </div>
                ))}
              </div>

              <GateDecisionCard gateDecision={gateDecision} setGateDecision={setGateDecision} approveLabel="Approve Silver" rejectLabel="Reject Silver" regenerateLabel="Regenerate Silver" />
              <button
                onClick={handleSubmit}
                disabled={hydrating || submitting || !selectedRunId}
                className="flex w-full items-center justify-center gap-2 rounded-lg bg-accent-blue px-5 py-3 text-sm font-bold text-white shadow-lg transition-colors hover:bg-blue-600 disabled:cursor-not-allowed disabled:opacity-50"
              >
                {submitting ? <Loader2 size={15} className="animate-spin" /> : <CheckCircle2 size={15} />}
                Submit {gate5Name} & Continue Pipeline
              </button>
            </div>
            ) : isGate4 ? (
            <div className="space-y-4">
              <div className="rounded-[24px] border border-[#1d2940] bg-[#0d1729] p-5">
                <h3 className="text-base font-bold text-text-primary mb-3">Bronze Review</h3>
                {bronzeReviewFeeds.length === 0 && (
                  <div className="rounded-2xl border border-[#22304b] bg-[#0b1424] p-4 text-sm text-text-secondary">
                    Bronze scripts are not loaded yet. Submit is still available if {gate4Name} is pending.
                  </div>
                )}
                {((bronzeReview?.bronze_review_artifact?.feeds) || []).map((feed, index) => (
                  <div key={`${feed.entity || index}`} className="mb-3 rounded-[20px] border border-[#22304b] bg-[#0f1a2e] p-4 space-y-3">
                    <div className="flex items-center justify-between gap-3">
                      <div className="text-sm font-semibold text-text-primary">{feed.vendor || 'Vendor'}.{feed.entity || 'Feed'}</div>
                      <span className="rounded-full border border-emerald-500/20 bg-emerald-500/10 px-2.5 py-1 text-[10px] font-semibold text-emerald-400">Ready</span>
                    </div>
                    <ReviewBlock label="Source Type" value={feed.source_type || '-'} />
                    <ReviewBlock label="File Format" value={feed.file_format || '-'} />
                    <ReviewBlock label="Primary Keys" value={(feed.primary_keys || []).join(', ') || '-'} />
                    <ReviewBlock label="Watermark Column" value={feed.watermark_column || '-'} />
                    <ReviewBlock label="Landing Path" value={feed.landing_path || '-'} />
                    <ReviewBlock label="Bronze Output Path" value={feed.bronze_output_path || '-'} />
                    <ReviewBlock label="Checkpoint Path" value={feed.checkpoint_path || '-'} />
                    <ReviewBlock label="Generated Bronze Script" value={feed.generated_bronze_script || '-'} />
                  </div>
                ))}
              </div>

              <GateDecisionCard gateDecision={gateDecision} setGateDecision={setGateDecision} approveLabel="Approve Bronze" rejectLabel="Reject Bronze" regenerateLabel="Regenerate Bronze" />
              <button
                onClick={handleSubmit}
                disabled={hydrating || submitting || !selectedRunId}
                className="flex w-full items-center justify-center gap-2 rounded-lg bg-accent-blue px-5 py-3 text-sm font-bold text-white shadow-lg transition-colors hover:bg-blue-600 disabled:cursor-not-allowed disabled:opacity-50"
              >
                {submitting ? <Loader2 size={15} className="animate-spin" /> : <CheckCircle2 size={15} />}
                Submit {gate4Name} & Generate Silver
              </button>
            </div>
            ) : isGate3 ? (
            <div className="space-y-4">
              <div className="rounded-[24px] border border-[#1d2940] bg-[#0d1729] p-5">
                <div className="flex items-center justify-between gap-3 mb-4">
                  <div>
                    <h3 className="text-base font-bold text-text-primary">{isSftpRun ? 'File Schema Enrichment Summary' : 'Enrichment Summary'}</h3>
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

              {isSftpRun && Array.isArray(enrichmentReview?.feed_semantic_summary) && enrichmentReview.feed_semantic_summary.length > 0 && (
                <div className="rounded-[24px] border border-[#1d2940] bg-[#0d1729] p-5">
                  <h3 className="text-base font-bold text-text-primary mb-3">Per Feed Breakdown</h3>
                  <div className="space-y-3">
                    {enrichmentReview.feed_semantic_summary.map((feed, index) => (
                      <FileSemanticFeedCard key={`${feed.feed_id || feed.entity || index}`} feed={feed} />
                    ))}
                  </div>
                </div>
              )}

              <div className="rounded-[24px] border border-[#1d2940] bg-[#0d1729] p-5">
                <h3 className="text-base font-bold text-text-primary mb-3">Semantic Types</h3>
                <div className="flex flex-wrap gap-2">
                  {Object.entries(enrichmentReview?.semantic_counts || {}).map(([key, value]) => (
                    <span key={key} className="px-3 py-1 rounded-full text-xs font-medium bg-bg-border text-text-secondary border border-bg-border">
                      {key}: {value}
                    </span>
                  ))}
                </div>
              </div>

              <div className="rounded-[24px] border border-[#1d2940] bg-[#0d1729] p-5">
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
                      <label key={key} className="rounded-[20px] border border-[#22304b] bg-[#0d1729] p-5 flex items-start gap-3 cursor-pointer hover:border-[#35507d] transition-colors">
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
                      <label key={key} className="rounded-[20px] border border-[#22304b] bg-[#0d1729] p-5 flex items-start gap-3 cursor-pointer hover:border-[#35507d] transition-colors">
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
              {queue.length === 0 ? `No KPIs in queue. Select a run with ${gate1Name} pending.` : 'No KPIs match the current filter.'}
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
                <p className="text-xs text-gray-500 mt-1">This page shows runs paused at KPI, table/feed, enrichment, bronze, and silver review.</p>
              </div>
            </div>
          )}
        </div>

        <div className="w-72 flex-shrink-0 flex flex-col gap-3">
          <div className="rounded-[24px] border border-[#1d2940] bg-[#0d1729] p-4">
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
                ) : isGate4 ? (
                <>
                  <CountRow label="Bronze Plans" value={bronzeReviewFeeds.length} color="text-gray-300" />
                  <CountRow label="Decision" value={gateDecision === 'APPROVED' ? 'Approve' : gateDecision === 'REJECTED' ? 'Reject' : 'Regenerate'} color={gateDecision === 'APPROVED' ? 'text-accent-green' : gateDecision === 'REJECTED' ? 'text-accent-red' : 'text-accent-blue'} pulse />
                </>
                ) : isGate5 ? (
                <>
                  <CountRow label="Silver Scripts" value={silverReviewItems.length} color="text-gray-300" />
                  <CountRow label="Decision" value={gateDecision === 'APPROVED' ? 'Approve' : gateDecision === 'REJECTED' ? 'Reject' : 'Regenerate'} color={gateDecision === 'APPROVED' ? 'text-accent-green' : gateDecision === 'REJECTED' ? 'text-accent-red' : 'text-accent-blue'} pulse />
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
                      : isGate4 || isGate5
                      ? (gateReviewReady ? 100 : 0)
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

          <div className="rounded-[24px] border border-[#1d2940] bg-[#0d1729] p-4">
            <div className="flex items-center gap-2 mb-2">
              <Timer size={14} className="text-accent-amber" />
              <span className="text-xs font-medium text-gray-300">Review State</span>
            </div>
            <p className="text-2xl font-mono font-bold text-accent-amber">{hydrating ? 'SYNC' : 'READY'}</p>
            <p className="text-[10px] text-gray-600 mt-1">Pipeline is paused</p>
          </div>

          <button
            onClick={isGate3 ? () => setGate3Decision('APPROVED') : (isGate4 || isGate5) ? () => setGateDecision('APPROVED') : isGate2 ? (isSftpRun ? handleSelectAllFeeds : handleSelectAllTables) : handleAutoApproveAll}
            className="flex items-center justify-center gap-2 px-4 py-3 bg-accent-green/10 hover:bg-accent-green/20 border border-accent-green/25 text-accent-green text-sm font-semibold rounded-xl transition-colors"
          >
            <CheckCircle size={15} />
            {isGate3 || isGate4 || isGate5 ? 'Set Approve' : isGate2 ? (isSftpRun ? 'Select All Feeds' : 'Select All Tables') : 'Auto-approve All'}
          </button>

          <div className="rounded-[20px] border border-[#22304b] bg-[#0d1729] p-3">
            <div className="flex items-start gap-2">
              <Shield size={12} className="text-gray-600 mt-0.5 flex-shrink-0" />
              <p className="text-[10px] text-gray-600 leading-relaxed">
                {isGate2
                  ? (isSftpRun
                    ? `${gate2Name} validates the discovered SFTP feeds. Review entity, source file, sample rows, columns, keys, and measures before approving the feed set.`
                    : 'Certified tables become the source set for metadata discovery, profiling, and enrichment.')
                  : isGate3
                  ? `Approving ${gate3Name} generates Bronze review artifacts. Rejecting keeps the run paused for rework.`
                  : isGate4
                  ? `Approving ${gate4Name} accepts the Bronze scripts and starts Silver script generation.`
                  : isGate5
                  ? `Approving ${gate5Name} accepts the Silver scripts and continues downstream validation.`
                  : 'Approvals are final once submitted. Rejected KPIs will be excluded from the final export.'}
              </p>
            </div>
          </div>
        </div>
      </div>

      {canSubmitReview && (
        <motion.div
          initial={{ y: 80, opacity: 0 }}
          animate={{ y: 0, opacity: 1 }}
          exit={{ y: 80, opacity: 0 }}
          className="fixed bottom-0 left-0 right-0 z-30 flex items-center justify-between px-6 py-4 bg-bg-card border-t border-bg-border shadow-2xl"
          style={{ background: 'rgba(17,24,39,0.95)', backdropFilter: 'blur(10px)' }}
        >
          <div className="flex items-center gap-4 text-sm">
            {isGate4 || isGate5 ? (
              <>
                <span className={gateDecision === 'APPROVED' ? 'text-accent-green font-semibold' : gateDecision === 'REJECTED' ? 'text-accent-red font-semibold' : 'text-accent-blue font-semibold'}>
                  {gateDecision === 'APPROVED' ? 'Approve selected' : gateDecision === 'REJECTED' ? 'Reject selected' : 'Regenerate selected'}
                </span>
                <span className="text-gray-500">{isGate4 ? `${bronzeReviewFeeds.length} Bronze plan(s)` : `${silverReviewItems.length} Silver script(s)`}</span>
              </>
            ) : isGate3 ? (
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
                {isGate3 || isGate2 || isGate4 || isGate5 ? <CheckCircle2 size={14} /> : <Send size={14} />}
                {isGate5 ? `Submit ${gate5Name} & Continue Pipeline ->` : isGate4 ? `Submit ${gate4Name} & Generate Silver ->` : isGate3 ? `Submit ${gate3Name} & Generate Bronze ->` : isGate2 ? `Submit ${gate2Name} & Resume Pipeline ->` : 'Submit All Decisions & Resume Pipeline ->'}
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
    id: row.queue_id || row.id,
    queue_id: row.queue_id || row.id,
    item_id: row.item_id,
    run_id: row.run_id,
    source: row.source ? normalizeSourceValue(row.source) : undefined,
    item_type: row.item_type || 'METADATA',
    name: row.name,
    definition: row.definition,
    category: row.category,
    domain: row.domain,
    confidence: row.confidence,
    status: row.status,
    grounded: row.grounded,
    explicit: row.explicit,
    kpi_detail: row.kpi_detail || {},
    modified_detail: row.modified_detail || null,
    gate_status: row.gate_status,
    decision: row.decision || (row.gate_status !== 'PENDING' ? row.gate_status : null),
    reviewer_id: row.reviewer_id,
    rejection_reason: row.rejection_reason,
    auto_approved: row.auto_approved === true || row.auto_approved === 'true',
    queued_at: row.queued_at,
    decided_at: row.decided_at,
    timeout_at: row.timeout_at
  }
}

function normalizeSourceValue(source) {
  return String(source || '').toLowerCase()
}

function getReviewItemRunId(item) {
  if (item?.run_id) return String(item.run_id)
  const key = String(item?.queue_id || item?.item_id || item?.id || '')
  return key.includes(':') ? key.split(':')[0] : ''
}

function filterReviewQueue(items, runId, source) {
  const expectedRunId = String(runId || '')
  const expectedSource = normalizeSourceValue(source)
  return (items || []).filter((item) => {
    const itemRunId = getReviewItemRunId(item)
    if (expectedRunId && itemRunId && itemRunId !== expectedRunId) return false
    const itemSource = normalizeSourceValue(item?.source)
    if (itemSource && expectedSource && itemSource !== expectedSource) return false
    return true
  })
}

function reviewPayloadMatchesRun(payload, runId, source) {
  if (!payload) return false
  if (payload.run_id && String(payload.run_id) !== String(runId || '')) return false
  if (payload.runId && String(payload.runId) !== String(runId || '')) return false
  const payloadSource = normalizeSourceValue(payload.source)
  const expectedSource = normalizeSourceValue(source)
  if (payloadSource && expectedSource && payloadSource !== expectedSource) return false
  return true
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
    <div className="rounded-2xl border border-[#22304b] bg-[#0b1424] px-3 py-3">
      <div className="text-xs text-[#7f8eab]">{label}</div>
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

function FileSemanticFeedCard({ feed }) {
  const semanticCounts = Object.entries(feed?.semantic_counts || {})
  return (
    <div className="rounded-[20px] border border-[#22304b] bg-[#0f1a2e] p-4">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div>
          <div className="text-sm font-semibold text-text-primary">
            {feed.vendor || 'Vendor'}.{feed.entity || feed.feed_id || 'feed'}
          </div>
          <div className="text-xs text-text-secondary mt-1">
            {feed.format || 'unknown'}{feed.file_name ? ` • ${feed.file_name}` : ''}
          </div>
        </div>
        <div className="text-xs text-text-secondary">
          {Number(feed.sample_row_count || 0)} sample rows
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-2 mt-3">
        <StatTile label="Columns" value={Number(feed.column_count || 0)} />
        <StatTile label="PII" value={Number(feed.pii_count || 0)} />
        <StatTile label="Join Keys" value={Number(feed.join_key_count || 0)} />
        <StatTile label="Measures" value={Number(feed.measure_count || 0)} />
      </div>

      {semanticCounts.length > 0 && (
        <div className="flex flex-wrap gap-2 mt-3">
          {semanticCounts.map(([key, value]) => (
            <span key={key} className="rounded-full border border-[#2c3f5f] bg-[#111b2d] px-2 py-1 text-[10px] font-medium text-text-secondary">
              {key}: {value}
            </span>
          ))}
        </div>
      )}
    </div>
  )
}

export default HitlQueue

function GateDecisionCard({ gateDecision, setGateDecision, approveLabel, rejectLabel, regenerateLabel }) {
  return (
    <div className="rounded-[24px] border border-[#1d2940] bg-[#0d1729] p-5">
      <h3 className="text-base font-bold text-text-primary mb-3">Decision</h3>
      <div className="grid grid-cols-3 gap-3">
        <button
          onClick={() => setGateDecision('APPROVED')}
          className={`px-4 py-3 rounded-lg border text-sm font-semibold transition-colors ${
            gateDecision === 'APPROVED'
              ? 'bg-accent-green/15 border-accent-green/30 text-accent-green'
              : 'border-bg-border text-text-secondary hover:border-gray-600'
          }`}
        >
          {approveLabel}
        </button>
        <button
          onClick={() => setGateDecision('REJECTED')}
          className={`px-4 py-3 rounded-lg border text-sm font-semibold transition-colors ${
            gateDecision === 'REJECTED'
              ? 'bg-accent-red/15 border-accent-red/30 text-accent-red'
              : 'border-bg-border text-text-secondary hover:border-gray-600'
          }`}
        >
          {rejectLabel}
        </button>
        <button
          onClick={() => setGateDecision('REGENERATE')}
          className={`px-4 py-3 rounded-lg border text-sm font-semibold transition-colors ${
            gateDecision === 'REGENERATE'
              ? 'bg-accent-blue/15 border-accent-blue/30 text-accent-blue'
              : 'border-bg-border text-text-secondary hover:border-gray-600'
          }`}
        >
          {regenerateLabel}
        </button>
      </div>
    </div>
  )
}

function ReviewBlock({ label, value }) {
  const text = value || '-'
  const isScript = /script/i.test(label) && text !== '-'

  const copyValue = async () => {
    await navigator.clipboard.writeText(text)
  }

  const downloadValue = () => {
    const blob = new Blob([text], { type: 'text/plain;charset=utf-8' })
    const url = window.URL.createObjectURL(blob)
    const anchor = document.createElement('a')
    const fileName = `${label || 'generated_script'}`.replace(/[^\w.-]+/g, '_').toLowerCase()
    anchor.href = url
    anchor.download = fileName.endsWith('.py') ? fileName : `${fileName}.py`
    document.body.appendChild(anchor)
    anchor.click()
    anchor.remove()
    window.URL.revokeObjectURL(url)
  }

  return (
    <div>
      <div className="mb-1 flex items-center justify-between gap-3">
        <div className="text-[11px] uppercase tracking-wider text-[#7f8eab]">{label}</div>
        {isScript && (
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={copyValue}
              className="inline-flex items-center gap-1 rounded-md border border-[#22304b] px-2 py-1 text-[10px] font-semibold text-[#aab8d0] hover:border-[#3f82ff] hover:text-white"
            >
              <Copy size={11} />
              Copy
            </button>
            <button
              type="button"
              onClick={downloadValue}
              className="inline-flex items-center gap-1 rounded-md border border-[#22304b] px-2 py-1 text-[10px] font-semibold text-[#aab8d0] hover:border-[#3f82ff] hover:text-white"
            >
              <Download size={11} />
              Download
            </button>
          </div>
        )}
      </div>
      <pre className="max-h-64 overflow-auto whitespace-pre-wrap break-words rounded-2xl border border-[#22304b] bg-[#09111f] p-3 text-xs text-text-secondary">
        {text}
      </pre>
    </div>
  )
}
