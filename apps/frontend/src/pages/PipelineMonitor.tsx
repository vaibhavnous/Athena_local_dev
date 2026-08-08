// @ts-nocheck
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import { AlertTriangle, CheckCircle2, ChevronDown, ChevronUp, Circle, Clock3, Code2, Copy, Download, FileText, Loader2, Play, RefreshCcw, RotateCcw, Square, X } from 'lucide-react'
import useAthenaStore from '../store/useAthenaStore'
import PipelineLogsPanel from '../components/pipeline/PipelineLogsPanel'
import { PageHeader } from '../components/shared/DashboardLayout'
import RunReportDialog from '../components/shared/RunReportDialog'
import { formatPipelineStepLabel, getGateDisplayName, getPhaseGroups, getPipelineSteps, isGenerationFirstDatabaseRun, isSnowflakeDbtRun, normalizeState, statusTone, summarizeRunSource } from '../utils/pipelinePhases'
import { isDemoFallbackRun } from '../utils/demoFallbacks'
import {
  abortRun,
  continueStage,
  fetchKpiReviews,
  getBronzeReview,
  getEnrichmentReviews,
  getGoldReview,
  getMetadataDdlReview,
  getRunStatus,
  getRunScripts,
  getSilverMergeKeyReview,
  getSilverReview,
  getTableReviews,
  restartRun,
  resumeFromFailure,
  retryFailedStage,
} from '../api/athenaApi'
import { isTransientReadError } from '../utils/apiErrors'
import { activeReviewKey, hasRenderableReviewData } from '../utils/reviewReadiness'

const ACTIVE_RUN_REFRESH_INTERVAL_MS = 5000
const ACTIVE_RUN_FAST_REFRESH_INTERVAL_MS = 1500
const REVIEW_READY_POLL_INTERVAL_MS = 1500
const HIDDEN_CODE_REVIEW_STEPS = new Set(['metadata_ddl_review', 'gate4', 'gate5', 'gold_review'])

async function fetchReviewPayload(runId, reviewKey) {
  if (reviewKey === 'metadata_ddl_review') return getMetadataDdlReview(runId)
  if (reviewKey === 'silver_merge_key_review') return getSilverMergeKeyReview(runId)
  if (reviewKey === 'gold_review') return getGoldReview(runId)
  if (reviewKey === 5) return getSilverReview(runId)
  if (reviewKey === 4) return getBronzeReview(runId)
  if (reviewKey === 3) return getEnrichmentReviews(runId)
  if (reviewKey === 2) return getTableReviews(runId)
  return fetchKpiReviews(runId)
}

function reviewPath(runId, reviewKey) {
  const encodedRunId = encodeURIComponent(runId)
  return typeof reviewKey === 'string'
    ? `/app/hitl?runId=${encodedRunId}&review=${encodeURIComponent(reviewKey)}`
    : `/app/hitl?runId=${encodedRunId}&gate=${reviewKey}`
}

export function reviewWaitPatchFromLogs(logs = []) {
  for (let index = logs.length - 1; index >= 0; index -= 1) {
    const log = logs[index] || {}
    const text = [log.stage, log.step_name, log.message].filter(Boolean).join(' ')
    const status = /\bstatus[=:]\s*([A-Z_]+)/i.exec(text)?.[1]?.toUpperCase()
    if (status && ['RUNNING', 'PROCESSING', 'COMPLETED', 'SUCCESS', 'FAILED', 'ABORTED', 'CANCELLED'].includes(status)) {
      return null
    }
    if (status !== 'HITL_WAIT' && !/\bHITL_WAIT\b/i.test(text)) continue

    if (/\bmetadata_ddl_review\b/i.test(text)) {
      return {
        status: 'HITL_WAIT',
        next_gate: 0,
        next_review_key: 'metadata_ddl_review',
        background_stage: 'metadata_ddl_review',
        resume_message: 'Metadata DDL Review is loading.',
      }
    }
    if (/\bsilver_merge_key_review\b/i.test(text)) {
      return {
        status: 'HITL_WAIT',
        next_gate: 0,
        next_review_key: 'silver_merge_key_review',
        background_stage: 'silver_merge_key_review',
        resume_message: 'Silver Merge Key Review is loading.',
      }
    }
    if (/\bgold_review\b/i.test(text)) {
      return {
        status: 'HITL_WAIT',
        next_gate: 0,
        next_review_key: 'gold_review',
        background_stage: 'gold_review',
        resume_message: 'Gold Code Review is loading.',
      }
    }

    const gate = Number(/\bgate([1-5])\b/i.exec(text)?.[1] || 0)
    if (gate) {
      return {
        status: 'HITL_WAIT',
        next_gate: gate,
        next_review_key: null,
        background_stage: `gate${gate}`,
        resume_message: `${getGateDisplayName(gate, '')} is loading.`,
      }
    }
  }
  return null
}

export function pipelineActivityFromLogs(logs = []) {
  for (let index = logs.length - 1; index >= 0; index -= 1) {
    const log = logs[index] || {}
    const stage = String(log.stage || log.step_name || '').toLowerCase()
    if (stage.endsWith('_router') || stage === 'runs_router') continue
    const text = [log.stage, log.step_name, log.message].filter(Boolean).join(' ').toUpperCase()
    if (/\b(PIPELINE_COMPLETED|COMPLETED PIPELINE|STATUS[=:]\s*(FAILED|ABORTED|CANCELLED|CANCELED))\b/.test(text)) {
      return false
    }
    return true
  }
  return null
}

function shouldUseStatusRefresh(run) {
  const status = normalizeState(run?.status)
  const stageKey = String(run?.external_execution?.stage_key || run?.background_stage || '').trim()
  const externalState = normalizeState(run?.external_execution?.status)
  return (
    ['RUNNING', 'PROCESSING', 'SUBMITTED'].includes(status) ||
    (stageKey && ['RUNNING', 'PROCESSING', 'SUBMITTED'].includes(externalState))
  )
}

function activeRunRefreshDelay(run) {
  return shouldUseStatusRefresh(run) ? ACTIVE_RUN_FAST_REFRESH_INTERVAL_MS : ACTIVE_RUN_REFRESH_INTERVAL_MS
}

function furthestActivePhase(phases = []) {
  for (let index = phases.length - 1; index >= 0; index -= 1) {
    const phase = phases[index]
    if (phase?.steps?.some((step) => ['RUNNING', 'HITL_WAIT'].includes(normalizeState(step.state)))) {
      return { phase, index }
    }
  }
  return null
}

function PipelineMonitor() {
  const navigate = useNavigate()
  const location = useLocation()
  const { runs, activeRunId, setActiveRun, updateRun, setServerOnline, addNotification, addRun } = useAthenaStore()
  const pendingRun = location.state?.pendingRun || null
  const routedActiveRunId = location.state?.activeRunId || null
  const storeActiveRun = activeRunId ? runs.find((run) => run.id === activeRunId) || null : null
  const pendingStartedAt = pendingRun?.startedAt ? Date.parse(pendingRun.startedAt) : 0
  const activeStartedAt = storeActiveRun?.started_at ? Date.parse(storeActiveRun.started_at) : 0
  const suppressStaleActiveRun = Boolean(
    pendingRun &&
      storeActiveRun &&
      pendingStartedAt &&
      (!activeStartedAt || activeStartedAt < pendingStartedAt)
  )
  const activeRun = suppressStaleActiveRun ? null : storeActiveRun
  const activeRunStableId = activeRun?.id || null
  const activeRunIsDemoFallback = isDemoFallbackRun(activeRun)
  const activeRunDemoScriptBundles = useMemo(
    () =>
      activeRun
        ? {
            bronze: activeRun.bronze || null,
            silver: activeRun.silver || null,
            gold: activeRun.gold || null,
          }
        : null,
    [activeRun]
  )
  const activeRunRequestInFlightRef = useRef(false)
  const reviewAutoOpenSessionRef = useRef(new Set())
  const lastLogTriggeredRefreshRef = useRef(0)
  const latestActiveRunRef = useRef(activeRun)
  const [preparingReviewKey, setPreparingReviewKey] = useState(null)
  const [observedPipelineActivity, setObservedPipelineActivity] = useState(false)
  const actualSteps = useMemo(() => getPipelineSteps(activeRun), [activeRun])
  const actualPhases = useMemo(() => getPhaseGroups(activeRun, actualSteps), [activeRun, actualSteps])
  const pendingReviewKey = activeReviewKey(activeRun)
  const pendingReviewStatus = normalizeState(activeRun?.status)
  const pendingReviewWaiting = ['HITL_WAIT', 'PAUSED_FOR_HITL', 'PENDING_REVIEW'].includes(pendingReviewStatus)
  const pendingReviewIsFileSource = ['sftp', 'adls_gen2'].includes(String(activeRun?.source || '').toLowerCase())

  useEffect(() => {
    latestActiveRunRef.current = activeRun
  }, [activeRun])

  useEffect(() => {
    setObservedPipelineActivity(false)
  }, [activeRunStableId])

  useEffect(() => {
    if (!pendingRun || !activeRun?.id || suppressStaleActiveRun) return
    navigate(location.pathname, { replace: true, state: null })
  }, [activeRun?.id, location.pathname, navigate, pendingRun, suppressStaleActiveRun])

  useEffect(() => {
    if (!routedActiveRunId) return
    if (routedActiveRunId !== activeRunId) setActiveRun(routedActiveRunId)
    navigate(location.pathname, { replace: true, state: null })
  }, [activeRunId, location.pathname, navigate, routedActiveRunId, setActiveRun])

  const refreshActiveRunNow = useCallback(async () => {
    if (!activeRunStableId || activeRunIsDemoFallback || activeRunRequestInFlightRef.current) return false

    activeRunRequestInFlightRef.current = true
    try {
      const data = (await getRunStatus(activeRunStableId))?.run
      if (!data) throw new Error('Run status response did not include a run snapshot.')
      updateRun(activeRunStableId, data)
      setServerOnline(true)
      return true
    } catch (error) {
      if (!isTransientReadError(error)) {
        setServerOnline(false)
        console.warn('[PipelineMonitor] Failed to refresh active run', error)
      } else {
        console.debug('[PipelineMonitor] Active run refresh timed out; keeping existing data')
      }
      return false
    } finally {
      activeRunRequestInFlightRef.current = false
    }
  }, [activeRunStableId, activeRunIsDemoFallback, updateRun, setServerOnline])

  const handleLogsUpdated = useCallback((newLogs = []) => {
    const activity = pipelineActivityFromLogs(newLogs)
    if (activity !== null) setObservedPipelineActivity(activity)
    const reviewPatch = reviewWaitPatchFromLogs(newLogs)
    if (reviewPatch && activeRunStableId) {
      updateRun(activeRunStableId, reviewPatch)
    }
    const now = Date.now()
    if (now - lastLogTriggeredRefreshRef.current < 2000) return
    lastLogTriggeredRefreshRef.current = now
    void refreshActiveRunNow()
  }, [activeRunStableId, refreshActiveRunNow, updateRun])

  useEffect(() => {
    if (!activeRunStableId || activeRunIsDemoFallback) return
    let cancelled = false
    let timer: number | null = null

    const scheduleNext = (delay = activeRunRefreshDelay(latestActiveRunRef.current)) => {
      if (!cancelled) {
        timer = window.setTimeout(refreshActiveRun, delay)
      }
    }

    const refreshActiveRun = async () => {
      try {
        await refreshActiveRunNow()
      } finally {
        scheduleNext()
      }
    }

    refreshActiveRun()
    return () => {
      cancelled = true
      if (timer !== null) window.clearTimeout(timer)
    }
  }, [activeRunStableId, activeRunIsDemoFallback, refreshActiveRunNow])

  useEffect(() => {
    if (!activeRunStableId || activeRunIsDemoFallback) return
    if (!pendingReviewKey || !pendingReviewWaiting) {
      setPreparingReviewKey(null)
      // Keep the session marker so a stale HITL snapshot cannot reopen a review
      // that the user has already seen. Reviews remain available from the run card.
      return
    }

    const sessionKey = `${activeRunStableId}:${pendingReviewKey}`
    if (reviewAutoOpenSessionRef.current.has(sessionKey)) return
    setPreparingReviewKey(pendingReviewKey)

    let cancelled = false
    let timer: number | null = null

    const scheduleNext = () => {
      if (!cancelled) timer = window.setTimeout(checkReviewReady, REVIEW_READY_POLL_INTERVAL_MS)
    }

    const openPreparedReview = (payload) => {
      if (cancelled) return
      updateRun(activeRunStableId, payload)
      setPreparingReviewKey(null)
      reviewAutoOpenSessionRef.current.add(sessionKey)
      setActiveRun(activeRunStableId)
      navigate(reviewPath(activeRunStableId, pendingReviewKey), {
        state: { backgroundLocation: location },
      })
    }

    const checkReviewReady = async () => {
      try {
        const currentRun = latestActiveRunRef.current
        if (hasRenderableReviewData(currentRun, pendingReviewKey, pendingReviewIsFileSource)) {
          openPreparedReview(currentRun)
          return
        }

        const payload = await fetchReviewPayload(activeRunStableId, pendingReviewKey)
        if (hasRenderableReviewData(payload, pendingReviewKey, pendingReviewIsFileSource)) {
          openPreparedReview(payload)
          return
        }
      } catch (error) {
        if (!isTransientReadError(error)) {
          console.debug('[PipelineMonitor] Review artifacts are not ready yet', error)
        }
      }
      scheduleNext()
    }

    checkReviewReady()
    return () => {
      cancelled = true
      if (timer !== null) window.clearTimeout(timer)
    }
  }, [
    activeRunIsDemoFallback,
    activeRunStableId,
    location,
    navigate,
    pendingReviewIsFileSource,
    pendingReviewKey,
    pendingReviewWaiting,
    setActiveRun,
    updateRun,
  ])

  const renderedPhases = useMemo(
    () => markNextPendingStage(
      actualPhases
        .map((phase) => buildPipelineDisplayPhase(phase, actualSteps, activeRun))
        .map((phase) => markPreparingReview(phase, preparingReviewKey)),
      activeRun,
      observedPipelineActivity,
    ),
    [activeRun, actualPhases, actualSteps, observedPipelineActivity, preparingReviewKey]
  )

  const defaultExpandedPhase = useMemo(() => {
    if (!renderedPhases?.length) return 'phase-1'
    const active = furthestActivePhase(renderedPhases)
    if (active?.phase) return active.phase.id
    const firstIncomplete = renderedPhases.find((phase) => phase.completed < phase.total)
    return firstIncomplete?.id || renderedPhases[renderedPhases.length - 1].id
  }, [renderedPhases])

  const [expandedPhase, setExpandedPhase] = useState(defaultExpandedPhase)
  const autoExpandedPhaseRef = useRef(defaultExpandedPhase)
  const previousRunIdRef = useRef<string | null>(null)

  useEffect(() => {
    if (!activeRun?.id || !defaultExpandedPhase) return

    const runChanged = previousRunIdRef.current !== activeRun.id
    if (!runChanged && autoExpandedPhaseRef.current === defaultExpandedPhase) return

    previousRunIdRef.current = activeRun.id
    autoExpandedPhaseRef.current = defaultExpandedPhase
    setExpandedPhase(defaultExpandedPhase)
  }, [defaultExpandedPhase, activeRun?.id])

  const monitorRun = activeRun
  const runLabel = summarizeRunSource(monitorRun)
  const isFailedRun = String(monitorRun?.status || '').toUpperCase() === 'FAILED'
  const isStageConfirmationPaused =
    String(monitorRun?.status || '').toUpperCase() === 'PAUSED_FOR_STAGE_CONFIRMATION' ||
    Boolean(monitorRun?.stage_confirmation?.awaiting_confirmation)
  const [dismissedFailureBannerFor, setDismissedFailureBannerFor] = useState<string | null>(null)
  const [autoAdvanceStages, setAutoAdvanceStages] = useState(false)
  const [stageConfirmSubmitting, setStageConfirmSubmitting] = useState(false)
  const [failureActionSubmitting, setFailureActionSubmitting] = useState('')
  const [rerunningStepKey, setRerunningStepKey] = useState('')
  const [scriptBundles, setScriptBundles] = useState(null)
  const [reportOpen, setReportOpen] = useState(false)
  const shownReportRef = useRef(new Set())

  useEffect(() => {
    if (!activeRunStableId) {
      setScriptBundles(null)
      return
    }

    if (activeRunIsDemoFallback) {
      setScriptBundles(activeRunDemoScriptBundles)
      return
    }

    let cancelled = false
    const loadScripts = async () => {
      try {
        const payload = await getRunScripts(activeRunStableId)
        if (cancelled) return
        setScriptBundles(payload)
        updateRun(activeRunStableId, {
          bronze: payload?.bronze,
          silver: payload?.silver,
          gold: payload?.gold,
        })
      } catch (error) {
        if (!cancelled && !isTransientReadError(error)) {
          console.warn('[PipelineMonitor] Failed to load run scripts', error)
        }
      }
    }

    loadScripts()
    const timer = window.setInterval(loadScripts, 10000)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [activeRunStableId, activeRunIsDemoFallback, activeRunDemoScriptBundles, updateRun])

  const monitorRunWithScripts = useMemo(() => {
    if (!monitorRun || !scriptBundles) return monitorRun
    return {
      ...monitorRun,
      bronze: scriptBundles.bronze || monitorRun.bronze,
      silver: scriptBundles.silver || monitorRun.silver,
      gold: scriptBundles.gold || monitorRun.gold,
    }
  }, [monitorRun, scriptBundles])

  useEffect(() => {
    if (!isFailedRun) {
      setDismissedFailureBannerFor(null)
    } else if (dismissedFailureBannerFor && dismissedFailureBannerFor !== activeRun?.id) {
      setDismissedFailureBannerFor(null)
    }
  }, [activeRun?.id, dismissedFailureBannerFor, isFailedRun])

  const failureSummary = useMemo(() => buildFailureSummary(monitorRun), [monitorRun])
  const stageConfirmation = monitorRun?.stage_confirmation || null
  const stageScriptReview = useMemo(() => buildStageScriptReview(monitorRunWithScripts), [monitorRunWithScripts])

  useEffect(() => {
    const report = monitorRun?.run_report
    const reportKey = `${monitorRun?.id || ''}:${report?.generated_at || ''}`
    const storageKey = `athena:run-report:${reportKey}`
    let alreadyShown = shownReportRef.current.has(reportKey)
    try {
      alreadyShown ||= window.sessionStorage.getItem(storageKey) === 'shown'
    } catch {
      // Session storage can be disabled; the in-memory guard still prevents duplicates.
    }
    if (
      report?.generated_at &&
      normalizeState(monitorRun?.report_generation_status) === 'COMPLETED' &&
      !alreadyShown
    ) {
      shownReportRef.current.add(reportKey)
      try {
        window.sessionStorage.setItem(storageKey, 'shown')
      } catch {
        // The report remains available when browser storage is restricted.
      }
      setReportOpen(true)
    }
  }, [monitorRun?.id, monitorRun?.report_generation_status, monitorRun?.run_report])

  if (!activeRun) {
    const title = pendingRun ? 'Starting pipeline run' : 'No active pipeline'
    const message = pendingRun
      ? `Waiting for backend to create ${pendingRun.label || 'the new run'}.`
      : 'Start a new run from the top-right action.'
    return (
      <div className="flex min-h-[620px] items-center justify-center rounded-lg border border-[#253044] bg-[#111827]">
        <div className="text-center">
          <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-lg border border-[#253044] bg-[#0b1120] text-slate-500">
            <Play size={24} />
          </div>
          <div className="mt-5 text-xl font-semibold text-white">{title}</div>
          <p className="mt-2 text-sm text-slate-400">{message}</p>
        </div>
      </div>
    )
  }

  const handleRetryFailedStage = async () => {
    if (!activeRun?.id) return
    setFailureActionSubmitting('retry')
    try {
      await retryFailedStage(activeRun.id)
      const refreshed = (await getRunStatus(activeRun.id))?.run
      updateRun(activeRun.id, refreshed)
      addNotification({
        type: 'success',
        title: 'Failed stage retried',
        message: `Retry submitted for ${failureSummary.failedStage}.`,
        duration: 3500,
      })
    } catch (error) {
      addNotification({
        type: 'error',
        title: 'Retry failed stage failed',
        message: error.message || 'Unable to retry the failed stage.',
        duration: 4500,
      })
    } finally {
      setFailureActionSubmitting('')
    }
  }

  const handleResumeFromFailure = async () => {
    if (!activeRun?.id) return
    setFailureActionSubmitting('resume')
    try {
      await resumeFromFailure(activeRun.id)
      const refreshed = (await getRunStatus(activeRun.id))?.run
      updateRun(activeRun.id, refreshed)
      addNotification({
        type: 'success',
        title: 'Failure resume submitted',
        message: 'The pipeline is resuming from its saved failure state.',
        duration: 3500,
      })
    } catch (error) {
      addNotification({
        type: 'error',
        title: 'Resume from failure failed',
        message: error.message || 'Unable to resume the failed run.',
        duration: 4500,
      })
    } finally {
      setFailureActionSubmitting('')
    }
  }

  const handleRestartFailedRun = async () => {
    if (!activeRun?.id) return
    setFailureActionSubmitting('restart')
    try {
      const restarted = await restartRun(activeRun.id)
      const nextRun = (await getRunStatus(restarted.run_id))?.run
      addRun(nextRun)
      setActiveRun(nextRun.id)
      addNotification({
        type: 'success',
        title: 'Run restarted',
        message: `Started a new run from ${activeRun.brd_filename || activeRun.id}.`,
        duration: 3500,
      })
    } catch (error) {
      addNotification({
        type: 'error',
        title: 'Restart failed',
        message: error.message || 'Unable to restart the failed run.',
        duration: 4500,
      })
    } finally {
      setFailureActionSubmitting('')
    }
  }

  const handleContinueStage = async () => {
    if (!activeRun?.id) return
    setStageConfirmSubmitting(true)
    try {
      const continuation = await continueStage(activeRun.id, autoAdvanceStages)
      const nextStageKey = String(continuation?.next_stage_key || stageConfirmation?.next_stage_key || '').trim()
      updateRun(activeRun.id, {
        id: activeRun.id,
        status: 'RUNNING',
        background_stage: nextStageKey || undefined,
        next_gate: 0,
        next_review_key: null,
        stage_confirmation: null,
        resume_message: continuation?.resume_message || `${stageConfirmation?.next_stage_label || 'The next stage'} is starting.`,
      })
      const refreshed = (await getRunStatus(activeRun.id))?.run
      updateRun(activeRun.id, refreshed)
      addNotification({
        type: 'success',
        title: 'Stage continued',
        message: autoAdvanceStages
          ? 'Auto-advance is enabled for the remaining stages in this run.'
          : `Continuing to ${stageConfirmation?.next_stage_label || 'the next stage'}.`,
        duration: 3500,
      })
    } catch (error) {
      addNotification({
        type: 'error',
        title: 'Unable to continue stage',
        message: error.message || 'The backend could not continue this run.',
        duration: 4500,
      })
    } finally {
      setStageConfirmSubmitting(false)
    }
  }

  const handleCancelRun = async () => {
    if (!activeRun?.id) return
    setStageConfirmSubmitting(true)
    try {
      await abortRun(activeRun.id)
      updateRun(activeRun.id, { status: 'ABORTED', completed_at: new Date().toISOString() })
      addNotification({ type: 'amber', title: 'Run Aborted', message: 'The run was cancelled before the next stage.', duration: 3500 })
    } catch (error) {
      addNotification({ type: 'error', title: 'Abort failed', message: error.message || 'Unable to cancel the run.', duration: 4500 })
    } finally {
      setStageConfirmSubmitting(false)
    }
  }

  const handleOpenGateReview = (step = null) => {
    if (!activeRun?.id) return
    setActiveRun(activeRun.id)
    const modalNavigation = { state: { backgroundLocation: location } }
    if (step?.key === 'silver_merge_key_review') {
      navigate(`/app/hitl?runId=${encodeURIComponent(activeRun.id)}&review=silver_merge_key_review`, modalNavigation)
      return
    }
    const stepGate = /^gate([1-5])$/.exec(String(step?.key || ''))?.[1]
    if (!stepGate && activeRun.next_review_key) {
      navigate(`/app/hitl?runId=${encodeURIComponent(activeRun.id)}&review=${encodeURIComponent(activeRun.next_review_key)}`, modalNavigation)
      return
    }
    const gate = Number(stepGate || activeRun.next_gate || 0)
    navigate(gate ? `/app/hitl?runId=${encodeURIComponent(activeRun.id)}&gate=${gate}` : '/app/hitl', modalNavigation)
  }

  const handleRerunStep = async (step) => {
    if (!activeRun?.id || rerunningStepKey) return
    setRerunningStepKey(step.key)
    try {
      const restarted = await restartRun(activeRun.id)
      const nextRun = (await getRunStatus(restarted.run_id))?.run
      addRun(nextRun)
      setActiveRun(nextRun.id)
      addNotification({
        type: 'success',
        title: 'Re-run started',
        message: `A new run was started after selecting ${step.label}.`,
        duration: 3500,
      })
    } catch (error) {
      addNotification({
        type: 'error',
        title: 'Re-run failed',
        message: error.message || `Unable to re-run ${step.label}.`,
        duration: 4500,
      })
    } finally {
      setRerunningStepKey('')
    }
  }

  const handleCopyScript = async (script) => {
    try {
      await navigator.clipboard.writeText(formatScriptBody(script))
      addNotification({
        type: 'success',
        title: 'Script copied',
        message: `${script.title} was copied to the clipboard.`,
        duration: 3000,
      })
    } catch (error) {
      addNotification({
        type: 'error',
        title: 'Copy failed',
        message: error?.message || 'Unable to copy the script.',
        duration: 4000,
      })
    }
  }

  const handleDownloadScript = (script) => {
    try {
      const body = formatScriptBody(script)
      const blob = new Blob([body], { type: 'text/plain;charset=utf-8' })
      const url = window.URL.createObjectURL(blob)
      const anchor = document.createElement('a')
      const fallbackName = `${script.layer}_${script.title || 'script'}`.replace(/[^\w.-]+/g, '_')
      const fileName = script.script_path?.split(/[\\/]/).pop() || `${fallbackName}.py`
      anchor.href = url
      anchor.download = fileName
      document.body.appendChild(anchor)
      anchor.click()
      anchor.remove()
      window.URL.revokeObjectURL(url)
    } catch (error) {
      addNotification({
        type: 'error',
        title: 'Download failed',
        message: error?.message || 'Unable to download the script.',
        duration: 4000,
      })
    }
  }

  const handleOpenLineage = (preferredLayer = '') => {
    if (!activeRun?.id) return
    const params = new URLSearchParams({ runId: String(activeRun.id) })
    if (preferredLayer) params.set('layer', preferredLayer)
    navigate(`/app/data-migration?${params.toString()}`)
  }

  return (
    <div className="flex min-h-full flex-col gap-3 md:h-full md:min-h-0">
      <PageHeader
        eyebrow="Data Discovery"
        title="Live pipeline monitor."
        description={<span>BRD: <strong className="font-semibold text-text-secondary">{monitorRun.brd_filename || runLabel}</strong>{' '}Run ID: <strong className="font-semibold text-text-secondary">{monitorRun.id}</strong></span>}
        actions={
          <div className="flex items-center gap-2">
            <StatusPill status={monitorRun.status} tone={statusTone(monitorRun.status)} />
            <button
              type="button"
              onClick={handleCancelRun}
              disabled={stageConfirmSubmitting || ['COMPLETED', 'FAILED', 'ABORTED', 'CANCELLED', 'CANCELED'].includes(normalizeState(monitorRun.status))}
              className="inline-flex h-9 items-center justify-center gap-2 rounded-lg border border-red-500/40 bg-red-500/10 px-3 text-xs font-semibold text-red-400 transition-colors hover:bg-red-500/20 disabled:cursor-not-allowed disabled:opacity-40"
            >
              <Square size={12} />
              {stageConfirmSubmitting ? 'Cancelling...' : 'Cancel Run'}
            </button>
          </div>
        }
        compact
      />

        {isFailedRun && dismissedFailureBannerFor !== monitorRun.id && (
          <div className="rounded-lg border border-red-500/35 bg-[#17111d] px-4 py-3 shadow-[0_8px_24px_rgba(0,0,0,0.18)]">
            <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
            <div className="flex min-w-0 items-start gap-3">
              <div className="flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-lg border border-red-500/30 bg-red-500/10 text-red-400">
                <AlertTriangle size={15} />
              </div>
              <div className="min-w-0">
                <div className="flex flex-wrap items-center gap-2 text-xs">
                  <div className="flex min-w-0 items-center gap-2 font-semibold text-white">
                    <FileText size={13} className="text-[#b8c3d9]" />
                    <span className="max-w-[420px] truncate">{monitorRun.brd_filename || 'BRD File Name'}</span>
                  </div>
                  <span className="rounded-md border border-red-500/35 bg-red-500/12 px-2 py-0.5 text-[10px] font-semibold text-red-400">
                    Failed
                  </span>
                  <span className="text-[#d4d9e5]">at `{failureSummary.failedStage}`</span>
                  <span className="text-[#9da7bb]">{failureSummary.progressLabel}</span>
                </div>
                {monitorRun?.error && (
                  <div className="mt-1 max-w-[920px] truncate text-xs text-red-300/90">
                    {monitorRun.error}
                  </div>
                )}
                <div className="mt-1 flex items-center gap-1.5 text-xs text-[#9da7bb]">
                  <Clock3 size={12} />
                  {failureSummary.timeAgo}
                </div>
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-2 xl:justify-end">
              <button
                onClick={handleRetryFailedStage}
                disabled={failureActionSubmitting !== ''}
                className="inline-flex h-9 items-center gap-1.5 rounded-lg border border-amber-500/35 bg-amber-500/10 px-3 text-xs font-semibold text-amber-400 transition-colors hover:bg-amber-500/15 disabled:cursor-not-allowed disabled:opacity-50"
              >
                <RotateCcw size={13} />
                {failureActionSubmitting === 'retry' ? 'Retrying...' : 'Retry Failed Stage'}
              </button>
              <button
                onClick={handleResumeFromFailure}
                disabled={failureActionSubmitting !== ''}
                className="inline-flex h-9 items-center gap-1.5 rounded-lg border border-[#3f82ff]/40 bg-[#3f82ff]/10 px-3 text-xs font-semibold text-[#3f82ff] transition-colors hover:bg-[#3f82ff]/15 disabled:cursor-not-allowed disabled:opacity-50"
              >
                <Play size={13} />
                {failureActionSubmitting === 'resume' ? 'Resuming...' : 'Resume from Failure'}
              </button>
              <button
                onClick={handleRestartFailedRun}
                disabled={failureActionSubmitting !== ''}
                className="inline-flex h-9 items-center gap-1.5 rounded-lg border border-[#2e394d] bg-[#101827] px-3 text-xs font-semibold text-white transition-colors hover:bg-[#152033] disabled:cursor-not-allowed disabled:opacity-50"
              >
                <RefreshCcw size={13} />
                {failureActionSubmitting === 'restart' ? 'Restarting...' : 'Restart'}
              </button>
              <button
                onClick={() => setDismissedFailureBannerFor(monitorRun.id)}
                className="flex h-9 w-9 items-center justify-center rounded-lg border border-[#2e394d] bg-transparent text-[#8d96a9] transition-colors hover:bg-white/5 hover:text-white"
                aria-label="Dismiss failure banner"
              >
                <X size={14} />
              </button>
            </div>
            </div>
          </div>
        )}

      <div className="flex flex-col gap-4 md:min-h-0 md:flex-1 md:flex-row">
        <section className="flex min-h-[360px] flex-col overflow-hidden rounded-lg border border-[#253044] bg-[#080e1d] md:min-h-0 md:w-1/3 md:flex-shrink-0">
          <div className="flex shrink-0 items-center justify-between border-b border-[#253044] bg-gradient-to-r from-[#111827] to-[#111827]/50 px-4 py-3">
            <div>
              <h4 className="text-sm font-semibold text-gray-100">Pipeline Stages</h4>
              <p className="mt-1 text-xs text-gray-500">Live stage progress and review gates</p>
            </div>
            <span className="rounded-md border border-[#253044] bg-[#0b1120] px-2 py-1 text-[10px] font-medium text-[#8a9ab7]">
              {renderedPhases.length} phases
            </span>
          </div>
          <div className="min-h-0 flex-1 divide-y divide-[#253044] overflow-y-auto">
            {renderedPhases.map((phase, index) => {
              const expanded = expandedPhase === phase.id
              const tone = statusTone(phase.status)
              return (
                <div key={phase.id}>
                  <button
                    onClick={() => setExpandedPhase(expanded ? '' : phase.id)}
                    className={`flex w-full items-center gap-3 px-3 py-2.5 text-left transition-colors ${
                      expanded ? 'bg-[#101735]' : 'bg-[#080e1d] hover:bg-[#0f1728]'
                    }`}
                  >
                    <div className="flex min-w-0 flex-1 items-center gap-3">
                      <PhaseNumber index={index + 1} tone={tone} />
                      <div className="min-w-0">
                        <div className={`text-xs font-semibold leading-tight ${expanded || tone !== 'slate' ? 'text-white' : 'text-[#7d8daa]'}`}>
                          {phase.label}
                        </div>
                        {tone !== 'slate' && (
                          <div className="mt-0.5 text-[10px] text-[#8a9ab7]">
                            {phase.completed}/{phase.total} stages complete
                          </div>
                        )}
                      </div>
                    </div>
                    <div className="ml-2 flex flex-shrink-0 items-center gap-2">
                      <StatusPill status={phase.status} tone={tone} compact={!expanded} step={phase.steps.find((step) => ['RUNNING', 'HITL_WAIT'].includes(normalizeState(step.state)))} />
                      {tone !== 'slate' && (expanded ? <ChevronUp size={13} className="text-[#64748b]" /> : <ChevronDown size={13} className="text-[#64748b]" />)}
                    </div>
                  </button>

                  <AnimatePresence initial={false} mode="sync">
                    {expanded && (
                      <motion.div
                        key={`${phase.id}-content`}
                        initial={{ height: 0, opacity: 0 }}
                        animate={{ height: 'auto', opacity: 1 }}
                        exit={{ height: 0, opacity: 0 }}
                        transition={{ duration: 0.28, ease: 'easeOut' }}
                        className="overflow-hidden bg-[#080e1d]"
                      >
                        <div className="mb-1 ml-5 mt-1 px-4 pb-1 pt-2">
                          <div>
                            <div>
                              {phase.steps.map((step, stepIndex) => (
                                <StepRow
                                  key={step.key}
                                  step={step}
                                  index={stepIndex}
                                  isLast={stepIndex === phase.steps.length - 1}
                                  onOpenReview={() => handleOpenGateReview(step)}
                                  onOpenReport={() => setReportOpen(true)}
                                  onRerun={() => handleRerunStep(step)}
                                  rerunning={rerunningStepKey === step.key}
                                />
                              ))}
                            </div>
                          </div>
                        </div>
                      </motion.div>
                    )}
                  </AnimatePresence>
                </div>
              )
            })}
          </div>
        </section>

        <section className="min-h-[460px] min-w-0 flex-1 md:min-h-0">
          <PipelineLogsPanel runId={activeRun.run_id || activeRun.id} isActive onLogsUpdated={handleLogsUpdated} />
        </section>
      </div>

      <RunReportDialog
        isOpen={reportOpen}
        onClose={() => setReportOpen(false)}
        report={monitorRun.run_report}
      />

      {/* ponytail: AppShell owns the compact stage gate; keep the richer script-review overlay dormant until it has a distinct trigger. */}
      {false && isStageConfirmationPaused && stageConfirmation?.awaiting_confirmation && (
        <div className="fixed inset-0 z-40 flex items-center justify-center bg-black/45 px-6 backdrop-blur-sm">
          <div className="max-h-[92vh] w-full max-w-[980px] overflow-hidden rounded-[26px] border border-[#24344d] bg-[#131d2f] shadow-[0_24px_80px_rgba(0,0,0,0.35)]">
            <div className="flex items-start gap-5 px-8 py-8">
              <div className="flex h-16 w-16 flex-shrink-0 items-center justify-center rounded-[20px] bg-emerald-500/12 text-emerald-400">
                <CheckCircle2 size={28} />
              </div>
              <div>
                <div className="text-[18px] font-semibold text-white">Stage Completed</div>
                <div className="mt-1 text-[15px] text-[#dbe2ef]">
                  {stageConfirmation.last_completed_stage_label || 'Current stage'} finished successfully.
                </div>
              </div>
            </div>

            <div className="max-h-[calc(92vh-132px)] overflow-y-auto border-t border-[#27374f] px-8 py-6">
              <div className="rounded-[20px] border border-[#29456d] bg-[#16233b] px-6 py-5">
                <div className="text-sm text-[#8ea2c5]">Next stage</div>
                <div className="mt-1 text-[17px] font-semibold text-white">
                  {stageConfirmation.next_stage_label || 'Next Stage'}
                </div>
              </div>

              {stageScriptReview && (
                <div className="mt-5 rounded-[20px] border border-[#29456d] bg-[#0b1424] p-4">
                  <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
                    <div>
                      <div className="flex items-center gap-2 text-sm font-semibold text-white">
                        <Code2 size={15} className="text-[#7fb0ff]" />
                        Review {stageScriptReview.label} before continuing
                      </div>
                      <div className="mt-1 text-xs text-[#8ea2c5]">
                        Copy or download the generated script, then continue to {stageConfirmation.next_stage_label || 'the next stage'}.
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <button
                        type="button"
                        onClick={() => handleOpenLineage(stageScriptReview.layer)}
                        className="rounded-lg border border-[#2f6e62] px-3 py-2 text-xs font-semibold text-[#b7f5e7] transition-colors hover:bg-[#12352f]"
                      >
                        View Lineage
                      </button>
                    </div>
                  </div>

                  <div className="space-y-3">
                    {stageScriptReview.scripts.map((script) => (
                      <div key={script.ui_key} className="rounded-2xl border border-[#22304b] bg-[#101a2b] p-3">
                        <div className="mb-2 flex flex-wrap items-center justify-between gap-3">
                          <div className="min-w-0">
                            <div className="truncate text-sm font-semibold text-white">{script.title}</div>
                            <div className="mt-0.5 truncate text-[11px] text-[#7d8daa]">{script.target_table || script.source_table || script.script_path || '-'}</div>
                          </div>
                          <div className="flex items-center gap-2">
                            <button
                              type="button"
                              onClick={() => handleCopyScript(script)}
                              className="inline-flex items-center gap-1 rounded-md border border-[#2d4263] px-2 py-1 text-[11px] font-semibold text-[#aab8d0] hover:border-[#3f82ff] hover:text-white"
                            >
                              <Copy size={11} />
                              Copy
                            </button>
                            <button
                              type="button"
                              onClick={() => handleDownloadScript(script)}
                              className="inline-flex items-center gap-1 rounded-md border border-[#2d4263] px-2 py-1 text-[11px] font-semibold text-[#aab8d0] hover:border-[#3f82ff] hover:text-white"
                            >
                              <Download size={11} />
                              Download
                            </button>
                          </div>
                        </div>
                        <pre className="max-h-64 overflow-auto whitespace-pre-wrap rounded-xl border border-[#22304b] bg-[#08111f] p-3 text-xs leading-relaxed text-[#c9d5e8]">
                          {formatScriptBody(script)}
                        </pre>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              <div className="mt-6 text-center text-[15px] text-[#aeb8ca]">
                Do you want to proceed to the next stage?
              </div>

              <label className="mt-6 flex items-center gap-3 text-[15px] text-[#aeb8ca]">
                <input
                  type="checkbox"
                  checked={autoAdvanceStages}
                  onChange={(event) => setAutoAdvanceStages(event.target.checked)}
                  className="h-5 w-5 accent-[#3f82ff]"
                />
                Don't ask again — auto-advance between stages
              </label>

              <div className="mt-7 grid grid-cols-2 gap-4">
                <button
                  onClick={handleCancelRun}
                  disabled={stageConfirmSubmitting}
                  className="inline-flex h-14 items-center justify-center gap-2 rounded-[18px] border border-[#2b3950] bg-transparent text-[15px] font-semibold text-[#d1d7e4] transition-colors hover:bg-white/5 disabled:opacity-50"
                >
                  <X size={18} />
                  Cancel Run
                </button>
                <button
                  onClick={handleContinueStage}
                  disabled={stageConfirmSubmitting}
                  className="inline-flex h-14 items-center justify-center gap-2 rounded-[18px] bg-[#4b84f7] text-[15px] font-semibold text-white transition-colors hover:bg-[#5d90f7] disabled:opacity-50"
                >
                  <Play size={18} />
                  {stageConfirmSubmitting ? 'Continuing...' : 'Continue'}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function buildFailureSummary(run) {
  const steps = Array.isArray(run?.pipeline_steps) && run.pipeline_steps.length
    ? run.pipeline_steps
    : Array.isArray(run?.stages)
    ? run.stages
    : []

  const failedStep = steps.find((step) => normalizeState(step?.state || step?.status) === 'FAILED')
  const completedCount = steps.filter((step) => {
    const state = normalizeState(step?.state || step?.status)
    return state === 'COMPLETED'
  }).length
  const failedStageLabel =
    run?.failed_stage_label ||
    run?.failed_stage_key ||
    formatPipelineStepLabel(failedStep?.label || failedStep?.name, failedStep?.key) ||
    failedStep?.key ||
    failedStep?.id ||
    'stage_unknown'

  return {
    failedStage: failedStageLabel,
    progressLabel: `${completedCount}/${steps.length || 0} stages done`,
    timeAgo: formatTimeAgo(run?.completed_at || run?.updated_at || run?.started_at),
  }
}

function PhaseNumber({ index, tone }) {
  const toneClass =
    tone === 'emerald'
      ? 'border-emerald-500/40 text-emerald-400'
      : tone === 'blue'
      ? 'border-[#3f82ff] text-[#3f82ff]'
      : tone === 'amber'
      ? 'border-amber-400/45 text-amber-300'
      : tone === 'red'
      ? 'border-red-400/40 text-red-400'
      : 'border-[#253044] text-[#64748b]'

  return (
    <div className="relative h-7 w-7 flex-shrink-0">
      <div className={`relative flex h-7 w-7 items-center justify-center rounded-full border-2 bg-[#080e1d] text-[11px] font-bold ${toneClass}`}>
        {tone === 'emerald' ? <CheckCircle2 size={13} /> : index}
      </div>
    </div>
  )
}

function StatusPill({ status, tone }) {
  const label = status === 'Waiting' ? 'Review' : status
  const color =
    tone === 'emerald'
      ? 'text-emerald-400'
      : tone === 'blue'
      ? 'text-[#3f82ff]'
      : tone === 'amber'
      ? 'text-amber-300'
      : tone === 'red'
      ? 'text-red-400'
      : 'text-[#7d8daa]'

  return (
    <div className={`flex items-center gap-2 text-[10px] font-medium ${color}`}>
      <span className={`h-2 w-2 rounded-full bg-current ${status === 'Running' ? 'animate-pulse' : ''}`} />
      {label}
    </div>
  )
}

function StepRow({ step, index = 0, isLast = false, onOpenReview, onOpenReport, onRerun, rerunning = false }) {
  const state = normalizeState(step.state)
  const complete = state === 'COMPLETED'
  const waiting = state === 'HITL_WAIT'
  const running = state === 'RUNNING'
  const failed = state === 'FAILED'
  const isGate = /^gate[1-5]$/.test(String(step.key || ''))
  const isNamedReview = step.key === 'silver_merge_key_review' || step.key === 'gold_review'
  const canOpenReview = waiting && (isGate || isNamedReview) && onOpenReview

  return (
    <motion.div
      layout
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.24, delay: Math.min(index * 0.035, 0.18), ease: 'easeOut' }}
      role={canOpenReview ? 'button' : undefined}
      tabIndex={canOpenReview ? 0 : undefined}
      onClick={canOpenReview ? onOpenReview : undefined}
      onKeyDown={
        canOpenReview
          ? (event) => {
              if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault()
                onOpenReview()
              }
            }
          : undefined
      }
      className={`group flex min-h-[38px] items-stretch ${
        canOpenReview ? 'cursor-pointer rounded-lg transition-colors hover:bg-white/[0.03]' : ''
      }`}
    >
      <div className="flex w-8 min-w-8 flex-col items-center">
        <div className={`relative flex h-[22px] w-[22px] flex-shrink-0 items-center justify-center rounded-full border-2 ${
          complete
            ? 'border-emerald-500 bg-emerald-500/10 text-emerald-400'
            : waiting
            ? 'border-amber-400 bg-amber-500/10 text-amber-300'
            : running
            ? 'border-[#3f82ff] bg-[#3f82ff]/10 text-[#3f82ff]'
            : failed
            ? 'border-red-400 bg-red-500/10 text-red-400'
            : 'border-[#253044] bg-[#0b1120] text-[#64748b]'
        }`}>
          {complete
            ? <CheckCircle2 size={12} />
            : running
            ? <Loader2 size={12} className="animate-spin" data-running-indicator="rotation" />
            : <Circle size={10} />}
        </div>
        {!isLast && <div className={`mt-1 w-px flex-1 ${complete ? 'bg-emerald-500/30' : 'bg-[#253044]'}`} />}
      </div>
      <div className="ml-2 flex min-w-0 flex-1 items-start justify-between gap-2 pb-3 pt-0.5">
        <div className={`min-w-0 truncate text-xs font-medium leading-tight ${complete || waiting || running ? 'text-[#d1d5db]' : 'text-[#6b7280]'}`}>
          {step.label}
        </div>
        {step.preparingReview && (
          <span className="shrink-0 text-[10px] font-medium text-[#3f82ff]">Loading…</span>
        )}
        {step.inferredProgress && (
          <span className="shrink-0 text-[10px] font-medium text-[#3f82ff]">Starting…</span>
        )}
        {complete && step.key === 'report_generation' && (
          <button
            type="button"
            onClick={(event) => {
              event.stopPropagation()
              onOpenReport?.()
            }}
            className="inline-flex shrink-0 items-center gap-1 rounded border border-[#3f82ff]/40 bg-[#3f82ff]/10 px-1.5 py-0.5 text-[9px] font-medium text-[#78a9ff] transition-colors hover:bg-[#3f82ff]/20"
          >
            <FileText size={9} />
            View Report
          </button>
        )}
        {complete && onRerun && step.key !== 'report_generation' && (
          <button
            type="button"
            onClick={(event) => {
              event.stopPropagation()
              onRerun()
            }}
            disabled={rerunning}
            title={`Re-run ${step.label}`}
            className="inline-flex shrink-0 items-center gap-1 rounded border border-emerald-500/40 bg-[#0b1424] px-1.5 py-0.5 text-[9px] font-medium text-emerald-400 opacity-0 transition-opacity hover:bg-emerald-500/10 group-hover:opacity-100 focus-visible:opacity-100 disabled:cursor-wait disabled:opacity-60"
          >
            <RotateCcw size={8} className={rerunning ? 'animate-spin' : ''} />
            {rerunning ? 'Starting' : 'Re-run'}
          </button>
        )}
      </div>
    </motion.div>
  )
}

function formatTimeAgo(dateStr) {
  if (!dateStr) return 'just now'
  const diff = Math.max(0, Date.now() - new Date(dateStr).getTime())
  const seconds = Math.floor(diff / 1000)
  const minutes = Math.floor(seconds / 60)
  const hours = Math.floor(minutes / 60)
  const days = Math.floor(hours / 24)
  if (days > 0) return `${days}d ago`
  if (hours > 0) return `${hours}h ${minutes % 60}m ago`
  if (minutes > 0) return `${minutes}m ago`
  return `${seconds}s ago`
}

function buildStageScriptReview(run) {
  const completedLayer = String(run?.stage_confirmation?.last_completed_stage_key || '').toLowerCase()
  if (!['bronze', 'silver', 'gold'].includes(completedLayer)) return null

  const scripts = normalizeScripts(run, completedLayer)
  if (!scripts.length) return null

  return {
    layer: completedLayer,
    label: `${completedLayer.charAt(0).toUpperCase()}${completedLayer.slice(1)} scripts`,
    scripts,
  }
}

function normalizeScripts(run, layer) {
  const bundle = run?.[layer] || {}
  const rows = []
  const seen = new Set()

  for (const script of bundle?.scripts || []) {
    const scriptRunId = script.run_id || bundle?.run_id
    if (scriptRunId && String(scriptRunId) !== String(run.id || run.run_id)) continue

    const dimensionBody = script.dimension_script_body || script.dimension_body || ''
    const key = [
      layer,
      script.script_path || script.target_table || script.source_table || script.table || script.kpi_name,
      script.dimension_script_path || script.dimension_path || '',
    ].join('|')
    if (seen.has(key)) continue
    seen.add(key)

    rows.push({
      ...script,
      ui_key: key,
      layer,
      title:
        script.table ||
        script.kpi_name ||
        script.target_table ||
        script.script_path?.split(/[\\/]/).pop() ||
        `${layer} script`,
      body: script.script_body || '',
      dimension_body: dimensionBody,
      dimension_script_path: script.dimension_script_path || script.dimension_path || '',
    })
  }

  return rows
}

function formatScriptBody(script) {
  const body = script?.body || '# Script body is not available.'
  if (!script?.dimension_body) return body
  return `${body}\n\n# ---------------- Gold dimension script ----------------\n\n${script.dimension_body}`
}

export function markPreparingReview(phase, reviewKey) {
  if (!reviewKey) return phase
  const stepKey = typeof reviewKey === 'number' ? `gate${reviewKey}` : reviewKey
  let matched = false
  const steps = (phase.steps || []).map((step) => {
    if (step.key !== stepKey) return step
    matched = true
    return {
      ...step,
      state: 'RUNNING',
      complete: false,
      preparingReview: true,
      detail: 'Review content is loading.',
    }
  })
  if (!matched) return phase

  return {
    ...phase,
    steps,
    completed: steps.filter((step) => isCompletedStepState(step.state)).length,
    total: steps.length,
    status: 'Running',
  }
}

export function markNextPendingStage(phases = [], run = null, observedPipelineActivity = false) {
  const runState = normalizeState(run?.status)
  const terminal = ['COMPLETED', 'FAILED', 'ABORTED', 'CANCELLED', 'CANCELED', 'HITL_WAIT'].includes(runState)
  if (terminal || (runState !== 'RUNNING' && !observedPipelineActivity)) return phases

  const hasExplicitActiveStage = phases.some((phase) =>
    (phase.steps || []).some((step) =>
      ['RUNNING', 'HITL_WAIT', 'FAILED'].includes(normalizeState(step.state))
    )
  )
  if (hasExplicitActiveStage) return phases

  let promoted = false
  return phases.map((phase) => {
    const steps = (phase.steps || []).map((step) => {
      if (promoted || normalizeState(step.state) !== 'PENDING') return step
      promoted = true
      return {
        ...step,
        state: 'RUNNING',
        complete: false,
        inferredProgress: true,
        detail: step.detail || 'The previous stage completed. Waiting for the backend to report this stage.',
      }
    })
    if (!steps.some((step) => step.inferredProgress)) return phase
    return {
      ...phase,
      steps,
      completed: steps.filter((step) => isCompletedStepState(step.state)).length,
      total: steps.length,
      status: 'Running',
    }
  })
}

export function buildPipelineDisplayPhase(phase, allSteps = [], run = null) {
  const steps = Array.isArray(phase?.steps) ? phase.steps : []
  const byKey = new Map([...allSteps, ...steps].map((step) => [step.key, step]))
  const fileFlow = ['sftp', 'adls_gen2'].includes(String(run?.source || '').toLowerCase())
  const phaseState = phaseStatusToStepState(phase.status)
  const makeStep = (key, label, fallbackState = phaseState, forceState = false) => {
    const step = byKey.get(key)
    const state = normalizeState(forceState ? fallbackState : (step?.state || fallbackState))
    return {
      ...(step || {}),
      key,
      label,
      state,
      detail: step?.detail || '',
      complete: isCompletedStepState(state),
    }
  }
  const makeSynthetic = (key, label, state, detail = '') => ({
    key,
    label,
    state: normalizeState(state || phaseState),
    detail,
    complete: isCompletedStepState(state || phaseState),
  })

  let displaySteps = steps

  if (!fileFlow && phase.label === 'Code Generation & Reviews') {
    displaySteps = [
      ...(byKey.has('metadata_ddl') ? [makeStep('metadata_ddl', 'Metadata DDL Generation')] : []),
      ...(byKey.has('metadata_ddl_review') ? [makeStep('metadata_ddl_review', 'Metadata DDL Review')] : []),
      makeStep('bronze', 'Bronze Code Generation'),
      makeStep('gate4', 'Bronze Review', reviewAwareStepState(byKey.get('gate4'), phase, run, 4)),
      makeStep('silver_merge_key_resolution', 'Silver Merge Key Resolution'),
      makeStep('silver_merge_key_review', 'Silver Merge Key Review'),
      makeStep('silver', 'Silver Code Generation'),
      makeStep('gate5', 'Silver Review', reviewAwareStepState(byKey.get('gate5'), phase, run, 5)),
      makeStep('gold', 'Gold Code Generation'),
      makeStep('gold_review', 'Gold Code Review'),
    ]
  } else if (!fileFlow && ['Target Execution', 'Code Execution & Report Generation', 'Snowflake dbt Deployment & Build'].includes(phase.label)) {
    displaySteps = isGenerationFirstDatabaseRun(run) && isSnowflakeDbtRun(run)
      ? [
          ...(byKey.has('metadata_setup_execution') ? [makeStep('metadata_setup_execution', 'Metadata Setup Execution')] : []),
          makeStep('gold_code_execution', 'Code Execution'),
          ...(run?.report_generation_enabled || byKey.has('report_generation')
            ? [makeStep('report_generation', 'Report Generation')]
            : []),
        ]
      : [
          ...(byKey.has('metadata_setup_execution') ? [makeStep('metadata_setup_execution', 'Metadata Setup Execution')] : []),
          makeStep('bronze_code_execution', 'Bronze Target Execution'),
          makeStep('silver_code_execution', 'Silver Target Execution'),
          makeStep('gold_code_execution', 'Gold Target Execution'),
        ]
  } else if (phase.id === 'phase-1') {
    displaySteps = [
      makeStep('ingestion', 'BRD Ingest'),
      makeStep('memory', 'Memory Check'),
      makeStep('requirements', 'Requirement Extraction'),
      makeStep('kpis', 'KPI Extraction'),
      makeStep('gate1', 'KPI Review', reviewAwareStepState(byKey.get('gate1'), phase, run, 1)),
    ].filter((step) => byKey.has(step.key) || step.key !== 'memory')
  } else if (phase.id === 'phase-2') {
    displaySteps = fileFlow
      ? [
          makeStep('discovery', 'Feed Discovery'),
          makeStep('nomination', 'Feed Nomination'),
          makeStep('gate2', 'Feed Review', reviewAwareStepState(byKey.get('gate2'), phase, run, 2)),
          makeStep('schema', 'Schema Snapshot'),
          makeStep('profiling', 'Column Profiling'),
          makeStep('enrichment', 'Semantic Enrichment'),
          makeStep('gate3', 'Semantic Review', reviewAwareStepState(byKey.get('gate3'), phase, run, 3)),
        ]
      : [
          makeStep('nomination', 'Table Extraction'),
          makeStep('gate2', 'Table Review', reviewAwareStepState(byKey.get('gate2'), phase, run, 2)),
          makeStep('discovery', 'Column Extraction', byKey.get('discovery')?.state || byKey.get('schema')?.state || phaseState),
          makeStep('profiling', 'Column Profiling', byKey.get('profiling')?.state || phaseState),
          makeStep('enrichment', 'Semantic Enrichment', byKey.get('enrichment')?.state || phaseState),
          makeStep('gate3', 'Semantic Review', reviewAwareStepState(byKey.get('gate3'), phase, run, 3)),
        ]
  } else if (fileFlow && phase.id === 'phase-3') {
    displaySteps = [
      makeStep('pre_bronze_bootstrap_metadata', 'Bootstrap Metadata'),
      makeStep('plan_seal', 'Seal Approved Plan'),
      makeStep('plan_freshness', 'Validate Plan Freshness'),
      makeStep('pre_bronze_metadata_codegen', 'Metadata Code Generation'),
      makeStep('bronze', 'Bronze Code Generation'),
      makeStep('gate4', 'Bronze Review', reviewAwareStepState(byKey.get('gate4'), phase, run, 4)),
    ]
  } else if (!fileFlow && phase.id === 'phase-3') {
    const gate4State = reviewAwareStepState(byKey.get('gate4'), phase, run, 4)
    displaySteps = [
      makeStep('bronze', 'Bronze Code Generation'),
      makeStep('gate4', 'Bronze Review', gate4State),
      makeStep('bronze_code_execution', 'Bronze Code Execution'),
    ]
  } else if (fileFlow && phase.id === 'phase-4') {
    displaySteps = [
      makeStep('bronze_code_execution', 'Bronze Target Execution'),
      makeStep('bronze_runtime_validation', 'Bronze Runtime Validation'),
    ]
  } else if (!fileFlow && phase.id === 'phase-4') {
    const silverState = normalizeState(byKey.get('silver')?.state || phaseState)
    const silverExecutionState = normalizeState(byKey.get('silver_code_execution')?.state)
    const gate4State = reviewAwareStepState(byKey.get('gate4'), phase, run, 4)
    const goldState = byKey.get('gold')?.state
    const goldExecutionState = byKey.get('gold_code_execution')?.state
    const hasGoldProgress = ['RUNNING', 'HITL_WAIT', 'FAILED', 'COMPLETED'].includes(normalizeState(goldState)) ||
      ['RUNNING', 'HITL_WAIT', 'FAILED', 'COMPLETED'].includes(normalizeState(goldExecutionState))
    const gate5State = hasGoldProgress
      ? 'COMPLETED'
      : reviewAwareStepState(byKey.get('gate5'), phase, run, 5)
    const rawMergeReviewState = run?.next_review_key === 'silver_merge_key_review'
      ? 'HITL_WAIT'
      : byKey.get('silver_merge_key_review')?.state
    const mergeReviewState = rawMergeReviewState ? normalizeState(rawMergeReviewState) : ''
    const silverFlow = buildSilverPhaseStates(silverState, gate4State, gate5State, phase.status, hasGoldProgress, mergeReviewState, silverExecutionState)
    displaySteps = [
      makeSynthetic('silver_merge_key_resolution', 'Silver Merge Key Resolution', silverFlow.mergeResolution),
      makeSynthetic('silver_merge_key_review', 'Silver Merge Key Review', mergeReviewState || silverFlow.mergeReview, 'Merge keys are reviewed before Silver generation.'),
      makeStep('silver', 'Silver Code Generation', silverFlow.codeGeneration, true),
      makeStep('gate5', 'Silver Review', silverFlow.reviewGate, true),
      makeStep('silver_code_execution', 'Silver Code Execution', silverFlow.codeExecution, true),
    ]
  } else if (fileFlow && phase.id === 'phase-5') {
    displaySteps = [
      makeStep('silver_merge_key_resolution', 'Silver Merge Key Resolution'),
      makeStep('silver_merge_key_review', 'Silver Merge Key Review'),
      makeStep('silver', 'Silver Code Generation'),
      makeStep('gate5', 'Silver Code Review', reviewAwareStepState(byKey.get('gate5'), phase, run, 5)),
      makeStep('silver_code_execution', 'Silver Code Execution'),
      makeStep('silver_runtime_validation', 'Silver Runtime Validation'),
    ]
  } else if (!fileFlow && phase.id === 'phase-5') {
    const goldFlow = buildGoldPhaseStates(
      byKey.get('gold')?.state || phaseState,
      byKey.get('gold_code_execution')?.state,
      phase.status,
      run?.status
    )
    displaySteps = [
      makeStep('gold', 'Gold Code Generation', goldFlow.codeGeneration, true),
      makeStep('gold_code_execution', 'Gold Code Execution', goldFlow.codeExecution, true),
    ]
  } else if (fileFlow && phase.id === 'phase-6') {
    displaySteps = [
      makeStep('gold', 'Gold Code Generation'),
      makeStep('gold_review', 'Gold Code Review'),
      makeStep('gold_code_execution', 'Gold Code Execution'),
      makeStep('gold_runtime_validation', 'Gold Runtime Validation'),
      makeStep('final_publish', 'Final Publish (Target Gate 5)'),
      makeStep('finalize', 'Finalize Run'),
    ]
  }

  displaySteps = displaySteps.filter((step) => !HIDDEN_CODE_REVIEW_STEPS.has(step.key))
  displaySteps = clampLinearStepStates(displaySteps)

  const completed = displaySteps.filter((step) => isCompletedStepState(step.state)).length
  const waiting = displaySteps.find((step) => normalizeState(step.state) === 'HITL_WAIT')
  const running = displaySteps.find((step) => normalizeState(step.state) === 'RUNNING')
  const failed = displaySteps.find((step) => normalizeState(step.state) === 'FAILED')
  let status = phase.status
  if (failed) status = 'Failed'
  else if (waiting) status = 'Review'
  else if (running) status = 'Running'
  else if (displaySteps.length && completed === displaySteps.length) status = 'Done'

  return {
    ...phase,
    label: phase.label === 'Code Generation & Reviews' ? 'Code Generation' : phase.label,
    steps: displaySteps,
    completed,
    total: displaySteps.length,
    status,
  }
}

function clampLinearStepStates(steps = []) {
  let blocked = false
  return steps.map((step) => {
    const state = normalizeState(step.state)
    const complete = isCompletedStepState(state)
    if (!blocked && complete) return step
    if (!blocked) {
      blocked = true
      return { ...step, complete: false }
    }
    return { ...step, state: 'PENDING', complete: false }
  })
}

function phaseStatusToStepState(status) {
  const value = String(status || '').toLowerCase()
  if (value === 'done') return 'COMPLETED'
  if (value === 'running') return 'RUNNING'
  if (value === 'review') return 'HITL_WAIT'
  if (value === 'failed') return 'FAILED'
  return 'PENDING'
}

function reviewAwareStepState(step, phase, run = null, gate = 0) {
  if (step?.state) return normalizeState(step.state)
  const status = normalizeState(run?.status)
  if (Number(run?.next_gate || 0) === gate && status === 'HITL_WAIT') return 'HITL_WAIT'
  if (phase.status === 'Review') return 'HITL_WAIT'
  return phaseStatusToStepState(phase.status)
}

function buildSilverPhaseStates(silverState, gate4State, gate5State, phaseStatus, hasGoldProgress = false, mergeReviewState = '', silverExecutionState = '') {
  const normalizedSilver = normalizeState(silverState)
  const normalizedGate4 = normalizeState(gate4State)
  const normalizedGate = normalizeState(gate5State)
  const normalizedMergeReview = mergeReviewState ? normalizeState(mergeReviewState) : ''
  const normalizedSilverExecution = silverExecutionState ? normalizeState(silverExecutionState) : ''
  const normalizedPhase = String(phaseStatus || '').toLowerCase()

  if (hasGoldProgress) {
    return {
      mergeResolution: 'COMPLETED',
      mergeReview: 'COMPLETED',
      codeGeneration: 'COMPLETED',
      reviewGate: 'COMPLETED',
      codeExecution: 'COMPLETED',
    }
  }

  if (['RUNNING', 'FAILED', 'COMPLETED'].includes(normalizedSilverExecution)) {
    return {
      mergeResolution: 'COMPLETED',
      mergeReview: 'COMPLETED',
      codeGeneration: 'COMPLETED',
      reviewGate: 'COMPLETED',
      codeExecution: normalizedSilverExecution,
    }
  }

  if (normalizedMergeReview === 'HITL_WAIT') {
    return {
      mergeResolution: 'COMPLETED',
      mergeReview: 'HITL_WAIT',
      codeGeneration: 'PENDING',
      reviewGate: 'PENDING',
      codeExecution: 'PENDING',
    }
  }

  if (normalizedGate === 'HITL_WAIT' || normalizedGate === 'PAUSED_FOR_HITL') {
    return {
      mergeResolution: 'COMPLETED',
      mergeReview: 'COMPLETED',
      codeGeneration: 'COMPLETED',
      reviewGate: 'HITL_WAIT',
      codeExecution: 'PENDING',
    }
  }

  if (normalizedGate4 === 'HITL_WAIT') {
    return {
      mergeResolution: 'PENDING',
      mergeReview: 'PENDING',
      codeGeneration: 'PENDING',
      reviewGate: 'PENDING',
      codeExecution: 'PENDING',
    }
  }

  if (normalizedSilver === 'RUNNING') {
    return {
      mergeResolution: 'COMPLETED',
      mergeReview: 'COMPLETED',
      codeGeneration: 'RUNNING',
      reviewGate: 'PENDING',
      codeExecution: 'PENDING',
    }
  }

  if (normalizedSilver === 'FAILED') {
    return {
      mergeResolution: normalizedMergeReview === 'COMPLETED' ? 'COMPLETED' : 'FAILED',
      mergeReview: normalizedMergeReview === 'COMPLETED' ? 'COMPLETED' : 'PENDING',
      codeGeneration: 'FAILED',
      reviewGate: 'PENDING',
      codeExecution: 'PENDING',
    }
  }

  if (normalizedSilver === 'COMPLETED' || normalizedSilver === 'SUCCESS' || normalizedSilver === 'PIPELINE_COMPLETED') {
    if (!normalizedGate || normalizedGate === 'PENDING') {
      return {
        mergeResolution: 'COMPLETED',
        mergeReview: 'COMPLETED',
        codeGeneration: 'COMPLETED',
        reviewGate: 'HITL_WAIT',
        codeExecution: 'PENDING',
      }
    }

    return {
      mergeResolution: 'COMPLETED',
      mergeReview: 'COMPLETED',
      codeGeneration: 'COMPLETED',
      reviewGate: normalizedGate || 'PENDING',
      codeExecution: normalizedGate === 'COMPLETED' ? 'RUNNING' : 'PENDING',
    }
  }

  if (normalizedPhase === 'failed' || normalizedSilver === 'FAILED') {
    return {
      mergeResolution: 'FAILED',
      mergeReview: 'PENDING',
      codeGeneration: 'PENDING',
      reviewGate: 'PENDING',
      codeExecution: 'PENDING',
    }
  }

  return {
    mergeResolution: 'PENDING',
    mergeReview: 'PENDING',
    codeGeneration: 'PENDING',
    reviewGate: normalizedGate || 'PENDING',
    codeExecution: 'PENDING',
  }
}

function buildGoldPhaseStates(goldState, goldExecutionState, phaseStatus, runStatus) {
  const normalizedGold = normalizeState(goldState)
  const normalizedGoldExecution = goldExecutionState ? normalizeState(goldExecutionState) : ''
  const normalizedRun = normalizeState(runStatus)
  const normalizedPhase = String(phaseStatus || '').toLowerCase()

  if (['RUNNING', 'FAILED', 'COMPLETED'].includes(normalizedGoldExecution)) {
    return {
      codeGeneration: 'COMPLETED',
      codeExecution: normalizedGoldExecution,
    }
  }

  if (normalizedGold === 'RUNNING') {
    return {
      codeGeneration: 'RUNNING',
      codeExecution: 'PENDING',
    }
  }

  if (normalizedGold === 'FAILED') {
    return {
      codeGeneration: 'FAILED',
      codeExecution: 'PENDING',
    }
  }

  if (normalizedGold === 'COMPLETED') {
    return {
      codeGeneration: 'COMPLETED',
      codeExecution: normalizedRun === 'COMPLETED' || normalizedPhase === 'done' ? 'COMPLETED' : 'PENDING',
    }
  }

  return {
    codeGeneration: 'PENDING',
    codeExecution: 'PENDING',
  }
}

function isCompletedStepState(state) {
  return normalizeState(state) === 'COMPLETED'
}

export default PipelineMonitor
