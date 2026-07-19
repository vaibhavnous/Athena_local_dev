// @ts-nocheck
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import { AlertTriangle, CheckCircle2, ChevronDown, ChevronUp, Circle, Clock3, Code2, Copy, Download, FileText, Play, RefreshCcw, RotateCcw, X } from 'lucide-react'
import useAthenaStore from '../store/useAthenaStore'
import PipelineLogsPanel from '../components/pipeline/PipelineLogsPanel'
import { formatPipelineStepLabel, getGateDisplayName, getPhaseGroups, getPipelineSteps, normalizeState, statusTone, summarizeRunSource } from '../utils/pipelinePhases'
import { ENABLE_DEMO_FALLBACKS, getDemoRuns, isDemoFallbackRun } from '../utils/demoFallbacks'
import { abortRun, continueStage, getRun, getRunStatus, getRuns, getRunScripts, restartRun, resumeFromFailure, retryFailedStage } from '../api/athenaApi'

const ACTIVE_RUN_REFRESH_INTERVAL_MS = 5000
const ACTIVE_RUN_FAST_REFRESH_INTERVAL_MS = 1500

function isTimeoutError(error) {
  return error?.code === 'ECONNABORTED' || /timeout/i.test(error?.message || '')
}

function isTransientReadError(error) {
  return isTimeoutError(error) || Number(error?.status) === 503
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
  const { runs, activeRunId, setActiveRun, setRuns, updateRun, setServerOnline, addNotification, addRun } = useAthenaStore()
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
  const runsRequestInFlightRef = useRef(false)
  const activeRunRequestInFlightRef = useRef(false)
  const lastLogTriggeredRefreshRef = useRef(0)
  const latestRunsRef = useRef(runs)
  const latestActiveRunRef = useRef(activeRun)
  const actualSteps = useMemo(() => getPipelineSteps(activeRun), [activeRun])
  const actualPhases = useMemo(() => getPhaseGroups(activeRun, actualSteps), [activeRun, actualSteps])

  useEffect(() => {
    latestRunsRef.current = runs
  }, [runs])

  useEffect(() => {
    latestActiveRunRef.current = activeRun
  }, [activeRun])

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
      const currentRun = latestActiveRunRef.current
      const data = shouldUseStatusRefresh(currentRun)
        ? ((await getRunStatus(activeRunStableId))?.run || await getRun(activeRunStableId))
        : await getRun(activeRunStableId)
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

  const handleLogsUpdated = useCallback(() => {
    const now = Date.now()
    if (now - lastLogTriggeredRefreshRef.current < 2000) return
    lastLogTriggeredRefreshRef.current = now
    void refreshActiveRunNow()
  }, [refreshActiveRunNow])

  useEffect(() => {
    let cancelled = false
    let timer: number | null = null

    const scheduleNext = (delay = 8000) => {
      if (!cancelled) {
        timer = window.setTimeout(refreshRuns, delay)
      }
    }

    const refreshRuns = async () => {
      const currentRuns = latestRunsRef.current || []
      if (ENABLE_DEMO_FALLBACKS && currentRuns.length > 0 && currentRuns.every((run) => isDemoFallbackRun(run))) {
        setRuns(getDemoRuns())
        scheduleNext(2000)
        return
      }

      if (runsRequestInFlightRef.current) {
        scheduleNext()
        return
      }

      runsRequestInFlightRef.current = true
      try {
        const data = await getRuns()
        if (!cancelled && Array.isArray(data)) {
          setRuns(data)
          setServerOnline(true)
        }
      } catch (error) {
        if (!cancelled) {
          if (!isTransientReadError(error)) {
            setServerOnline(false)
            console.warn('[PipelineMonitor] Failed to refresh runs', error)
          } else {
            console.debug('[PipelineMonitor] Runs refresh timed out; keeping existing data')
          }
        }
      } finally {
        runsRequestInFlightRef.current = false
        scheduleNext()
      }
    }

    refreshRuns()
    return () => {
      cancelled = true
      if (timer !== null) window.clearTimeout(timer)
    }
  }, [setRuns, setServerOnline])

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

  // pipelinePhases is the single renderer contract; re-inferring completion here
  // previously promoted a review artifact into completed Silver execution.
  const renderedPhases = actualPhases

  const defaultExpandedPhase = useMemo(() => {
    if (!actualPhases?.length) return 'phase-1'
    const active = furthestActivePhase(actualPhases)
    if (active?.phase) return active.phase.id
    const firstIncomplete = actualPhases.find((phase) => phase.completed < phase.total)
    return firstIncomplete?.id || actualPhases[actualPhases.length - 1].id
  }, [actualPhases])

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
  const projectLabel = monitorRun?.project_name || monitorRun?.project?.name || ''
  const projectId = monitorRun?.project_id || monitorRun?.project?.id || ''
  const activeTone = statusTone(monitorRun?.status)
  const isFailedRun = String(monitorRun?.status || '').toUpperCase() === 'FAILED'
  const isStageConfirmationPaused =
    String(monitorRun?.status || '').toUpperCase() === 'PAUSED_FOR_STAGE_CONFIRMATION' ||
    Boolean(monitorRun?.stage_confirmation?.awaiting_confirmation)
  const [dismissedFailureBannerFor, setDismissedFailureBannerFor] = useState<string | null>(null)
  const [autoAdvanceStages, setAutoAdvanceStages] = useState(false)
  const [stageConfirmSubmitting, setStageConfirmSubmitting] = useState(false)
  const [failureActionSubmitting, setFailureActionSubmitting] = useState('')
  const [scriptBundles, setScriptBundles] = useState(null)

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
        if (!cancelled && !isTimeoutError(error)) {
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
  const currentStepSummary = useMemo(() => buildCurrentStepSummary(monitorRun), [monitorRun])
  const activeStatusLabel = formatRunStatusLabel(monitorRun, currentStepSummary)

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
      const refreshed = await getRun(activeRun.id)
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
      const refreshed = await getRun(activeRun.id)
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
      const nextRun = await getRun(restarted.run_id)
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
      const refreshed = await getRun(activeRun.id)
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
    if (step?.key === 'silver_merge_key_review') {
      navigate(`/app/hitl?runId=${encodeURIComponent(activeRun.id)}&review=silver_merge_key_review`)
      return
    }
    const stepGate = /^gate([1-5])$/.exec(String(step?.key || ''))?.[1]
    if (!stepGate && activeRun.next_review_key) {
      navigate(`/app/hitl?runId=${encodeURIComponent(activeRun.id)}&review=${encodeURIComponent(activeRun.next_review_key)}`)
      return
    }
    const gate = Number(stepGate || activeRun.next_gate || 0)
    navigate(gate ? `/app/hitl?runId=${encodeURIComponent(activeRun.id)}&gate=${gate}` : '/app/hitl')
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
    <div className="flex h-full min-h-[calc(100vh-116px)] flex-col">
      <div className="mb-7 flex flex-col gap-4">
        <div className="flex min-h-[72px] items-center justify-between rounded-xl border border-[#1d2940] bg-[#09111f] px-5">
          <div className="flex min-w-0 flex-wrap items-center gap-x-5 gap-y-1 pr-4 text-[12px] text-[#8ea0c3]">
            {projectId && (
              <span className="min-w-0 truncate">
                Project: <strong className="font-semibold text-[#c4cee0]">{projectLabel || projectId}</strong>
              </span>
            )}
            <span className="min-w-0 truncate">
              Pipeline: <strong className="font-semibold text-[#c4cee0]">{runLabel}</strong>
            </span>
            <span className="font-mono text-[11px] text-[#7183a4]" title={monitorRun.id}>
              Run ID: {monitorRun.id}
            </span>
          </div>
          <div className="flex flex-shrink-0 items-center gap-3">
            <div className={`inline-flex items-center gap-2 rounded-md border px-3 py-2 text-[12px] font-semibold ${
              activeTone === 'amber'
                ? 'border-amber-400/50 bg-amber-500/10 text-amber-300'
                : activeTone === 'emerald'
                ? 'border-emerald-400/35 bg-emerald-500/10 text-emerald-400'
                : activeTone === 'blue'
                ? 'border-[#3f82ff]/40 bg-[#3f82ff]/10 text-[#3f82ff]'
                : activeTone === 'red'
                ? 'border-red-400/35 bg-red-500/10 text-red-400'
                : 'border-[#253044] bg-[#0b1120] text-slate-300'
            }`}>
              <span className="h-2 w-2 rounded-full bg-current" />
              {activeStatusLabel}
            </div>
          </div>
        </div>

        {isFailedRun && dismissedFailureBannerFor !== monitorRun.id && (
          <div className="rounded-2xl border border-red-500/35 bg-[#17111d] px-6 py-5 shadow-[0_12px_40px_rgba(0,0,0,0.22)]">
            <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
            <div className="flex min-w-0 items-start gap-4">
              <div className="flex h-10 w-10 flex-shrink-0 items-center justify-center rounded-xl border border-red-500/30 bg-red-500/10 text-red-400">
                <AlertTriangle size={18} />
              </div>
              <div className="min-w-0">
                <div className="flex flex-wrap items-center gap-3 text-sm">
                  <div className="flex min-w-0 items-center gap-2 font-semibold text-white">
                    <FileText size={15} className="text-[#b8c3d9]" />
                    <span className="max-w-[420px] truncate">{monitorRun.brd_filename || 'BRD File Name'}</span>
                  </div>
                  <span className="rounded-lg border border-red-500/35 bg-red-500/12 px-2.5 py-1 text-xs font-semibold text-red-400">
                    Failed
                  </span>
                  <span className="text-[#d4d9e5]">at `{failureSummary.failedStage}`</span>
                  <span className="text-[#9da7bb]">{failureSummary.progressLabel}</span>
                </div>
                {monitorRun?.error && (
                  <div className="mt-2 max-w-[920px] truncate text-sm text-red-300/90">
                    {monitorRun.error}
                  </div>
                )}
                <div className="mt-2 flex items-center gap-2 text-sm text-[#9da7bb]">
                  <Clock3 size={14} />
                  {failureSummary.timeAgo}
                </div>
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-3 xl:justify-end">
              <button
                onClick={handleRetryFailedStage}
                disabled={failureActionSubmitting !== ''}
                className="inline-flex h-11 items-center gap-2 rounded-xl border border-amber-500/35 bg-amber-500/10 px-5 text-sm font-semibold text-amber-400 transition-colors hover:bg-amber-500/15 disabled:cursor-not-allowed disabled:opacity-50"
              >
                <RotateCcw size={16} />
                {failureActionSubmitting === 'retry' ? 'Retrying...' : 'Retry Failed Stage'}
              </button>
              <button
                onClick={handleResumeFromFailure}
                disabled={failureActionSubmitting !== ''}
                className="inline-flex h-11 items-center gap-2 rounded-xl border border-[#3f82ff]/40 bg-[#3f82ff]/10 px-5 text-sm font-semibold text-[#3f82ff] transition-colors hover:bg-[#3f82ff]/15 disabled:cursor-not-allowed disabled:opacity-50"
              >
                <Play size={16} />
                {failureActionSubmitting === 'resume' ? 'Resuming...' : 'Resume from Failure'}
              </button>
              <button
                onClick={handleRestartFailedRun}
                disabled={failureActionSubmitting !== ''}
                className="inline-flex h-11 items-center gap-2 rounded-xl border border-[#2e394d] bg-[#101827] px-5 text-sm font-semibold text-white transition-colors hover:bg-[#152033] disabled:cursor-not-allowed disabled:opacity-50"
              >
                <RefreshCcw size={16} />
                {failureActionSubmitting === 'restart' ? 'Restarting...' : 'Restart'}
              </button>
              <button
                onClick={() => setDismissedFailureBannerFor(monitorRun.id)}
                className="flex h-11 w-11 items-center justify-center rounded-xl border border-[#2e394d] bg-transparent text-[#8d96a9] transition-colors hover:bg-white/5 hover:text-white"
                aria-label="Dismiss failure banner"
              >
                <X size={16} />
              </button>
            </div>
            </div>
          </div>
        )}

        <div className="flex items-center justify-between">
          <div className="text-[12px] font-semibold tracking-[0.24em] text-[#5f708f]">
            Discovery Progress
          </div>
          <div className="flex items-center gap-3">
          {runs.length > 1 && (
            <select
              value={activeRun.id}
              onChange={(event) => setActiveRun(event.target.value)}
              className="h-9 rounded-md border border-[#253044] bg-[#111827] px-3 text-xs text-slate-300 outline-none transition-colors focus:border-[#3f82ff]"
            >
              {runs.map((run) => (
                <option key={run.id} value={run.id}>
                  {run.project_name ? `${run.project_name} - ` : ''}{run.brd_filename || 'Pipeline'} - {String(run.id).slice(0, 8)}
                </option>
              ))}
            </select>
              )}
          </div>
        </div>

      </div>

      <div className="grid min-h-0 flex-1 gap-5 xl:grid-cols-[520px_minmax(0,1fr)]">
        <section className="min-h-0 overflow-hidden rounded-lg border border-[#253044] bg-[#080e1d]">
          <div className="divide-y divide-[#253044]">
            {renderedPhases.map((phase, index) => {
              const expanded = expandedPhase === phase.id
              const tone = statusTone(phase.status)
              return (
                <div key={phase.id}>
                  <button
                    onClick={() => setExpandedPhase(expanded ? '' : phase.id)}
                    className={`flex w-full items-center justify-between px-4 text-left transition-colors ${
                      expanded ? 'bg-[#101735]' : 'bg-[#080e1d] hover:bg-[#0f1728]'
                    } ${expanded ? 'py-4' : 'py-3.5'}`}
                  >
                    <div className="flex min-w-0 items-center gap-4">
                      <PhaseNumber index={index + 1} tone={tone} status={phase.status} />
                      <div className="min-w-0">
                        <div className={`truncate text-[14px] font-semibold ${expanded ? 'text-white' : 'text-[#7d8daa]'}`}>
                          {phase.label}
                        </div>
                        {expanded && (
                          <div className="mt-1 text-xs text-[#8a9ab7]">
                            {phase.completed}/{phase.total} stages complete
                          </div>
                        )}
                      </div>
                    </div>
                    <div className="ml-4 flex items-center gap-3">
                      <StatusPill status={phase.status} tone={tone} compact={!expanded} step={phase.steps.find((step) => ['RUNNING', 'HITL_WAIT'].includes(normalizeState(step.state)))} />
                      {expanded ? <ChevronUp size={14} className="text-[#64748b]" /> : <ChevronDown size={14} className="text-[#64748b]" />}
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
                        <div className="px-6 pb-6 pt-1">
                          <div className="ml-[18px] border-l border-[#2b3648] pl-7">
                            <div className="space-y-5">
                              {phase.steps.map((step, stepIndex) => (
                                <StepRow key={step.key} step={step} index={stepIndex} onOpenReview={() => handleOpenGateReview(step)} />
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

        <section className="flex min-h-0 flex-col overflow-hidden rounded-lg border border-[#253044] bg-[#111827]">
          <div className="flex h-[78px] items-center justify-between border-b border-[#253044] px-5">
            <div>
              <div className="text-[16px] font-semibold text-white">Execution Logs</div>
              <div className="mt-1 text-[13px] text-[#7d8daa]">Real-time pipeline execution monitoring</div>
            </div>
            <div className={`inline-flex items-center gap-2 rounded-md border px-3 py-2 text-[13px] font-semibold ${
              activeTone === 'amber'
                ? 'border-amber-400/50 bg-amber-500/10 text-amber-300'
                : activeTone === 'emerald'
                ? 'border-emerald-400/35 bg-emerald-500/10 text-emerald-400'
                : activeTone === 'blue'
                ? 'border-[#3f82ff]/40 bg-[#3f82ff]/10 text-[#3f82ff]'
                : activeTone === 'red'
                ? 'border-red-400/35 bg-red-500/10 text-red-400'
                : 'border-[#253044] bg-[#0b1120] text-slate-300'
            }`}>
              <span className="h-2 w-2 rounded-full bg-current" />
              {activeStatusLabel}
            </div>
          </div>

          <div className="min-h-0 flex-1">
            <PipelineLogsPanel runId={activeRun.run_id || activeRun.id} isActive onLogsUpdated={handleLogsUpdated} />
          </div>
        </section>
      </div>

      {isStageConfirmationPaused && stageConfirmation?.awaiting_confirmation && (
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

function buildCurrentStepSummary(run) {
  if (!run) return null
  const fromBackend = run.current_pipeline_step
  const steps = Array.isArray(run.pipeline_steps) && run.pipeline_steps.length
    ? run.pipeline_steps
    : Array.isArray(run.stages)
    ? run.stages
    : []
  const step =
    fromBackend ||
    steps.find((item) => ['RUNNING', 'HITL_WAIT'].includes(normalizeState(item?.state || item?.status))) ||
    steps.find((item) => normalizeState(item?.state || item?.status) === 'FAILED') ||
    null

  const runStatus = normalizeState(run.status)
  const state = normalizeState(step?.state || step?.status || run.status)
  const label = formatPipelineStepLabel(step?.label || step?.name, step?.key) || step?.label || step?.key || ''
  const externalMessage = String(run.external_execution?.message || '').trim()
  const detail = externalMessage || step?.detail || run.resume_message || ''
  const nextGate = Number(run.next_gate || 0)
  const gateLabel = run.next_review_key === 'silver_merge_key_review'
    ? 'Silver Merge Key Review'
    : nextGate ? getGateDisplayName(nextGate, run.source) : label

  if (runStatus === 'HITL_WAIT' || state === 'HITL_WAIT') {
    return {
      state: 'HITL_WAIT',
      tone: 'amber',
      badge: 'Waiting for Review',
      headline: `${gateLabel || label || 'Review'} is waiting for approval`,
      detail,
    }
  }

  if (runStatus === 'RUNNING' || state === 'RUNNING') {
    return {
      state: 'RUNNING',
      tone: 'blue',
      badge: 'Running',
      headline: `${label || 'Pipeline stage'} is running`,
      detail,
    }
  }

  if (runStatus === 'FAILED' || state === 'FAILED') {
    return {
      state: 'FAILED',
      tone: 'red',
      badge: 'Failed',
      headline: `${label || 'Pipeline stage'} failed`,
      detail: run.error || detail,
    }
  }

  if (runStatus === 'COMPLETED') {
    return {
      state: 'COMPLETED',
      tone: 'emerald',
      badge: 'Complete',
      headline: 'Pipeline completed',
      detail: 'Generation stages are complete. Execution markers are UI-only for locally exported scripts.',
    }
  }

  return label
    ? {
        state,
        tone: statusTone(run.status),
        badge: String(run.status || 'Pending').replace(/_/g, ' '),
        headline: `${label} is ${String(run.status || 'pending').replace(/_/g, ' ').toLowerCase()}`,
        detail,
      }
    : null
}

function formatRunStatusLabel(run, currentStepSummary) {
  if (currentStepSummary?.badge && currentStepSummary?.headline) {
    if (currentStepSummary.state === 'HITL_WAIT') return currentStepSummary.badge
    if (currentStepSummary.state === 'RUNNING') return 'Running'
    return currentStepSummary.badge
  }
  return String(run?.status || 'Waiting').replace(/_/g, ' ')
}

function PhaseNumber({ index, tone, status }) {
  const running = status === 'Running'
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
    <div className="relative h-8 w-8 flex-shrink-0">
      {running && (
        <>
          <span className="absolute inset-0 rounded-full border border-[#3f82ff]/35 animate-ping" />
          <span className="absolute inset-0 rounded-full border-2 border-transparent border-t-[#3f82ff] animate-spin" />
        </>
      )}
      <div className={`relative flex h-8 w-8 items-center justify-center rounded-full border bg-[#080e1d] text-[14px] font-semibold ${toneClass}`}>
        {index}
      </div>
    </div>
  )
}

function StatusPill({ status, tone, compact, step }) {
  const stepLabel = step?.label || ''
  const label = stepLabel && ['Review', 'Running'].includes(status)
    ? compact
      ? stepLabel
      : status === 'Review'
      ? `${stepLabel} waiting`
      : `${stepLabel} running`
    : compact
    ? status
    : status === 'Waiting'
    ? 'Review'
    : status
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
    <div className={`flex items-center gap-2 text-xs font-medium ${color}`}>
      <span className={`h-2 w-2 rounded-full bg-current ${status === 'Running' ? 'animate-pulse' : ''}`} />
      {label}
    </div>
  )
}

function StepRow({ step, index = 0, onOpenReview }) {
  const state = normalizeState(step.state)
  const complete = state === 'COMPLETED'
  const waiting = state === 'HITL_WAIT'
  const running = state === 'RUNNING'
  const failed = state === 'FAILED'
  const isGate = /^gate[1-5]$/.test(String(step.key || ''))
  const isNamedReview = step.key === 'silver_merge_key_review'
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
      className={`flex min-h-[38px] items-center gap-4 ${
        canOpenReview ? 'cursor-pointer rounded-lg transition-colors hover:bg-white/[0.03]' : ''
      }`}
    >
      <div className="relative h-7 w-7 flex-shrink-0">
        {running && (
          <>
            <span className="absolute inset-0 rounded-full border border-[#3f82ff]/35 animate-ping" />
            <span className="absolute inset-0 rounded-full border-2 border-transparent border-t-[#3f82ff] animate-spin" />
          </>
        )}
        <div className={`relative flex h-7 w-7 items-center justify-center rounded-full border ${
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
          {complete ? <CheckCircle2 size={14} /> : <Circle size={running ? 9 : 11} className={running ? 'animate-pulse' : ''} />}
        </div>
      </div>
      <div className="min-w-0">
        <div className={`truncate text-[14px] font-semibold ${complete || waiting || running ? 'text-white' : 'text-[#7d8daa]'}`}>
          {step.label}
        </div>
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

export function buildPipelineDisplayPhase(phase, allSteps = [], run = null) {
  const steps = Array.isArray(phase?.steps) ? phase.steps : []
  const byKey = new Map([...allSteps, ...steps].map((step) => [step.key, step]))
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

  if (phase.id === 'phase-1') {
    displaySteps = [
      makeStep('ingestion', 'BRD Ingest'),
      makeStep('memory', 'Memory Check'),
      makeStep('requirements', 'Requirement Extraction'),
      makeStep('kpis', 'KPI Extraction'),
      makeStep('gate1', 'KPI Review', reviewAwareStepState(byKey.get('gate1'), phase, run, 1)),
    ].filter((step) => byKey.has(step.key) || step.key !== 'memory')
  } else if (phase.id === 'phase-2') {
    displaySteps = [
      makeStep('nomination', 'Table Extraction'),
      makeStep('gate2', byKey.has('gate2') && String(byKey.get('gate2')?.label || '').toLowerCase().includes('feed') ? 'Feed Review' : 'Table Review', reviewAwareStepState(byKey.get('gate2'), phase, run, 2)),
      makeStep('discovery', 'Column Extraction', byKey.get('discovery')?.state || byKey.get('schema')?.state || phaseState),
      makeStep('profiling', 'Column Profiling', byKey.get('profiling')?.state || phaseState),
      makeStep('enrichment', 'Semantic Enrichment', byKey.get('enrichment')?.state || phaseState),
      makeStep('gate3', 'Semantic Review', reviewAwareStepState(byKey.get('gate3'), phase, run, 3)),
    ]
  } else if (phase.id === 'phase-3') {
    const gate4State = reviewAwareStepState(byKey.get('gate4'), phase, run, 4)
    displaySteps = [
      makeStep('bronze', 'Bronze Code Generation'),
      makeStep('gate4', 'Bronze Review', gate4State),
      makeStep('bronze_code_execution', 'Bronze Code Execution'),
    ]
  } else if (phase.id === 'phase-4') {
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
  } else if (phase.id === 'phase-5') {
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
  }

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
