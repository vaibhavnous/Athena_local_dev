// @ts-nocheck
import React, { useEffect, useMemo, useState } from 'react'
import { AlertTriangle, CheckCircle2, ChevronDown, ChevronUp, Circle, Clock3, FileText, Play, RefreshCcw, RotateCcw, Users, X } from 'lucide-react'
import useAthenaStore from '../store/useAthenaStore'
import PipelineLogsPanel from '../components/pipeline/PipelineLogsPanel'
import { getPhaseGroups, statusTone, summarizeRunSource } from '../utils/pipelinePhases'
import { abortRun, continueStage, getRun, getRuns } from '../api/athenaApi'

function PipelineMonitor() {
  const { runs, activeRunId, setActiveRun, setRuns, updateRun, setServerOnline, addNotification } = useAthenaStore()
  const activeRun = runs.find((run) => run.id === activeRunId) || runs[0] || null
  const phases = useMemo(() => getPhaseGroups(activeRun), [activeRun])

  useEffect(() => {
    let cancelled = false

    const refreshRuns = async () => {
      try {
        const data = await getRuns()
        if (!cancelled && Array.isArray(data)) {
          setRuns(data)
          setServerOnline(true)
        }
      } catch (error) {
        if (!cancelled) setServerOnline(false)
        if (!cancelled) console.warn('[PipelineMonitor] Failed to refresh runs', error)
      }
    }

    refreshRuns()
    const timer = window.setInterval(refreshRuns, 8000)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [setRuns, setServerOnline])

  useEffect(() => {
    if (!activeRun?.id) return
    let cancelled = false

    const refreshActiveRun = async () => {
      try {
        const data = await getRun(activeRun.id)
        if (!cancelled) {
          updateRun(activeRun.id, data)
          setServerOnline(true)
        }
      } catch (error) {
        if (!cancelled) setServerOnline(false)
        if (!cancelled) console.warn('[PipelineMonitor] Failed to refresh active run', error)
      }
    }

    refreshActiveRun()
    const timer = window.setInterval(refreshActiveRun, 5000)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [activeRun?.id, updateRun, setServerOnline])

  const defaultExpandedPhase = useMemo(() => {
    if (!phases?.length) return 'phase-1'
    const activePhase = phases.find((phase) =>
      phase.steps.some((step) => ['RUNNING', 'HITL_WAIT'].includes(step.state))
    )
    if (activePhase) return activePhase.id
    const firstIncomplete = phases.find((phase) => phase.completed < phase.total)
    return firstIncomplete?.id || phases[0].id
  }, [phases])

  const [expandedPhase, setExpandedPhase] = useState(defaultExpandedPhase)

  useEffect(() => {
    setExpandedPhase(defaultExpandedPhase)
  }, [defaultExpandedPhase, activeRun?.id])

  const runLabel = summarizeRunSource(activeRun)
  const activeTone = statusTone(activeRun?.status)
  const isFailedRun = String(activeRun?.status || '').toUpperCase() === 'FAILED'
  const isStageConfirmationPaused = String(activeRun?.status || '').toUpperCase() === 'PAUSED_FOR_STAGE_CONFIRMATION'
  const [dismissedFailureBannerFor, setDismissedFailureBannerFor] = useState<string | null>(null)
  const [autoAdvanceStages, setAutoAdvanceStages] = useState(false)
  const [stageConfirmSubmitting, setStageConfirmSubmitting] = useState(false)

  useEffect(() => {
    if (!isFailedRun) {
      setDismissedFailureBannerFor(null)
    } else if (dismissedFailureBannerFor && dismissedFailureBannerFor !== activeRun?.id) {
      setDismissedFailureBannerFor(null)
    }
  }, [activeRun?.id, dismissedFailureBannerFor, isFailedRun])

  const failureSummary = useMemo(() => buildFailureSummary(activeRun), [activeRun])
  const stageConfirmation = activeRun?.stage_confirmation || null

  if (!activeRun) {
    return (
      <div className="flex min-h-[620px] items-center justify-center rounded-lg border border-[#253044] bg-[#111827]">
        <div className="text-center">
          <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-lg border border-[#253044] bg-[#0b1120] text-slate-500">
            <Play size={24} />
          </div>
          <div className="mt-5 text-xl font-semibold text-white">No active pipeline</div>
          <p className="mt-2 text-sm text-slate-400">Start a new run from the top-right action.</p>
        </div>
      </div>
    )
  }

  const handleUnavailableFailureAction = (label) => {
    addNotification({
      type: 'amber',
      title: `${label} not available`,
      message: 'The backend does not expose this recovery action yet. The UI banner is ready once those endpoints are added.',
      duration: 4500
    })
  }

  const handleContinueStage = async () => {
    if (!activeRun?.id) return
    setStageConfirmSubmitting(true)
    try {
      await continueStage(activeRun.id, autoAdvanceStages)
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

  return (
    <div className="flex h-full min-h-[calc(100vh-116px)] flex-col">
      <div className="mb-7 flex flex-col gap-4">
        <div className="flex min-h-[72px] items-center justify-between rounded-xl border border-[#1d2940] bg-[#09111f] px-5">
          <div className="min-w-0 pr-4 text-[14px] font-semibold tracking-[0.12em] text-[#8ea0c3]">
            <span className="truncate">Pipeline - {runLabel} - Run ID - {activeRun.id}</span>
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
              {String(activeRun.status || 'Waiting').replace(/_/g, ' ')}
            </div>
          </div>
        </div>

        {isFailedRun && dismissedFailureBannerFor !== activeRun.id && (
          <div className="flex flex-wrap items-center justify-between gap-4 rounded-2xl border border-red-500/35 bg-[#17111d] px-6 py-4 shadow-[0_12px_40px_rgba(0,0,0,0.22)]">
            <div className="flex min-w-0 items-center gap-4">
              <div className="flex h-10 w-10 flex-shrink-0 items-center justify-center rounded-xl border border-red-500/30 bg-red-500/10 text-red-400">
                <AlertTriangle size={18} />
              </div>
              <div className="min-w-0">
                <div className="flex flex-wrap items-center gap-3 text-sm">
                  <div className="flex min-w-0 items-center gap-2 font-semibold text-white">
                    <FileText size={15} className="text-[#b8c3d9]" />
                    <span className="max-w-[420px] truncate">{activeRun.brd_filename || 'BRD File Name'}</span>
                  </div>
                  <span className="rounded-lg border border-red-500/35 bg-red-500/12 px-2.5 py-1 text-xs font-semibold text-red-400">
                    Failed
                  </span>
                  <span className="text-[#d4d9e5]">at `{failureSummary.failedStage}`</span>
                  <span className="text-[#9da7bb]">{failureSummary.progressLabel}</span>
                </div>
                <div className="mt-2 flex items-center gap-2 text-sm text-[#9da7bb]">
                  <Clock3 size={14} />
                  {failureSummary.timeAgo}
                </div>
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-3">
              <button
                onClick={() => handleUnavailableFailureAction('Retry Failed Stage')}
                className="inline-flex h-11 items-center gap-2 rounded-xl border border-amber-500/35 bg-amber-500/10 px-5 text-sm font-semibold text-amber-400 transition-colors hover:bg-amber-500/15"
              >
                <RotateCcw size={16} />
                Retry Failed Stage
              </button>
              <button
                onClick={() => handleUnavailableFailureAction('Resume from Failure')}
                className="inline-flex h-11 items-center gap-2 rounded-xl border border-[#3f82ff]/40 bg-[#3f82ff]/10 px-5 text-sm font-semibold text-[#3f82ff] transition-colors hover:bg-[#3f82ff]/15"
              >
                <Play size={16} />
                Resume from Failure
              </button>
              <button
                onClick={() => handleUnavailableFailureAction('Restart')}
                className="inline-flex h-11 items-center gap-2 rounded-xl border border-[#2e394d] bg-[#101827] px-5 text-sm font-semibold text-white transition-colors hover:bg-[#152033]"
              >
                <RefreshCcw size={16} />
                Restart
              </button>
              <button
                onClick={() => setDismissedFailureBannerFor(activeRun.id)}
                className="flex h-11 w-11 items-center justify-center rounded-xl border border-[#2e394d] bg-transparent text-[#8d96a9] transition-colors hover:bg-white/5 hover:text-white"
                aria-label="Dismiss failure banner"
              >
                <X size={16} />
              </button>
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
                  {run.brd_filename || run.id}
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
            {phases.map((phase, index) => {
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
                      <StatusPill status={phase.status} tone={tone} compact={!expanded} />
                      {expanded ? <ChevronUp size={14} className="text-[#64748b]" /> : <ChevronDown size={14} className="text-[#64748b]" />}
                    </div>
                  </button>

                  {expanded && (
                    <div className="bg-[#080e1d] px-6 pb-6 pt-1">
                      <div className="ml-[18px] border-l border-[#2b3648] pl-7">
                        <div className="space-y-5">
                          {phase.steps.map((step) => (
                            <StepRow key={step.key} step={step} />
                          ))}
                        </div>
                      </div>
                    </div>
                  )}
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
              {String(activeRun.status || 'Waiting').replace(/_/g, ' ')}
            </div>
          </div>

          <div className="min-h-0 flex-1">
            <PipelineLogsPanel runId={activeRun.run_id || activeRun.id} isActive />
          </div>
        </section>
      </div>

      {isStageConfirmationPaused && stageConfirmation?.awaiting_confirmation && (
        <div className="fixed inset-0 z-40 flex items-center justify-center bg-black/45 px-6 backdrop-blur-sm">
          <div className="w-full max-w-[670px] overflow-hidden rounded-[26px] border border-[#24344d] bg-[#131d2f] shadow-[0_24px_80px_rgba(0,0,0,0.35)]">
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

            <div className="border-t border-[#27374f] px-8 py-6">
              <div className="rounded-[20px] border border-[#29456d] bg-[#16233b] px-6 py-5">
                <div className="text-sm text-[#8ea2c5]">Next stage</div>
                <div className="mt-1 text-[17px] font-semibold text-white">
                  {stageConfirmation.next_stage_label || 'Next Stage'}
                </div>
              </div>

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

  const failedStep = steps.find((step) => String(step?.state || step?.status || '').toUpperCase() === 'FAILED')
  const completedCount = steps.filter((step) => {
    const state = String(step?.state || step?.status || '').toUpperCase()
    return state === 'COMPLETED' || state === 'SUCCESS'
  }).length

  return {
    failedStage: failedStep?.key || failedStep?.id || failedStep?.name || 'stage_unknown',
    progressLabel: `${completedCount}/${steps.length || 0} stages done`,
    timeAgo: formatTimeAgo(run?.completed_at || run?.updated_at || run?.started_at),
  }
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

function StatusPill({ status, tone, compact }) {
  const label = compact ? status : status === 'Waiting' ? 'Review' : status
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

function StepRow({ step }) {
  const complete = step.state === 'COMPLETED'
  const waiting = step.state === 'HITL_WAIT'
  const running = step.state === 'RUNNING'
  const failed = step.state === 'FAILED'

  return (
    <div className="flex items-center gap-4">
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
          {waiting ? <Users size={14} /> : complete ? <CheckCircle2 size={14} /> : <Circle size={running ? 9 : 11} className={running ? 'animate-pulse' : ''} />}
        </div>
      </div>
      <div className="min-w-0">
        <div className={`truncate text-[14px] font-semibold ${complete || waiting || running ? 'text-white' : 'text-[#7d8daa]'}`}>
          {step.label}
        </div>
        {step.detail ? <div className="mt-0.5 truncate text-xs text-[#64748b]">{step.detail}</div> : null}
      </div>
    </div>
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

export default PipelineMonitor
