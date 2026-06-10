// @ts-nocheck
import React, { useEffect, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { AlertCircle, CheckCircle2, ChevronDown, ChevronRight, ChevronUp, Circle, Play, Users } from 'lucide-react'
import useAthenaStore from '../store/useAthenaStore'
import PipelineLogsPanel from '../components/pipeline/PipelineLogsPanel'
import { getPhaseGroups, statusTone, summarizeRunSource } from '../utils/pipelinePhases'
import { getRun, getRuns } from '../api/athenaApi'

function PipelineMonitor() {
  const navigate = useNavigate()
  const { runs, activeRunId, setActiveRun, setRuns, updateRun, setServerOnline } = useAthenaStore()
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

  const runLabel = summarizeRunSource(activeRun)
  const activeTone = statusTone(activeRun.status)
  const activeGate = Number(activeRun.next_gate || 0)
  const hasGateReview = activeGate >= 1 && activeGate <= 5

  const openGateReview = () => {
    setActiveRun(activeRun.id)
    navigate('/app/hitl')
  }

  return (
    <div className="flex h-full min-h-[calc(100vh-116px)] flex-col">
      <div className="mb-7 flex items-center justify-between">
        <div className="text-[14px] font-semibold tracking-[0.24em] text-[#7d8daa]">
          Pipeline - {runLabel}
        </div>
        <div className="flex items-center gap-3">
          {hasGateReview && (
            <button
              onClick={openGateReview}
              className="inline-flex h-9 items-center gap-2 rounded-md border border-amber-400/35 bg-amber-500/10 px-3 text-xs font-semibold text-amber-200 transition-colors hover:bg-amber-500/15"
            >
              <AlertCircle size={14} />
              Review Gate {activeGate}
              <ChevronRight size={14} />
            </button>
          )}
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

      {hasGateReview && (
        <button
          onClick={openGateReview}
          className="mb-5 flex w-full items-center justify-between rounded-lg border border-amber-400/25 bg-amber-500/10 px-4 py-3 text-left text-amber-100 transition-colors hover:bg-amber-500/15"
        >
          <div className="flex min-w-0 items-center gap-3">
            <AlertCircle size={18} className="flex-shrink-0 text-amber-300" />
            <div className="min-w-0">
              <div className="text-sm font-semibold">Gate {activeGate} review required</div>
              <div className="mt-0.5 truncate text-xs text-amber-100/70">
                {activeRun.resume_message || 'Review the pending artifact before the pipeline continues.'}
              </div>
            </div>
          </div>
          <div className="ml-4 flex flex-shrink-0 items-center gap-2 text-xs font-semibold text-amber-200">
            Open review
            <ChevronRight size={15} />
          </div>
        </button>
      )}

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
    </div>
  )
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

export default PipelineMonitor
