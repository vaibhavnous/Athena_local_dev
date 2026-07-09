// @ts-nocheck
import React, { useEffect, useMemo, useState } from 'react'
import {
  CalendarDays,
  CheckCircle2,
  ChevronDown,
  FileText,
  Info,
  RefreshCw,
  Search,
} from 'lucide-react'
import useAthenaStore from '../store/useAthenaStore'
import { getRun, getRuns } from '../api/athenaApi'
import { getPhaseGroups, statusTone } from '../utils/pipelinePhases'

const FILTERS = ['All', 'Running', 'Completed', 'Failed', 'Cancelled', 'Hitl wait']

function matchesStatusFilter(statusValue, filterValue) {
  const status = String(statusValue || '').toUpperCase()
  if (filterValue === 'All') return true
  if (filterValue === 'Running') return ['RUNNING', 'PROCESSING', 'SUBMITTED', 'IN_PROGRESS'].includes(status)
  if (filterValue === 'Completed') return ['SUCCESS', 'COMPLETED', 'PIPELINE_COMPLETED'].includes(status)
  if (filterValue === 'Failed') return status === 'FAILED'
  if (filterValue === 'Cancelled') return ['ABORTED', 'CANCELLED', 'CANCELED'].includes(status)
  if (filterValue === 'Hitl wait') return ['HITL_WAIT', 'PAUSED_FOR_HITL', 'PENDING_REVIEW'].includes(status) || status.includes('HITL')
  return status === String(filterValue || '').toUpperCase()
}

function isRunningStatus(statusValue) {
  return ['RUNNING', 'PROCESSING', 'SUBMITTED', 'IN_PROGRESS'].includes(String(statusValue || '').toUpperCase())
}

function RunHistoryPage() {
  const { runs, setRuns, updateRun, setServerOnline } = useAthenaStore()
  const [selectedRunId, setSelectedRunId] = useState(null)
  const [query, setQuery] = useState('')
  const [filter, setFilter] = useState('All')
  const [detailRun, setDetailRun] = useState(null)

  useEffect(() => {
    if (!selectedRunId && runs[0]?.id) setSelectedRunId(runs[0].id)
  }, [runs, selectedRunId])

  useEffect(() => {
    let cancelled = false

    const loadRuns = async () => {
      try {
        const data = await getRuns()
        if (!cancelled && Array.isArray(data)) {
          setRuns(data)
          setServerOnline(true)
        }
      } catch (error) {
        if (!cancelled) setServerOnline(false)
        if (!cancelled) console.warn('[RunHistoryPage] Failed to refresh runs', error)
      }
    }

    loadRuns()
    const timer = window.setInterval(loadRuns, 8000)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [setRuns, setServerOnline])

  useEffect(() => {
    if (!selectedRunId) return
    let cancelled = false

    const loadRun = async () => {
      try {
        const data = await getRun(selectedRunId)
        if (!cancelled) {
          setDetailRun(data)
          updateRun(selectedRunId, data)
          setServerOnline(true)
        }
      } catch (error) {
        if (!cancelled) setServerOnline(false)
        if (!cancelled) console.warn('[RunHistoryPage] Failed to load run detail', error)
      }
    }

    loadRun()
    const timer = window.setInterval(loadRun, 5000)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [selectedRunId, updateRun, setServerOnline])

  const filteredRuns = useMemo(() => {
    return (runs || []).filter((run) => {
      const text = `${run.id} ${run.brd_filename || ''}`.toLowerCase()
      const queryMatch = !query.trim() || text.includes(query.trim().toLowerCase())
      const filterMatch = matchesStatusFilter(run.status, filter)
      return queryMatch && filterMatch
    })
  }, [runs, query, filter])

  const selectedRun =
    (detailRun && detailRun.id === selectedRunId ? detailRun : null) ||
    filteredRuns.find((run) => run.id === selectedRunId) ||
    runs.find((run) => run.id === selectedRunId) ||
    null

  const phases = getPhaseGroups(selectedRun)

  return (
    <div className="flex h-full min-h-[calc(100vh-116px)] flex-col">
      <div className="mb-5 flex items-center justify-between border-b border-[#253044] pb-5">
        <div className="flex items-center gap-3">
          <RefreshCw size={20} className="text-[#3f82ff]" />
          <h1 className="text-[22px] font-semibold text-white">Pipeline History</h1>
          <span className="rounded-full border border-[#253044] bg-[#0b1120] px-3 py-1 text-sm text-white">
            {runs.length} runs
          </span>
        </div>
        <button
          onClick={async () => {
            const data = await getRuns()
            setRuns(Array.isArray(data) ? data : [])
          }}
          className="inline-flex h-10 items-center gap-2 rounded-lg border border-[#253044] bg-[#202b3b] px-4 text-sm font-semibold text-white transition-colors hover:bg-[#263448]"
        >
          <RefreshCw size={15} />
          Refresh
        </button>
      </div>

      <div className="grid min-h-0 flex-1 xl:grid-cols-[384px_minmax(0,1fr)]">
        <section className="flex min-h-0 flex-col border-r border-[#253044] pr-4">
          <div className="pb-4">
            <div className="relative">
              <Search size={16} className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-[#8a9ab7]" />
              <input
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder="Search by discovered run ID or filename..."
                className="h-10 w-full rounded-lg border border-[#253044] bg-[#080e1d] pl-10 pr-3 text-sm text-white outline-none transition-colors placeholder:text-[#8a9ab7] focus:border-[#3f82ff]"
              />
            </div>
            <div className="mt-3 flex flex-wrap gap-2">
              {FILTERS.map((item) => (
                <button
                  key={item}
                  onClick={() => setFilter(item)}
                  className={`rounded-full border px-3 py-1.5 text-xs font-semibold transition-colors ${
                    filter === item
                      ? 'border-[#3f82ff] bg-[#1f325d] text-[#3f82ff]'
                      : 'border-[#253044] bg-[#080e1d] text-white hover:bg-[#111827]'
                  }`}
                >
                  {item}
                </button>
              ))}
            </div>
          </div>

          <div className="min-h-0 flex-1 overflow-y-auto border-t border-[#253044]">
            {filteredRuns.map((run) => {
              const active = run.id === selectedRunId
              const tone = statusTone(run.status)
              return (
                <button
                  key={run.id}
                  onClick={() => setSelectedRunId(run.id)}
                  className={`w-full border-b border-[#253044] px-5 py-4 text-left transition-colors ${
                    active ? 'bg-[#101735] shadow-[inset_3px_0_0_0_#3f82ff]' : 'hover:bg-[#111827]'
                  }`}
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <div className="flex items-center gap-2">
                        <FileText size={15} className="text-slate-300" />
                        <div className="truncate text-sm font-semibold text-white">
                          {run.brd_filename || 'Untitled run'}
                        </div>
                      </div>
                      <div className="mt-3 flex items-center gap-3 text-xs text-white">
                        <span className="truncate font-mono">{String(run.id).slice(0, 9)}...</span>
                        <span className="flex items-center gap-1">
                          <CalendarDays size={12} />
                          {formatCompactDate(run.started_at)}
                        </span>
                      </div>
                    </div>
                    <StatusPill status={run.status} tone={tone} />
                  </div>
                </button>
              )
            })}
          </div>
        </section>

        <section className="min-h-0 overflow-y-auto pl-6">
          {selectedRun ? (
            <div>
              <div className="mb-6 flex items-start justify-between gap-4">
                <div className="min-w-0">
                  <h2 className="truncate text-[20px] font-semibold uppercase text-white">
                    {selectedRun.brd_filename || 'Untitled run'}
                  </h2>
                  <div className="mt-2 font-mono text-xs text-white">{selectedRun.id}</div>
                </div>
                <div className="flex items-center gap-4">
                  <StatusPill status={selectedRun.status} tone={statusTone(selectedRun.status)} large />
                  <RefreshCw size={18} className="text-white" />
                </div>
              </div>

              <div className="rounded-lg border border-[#253044] bg-[#111827] px-5 py-5">
                <div className="mb-4 text-sm font-semibold text-white">Run Info</div>
                <div className="grid gap-3 text-sm">
                  <InfoRow icon={Info} label="Project Name" value={selectedRun.project_name || selectedRun.brd_filename || '-'} />
                  <InfoRow icon={Info} label="Project Description" value={selectedRun.project_description || 'NA'} />
                  <InfoRow icon={Info} label="Source" value={formatSource(selectedRun.source)} />
                  <InfoRow icon={Info} label="Database Type" value={selectedRun.database_type || '-'} />
                  <InfoRow icon={Info} label="Database Name" value={selectedRun.database_name || '-'} />
                  <InfoRow icon={CalendarDays} label="Started" value={formatFullDate(selectedRun.started_at)} />
                  <InfoRow icon={CalendarDays} label="Last Updated" value={formatFullDate(selectedRun.completed_at || selectedRun.updated_at || selectedRun.started_at)} />
                  <InfoRow icon={FileText} label="Knowledge Base" value={selectedRun.knowledge_base || 'Not used'} />
                </div>
              </div>

              <div className="mt-7">
                <div className="mb-3 text-[10px] font-semibold uppercase tracking-[0.28em] text-[#6e7b96]">
                  Pipeline - Business Requirements Document - {selectedRun.brd_filename || 'Untitled run'}
                </div>
                <div className="space-y-2 rounded-xl border border-[#1f2c45] bg-[#070d1b] p-3 shadow-[0_18px_50px_rgba(0,0,0,0.28)]">
                  {phases.map((phase, index) => (
                    <PhaseRow key={phase.id} phase={phase} index={index + 1} />
                  ))}
                </div>
              </div>
            </div>
          ) : (
            <div className="flex h-full min-h-[420px] items-center justify-center rounded-lg border border-[#253044] bg-[#111827]">
              <div className="text-center">
                <div className="text-lg font-semibold text-white">No run selected</div>
                <p className="mt-2 text-sm text-slate-400">Pick a pipeline run from the left column.</p>
              </div>
            </div>
          )}
        </section>
      </div>
    </div>
  )
}

function InfoRow({ icon: Icon, label, value }) {
  return (
    <div className="grid grid-cols-[170px_minmax(0,1fr)] items-start gap-4">
      <div className="flex items-center gap-2 text-white">
        <Icon size={14} className="text-slate-300" />
        <span>{label}</span>
      </div>
      <div className="min-w-0 break-words font-mono text-white">{value || '-'}</div>
    </div>
  )
}

function PhaseRow({ phase, index }) {
  const [expanded, setExpanded] = useState(true)
  const displaySteps = getHistoryDisplaySteps(phase)
  const completed = displaySteps.filter((step) => isCompletedStep(step.state)).length
  const total = displaySteps.length || phase.total || 0
  const tone = statusTone(phase.status)
  const done = phase.status === 'Done'
  const running = phase.status === 'Running'
  const review = phase.status === 'Review'
  const toneText =
    tone === 'emerald'
      ? 'text-emerald-400'
      : tone === 'blue'
      ? 'text-[#3f82ff]'
      : tone === 'amber'
      ? 'text-amber-300'
      : tone === 'red'
      ? 'text-red-400'
      : 'text-[#64748b]'

  return (
    <div className="overflow-hidden rounded-lg border border-[#1d2940] bg-[#090f20]">
      <button
        type="button"
        onClick={() => setExpanded((value) => !value)}
        className="flex w-full items-center justify-between px-4 py-3 text-left transition-colors hover:bg-[#0d1730]"
      >
        <div className="flex min-w-0 items-center gap-3">
          <div className="relative h-8 w-8 flex-shrink-0">
            {running && (
              <>
                <span className="absolute inset-0 rounded-full border border-[#27d6a2]/35 animate-ping" />
                <span className="absolute inset-0 rounded-full border-2 border-transparent border-t-[#27d6a2] animate-spin" />
              </>
            )}
            <div className={`relative flex h-8 w-8 items-center justify-center rounded-full border bg-[#0c1426] ${
              done
                ? 'border-emerald-500 text-emerald-400'
                : running
                ? 'border-[#3f82ff] text-[#3f82ff]'
                : review
                ? 'border-amber-300 text-amber-300'
                : 'border-[#263753] text-[#60708d]'
            }`}>
              {done ? <CheckCircle2 size={15} /> : <span className="text-xs font-bold">{index}</span>}
            </div>
          </div>
          <div className="min-w-0">
            <div className={`truncate text-sm font-bold ${done || running || review ? 'text-white' : 'text-[#8da1c8]'}`}>
              {phase.label}
            </div>
            <div className="mt-1 text-xs text-[#91a4cb]">{completed}/{total} stages complete</div>
          </div>
        </div>
        <div className={`flex items-center gap-3 text-xs font-bold ${toneText}`}>
          <span className={`h-2 w-2 rounded-full bg-current ${running ? 'animate-pulse' : ''}`} />
          {phase.status}
          <ChevronDown size={15} className={`text-[#667795] transition-transform ${expanded ? 'rotate-180' : ''}`} />
        </div>
      </button>

      {expanded && (
        <div className="pb-4 pl-9 pr-4">
          <div className="border-l border-[#23324f] pl-4">
            {displaySteps.map((step) => (
              <StageTreeRow key={step.key} step={step} />
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function StageTreeRow({ step }) {
  const state = String(step.state || '').toUpperCase()
  const done = isCompletedStep(state)
  const running = ['RUNNING', 'PROCESSING', 'IN_PROGRESS', 'SUBMITTED'].includes(state)
  const review = ['HITL_WAIT', 'PAUSED_FOR_HITL', 'PENDING_REVIEW'].includes(state) || step.key.includes('review') || step.key.startsWith('gate')
  const failed = state === 'FAILED'
  const toneClass = failed
    ? 'border-red-400 text-red-400'
    : done
    ? 'border-emerald-500 text-emerald-400'
    : running
    ? 'border-[#3f82ff] text-[#3f82ff]'
    : review
    ? 'border-amber-300 text-amber-300'
    : 'border-[#2a3a58] text-[#6f809e]'

  return (
    <div className="relative flex min-h-[36px] items-center gap-3">
      <span className="absolute -left-[21px] top-1/2 h-px w-4 -translate-y-1/2 bg-[#23324f]" />
      <span className={`flex h-4 w-4 flex-shrink-0 items-center justify-center rounded-full border bg-[#0b1326] ${toneClass}`}>
        <span className={`h-1.5 w-1.5 rounded-full bg-current ${running ? 'animate-pulse' : ''}`} />
      </span>
      <div className={`text-xs font-bold ${done || running || review ? 'text-white' : 'text-[#7f91b4]'}`}>
        {step.label}
      </div>
    </div>
  )
}

function getHistoryDisplaySteps(phase) {
  const steps = Array.isArray(phase.steps) ? phase.steps : []
  const byKey = new Map(steps.map((step) => [step.key, step]))
  const phaseState = phaseStatusToStepState(phase.status)

  const actual = (key, label, fallbackState = phaseState) => {
    const step = byKey.get(key)
    return {
      key,
      label,
      state: step?.state || fallbackState,
      detail: step?.detail || '',
    }
  }

  if (phase.id === 'phase-1') {
    return clampLinearHistorySteps([
      actual('ingestion', 'BRD Ingest'),
      actual('memory', 'Memory Check'),
      actual('requirements', 'Requirement Extraction'),
      actual('kpis', 'KPI Extraction'),
      actual('gate1', 'KPI Review', reviewAwareState(byKey.get('gate1'), phase)),
    ])
  }

  if (phase.id === 'phase-2') {
    return clampLinearHistorySteps([
      actual('nomination', 'Table Extraction'),
      actual('gate2', 'Table Review', reviewAwareState(byKey.get('gate2'), phase)),
      actual('discovery', 'Column Extraction', byKey.get('discovery')?.state || byKey.get('schema')?.state || phaseState),
      actual('profiling', 'Column Profiling', byKey.get('profiling')?.state || phaseState),
      actual('enrichment', 'Semantic Enrichment', byKey.get('enrichment')?.state || phaseState),
      actual('gate3', 'Semantic Review', reviewAwareState(byKey.get('gate3'), phase)),
    ])
  }

  if (phase.id === 'phase-3') {
    return clampLinearHistorySteps([
      actual('bronze', 'Bronze Code Generation'),
      actual('gate4', 'Bronze Review', reviewAwareState(byKey.get('gate4'), phase)),
    ])
  }

  if (phase.id === 'phase-4') {
    const silverState = byKey.get('silver')?.state || phaseState
    const gate4State = reviewAwareState(byKey.get('gate4'), phase)
    const gate5State = reviewAwareState(byKey.get('gate5'), phase)
    const mergeReviewState = byKey.get('silver_merge_key_review')?.state
    const silverFlow = buildHistorySilverPhaseStates(silverState, gate4State, gate5State, phase.status)
    return clampLinearHistorySteps([
      actual('silver_merge_key_resolution', 'Silver Merge Key Resolution', silverFlow.mergeResolution),
      actual('silver_merge_key_review', 'Silver Merge Key Review', mergeReviewState || silverFlow.mergeReview),
      actual('silver', 'Silver Code Generation', silverFlow.codeGeneration),
      actual('gate5', 'Silver Review', silverFlow.reviewGate),
      actual('silver_code_execution', 'Silver Code Execution'),
    ])
  }

  if (phase.id === 'phase-5') {
    return clampLinearHistorySteps([
      actual('gold', 'Gold Code Generation'),
      actual('gold_code_execution', 'Gold Code Execution'),
    ])
  }

  return clampLinearHistorySteps(steps.map((step) => ({ ...step, label: step.label || step.key })))
}

function clampLinearHistorySteps(steps = []) {
  let blocked = false
  return steps.map((step) => {
    const state = String(step.state || '').toUpperCase()
    const complete = isCompletedStep(state)
    if (!blocked && complete) return step
    if (!blocked) {
      blocked = true
      return step
    }
    return { ...step, state: 'PENDING' }
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

function reviewAwareState(step, phase) {
  if (step?.state) return step.state
  if (phase.status === 'Review') return 'HITL_WAIT'
  return phaseStatusToStepState(phase.status)
}

function buildHistorySilverPhaseStates(silverState, gate4State, gate5State, phaseStatus) {
  const normalizedSilver = String(silverState || '').toUpperCase()
  const normalizedGate4 = String(gate4State || '').toUpperCase()
  const normalizedGate = String(gate5State || '').toUpperCase()
  const normalizedPhase = String(phaseStatus || '').toLowerCase()

  if (normalizedPhase === 'done') {
    return {
      mergeResolution: 'COMPLETED',
      mergeReview: 'COMPLETED',
      codeGeneration: 'COMPLETED',
      reviewGate: 'COMPLETED',
    }
  }

  if (normalizedGate === 'HITL_WAIT' || normalizedGate === 'PAUSED_FOR_HITL') {
    return {
      mergeResolution: 'COMPLETED',
      mergeReview: 'COMPLETED',
      codeGeneration: 'COMPLETED',
      reviewGate: 'HITL_WAIT',
    }
  }

  if (normalizedGate4 === 'HITL_WAIT' || normalizedGate4 === 'PAUSED_FOR_HITL') {
    return {
      mergeResolution: 'COMPLETED',
      mergeReview: 'HITL_WAIT',
      codeGeneration: 'PENDING',
      reviewGate: 'PENDING',
    }
  }

  if (normalizedSilver === 'RUNNING') {
    return {
      mergeResolution: 'COMPLETED',
      mergeReview: 'COMPLETED',
      codeGeneration: 'RUNNING',
      reviewGate: 'PENDING',
    }
  }

  if (normalizedSilver === 'COMPLETED' || normalizedSilver === 'SUCCESS' || normalizedSilver === 'PIPELINE_COMPLETED') {
    if (!normalizedGate || normalizedGate === 'PENDING') {
      return {
        mergeResolution: 'COMPLETED',
        mergeReview: 'HITL_WAIT',
        codeGeneration: 'PENDING',
        reviewGate: 'PENDING',
      }
    }

    return {
      mergeResolution: 'COMPLETED',
      mergeReview: 'COMPLETED',
      codeGeneration: 'COMPLETED',
      reviewGate: normalizedGate || 'PENDING',
    }
  }

  if (normalizedPhase === 'failed' || normalizedSilver === 'FAILED') {
    return {
      mergeResolution: 'FAILED',
      mergeReview: 'PENDING',
      codeGeneration: 'PENDING',
      reviewGate: 'PENDING',
    }
  }

  return {
    mergeResolution: 'PENDING',
    mergeReview: 'PENDING',
    codeGeneration: 'PENDING',
    reviewGate: normalizedGate || 'PENDING',
  }
}

function isCompletedStep(state) {
  return ['COMPLETED', 'SUCCESS', 'PIPELINE_COMPLETED'].includes(String(state || '').toUpperCase())
}

function StatusPill({ status, tone, large = false }) {
  const toneClasses = {
    emerald: 'border-emerald-400/25 bg-emerald-500/10 text-emerald-400',
    blue: 'border-[#3f82ff]/35 bg-[#3f82ff]/10 text-[#3f82ff]',
    amber: 'border-amber-400/35 bg-amber-500/10 text-amber-300',
    red: 'border-red-400/35 bg-red-500/10 text-red-400',
    slate: 'border-[#253044] bg-[#0b1120] text-slate-300',
  }
  return (
    <div className={`inline-flex items-center gap-2 rounded-full border font-semibold ${toneClasses[tone] || toneClasses.slate} ${
      large ? 'px-4 py-2 text-sm' : 'px-3 py-1.5 text-xs'
    }`}>
      <span className={`h-2 w-2 rounded-full bg-current ${isRunningStatus(status) ? 'animate-pulse' : ''}`} />
      {statusLabel(status)}
    </div>
  )
}

function statusLabel(status) {
  const value = String(status || '').replace(/_/g, ' ').trim()
  return value ? value[0].toUpperCase() + value.slice(1).toLowerCase() : 'Pending'
}

function formatSource(value) {
  const source = String(value || 'database')
  if (source === 'adls_gen2') return 'ADLS Gen2'
  if (source === 'sftp') return 'SFTP'
  return 'Database'
}

function formatCompactDate(value) {
  if (!value) return 'Unknown'
  return new Date(value).toLocaleString('en-IN', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function formatFullDate(value) {
  if (!value) return '-'
  return new Date(value).toLocaleString('en-IN', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

export default RunHistoryPage
