// @ts-nocheck
import React, { useEffect, useMemo, useRef, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import {
  CalendarDays,
  CheckCircle2,
  ChevronDown,
  Clock,
  FileText,
  History,
  Info,
  RefreshCw,
  Search,
} from 'lucide-react'
import useAthenaStore from '../store/useAthenaStore'
import { getRun, getRuns } from '../api/athenaApi'
import { PageHeader } from '../components/shared/DashboardLayout'
import { getPhaseGroups, normalizeState, statusTone } from '../utils/pipelinePhases'

const FILTERS = ['All', 'Running', 'Pending', 'Hitl wait', 'Paused for hitl', 'Pending review', 'Completed', 'Failed', 'Cancelled']

function matchesStatusFilter(statusValue, filterValue) {
  const status = normalizeState(statusValue)
  const rawStatus = String(statusValue || '').toUpperCase()
  if (filterValue === 'All') return true
  if (filterValue === 'Running') return status === 'RUNNING'
  if (filterValue === 'Pending') return rawStatus === 'PENDING'
  if (filterValue === 'Completed') return status === 'COMPLETED'
  if (filterValue === 'Failed') return status === 'FAILED'
  if (filterValue === 'Cancelled') return ['ABORTED', 'CANCELLED', 'CANCELED'].includes(rawStatus)
  if (filterValue === 'Hitl wait') return rawStatus === 'HITL_WAIT'
  if (filterValue === 'Paused for hitl') return rawStatus === 'PAUSED_FOR_HITL'
  if (filterValue === 'Pending review') return rawStatus === 'PENDING_REVIEW'
  return status === String(filterValue || '').toUpperCase()
}

function isRunningStatus(statusValue) {
  return normalizeState(statusValue) === 'RUNNING'
}

function RunHistoryPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const { runs, setRuns, updateRun, setServerOnline } = useAthenaStore()
  const [selectedRunId, setSelectedRunId] = useState(searchParams.get('runId'))
  const [query, setQuery] = useState('')
  const [filter, setFilter] = useState('All')
  const [detailRun, setDetailRun] = useState(null)
  const [runInfoOpen, setRunInfoOpen] = useState(false)
  const runsRequestInFlightRef = useRef(false)
  const detailRequestInFlightRef = useRef<string | null>(null)

  useEffect(() => {
    if (!selectedRunId && runs[0]?.id) setSelectedRunId(runs[0].id)
  }, [runs, selectedRunId])

  const selectRun = (runId) => {
    setSelectedRunId(runId)
    const nextParams = new URLSearchParams(searchParams)
    nextParams.set('runId', runId)
    setSearchParams(nextParams, { replace: true })
  }

  useEffect(() => {
    setRunInfoOpen(false)
  }, [selectedRunId])

  useEffect(() => {
    let cancelled = false

    const loadRuns = async () => {
      if (runsRequestInFlightRef.current) return
      runsRequestInFlightRef.current = true
      try {
        const data = await getRuns()
        if (!cancelled && Array.isArray(data)) {
          setRuns(data)
          setServerOnline(true)
        }
      } catch (error) {
        if (!cancelled) setServerOnline(false)
        if (!cancelled) console.warn('[RunHistoryPage] Failed to refresh runs', error)
      } finally {
        runsRequestInFlightRef.current = false
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
      if (detailRequestInFlightRef.current === selectedRunId) return
      detailRequestInFlightRef.current = selectedRunId
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
      } finally {
        if (detailRequestInFlightRef.current === selectedRunId) {
          detailRequestInFlightRef.current = null
        }
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
    <div className="flex min-h-full flex-col gap-4 lg:h-full lg:min-h-0">
      <PageHeader
        eyebrow="Run History"
        title="Pipeline run history."
        description={`${filteredRuns.length}${filteredRuns.length !== runs.length ? ` of ${runs.length}` : ''} run${runs.length === 1 ? '' : 's'} available for inspection and rerun.`}
        icon={History}
        actions={
          <button
            onClick={async () => {
              const data = await getRuns()
              setRuns(Array.isArray(data) ? data : [])
            }}
            className="btn-secondary inline-flex items-center justify-center gap-2 text-xs"
          >
            <RefreshCw size={13} />
            Refresh
          </button>
        }
      />

      <div className="flex flex-col overflow-hidden rounded-lg border border-[#253044] bg-[#111827] shadow-card lg:min-h-0 lg:flex-1 lg:flex-row">
        <section className="flex max-h-[440px] min-h-[320px] flex-col border-b border-[#253044] lg:h-full lg:max-h-none lg:min-h-0 lg:w-80 lg:flex-shrink-0 lg:border-b-0 lg:border-r">
          <div className="flex-shrink-0 border-b border-[#253044] p-3">
            <div className="relative">
              <Search size={13} className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-[#8a9ab7]" />
              <input
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder="Search by discovered run ID or filename..."
                className="h-9 w-full rounded-lg border border-[#253044] bg-[#080e1d] pl-9 pr-3 text-xs text-white outline-none transition-colors placeholder:text-[#8a9ab7] focus:border-[#3f82ff]"
              />
            </div>
            <div className="mt-2 flex flex-wrap gap-1.5">
              {FILTERS.map((item) => (
                <button
                  key={item}
                  onClick={() => setFilter(item)}
                  className={`rounded-full border px-2.5 py-1 text-[10px] font-medium transition-colors ${
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

          <div className="min-h-0 flex-1 overflow-y-auto">
            {filteredRuns.map((run) => {
              const active = run.id === selectedRunId
              const tone = statusTone(run.status)
              return (
                <button
                  key={run.id}
                  onClick={() => selectRun(run.id)}
                  className={`w-full border-b border-[#253044] border-l-2 px-4 py-3 text-left transition-colors ${
                    active ? 'border-l-[#3f82ff] bg-[#101735]' : 'border-l-transparent hover:bg-[#151f31]'
                  }`}
                >
                  <div className="mb-1.5 flex items-start justify-between gap-2">
                    <div className="min-w-0">
                      <div className="flex items-center gap-2">
                        <FileText size={12} className="mt-0.5 flex-shrink-0 text-[#8a9ab7]" />
                        <div className="truncate text-xs font-semibold text-white">
                          {run.brd_filename || 'Untitled run'}
                        </div>
                      </div>
                      <div className="mt-2 flex items-center gap-3 text-[10px] text-[#8a9ab7]">
                        <span className="max-w-[95px] truncate font-mono">{String(run.id).slice(0, 8)}...</span>
                        <span className="flex items-center gap-1">
                          <CalendarDays size={9} />
                          {formatCompactDate(run.started_at)}
                        </span>
                        <span className="ml-auto flex items-center gap-1 whitespace-nowrap">
                          <Clock size={9} />
                          {formatRelativeTime(run.updated_at || run.completed_at || run.started_at)}
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

        <section className="min-h-[520px] min-w-0 flex-1 overflow-y-auto p-4 sm:p-5 lg:min-h-0">
          {selectedRun ? (
            <div>
              <div className="mb-5 flex items-start justify-between gap-4">
                <div className="min-w-0">
                  <h2 className="truncate text-base font-bold text-white">
                    {selectedRun.brd_filename || 'Untitled run'}
                  </h2>
                  <div className="mt-2 font-mono text-xs text-white">{selectedRun.id}</div>
                </div>
                <div className="flex items-center gap-4">
                  <StatusPill status={selectedRun.status} tone={statusTone(selectedRun.status)} large />
                  <RefreshCw size={18} className="text-white" />
                </div>
              </div>

              <div className="overflow-hidden rounded-lg border border-[#253044] bg-[#0d1525]">
                <button
                  type="button"
                  onClick={() => setRunInfoOpen((open) => !open)}
                  aria-expanded={runInfoOpen}
                  className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left transition-colors hover:bg-white/[0.03]"
                >
                  <span className="text-xs font-semibold text-white">Run Info</span>
                  <span className="flex items-center gap-1.5 text-[11px] text-[#8a9ab7]">
                    {runInfoOpen ? 'Hide' : 'Show'}
                    <ChevronDown size={13} className={`transition-transform ${runInfoOpen ? 'rotate-180' : '-rotate-90'}`} />
                  </span>
                </button>
                {runInfoOpen && (
                  <div className="grid gap-2.5 border-t border-[#253044] px-4 py-3 text-xs">
                    <InfoRow icon={Info} label="Project Name" value={selectedRun.project_name || selectedRun.brd_filename || '-'} />
                    <InfoRow icon={Info} label="Project Description" value={selectedRun.project_description || 'NA'} />
                    <InfoRow icon={Info} label="Source" value={formatSource(selectedRun.source)} />
                    <InfoRow icon={Info} label="Database Type" value={selectedRun.database_type || '-'} />
                    <InfoRow icon={Info} label="Database Name" value={selectedRun.database_name || '-'} />
                    <InfoRow icon={CalendarDays} label="Started" value={formatFullDate(selectedRun.started_at)} />
                    <InfoRow icon={CalendarDays} label="Last Updated" value={formatFullDate(selectedRun.completed_at || selectedRun.updated_at || selectedRun.started_at)} />
                    <InfoRow icon={FileText} label="Knowledge Base" value={selectedRun.knowledge_base || 'Not used'} />
                  </div>
                )}
              </div>

              <div className="mt-5">
                <div className="mb-3 text-xs font-semibold text-[#c4cee0]">Stages by Phase</div>
                <div className="overflow-hidden rounded-lg border border-[#253044] bg-[#0d1525]">
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
    <div className="grid items-start gap-1.5 sm:grid-cols-[140px_minmax(0,1fr)] sm:gap-3">
      <div className="flex items-center gap-2 text-white">
        <Icon size={14} className="text-slate-300" />
        <span>{label}</span>
      </div>
      <div className="min-w-0 break-words font-mono text-white">{value || '-'}</div>
    </div>
  )
}

function PhaseRow({ phase, index }) {
  const [expanded, setExpanded] = useState(false)
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
    <div className="border-b border-[#253044] last:border-b-0">
      <button
        type="button"
        onClick={() => setExpanded((value) => !value)}
        className="flex w-full items-center justify-between px-4 py-2.5 text-left transition-colors hover:bg-white/[0.03]"
      >
        <div className="flex min-w-0 items-center gap-3">
          <div className={`flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full border-2 bg-[#0c1426] ${
              done
                ? 'border-emerald-500 text-emerald-400'
                : running
                ? 'border-[#3f82ff] text-[#3f82ff]'
                : review
                ? 'border-amber-300 text-amber-300'
                : 'border-[#263753] text-[#60708d]'
            }`}>
            {done ? <CheckCircle2 size={12} /> : <span className="text-[10px] font-bold">{index}</span>}
          </div>
          <div className="min-w-0">
            <div className={`truncate text-xs font-semibold ${done || running || review ? 'text-white' : 'text-[#8da1c8]'}`}>
              {phase.label}
            </div>
            <div className="mt-0.5 text-[10px] text-[#91a4cb]">{completed}/{total} stages</div>
          </div>
        </div>
        <div className={`flex items-center gap-2 text-[10px] font-medium ${toneText}`}>
          <span className={`h-1.5 w-1.5 rounded-full bg-current ${running ? 'animate-pulse' : ''}`} />
          {phase.status}
          <ChevronDown size={12} className={`text-[#667795] transition-transform ${expanded ? 'rotate-180' : ''}`} />
        </div>
      </button>

      {expanded && (
        <div className="bg-[#080e1d]/50 px-3 pb-2">
          <div>
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
  const state = normalizeState(step.state)
  const done = isCompletedStep(state)
  const running = state === 'RUNNING'
  const review = state === 'HITL_WAIT' || step.key.includes('review') || step.key.startsWith('gate')
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
    <div className="flex min-h-[30px] items-center gap-2 rounded px-2 py-1.5 transition-colors hover:bg-white/[0.03]">
      <span className={`flex h-2 w-2 flex-shrink-0 items-center justify-center rounded-full bg-current ${toneClass} ${running ? 'animate-pulse' : ''}`}>
      </span>
      <div className={`text-xs ${done || running || review ? 'text-[#c4cee0]' : 'text-[#7f91b4]'}`}>
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
      state: normalizeState(step?.state || fallbackState),
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
      actual('bronze_code_execution', 'Bronze Code Execution'),
    ])
  }

  if (phase.id === 'phase-4') {
    const silverState = normalizeState(byKey.get('silver')?.state || phaseState)
    const silverExecutionState = byKey.get('silver_code_execution')?.state ? normalizeState(byKey.get('silver_code_execution')?.state) : ''
    const gate4State = reviewAwareState(byKey.get('gate4'), phase)
    const gate5State = reviewAwareState(byKey.get('gate5'), phase)
    const mergeReviewState = byKey.get('silver_merge_key_review')?.state ? normalizeState(byKey.get('silver_merge_key_review')?.state) : ''
    const silverFlow = buildHistorySilverPhaseStates(silverState, gate4State, gate5State, phase.status, mergeReviewState, silverExecutionState)
    return clampLinearHistorySteps([
      actual('silver_merge_key_resolution', 'Silver Merge Key Resolution', silverFlow.mergeResolution),
      actual('silver_merge_key_review', 'Silver Merge Key Review', mergeReviewState || silverFlow.mergeReview),
      actual('silver', 'Silver Code Generation', silverFlow.codeGeneration),
      actual('gate5', 'Silver Review', silverFlow.reviewGate),
      actual('silver_code_execution', 'Silver Code Execution', silverFlow.codeExecution),
    ])
  }

  if (phase.id === 'phase-5') {
    const goldFlow = buildHistoryGoldPhaseStates(
      byKey.get('gold')?.state || phaseState,
      byKey.get('gold_code_execution')?.state,
      phase.status
    )
    return clampLinearHistorySteps([
      actual('gold', 'Gold Code Generation', goldFlow.codeGeneration),
      actual('gold_code_execution', 'Gold Code Execution', goldFlow.codeExecution),
    ])
  }

  return clampLinearHistorySteps(steps.map((step) => ({ ...step, label: step.label || step.key })))
}

function clampLinearHistorySteps(steps = []) {
  let blocked = false
  return steps.map((step) => {
    const state = normalizeState(step.state)
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
  if (step?.state) return normalizeState(step.state)
  if (phase.status === 'Review') return 'HITL_WAIT'
  return phaseStatusToStepState(phase.status)
}

function buildHistorySilverPhaseStates(silverState, gate4State, gate5State, phaseStatus, mergeReviewState = '', silverExecutionState = '') {
  const normalizedSilver = normalizeState(silverState)
  const normalizedGate4 = normalizeState(gate4State)
  const normalizedGate = normalizeState(gate5State)
  const normalizedMergeReview = mergeReviewState ? normalizeState(mergeReviewState) : ''
  const normalizedSilverExecution = silverExecutionState ? normalizeState(silverExecutionState) : ''
  const normalizedPhase = String(phaseStatus || '').toLowerCase()

  if (['RUNNING', 'FAILED', 'COMPLETED'].includes(normalizedSilverExecution)) {
    return {
      mergeResolution: 'COMPLETED',
      mergeReview: 'COMPLETED',
      codeGeneration: 'COMPLETED',
      reviewGate: 'COMPLETED',
      codeExecution: normalizedSilverExecution,
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

  if (normalizedMergeReview === 'HITL_WAIT') {
    return {
      mergeResolution: 'COMPLETED',
      mergeReview: 'HITL_WAIT',
      codeGeneration: 'PENDING',
      reviewGate: 'PENDING',
      codeExecution: 'PENDING',
    }
  }

  if (normalizedGate4 === 'HITL_WAIT' || normalizedGate4 === 'PAUSED_FOR_HITL') {
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

function buildHistoryGoldPhaseStates(goldState, goldExecutionState, phaseStatus) {
  const normalizedGold = normalizeState(goldState)
  const normalizedGoldExecution = goldExecutionState ? normalizeState(goldExecutionState) : ''
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
      codeExecution: normalizedPhase === 'done' ? 'COMPLETED' : 'PENDING',
    }
  }

  return {
    codeGeneration: 'PENDING',
    codeExecution: 'PENDING',
  }
}

function isCompletedStep(state) {
  return normalizeState(state) === 'COMPLETED'
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
      large ? 'px-3 py-1.5 text-xs' : 'px-2.5 py-1 text-[10px]'
    }`}>
      <span className={`h-2 w-2 rounded-full bg-current ${isRunningStatus(status) ? 'animate-pulse' : ''}`} />
      {statusLabel(status)}
    </div>
  )
}

function statusLabel(status) {
  const rawValue = String(status || '').toUpperCase()
  const value = ['SUCCESS', 'PIPELINE_COMPLETED'].includes(rawValue)
    ? 'Completed'
    : rawValue.replace(/_/g, ' ').trim()
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

function formatRelativeTime(value) {
  if (!value) return ''
  const elapsed = Math.max(0, Date.now() - new Date(value).getTime())
  const minutes = Math.floor(elapsed / 60000)
  if (minutes < 1) return 'just now'
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  return `${Math.floor(hours / 24)}d ago`
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
