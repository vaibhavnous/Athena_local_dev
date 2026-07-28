// @ts-nocheck
import React, { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Loader2,
  AlertCircle,
  Users,
  Clock,
  CheckCircle2,
  RotateCcw,
  XCircle,
  ChevronDown,
  ChevronRight,
} from 'lucide-react'

// ─── Phase Configuration ──────────────────────────────────────────────────────
// To move a stage to a different phase, change its stageIds entry below.
// Order within stageIds determines display order.

export const PIPELINE_PHASES = [
  {
    id: 'phase_1',
    number: 1,
    label: 'Discovery & Requirement Intelligence',
    stageIds: ['stage_01', 'stage_02', 'stage_03', 'stage_04', 'stage_hitl'],
  },
  {
    id: 'phase_2',
    number: 2,
    label: 'Source & Metadata Intelligence',
    stageIds: ['stage_06', 'stage_06b', 'stage_07', 'stage_08', 'stage_09', 'stage_09b'],
  },
  {
    id: 'phase_3',
    number: 3,
    label: 'Bronze Layer (Ingestion)',
    stageIds: ['stage_10', 'stage_11'],
  },
  {
    id: 'phase_4',
    number: 4,
    label: 'Silver Layer (Transformation)',
    stageIds: ['stage_12', 'stage_12b', 'stage_12c', 'stage_13'],
  },
  {
    id: 'phase_5',
    number: 5,
    label: 'Gold Layer (Analytics)',
    stageIds: ['stage_14', 'stage_15'],
  },
]

export const SNOWFLAKE_DATABASE_PIPELINE_PHASES = [
  {
    id: 'snowflake_database_phase_1',
    number: 1,
    label: 'Discovery & Requirement Intelligence',
    stageIds: ['stage_01', 'stage_02', 'stage_03', 'stage_04', 'stage_hitl'],
  },
  {
    id: 'snowflake_database_phase_2',
    number: 2,
    label: 'Source & Metadata Intelligence',
    stageIds: ['stage_06', 'stage_06b', 'stage_07', 'stage_08', 'stage_09', 'stage_09b'],
  },
  {
    id: 'snowflake_database_phase_3',
    number: 3,
    label: 'Bronze Layer (Ingestion)',
    stageIds: ['stage_10', 'stage_11'],
  },
  {
    id: 'snowflake_database_phase_4',
    number: 4,
    label: 'Silver Layer (Transformation)',
    stageIds: ['stage_12', 'stage_12b', 'stage_12c', 'stage_13'],
  },
  {
    id: 'snowflake_database_phase_5',
    number: 5,
    label: 'Gold Layer (Analytics)',
    stageIds: ['stage_14', 'stage_15', 'stage_16'],
  },
]

export const DATA_LAKE_PIPELINE_PHASES = [
  {
    id: 'data_lake_phase_1',
    number: 1,
    label: 'Discovery & Requirement Intelligence',
    stageIds: ['stage_01', 'stage_02', 'stage_03', 'stage_04', 'stage_hitl'],
  },
  {
    id: 'data_lake_phase_2',
    number: 2,
    label: 'Feed & Metadata Intelligence',
    stageIds: [
      'stage_06',
      'stage_06b_feed_nomination',
      'stage_06c',
      'stage_07',
      'stage_07b',
      'stage_08',
      'stage_09',
      'stage_09b',
      'stage_09c',
    ],
  },
  {
    id: 'data_lake_phase_3',
    number: 3,
    label: 'Metadata Bootstrap & Source Validation',
    stageIds: ['stage_10', 'stage_10b', 'stage_10c', 'stage_11', 'stage_12', 'stage_13', 'stage_14'],
  },
  {
    id: 'data_lake_phase_4',
    number: 4,
    label: 'Bronze Layer (Ingestion & DQ)',
    stageIds: ['stage_15', 'stage_15a', 'stage_15b', 'stage_16'],
  },
  {
    id: 'data_lake_phase_5',
    number: 5,
    label: 'Silver Layer (Transformation & DQ)',
    stageIds: ['stage_17', 'stage_17b', 'stage_18'],
  },
  {
    id: 'data_lake_phase_6',
    number: 6,
    label: 'Gold Layer & Deployment',
    stageIds: ['stage_19', 'stage_19b', 'stage_20', 'stage_20b', 'stage_21', 'stage_22'],
  },
]

export const SNOWFLAKE_DATA_LAKE_PIPELINE_PHASES = [
  {
    id: 'snowflake_data_lake_phase_1',
    number: 1,
    label: 'Discovery & Requirement Intelligence',
    stageIds: ['stage_01', 'stage_02', 'stage_03', 'stage_04', 'stage_hitl'],
  },
  {
    id: 'snowflake_data_lake_phase_2',
    number: 2,
    label: 'Feed & Metadata Intelligence',
    stageIds: [
      'stage_06',
      'stage_06b_feed_nomination',
      'stage_06c',
      'stage_07',
      'stage_07b',
      'stage_08',
      'stage_09',
      // 'stage_09b',
      'stage_09c',
    ],
  },
  {
    id: 'snowflake_data_lake_phase_3',
    number: 3,
    label: 'Metadata Bootstrap & Source Validation',
    stageIds: ['stage_10', 'stage_10b', 'stage_10c', 'stage_11', 'stage_12', 'stage_13', 'stage_14'],
  },
  {
    id: 'snowflake_data_lake_phase_4',
    number: 4,
    label: 'Bronze Layer (Ingestion & DQ)',
    stageIds: ['stage_15', 'stage_15b', 'stage_16'],
  },
  {
    id: 'snowflake_data_lake_phase_5',
    number: 5,
    label: 'Silver Layer (Transformation & DQ)',
    stageIds: ['stage_17', 'stage_17b', 'stage_18'],
  },
  {
    id: 'snowflake_data_lake_phase_6',
    number: 6,
    label: 'Gold Layer & Deployment',
    stageIds: ['stage_19', 'stage_19b', 'stage_20', 'stage_21'],
  },
]

const DATA_LAKE_ONLY_STAGE_IDS = new Set([
  'stage_06b_feed_nomination',
  'stage_06c',
  'stage_07b',
  'stage_10b',
  'stage_10c',
  'stage_15a',
  'stage_15b',
  'stage_17',
  'stage_17b',
  'stage_18',
  'stage_19',
  'stage_19b',
  'stage_20',
  'stage_20b',
  'stage_21',
  'stage_22',
])

function isSnowflakeTarget(target) {
  const normalizedTarget = String(target || '').trim().toLowerCase()
  return normalizedTarget === 'snowflake' || normalizedTarget === 'snowflakes'
}

export function isDataLakePipeline(connectionType, stages = []) {
  const normalizedConnectionType = String(connectionType || '')
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, '_')

  if (normalizedConnectionType) {
    return normalizedConnectionType === 'data_lake'
  }

  return (
    stages.some((stage) => DATA_LAKE_ONLY_STAGE_IDS.has(stage.id ?? stage.stage_id))
  )
}

export function getPipelinePhases(connectionType, stages = [], target = '') {
  const isDataLake = isDataLakePipeline(connectionType, stages)
  const isSnowflake = isSnowflakeTarget(target)

  if (isSnowflake && isDataLake) return SNOWFLAKE_DATA_LAKE_PIPELINE_PHASES
  if (isSnowflake) return SNOWFLAKE_DATABASE_PIPELINE_PHASES
  if (isDataLake) return DATA_LAKE_PIPELINE_PHASES
  return PIPELINE_PHASES
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function derivePhaseStatus(phaseStages) {
  if (!phaseStages.length) return 'PENDING'
  if (phaseStages.some((s) => s.status === 'RUNNING')) return 'RUNNING'
  if (phaseStages.some((s) => s.status === 'HITL_WAIT')) return 'HITL_WAIT'
  if (phaseStages.some((s) => s.status === 'FAILED')) return 'FAILED'
  if (phaseStages.some((s) => s.status === 'CANCELLED')) return 'CANCELLED'
  if (phaseStages.every((s) => s.status === 'COMPLETED' || s.status === 'SUCCESS')) return 'COMPLETED'
  // Mix of COMPLETED + PENDING = partially done but not yet running
  if (phaseStages.some((s) => s.status === 'COMPLETED' || s.status === 'SUCCESS')) return 'PARTIAL'
  return 'PENDING'
}

/** Return the index of the active phase (first that is RUNNING/HITL_WAIT/FAILED),
 *  or the last COMPLETED/PARTIAL phase, or 0 as fallback. */
function findActivePhaseIndex(phases, stagesById) {
  // First priority: running or waiting
  for (let i = 0; i < phases.length; i++) {
    const ps = phases[i].stageIds.map((id) => stagesById[id]).filter(Boolean)
    const st = derivePhaseStatus(ps)
    if (st === 'RUNNING' || st === 'HITL_WAIT' || st === 'FAILED') return i
  }
  // Second priority: last partially/fully completed phase
  let lastDone = 0
  for (let i = 0; i < phases.length; i++) {
    const ps = phases[i].stageIds.map((id) => stagesById[id]).filter(Boolean)
    const st = derivePhaseStatus(ps)
    if (st === 'COMPLETED' || st === 'PARTIAL') lastDone = i
  }
  return lastDone
}

// ─── Status atoms ─────────────────────────────────────────────────────────────

function StageBubble({ status, size = 22 }) {
  const isRunning = status === 'RUNNING'
  const isHitl = status === 'HITL_WAIT' || status === 'PAUSED_FOR_HITL'
  const isFailed = status === 'FAILED'
  const isCompleted = status === 'COMPLETED' || status === 'SUCCESS'
  const isCancelled = status === 'CANCELLED'

  let ring = 'border-bg-border bg-bg-card'
  let icon = <Clock size={size * 0.55} className="text-gray-500" />

  if (isRunning) {
    ring = 'border-accent-blue/60 bg-blue-950/30'
    icon = <Loader2 size={size * 0.55} className="animate-spin text-accent-blue" />
  } else if (isHitl) {
    ring = 'border-accent-amber/60 bg-amber-950/20'
    icon = <Users size={size * 0.55} className="text-accent-amber" />
  } else if (isFailed) {
    ring = 'border-accent-red/60 bg-red-950/20'
    icon = <AlertCircle size={size * 0.55} className="text-accent-red" />
  } else if (isCancelled) {
    ring = 'border-gray-500/50 bg-gray-800/30'
    icon = <XCircle size={size * 0.55} className="text-gray-400" />
  } else if (isCompleted) {
    ring = 'border-accent-green/60 bg-accent-green/15'
    icon = <CheckCircle2 size={size * 0.55} className="text-accent-green" />
  }

  return (
    <div
      style={{ width: size, height: size, minWidth: size }}
      className={`flex items-center justify-center rounded-full border-2 flex-shrink-0 transition-all duration-300 ${ring} ${
        status === 'PENDING' ? 'opacity-40' : ''
      }`}
    >
      {isRunning && (
        <motion.div
          className="absolute rounded-full border-2 border-accent-blue/30 pointer-events-none"
          style={{ width: size, height: size }}
          animate={{ opacity: [0.3, 0.8, 0.3], scale: [1, 1.15, 1] }}
          transition={{ duration: 2, repeat: Infinity }}
        />
      )}
      {isHitl && (
        <motion.div
          className="absolute rounded-full border-2 border-accent-amber/30 pointer-events-none"
          style={{ width: size, height: size }}
          animate={{ opacity: [0.3, 0.8, 0.3], scale: [1, 1.15, 1] }}
          transition={{ duration: 1.8, repeat: Infinity }}
        />
      )}
      <div className="relative z-10 flex items-center justify-center">{icon}</div>
    </div>
  )
}

function PhaseStatusDot({ status }) {
  const map = {
    COMPLETED: 'bg-accent-green',
    RUNNING: 'bg-accent-blue animate-pulse',
    HITL_WAIT: 'bg-accent-amber animate-pulse',
    FAILED: 'bg-accent-red',
    PARTIAL: 'bg-accent-blue/50',
    CANCELLED: 'bg-gray-500',
    PENDING: 'bg-gray-600',
  }
  return (
    <span
      className={`inline-block w-2 h-2 rounded-full flex-shrink-0 ${map[status] ?? 'bg-gray-600'}`}
    />
  )
}

function PhaseStatusLabel({ status }) {
  const map = {
    COMPLETED: { label: 'Done', cls: 'text-accent-green' },
    RUNNING: { label: 'Running', cls: 'text-accent-blue' },
    HITL_WAIT: { label: 'Review', cls: 'text-accent-amber' },
    FAILED: { label: 'Failed', cls: 'text-accent-red' },
    PARTIAL: { label: 'In Progress', cls: 'text-accent-blue/70' },
    CANCELLED: { label: 'Cancelled', cls: 'text-gray-400' },
    PENDING: { label: 'Pending', cls: 'text-gray-500' },
  }
  const { label, cls } = map[status] ?? map.PENDING
  return <span className={`text-[10px] font-medium ${cls}`}>{label}</span>
}

// ─── Stage row inside an expanded phase ───────────────────────────────────────

function StageRow({ stage, isLast, onStageClick, onRetry }) {
  const isCompleted = stage.status === 'COMPLETED' || stage.status === 'SUCCESS'

  return (
    <div className="group flex items-stretch">
      {/* Left spine + bubble */}
      <div className="flex flex-col items-center" style={{ width: 32, minWidth: 32 }}>
        <StageBubble status={stage.status} size={22} />
        {!isLast && (
          <div className="flex-1 w-px mt-1" style={{ minHeight: 16 }}>
            <div
              className={`w-full h-full ${
                isCompleted ? 'bg-accent-green/30' : 'bg-bg-border/50'
              }`}
              style={{ width: 1, margin: '0 auto' }}
            />
          </div>
        )}
      </div>

      {/* Content */}
      <div
        className={`flex-1 flex items-center justify-between pb-3 ml-2 ${isLast ? '' : ''}`}
        onClick={() => onStageClick?.(stage)}
      >
        <div
          className={`flex-1 cursor-pointer hover:text-text-primary transition-colors ${
            stage.status === 'PENDING' ? 'opacity-40' : ''
          }`}
        >
          <p className="text-xs font-medium text-text-secondary leading-tight">
            {stage.name}
          </p>
          {stage.error && (
            <p className="text-[10px] text-accent-red mt-0.5 truncate max-w-xs">
              {stage.error}
            </p>
          )}
        </div>

        <div className="flex items-center gap-2 flex-shrink-0 ml-2">
          {isCompleted && onRetry && (
            <button
              onClick={(e) => { e.stopPropagation(); onRetry(stage) }}
              className="opacity-0 group-hover:opacity-100 transition-opacity flex items-center gap-1 px-1.5 py-0.5 rounded text-[9px] font-medium bg-bg-card border border-accent-green/40 text-accent-green hover:bg-accent-green/10"
            >
              <RotateCcw size={8} />
              Re-run
            </button>
          )}
        </div>
      </div>
    </div>
  )
}

// ─── Phase row (collapsed header or expanded with stages) ─────────────────────

function PhaseRow({ phase, phaseStages, phaseStatus, isActive, isExpanded, onToggle, onStageClick, onRetry, phaseNumber }) {
  const isDone = phaseStatus === 'COMPLETED'
  const isPending = phaseStatus === 'PENDING'

  // Header style based on status
  let headerBg = 'bg-bg-card/30 hover:bg-bg-card/50'
  let numberRing = 'border-bg-border text-gray-500'
  if (isActive && !isDone) {
    headerBg = 'bg-blue-950/20 hover:bg-blue-950/30'
    numberRing = 'border-accent-blue/60 text-accent-blue'
  } else if (isDone) {
    headerBg = 'bg-bg-card/20 hover:bg-bg-card/40'
    numberRing = 'border-accent-green/50 text-accent-green'
  } else if (phaseStatus === 'FAILED') {
    headerBg = 'bg-red-950/10 hover:bg-red-950/20'
    numberRing = 'border-accent-red/50 text-accent-red'
  }

  const completedCount = phaseStages.filter(
    (s) => s.status === 'COMPLETED' || s.status === 'SUCCESS',
  ).length

  return (
    <div className="w-full border-b border-white/20">
      {/* Phase header */}
      <button
        onClick={onToggle}
        className={`w-full flex items-center gap-3 px-3 py-2.5 transition-colors text-left rounded-lg ${headerBg} ${isPending ? 'cursor-default' : 'cursor-pointer'}`}
      >
        {/* Phase number badge */}
        <div
          className={`flex items-center justify-center rounded-full border-2 flex-shrink-0 font-bold transition-colors ${numberRing}`}
          style={{ width: 28, height: 28, fontSize: 11 }}
        >
          {isDone ? <CheckCircle2 size={13} /> : phaseNumber}
        </div>

        {/* Phase title + meta */}
        <div className="flex-1 min-w-0">
          <p
            className={`text-xs font-semibold leading-tight ${
              isPending ? 'text-gray-500' : 'text-text-primary'
            }`}
          >
            {phase.label}
          </p>
          {!isPending && (
            <p className="text-[10px] text-gray-500 mt-0.5">
              {completedCount}/{phaseStages.length} stages complete
            </p>
          )}
        </div>

        {/* Status + chevron */}
        <div className="flex items-center gap-2 flex-shrink-0">
          <PhaseStatusDot status={phaseStatus} />
          <PhaseStatusLabel status={phaseStatus} />
          {!isPending && (
            <span className="text-gray-600">
              {isExpanded ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
            </span>
          )}
        </div>
      </button>

      {/* Expanded stages */}
      <AnimatePresence initial={false}>
        {isExpanded && (
          <motion.div
            key="stages"
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.2, ease: 'easeInOut' }}
            style={{ overflow: 'hidden' }}
          >
            <div className="pl-4 pr-2 pt-2 pb-1 border-l-2 border-white/10 ml-5 mt-1 mb-1">
              {phaseStages.map((stage, idx) => (
                <StageRow
                  key={stage.id}
                  stage={stage}
                  isLast={idx === phaseStages.length - 1}
                  onStageClick={onStageClick}
                  onRetry={onRetry}
                />
              ))}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

// ─── Main component ───────────────────────────────────────────────────────────

export default function PhasedPipelineDag({ stages = [], connectionType, target, onStageClick, onRetry }) {
  const pipelinePhases = getPipelinePhases(connectionType, stages, target)

  // Build a quick lookup: stageId → stage object
  const stagesById = {}
  for (const s of stages) {
    const stageId = s.id ?? s.stage_id
    if (stageId) stagesById[stageId] = s
  }

  // Determine which phase is active
  const activePhaseIdx = findActivePhaseIndex(pipelinePhases, stagesById)

  // Only the active phase is expanded; completed and pending phases are collapsed.
  // User can manually expand any non-pending phase by clicking its header.
  const [expandedPhases, setExpandedPhases] = useState(() => {
    const init = {}
    pipelinePhases.forEach((_, i) => { init[i] = i === activePhaseIdx })
    return init
  })

  // When active phase changes: collapse the old one, expand the new one.
  const prevActive = React.useRef(activePhaseIdx)
  if (prevActive.current !== activePhaseIdx) {
    const prev = prevActive.current
    prevActive.current = activePhaseIdx
    setExpandedPhases((old) => ({ ...old, [prev]: false, [activePhaseIdx]: true }))
  }

  const togglePhase = (idx) => {
    const ps = pipelinePhases[idx].stageIds.map((id) => stagesById[id]).filter(Boolean)
    const status = derivePhaseStatus(ps)
    // Don't allow toggling a fully pending phase
    if (status === 'PENDING') return
    setExpandedPhases((prev) => ({ ...prev, [idx]: !prev[idx] }))
  }

  if (!stages || stages.length === 0) {
    return (
      <div className="flex items-center justify-center h-20 text-text-tertiary text-sm">
        No stages available
      </div>
    )
  }

  return (
    <div className="w-full h-full border border-bg-border rounded-lg bg-bg-card/50 overflow-y-auto">
      <div className="flex flex-col">
        {pipelinePhases.map((phase, idx) => {
          const phaseStages = phase.stageIds.map((id) => stagesById[id]).filter(Boolean)
          const phaseStatus = derivePhaseStatus(phaseStages)
          const isActive = idx === activePhaseIdx

          return (
            <PhaseRow
              key={phase.id}
              phase={phase}
              phaseStages={phaseStages}
              phaseStatus={phaseStatus}
              isActive={isActive}
              isExpanded={!!expandedPhases[idx]}
              onToggle={() => togglePhase(idx)}
              onStageClick={onStageClick}
              onRetry={onRetry}
              phaseNumber={phase.number}
            />
          )
        })}
      </div>
    </div>
  )
}
