// @ts-nocheck
import React from 'react'
import { motion } from 'framer-motion'
import {
  FileText,
  ClipboardList,
  BarChart2,
  Users,
  CheckCircle,
  Loader2,
  AlertCircle,
  Clock
} from 'lucide-react'
import StatusBadge from '../shared/StatusBadge'

const ICON_MAP = {
  FileText,
  ClipboardList,
  BarChart2,
  Users,
  CheckCircle,
  Loader2
}

/**
 * StageNode — a card representing a single pipeline stage.
 * @param {{
 *   stage: { id: string, name: string, icon: string, status: string, tokens: number, cost: number, error: string|null, attempts: number },
 *   onClick?: () => void,
 *   compact?: boolean
 * }} props
 */
function StageNode({ stage, onClick, compact = false }) {
  console.log("Stage ::: ", stage);
  const Icon = ICON_MAP[stage.icon] || FileText
  const isRunning = stage.status === 'RUNNING'
  const isHitlWait = stage.status === 'HITL_WAIT' || stage.status === 'PAUSED_FOR_HITL'
  const isFailed = stage.status === 'FAILED'
  const isCompleted = stage.status === 'COMPLETED' || stage.status === 'SUCCESS'
  const isPending = stage.status === 'PENDING'

  let borderClass = 'border-bg-border'
  let bgClass = 'bg-bg-card'
  let glowClass = ''

  if (isRunning) {
    borderClass = 'border-accent-blue/50'
    bgClass = 'bg-blue-950/30'
    glowClass = 'glow-blue'
  } else if (isHitlWait) {
    borderClass = 'border-accent-amber/50'
    bgClass = 'bg-amber-950/20'
    glowClass = 'glow-amber'
  } else if (isFailed) {
    borderClass = 'border-accent-red/50'
    bgClass = 'bg-red-950/20'
    glowClass = 'glow-red'
  } else if (isCompleted) {
    borderClass = 'border-accent-green/20'
    bgClass = 'bg-bg-card'
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3 }}
      onClick={onClick}
      className={`
        relative border rounded-xl transition-all duration-300 select-none
        ${bgClass} ${borderClass} ${glowClass}
        ${onClick ? 'cursor-pointer hover:border-gray-500 hover:scale-[1.01]' : ''}
        ${compact ? 'p-3' : 'p-4'}
        ${isPending ? 'opacity-50' : 'opacity-100'}
      `}
    >
      {/* Running pulse border animation */}
      {isRunning && (
        <motion.div
          className="absolute inset-0 rounded-xl border border-accent-blue/40 pointer-events-none"
          animate={{ opacity: [0.4, 1, 0.4] }}
          transition={{ duration: 2, repeat: Infinity, ease: 'easeInOut' }}
        />
      )}

      {/* HITL pulse */}
      {isHitlWait && (
        <motion.div
          className="absolute inset-0 rounded-xl border border-accent-amber/40 pointer-events-none"
          animate={{ opacity: [0.3, 0.8, 0.3] }}
          transition={{ duration: 1.8, repeat: Infinity, ease: 'easeInOut' }}
        />
      )}

      <div className={`flex items-start ${compact ? 'gap-2' : 'gap-2.5'}`}>
        {/* Icon */}
        <div
          className={`
            flex-shrink-0 rounded-lg flex items-center justify-center
            ${compact ? 'w-6 h-6' : 'w-7 h-7'}
            ${isRunning ? 'bg-accent-blue/15' : isHitlWait ? 'bg-accent-amber/15' : isFailed ? 'bg-accent-red/15' : isCompleted ? 'bg-accent-green/10' : 'bg-bg-border'}
          `}
        >
          {isRunning ? (
            <Loader2
              size={compact ? 11 : 13}
              className="animate-spin text-accent-blue"
            />
          ) : isFailed ? (
            <AlertCircle size={compact ? 11 : 13} className="text-accent-red" />
          ) : isHitlWait ? (
            <Users size={compact ? 11 : 13} className="text-accent-amber" />
          ) : isPending ? (
            <Clock size={compact ? 11 : 13} className="text-gray-600" />
          ) : (
            <Icon
              size={compact ? 11 : 13}
              className={isCompleted ? 'text-accent-green' : 'text-gray-400'}
            />
          )}
        </div>

        {/* Content */}
        <div className="flex-1 min-w-0">
          <div className="flex items-center justify-between gap-2 flex-wrap">
            <span className={`font-medium leading-tight ${compact ? 'text-xs' : 'text-sm'} ${isPending ? 'text-gray-600' : 'text-gray-200'}`}>
              {stage.name}
            </span>
            <StatusBadge status={stage.status} size="sm" />
          </div>

          {/* Metrics for completed stages */}
          {isCompleted && stage.tokens > 0 && !compact && (
            <div className="flex items-center gap-3 mt-2">
              <span className="text-[11px] font-mono text-gray-500">
                {stage.tokens.toLocaleString()} tok
              </span>
              <span className="text-[11px] font-mono text-gray-500">
                ${(stage.cost || 0).toFixed(4)}
              </span>
              {stage.attempts > 1 && (
                <span className="text-[11px] font-mono text-accent-amber">
                  {stage.attempts} attempts
                </span>
              )}
            </div>
          )}

          {/* Error snippet */}
          {isFailed && stage.error && (
            <p className="text-[11px] text-accent-red mt-1.5 font-mono leading-relaxed line-clamp-2">
              {stage.error.slice(0, 100)}
            </p>
          )}

          {/* Attempts badge */}
          {stage.attempts > 1 && isCompleted && compact && (
            <span className="text-[10px] font-mono text-accent-amber mt-1 block">
              {stage.attempts} attempts
            </span>
          )}
        </div>
      </div>
    </motion.div>
  )
}

export default StageNode

