// @ts-nocheck
import React, { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { Code2, ExternalLink, StopCircle, Clock, Cpu, FileText, Table2, ShieldCheck } from 'lucide-react'
import StatusBadge from '../shared/StatusBadge'
import CopyableId from '../shared/CopyableId'
import useAthenaStore from '../../store/useAthenaStore'
import { abortRun } from '../../api/athenaApi'

/**
 * RunCard — a card representing a pipeline run.
 * Shows ID, filename, provider, status, timing, and action buttons.
 * @param {{
 *   run: object,
 *   isActive?: boolean,
 *   onClick?: () => void,
 *   compact?: boolean
 * }} props
 */
function RunCard({ run, isActive, onClick, compact = false }) {
  const navigate = useNavigate()
  const { updateRun, addNotification, setActiveRun } = useAthenaStore()
  const [aborting, setAborting] = useState(false)
  const [elapsed, setElapsed] = useState('')
  const isStageConfirmationPaused =
    String(run?.status || '').toUpperCase() === 'PAUSED_FOR_STAGE_CONFIRMATION' ||
    Boolean(run?.stage_confirmation?.awaiting_confirmation)
  const reviewGate = getReviewGate(run)

  // Live elapsed timer for running runs
  useEffect(() => {
    if (run.status !== 'RUNNING' && run.status !== 'SUBMITTED') {
      if (run.started_at && run.completed_at) {
        const ms = new Date(run.completed_at) - new Date(run.started_at)
        setElapsed(formatDuration(ms))
      }
      return
    }

    const update = () => {
      const ms = Date.now() - new Date(run.started_at).getTime()
      setElapsed(formatDuration(ms))
    }
    update()
    const interval = setInterval(update, 1000)
    return () => clearInterval(interval)
  }, [run.status, run.started_at, run.completed_at])

  const handleAbort = async (e) => {
    e.stopPropagation()
    if (!confirm(`Abort run ${run.id}?`)) return
    setAborting(true)
    try {
      await abortRun(run.id)
    } catch {
      /* server offline — update locally */
    } finally {
      updateRun(run.id, { status: 'ABORTED', completed_at: new Date().toISOString() })
      addNotification({ type: 'amber', title: 'Run Aborted', message: `Run ${run.id.slice(0, 8)} was aborted`, duration: 4000 })
      setAborting(false)
    }
  }

  const handleViewDetail = (e) => {
    e.stopPropagation()
    navigate(`/app/runs/${run.id}`)
  }

  const handleResumeReview = (e) => {
    e.stopPropagation()
    setActiveRun(run.id)
    navigate(reviewPathForRun(run))
  }

  const providerLabel = {
    azure_openai: 'Azure',
    openai: 'OpenAI',
    anthropic: 'Anthropic'
  }[run.provider] || run.provider

  const providerColor = {
    azure_openai: 'bg-blue-500/10 text-accent-blue border-accent-blue/20',
    openai: 'bg-green-500/10 text-accent-green border-accent-green/20',
    anthropic: 'bg-purple-500/10 text-accent-purple border-accent-purple/20'
  }[run.provider] || 'bg-gray-700 text-gray-400'

  return (
    <div
      onClick={onClick}
      className={`
        card transition-all duration-200
        ${onClick ? 'cursor-pointer' : ''}
        ${isActive ? 'border-accent-blue/30 bg-blue-950/10 glow-blue' : 'hover:border-bg-border'}
        ${compact ? 'p-3' : 'p-4'}
      `}
    >
      {/* Top row */}
      <div className="flex items-start justify-between gap-2 flex-wrap mb-2">
        <div className="flex items-center gap-2 min-w-0">
          <CopyableId id={run.id} chars={10} />
          {isActive && (
            <span className="text-[10px] bg-accent-blue/15 text-accent-blue border border-accent-blue/25 rounded-full px-2 py-0.5 font-semibold">
              ACTIVE
            </span>
          )}
        </div>
        <StatusBadge status={run.status} size="sm" />
      </div>

      {/* Filename */}
      <div className="flex items-center gap-1.5 mb-3">
        <FileText size={12} className="text-text-tertiary flex-shrink-0" />
        <span className={`text-xs text-text-secondary truncate ${compact ? '' : 'max-w-[200px]'}`}>
          {run.brd_filename || 'Unknown file'}
        </span>
      </div>

      {/* Metadata row */}
      <div className="flex items-center gap-2 flex-wrap mb-3">
        <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-medium border ${providerColor}`}>
          <Cpu size={10} />
          {providerLabel}
          {run.deployment && <span className="opacity-70">/ {run.deployment}</span>}
        </span>

        <span className="flex items-center gap-1 text-[11px] text-text-tertiary">
          <Clock size={10} />
          {elapsed || formatTimeAgo(run.started_at)}
        </span>

        {run.total_cost > 0 && (
          <span className="text-[11px] font-mono text-text-tertiary">
            ${run.total_cost.toFixed(3)}
          </span>
        )}
      </div>

      {/* Action buttons */}
      {!compact && (
        <div className="flex gap-2">
          {!isStageConfirmationPaused && reviewGate && (
            <button
              onClick={handleResumeReview}
              className="flex items-center justify-center gap-1.5 px-3 py-1.5 text-xs font-medium text-accent-blue hover:text-white hover:bg-accent-blue border border-accent-blue/20 rounded-lg transition-colors"
            >
              {reviewGate === 'silver_merge_key_review' ? <Code2 size={11} /> : reviewGate === 1 ? <ShieldCheck size={11} /> : reviewGate === 2 ? <Table2 size={11} /> : reviewGate === 4 || reviewGate === 5 ? <Code2 size={11} /> : <ShieldCheck size={11} />}
              {reviewGate === 'silver_merge_key_review' ? 'Merge Key Review' : `Gate ${reviewGate}`}
            </button>
          )}
          <button
            onClick={handleViewDetail}
            className="flex-1 flex items-center justify-center gap-1.5 px-3 py-1.5 text-xs font-medium text-text-secondary hover:text-text-primary bg-bg-border hover:bg-bg-border rounded-lg transition-colors"
          >
            <ExternalLink size={11} />
            View Detail
          </button>

          {(run.status === 'RUNNING' || run.status === 'SUBMITTED' || run.status === 'HITL_WAIT') && (
            <button
              onClick={handleAbort}
              disabled={aborting}
              className="flex items-center justify-center gap-1.5 px-3 py-1.5 text-xs font-medium text-accent-red hover:text-white hover:bg-accent-red/15 border border-accent-red/20 rounded-lg transition-colors disabled:opacity-50"
            >
              <StopCircle size={11} />
              {aborting ? '…' : 'Abort'}
            </button>
          )}
        </div>
      )}
    </div>
  )
}

function getReviewGate(run) {
  const status = String(run?.status || '').toUpperCase()
  if (!['HITL_WAIT', 'PAUSED_FOR_HITL', 'PENDING_REVIEW'].includes(status)) return null
  if (run?.next_review_key) return run.next_review_key
  const gate = Number(run?.next_gate || 0)
  return [1, 2, 3, 4, 5].includes(gate) ? gate : null
}

function reviewPathForRun(run) {
  const runId = encodeURIComponent(run.id)
  if (run?.next_review_key) {
    return `/app/hitl?runId=${runId}&review=${encodeURIComponent(run.next_review_key)}`
  }
  return `/app/hitl?runId=${runId}&gate=${Number(run.next_gate || 0)}`
}

function formatDuration(ms) {
  if (ms < 0) return '0s'
  const s = Math.floor(ms / 1000)
  const m = Math.floor(s / 60)
  const h = Math.floor(m / 60)
  if (h > 0) return `${h}h ${m % 60}m`
  if (m > 0) return `${m}m ${s % 60}s`
  return `${s}s`
}

function formatTimeAgo(dateStr) {
  if (!dateStr) return ''
  const diff = Date.now() - new Date(dateStr).getTime()
  const s = Math.floor(diff / 1000)
  const m = Math.floor(s / 60)
  const h = Math.floor(m / 60)
  const d = Math.floor(h / 24)
  if (d > 0) return `${d}d ago`
  if (h > 0) return `${h}h ago`
  if (m > 0) return `${m}m ago`
  return 'just now'
}

export default RunCard

