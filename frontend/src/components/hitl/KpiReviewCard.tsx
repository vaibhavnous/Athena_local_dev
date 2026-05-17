// @ts-nocheck
import React, { useState } from 'react'
import { motion } from 'framer-motion'
import {
  Check, X, CheckCircle, ChevronDown, ChevronRight,
  Database, Eye, Ruler, Tag, Clock, User
} from 'lucide-react'
import StatusBadge from '../shared/StatusBadge'

/**
 * KpiReviewCard — reviews a single hitl_review_queue item.
 * Primary shape (item_type = METADATA):
 *   kpi.queue_id, kpi.item_id, kpi.item_type, kpi.gate_status,
 *   kpi.kpi_detail = { table_name, columns: [...] },
 *   kpi.reviewer_id, kpi.decided_at, kpi.auto_approved, kpi.rejection_reason
 *
 * @param {{
 *   kpi: object,
 *   onApprove: (kpiId: string | null) => void,
 *   onEdit: (kpi: object) => void,
 *   onReject: (kpiId: string, reason: string) => void,
 *   localDecision?: string,
 *   rejectionReason?: string
 * }} props
 */
function KpiReviewCard({ kpi, onApprove, onEdit, onReject, localDecision, rejectionReason }) {
  const [showRejectInput, setShowRejectInput] = useState(false)
  const [rejectText, setRejectText] = useState('')
  const [columnsExpanded, setColumnsExpanded] = useState(false)

  // queue_id is the authoritative DB key; id is used by fallback ai_store items
  const kpiId = kpi.queue_id || kpi.id

  const hasDecision     = localDecision || kpi.decision
  const currentDecision = localDecision || kpi.decision

  const decisionConfig = {
    APPROVED: { bg: 'bg-emerald-500/10 border-accent-green/30', label: 'Approved', icon: CheckCircle, color: 'text-accent-green' },
    REJECTED: { bg: 'bg-red-500/10 border-accent-red/30',       label: 'Rejected', icon: X,           color: 'text-accent-red'   }
  }
  const decConf = currentDecision ? decisionConfig[currentDecision] : null

  // ── METADATA content ──────────────────────────────────────────────────────
  const detail   = kpi.kpi_detail || {}
  const allCols  = Array.isArray(detail.columns) ? detail.columns : []
  const columns  = allCols.filter(c => c.column_name !== '__TABLE_SUMMARY__')
  const kpiName = kpi.name || detail.kpi_name || detail.name || 'Unnamed KPI'
  const kpiDefinition = kpi.definition || detail.kpi_description || detail.definition || detail.description || ''
  const tableName = columns.length > 0 ? (kpi.item_id || detail.table_name || 'Unknown Table') : kpiName

  const measures   = columns.filter(c => c.is_measure)
  const dimensions = columns.filter(c => c.is_dimension)
  const piiCols    = columns.filter(c => c.is_pii_candidate)

  const semanticTypeStyle = (type) => ({
    ID:              'bg-blue-500/10 text-accent-blue border-accent-blue/20',
    MEASURE:         'bg-purple-500/10 text-accent-purple border-accent-purple/20',
    DATE:            'bg-teal-500/10 text-accent-teal border-accent-teal/20',
    DIMENSION:       'bg-amber-500/10 text-accent-amber border-accent-amber/20',
    AUDIT_TIMESTAMP: 'bg-gray-500/10 text-text-tertiary border-bg-border',
  }[type] || 'bg-gray-500/10 text-text-tertiary border-bg-border')

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3 }}
      className={`
        card p-5 relative transition-all duration-200
        ${decConf ? `${decConf.bg} border` : 'border border-bg-border hover:border-gray-600'}
      `}
    >
      {/* Decision badge */}
      {hasDecision && decConf && (
        <div className={`absolute top-3 right-3 flex items-center gap-1.5 text-xs font-semibold ${decConf.color}`}>
          <decConf.icon size={13} />
          {decConf.label}
        </div>
      )}

      {/* ── Header ────────────────────────────────────────────────────────── */}
      <div className="mb-3 pr-28">
        <div className="flex items-center gap-2 mb-1.5 flex-wrap">
          <Database size={15} className="text-text-tertiary shrink-0" />
          <h3 className="text-base font-bold text-text-primary">{tableName}</h3>
          <span className="px-2 py-0.5 rounded-full text-[11px] font-medium bg-bg-border text-text-secondary border border-bg-border">
            {kpi.item_type || 'METADATA'}
          </span>
          {kpi.auto_approved && (
            <span className="px-2 py-0.5 rounded-full text-[11px] font-medium bg-teal-500/10 text-accent-teal border border-accent-teal/20">
              Auto-approved
            </span>
          )}
        </div>
        {columns.length > 0 && (
          <div className="flex items-center gap-2 text-xs text-text-tertiary flex-wrap">
            <span>{columns.length} columns</span>
            <span className="opacity-40">·</span>
            <span>{measures.length} measures</span>
            <span className="opacity-40">·</span>
            <span>{dimensions.length} dimensions</span>
            {piiCols.length > 0 && (
              <>
                <span className="opacity-40">·</span>
                <span className="text-accent-red flex items-center gap-0.5">
                  <Eye size={10} /> {piiCols.length} PII
                </span>
              </>
            )}
          </div>
        )}
        {columns.length === 0 && kpiDefinition && (
          <p className="text-sm text-text-secondary leading-relaxed mt-2">{kpiDefinition}</p>
        )}
        {columns.length === 0 && (
          <div className="flex items-center gap-2 text-xs text-text-tertiary flex-wrap mt-2">
            {kpi.domain && <span>{kpi.domain}</span>}
            {kpi.confidence !== undefined && (
              <>
                <span className="opacity-40">·</span>
                <span>{Math.round((kpi.confidence || 0) * 100)}% confidence</span>
              </>
            )}
          </div>
        )}
      </div>

      {/* ── Collapsible column table ─────────────────────────────────────── */}
      {columns.length > 0 && (
        <div className="mb-3">
          <button
            type="button"
            onClick={() => setColumnsExpanded(e => !e)}
            className="flex items-center gap-1.5 text-xs text-text-tertiary hover:text-text-secondary transition-colors mb-1.5"
          >
            {columnsExpanded ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
            {columnsExpanded ? 'Hide' : 'Show'} {columns.length} columns
          </button>
          {columnsExpanded && (
            <div className="rounded-lg border border-bg-border overflow-hidden">
              <div className="overflow-y-auto max-h-64">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="bg-bg-base border-b border-bg-border text-text-tertiary text-[10px] uppercase tracking-wide">
                      <th className="text-left px-3 py-2 font-medium">Column</th>
                      <th className="text-left px-3 py-2 font-medium">Description</th>
                      <th className="text-left px-3 py-2 font-medium">Type</th>
                      <th className="text-left px-3 py-2 font-medium">Flags</th>
                      <th className="text-left px-3 py-2 font-medium">Source</th>
                    </tr>
                  </thead>
                  <tbody>
                    {columns.map((col) => (
                      <tr key={col.column_name} className="border-b border-bg-border/40 hover:bg-bg-hover/20 transition-colors">
                        <td className="px-3 py-2">
                          <div className="font-medium text-text-primary">{col.column_name}</div>
                          {col.suggested_display_name && col.suggested_display_name !== col.column_name && (
                            <div className="text-text-tertiary">{col.suggested_display_name}</div>
                          )}
                        </td>
                        <td className="px-3 py-2 text-text-secondary max-w-[220px]">
                          <span className="line-clamp-2">{col.business_description}</span>
                        </td>
                        <td className="px-3 py-2">
                          <span className={`inline-block px-1.5 py-0.5 rounded border text-[10px] font-medium ${semanticTypeStyle(col.semantic_type)}`}>
                            {col.semantic_type}
                          </span>
                        </td>
                        <td className="px-3 py-2">
                          <div className="flex items-center gap-1.5">
                            {col.is_measure       && <span title="Measure">   <Ruler size={11} className="text-accent-purple" /></span>}
                            {col.is_dimension     && <span title="Dimension">  <Tag   size={11} className="text-accent-amber"  /></span>}
                            {col.is_pii_candidate && <span title="PII">        <Eye   size={11} className="text-accent-red"    /></span>}
                          </div>
                        </td>
                        <td className="px-3 py-2 text-text-tertiary whitespace-nowrap">{col.enrichment_source}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── Review metadata (committed state from DB) ────────────────────── */}
      {kpi.reviewer_id && kpi.decided_at && (
        <div className="mb-3 p-2.5 bg-bg-base rounded-lg border border-bg-border">
          <div className="flex items-center gap-3 text-xs text-text-tertiary flex-wrap">
            <StatusBadge status={kpi.decision || kpi.gate_status} size="sm" />
            <span className="flex items-center gap-1">
              <User size={10} /> {kpi.reviewer_id}
            </span>
            <span className="flex items-center gap-1">
              <Clock size={10} /> {new Date(kpi.decided_at).toLocaleString()}
            </span>
          </div>
        </div>
      )}

      {/* ── Action buttons ───────────────────────────────────────────────────── */}
      {!hasDecision && !showRejectInput && (
        <div className="flex gap-2 mt-auto">
          <button
            onClick={() => onApprove(kpiId)}
            className="flex-1 flex items-center justify-center gap-1.5 px-3 py-2 bg-accent-green/10 hover:bg-accent-green/20 border border-accent-green/25 text-accent-green text-sm font-semibold rounded-lg transition-colors"
          >
            <Check size={14} strokeWidth={2.5} />
            Approve
          </button>
          <button
            onClick={() => { setShowRejectInput(true); setRejectText('') }}
            className="flex-1 flex items-center justify-center gap-1.5 px-3 py-2 bg-red-500/10 hover:bg-red-500/20 border border-accent-red/25 text-accent-red text-sm font-semibold rounded-lg transition-colors"
          >
            <X size={14} strokeWidth={2.5} />
            Reject
          </button>
        </div>
      )}

      {/* ── Inline rejection textarea ────────────────────────────────────────── */}
      {!hasDecision && showRejectInput && (
        <div className="mt-3 space-y-2">
          <label className="text-xs font-medium text-accent-red">
            Rejection Reason <span className="text-accent-red">*</span>
          </label>
          <textarea
            autoFocus
            rows={2}
            value={rejectText}
            onChange={e => setRejectText(e.target.value)}
            onKeyDown={e => {
              if (e.key === 'Escape') { setShowRejectInput(false); setRejectText('') }
              if (e.key === 'Enter' && !e.shiftKey && rejectText.trim()) {
                e.preventDefault()
                onReject(kpiId, rejectText.trim())
                setShowRejectInput(false)
                setRejectText('')
              }
            }}
            placeholder="Describe why this item is being rejected…"
            className="w-full text-xs bg-bg-base border border-accent-red/40 rounded-lg px-3 py-2 text-text-primary placeholder-text-tertiary focus:outline-none focus:border-accent-red resize-none"
          />
          <div className="flex gap-2">
            <button
              onClick={() => { setShowRejectInput(false); setRejectText('') }}
              className="flex-1 px-3 py-1.5 text-xs font-semibold text-text-tertiary bg-bg-border hover:bg-bg-hover rounded-lg transition-colors"
            >
              Cancel
            </button>
            <button
              disabled={!rejectText.trim()}
              onClick={() => {
                onReject(kpiId, rejectText.trim())
                setShowRejectInput(false)
                setRejectText('')
              }}
              className="flex-1 flex items-center justify-center gap-1.5 px-3 py-1.5 text-xs font-semibold bg-accent-red/10 hover:bg-accent-red/20 border border-accent-red/30 text-accent-red rounded-lg transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
            >
              <X size={12} />
              Confirm Reject
            </button>
          </div>
        </div>
      )}

      {/* Rejection reason display (after decision) */}
      {currentDecision === 'REJECTED' && (rejectionReason || kpi.rejection_reason) && (
        <p className="mt-2 text-xs text-accent-red/80 italic">
          "{rejectionReason || kpi.rejection_reason}"
        </p>
      )}

      {/* Undo button */}
      {hasDecision && (
        <button
          onClick={() => { if (localDecision) onApprove(null) }}
          className="text-xs text-text-tertiary hover:text-text-secondary transition-colors mt-2"
        >
          ← Change decision
        </button>
      )}
    </motion.div>
  )
}

export default KpiReviewCard

