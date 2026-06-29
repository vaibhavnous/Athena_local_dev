// @ts-nocheck
import React, { useState } from 'react'
import { CheckCircle, XCircle } from 'lucide-react'

const TYPE_STYLES = {
  MEASURE: 'border-emerald-500/25 bg-emerald-500/10 text-emerald-300',
  DIMENSION: 'border-blue-500/25 bg-blue-500/10 text-blue-300',
  ID: 'border-cyan-500/25 bg-cyan-500/10 text-cyan-300',
  SURROGATE_KEY: 'border-cyan-500/25 bg-cyan-500/10 text-cyan-300',
  DATE: 'border-amber-500/25 bg-amber-500/10 text-amber-300',
  AUDIT_TIMESTAMP: 'border-amber-500/25 bg-amber-500/10 text-amber-300',
  PII: 'border-red-500/25 bg-red-500/10 text-red-300',
  FLAG: 'border-purple-500/25 bg-purple-500/10 text-purple-300',
}

function semanticTypeClass(type) {
  return TYPE_STYLES[String(type || '').toUpperCase()] || 'border-slate-500/25 bg-slate-500/10 text-slate-300'
}

function itemId(item) {
  return item?.queue_id || item?.id || item?.item_id
}

function SemanticReviewCard({ item, localDecision, rejectionReason, onApprove, onReject }) {
  const [reason, setReason] = useState(rejectionReason || '')
  const columns = item?.item_detail?.columns || []
  const decision = localDecision || item?.decision
  const id = itemId(item)

  return (
    <div className={`rounded-xl border bg-[#0f1a2e] p-4 transition-colors ${
      decision === 'APPROVED'
        ? 'border-emerald-500/35'
        : decision === 'REJECTED'
        ? 'border-red-500/35'
        : 'border-[#22304b]'
    }`}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="text-sm font-bold text-text-primary">
            {item?.item_detail?.table_name || item?.item_id || 'Semantic Enrichment'}
          </div>
          <div className="mt-1 text-xs text-text-secondary">
            {columns.length} enriched column{columns.length !== 1 ? 's' : ''}
          </div>
        </div>
        {decision && (
          <span className={`rounded-full border px-2.5 py-1 text-[10px] font-bold ${
            decision === 'APPROVED'
              ? 'border-emerald-500/25 bg-emerald-500/10 text-emerald-300'
              : 'border-red-500/25 bg-red-500/10 text-red-300'
          }`}>
            {decision}
          </span>
        )}
      </div>

      <div className="mt-4 overflow-hidden rounded-lg border border-[#22304b]">
        <div className="grid grid-cols-[1.2fr_1fr_1.5fr_0.8fr] gap-3 bg-[#0b1424] px-3 py-2 text-[10px] font-semibold uppercase tracking-wide text-[#7f8eab]">
          <span>Column</span>
          <span>Semantic Type</span>
          <span>Description</span>
          <span>Source</span>
        </div>
        <div className="divide-y divide-[#22304b]">
          {columns.length === 0 ? (
            <div className="px-3 py-4 text-sm text-text-secondary">No enriched columns returned for this item.</div>
          ) : (
            columns.map((column, index) => (
              <div key={`${column.column_name || index}`} className="grid grid-cols-[1.2fr_1fr_1.5fr_0.8fr] gap-3 px-3 py-3 text-xs">
                <div className="min-w-0">
                  <div className="truncate font-semibold text-text-primary">{column.suggested_display_name || column.column_name}</div>
                  <div className="mt-0.5 truncate font-mono text-[10px] text-[#7f8eab]">{column.column_name}</div>
                </div>
                <div>
                  <span className={`inline-flex rounded-md border px-2 py-1 text-[10px] font-semibold ${semanticTypeClass(column.semantic_type)}`}>
                    {column.semantic_type || 'UNKNOWN'}
                  </span>
                </div>
                <div className="text-text-secondary">{column.business_description || '-'}</div>
                <div className="text-[#7f8eab]">{column.enrichment_source || '-'}</div>
              </div>
            ))
          )}
        </div>
      </div>

      <div className="mt-4 flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
        <input
          value={reason}
          onChange={(event) => setReason(event.target.value)}
          placeholder="Rejection reason..."
          className="h-10 flex-1 rounded-lg border border-[#253044] bg-[#0a1220] px-3 text-xs text-text-primary outline-none placeholder:text-[#64748b] focus:border-accent-blue"
        />
        <div className="flex gap-2">
          <button
            type="button"
            onClick={() => onReject(id, reason || 'Rejected by reviewer')}
            className="inline-flex h-10 items-center gap-2 rounded-lg border border-red-500/25 px-3 text-xs font-bold text-red-300 transition-colors hover:bg-red-500/10"
          >
            <XCircle size={14} />
            Reject
          </button>
          <button
            type="button"
            onClick={() => onApprove(id)}
            className="inline-flex h-10 items-center gap-2 rounded-lg border border-emerald-500/25 px-3 text-xs font-bold text-emerald-300 transition-colors hover:bg-emerald-500/10"
          >
            <CheckCircle size={14} />
            Approve
          </button>
        </div>
      </div>
    </div>
  )
}

export default SemanticReviewCard
