// @ts-nocheck
import React, { useEffect, useMemo, useState } from 'react'
import { Check, CheckCircle, ChevronDown, ChevronRight, Clock3, Database, Pencil, RotateCcw, X, XCircle } from 'lucide-react'

const TYPE_STYLES = {
  MEASURE: 'border-emerald-500/25 bg-emerald-500/10 text-emerald-300',
  DIMENSION: 'border-blue-500/25 bg-blue-500/10 text-blue-300',
  ID: 'border-cyan-500/25 bg-cyan-500/10 text-cyan-300',
  SURROGATE_KEY: 'border-cyan-500/25 bg-cyan-500/10 text-cyan-300',
  DATE: 'border-amber-500/25 bg-amber-500/10 text-amber-300',
  AUDIT_TIMESTAMP: 'border-amber-500/25 bg-amber-500/10 text-amber-300',
  PII: 'border-red-500/25 bg-red-500/10 text-red-300',
  FLAG: 'border-purple-500/25 bg-purple-500/10 text-purple-300',
  UNKNOWN: 'border-slate-500/25 bg-slate-500/10 text-slate-300',
}

const SEMANTIC_TYPE_OPTIONS = ['MEASURE', 'DIMENSION', 'ID', 'SURROGATE_KEY', 'DATE', 'AUDIT_TIMESTAMP', 'PII', 'FLAG', 'UNKNOWN']
const EMPTY_COLUMNS = []

function semanticTypeClass(type) {
  return TYPE_STYLES[String(type || '').toUpperCase()] || TYPE_STYLES.UNKNOWN
}

function itemId(item) {
  return item?.queue_id || item?.id || item?.item_id
}

function normalizeColumns(columns = []) {
  return (columns || []).map((column) => ({
    ...column,
    suggested_display_name: column?.suggested_display_name || column?.column_name || '',
    semantic_type: String(column?.semantic_type || 'UNKNOWN').toUpperCase(),
    business_description: column?.business_description || '',
    enrichment_source: column?.enrichment_source || '-',
    is_measure: Boolean(column?.is_measure),
    is_dimension: Boolean(column?.is_dimension),
    is_pii_candidate: Boolean(column?.is_pii_candidate || column?.is_pii),
  }))
}

function formatDateTime(value) {
  if (!value) return '-'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return String(value)
  return date.toLocaleString()
}

function sourceLabel(value) {
  const normalized = String(value || '').toLowerCase()
  if (normalized.includes('llm')) return 'LLM'
  if (normalized.includes('cache')) return 'cache'
  return value || '-'
}

function BooleanCell({ value, editable, onChange }) {
  if (!editable) {
    return (
      <span className={value ? 'text-emerald-300' : 'text-[#8fa0bf]'}>
        {value ? <Check size={13} strokeWidth={2.5} /> : '-'}
      </span>
    )
  }

  return (
    <button
      type="button"
      onClick={() => onChange(!value)}
      className={`inline-flex h-7 w-7 items-center justify-center rounded-md border transition-colors ${
        value
          ? 'border-emerald-500/40 bg-emerald-500/15 text-emerald-300'
          : 'border-[#31415f] bg-[#0a1220] text-[#7f8eab] hover:border-[#4a5e84]'
      }`}
      title={value ? 'Enabled' : 'Disabled'}
    >
      {value ? <Check size={13} strokeWidth={2.5} /> : '-'}
    </button>
  )
}

function SemanticReviewCard({ item, localDecision, rejectionReason, onApprove, onReject }) {
  const [reason, setReason] = useState(rejectionReason || '')
  const [isEditing, setIsEditing] = useState(false)
  const [expanded, setExpanded] = useState(true)
  const columns = item?.item_detail?.columns || EMPTY_COLUMNS
  const decision = localDecision || item?.decision
  const id = itemId(item)
  const tableName = item?.item_detail?.table_name || item?.item_id || 'Semantic Review'
  const initialColumns = useMemo(() => normalizeColumns(columns), [columns])
  const initialSummary = item?.item_detail?.table_summary || item?.summary || 'Review enriched semantic labels and business descriptions before continuing.'
  const [draftColumns, setDraftColumns] = useState(() => initialColumns)
  const [draftSummary, setDraftSummary] = useState(initialSummary)
  const llmColumns = draftColumns.filter((column) => String(column.enrichment_source || '').toLowerCase().includes('llm')).length
  const queuedAt = item?.queued_at || item?.created_at
  const decidedAt = item?.decided_at
  const isApproved = decision === 'APPROVED'
  const isRejected = decision === 'REJECTED'

  useEffect(() => {
    setDraftColumns(initialColumns)
    setDraftSummary(initialSummary)
    setReason(rejectionReason || '')
    setIsEditing(false)
    setExpanded(true)
  }, [initialColumns, initialSummary, rejectionReason])

  const updateColumn = (index, field, value) => {
    setDraftColumns((prev) => prev.map((column, columnIndex) => (
      columnIndex === index ? { ...column, [field]: value } : column
    )))
  }

  const resetDraftColumns = () => {
    setDraftColumns(initialColumns)
    setDraftSummary(initialSummary)
    setIsEditing(false)
  }

  return (
    <div className={`overflow-hidden rounded-[12px] border transition-colors ${
      isApproved
        ? 'border-emerald-500/35 bg-[#0d302f]'
        : isRejected
        ? 'border-red-500/35 bg-[#2a1622]'
        : 'border-[#1f6b64]/45 bg-[#0d302f]'
    }`}>
      <div className="px-4 py-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <div className="flex min-w-0 items-center gap-2">
              <Database size={14} className="shrink-0 text-[#7fa8ff]" />
              <h3 className="truncate text-[15px] font-extrabold text-white">{tableName}</h3>
              <span className="rounded-md border border-[#31415f] bg-[#111b2d] px-2 py-0.5 text-[9px] font-bold uppercase text-[#c6d2e8]">
                Enrichment
              </span>
            </div>
            <div className="mt-2 flex flex-wrap items-center gap-3 text-[11px] text-[#9ca9bd]">
              <span>{draftColumns.length} column{draftColumns.length !== 1 ? 's' : ''}</span>
              <span>{llmColumns} LLM-enriched</span>
              {isEditing && <span className="rounded-full border border-[#3f82ff]/35 bg-[#3f82ff]/10 px-2 py-0.5 font-bold text-[#78a9ff]">Local edit mode</span>}
              <span className="inline-flex items-center gap-1"><Clock3 size={11} /> Queued: {formatDateTime(queuedAt)}</span>
              <span className="inline-flex items-center gap-1"><Clock3 size={11} /> Decided: {formatDateTime(decidedAt)}</span>
            </div>
          </div>

          <div className="flex items-center gap-2">
            {decision && (
              <span className={`inline-flex items-center gap-1 rounded-full px-2.5 py-1 text-[11px] font-bold ${
                isApproved ? 'text-emerald-300' : 'text-red-300'
              }`}>
                {isApproved ? <CheckCircle size={13} /> : <XCircle size={13} />}
                {isApproved ? 'Approved' : 'Rejected'}
              </span>
            )}
            <button
              type="button"
              onClick={() => setIsEditing((value) => !value)}
              className="inline-flex h-9 items-center gap-2 rounded-[8px] border border-[#31415f] bg-[#101a2b] px-3 text-xs font-semibold text-[#c6d2e8] transition-colors hover:border-[#4a5e84] hover:text-white"
            >
              <Pencil size={13} />
              {isEditing ? 'Done Editing' : 'Edit'}
            </button>
            {isEditing && (
              <button
                type="button"
                onClick={resetDraftColumns}
                className="inline-flex h-9 items-center gap-2 rounded-[8px] border border-[#31415f] bg-[#101a2b] px-3 text-xs font-semibold text-[#c6d2e8] transition-colors hover:border-[#4a5e84] hover:text-white"
              >
                <RotateCcw size={13} />
                Reset
              </button>
            )}
          </div>
        </div>

        <button
          type="button"
          onClick={() => setExpanded((value) => !value)}
          className="mt-3 inline-flex items-center gap-1 text-[11px] font-semibold text-[#c6d2e8] transition-colors hover:text-white"
        >
          {expanded ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
          {expanded ? `Hide columns (${draftColumns.length})` : `Show columns (${draftColumns.length})`}
        </button>
      </div>

      {expanded && (
        <div className="mx-4 mb-4 overflow-hidden rounded-[8px] border border-[#6f7f95]/30">
          <div className="grid grid-cols-[1.05fr_1.15fr_1.2fr_2fr_0.65fr_0.3fr_0.3fr_0.35fr] gap-2 bg-[#101827] px-3 py-2 text-[10px] font-bold uppercase text-[#9ca9bd]">
            <span>Column</span>
            <span>Display Name</span>
            <span>Semantic Type</span>
            <span>Description</span>
            <span>Source</span>
            <span>M</span>
            <span>D</span>
            <span>PII</span>
          </div>

          {draftColumns.length === 0 ? (
            <div className="px-3 py-5 text-sm text-[#b6c2d5]">No enriched columns returned for this item.</div>
          ) : (
            <div className="divide-y divide-[#6f7f95]/35">
              {draftColumns.map((column, index) => (
                <div
                  key={`${column.column_name || index}`}
                  className="grid grid-cols-[1.05fr_1.15fr_1.2fr_2fr_0.65fr_0.3fr_0.3fr_0.35fr] items-center gap-2 px-3 py-2 text-xs text-[#d7e2f2]"
                >
                  <span className="min-w-0 truncate font-bold text-white" title={column.column_name}>
                    {column.column_name || '-'}
                  </span>

                  {isEditing ? (
                    <input
                      value={column.suggested_display_name}
                      onChange={(event) => updateColumn(index, 'suggested_display_name', event.target.value)}
                      className="h-8 min-w-0 rounded-md border border-[#31415f] bg-[#0a1220] px-2 text-xs text-white outline-none focus:border-[#78a9ff]"
                    />
                  ) : (
                    <span className="min-w-0 truncate text-[#c6d2e8]" title={column.suggested_display_name}>
                      {column.suggested_display_name || '-'}
                    </span>
                  )}

                  {isEditing ? (
                    <select
                      value={column.semantic_type || 'UNKNOWN'}
                      onChange={(event) => updateColumn(index, 'semantic_type', event.target.value)}
                      className="h-8 min-w-0 rounded-md border border-[#31415f] bg-[#0a1220] px-2 text-[10px] font-semibold text-white outline-none focus:border-[#78a9ff]"
                    >
                      {SEMANTIC_TYPE_OPTIONS.map((option) => (
                        <option key={option} value={option}>{option}</option>
                      ))}
                    </select>
                  ) : (
                    <span className={`inline-flex w-fit rounded-md border px-2 py-1 text-[10px] font-bold ${semanticTypeClass(column.semantic_type)}`}>
                      {column.semantic_type || 'UNKNOWN'}
                    </span>
                  )}

                  {isEditing ? (
                    <textarea
                      value={column.business_description}
                      onChange={(event) => updateColumn(index, 'business_description', event.target.value)}
                      rows={2}
                      className="min-h-[34px] w-full resize-y rounded-md border border-[#31415f] bg-[#0a1220] px-2 py-1.5 text-xs text-white outline-none focus:border-[#78a9ff]"
                    />
                  ) : (
                    <span className="min-w-0 text-[#c6d2e8]" title={column.business_description}>
                      {column.business_description || '-'}
                    </span>
                  )}

                  <span className={`w-fit rounded-md px-2 py-1 text-[10px] font-bold ${
                    String(column.enrichment_source || '').toLowerCase().includes('llm')
                      ? 'bg-[#20395b] text-[#8eb9ff]'
                      : 'bg-[#263247] text-[#b7c4d9]'
                  }`}>
                    {sourceLabel(column.enrichment_source)}
                  </span>

                  <BooleanCell value={column.is_measure} editable={isEditing} onChange={(value) => updateColumn(index, 'is_measure', value)} />
                  <BooleanCell value={column.is_dimension} editable={isEditing} onChange={(value) => updateColumn(index, 'is_dimension', value)} />
                  <BooleanCell value={column.is_pii_candidate} editable={isEditing} onChange={(value) => updateColumn(index, 'is_pii_candidate', value)} />
                </div>
              ))}
            </div>
          )}

          {draftColumns.length > 0 && (
            <div className="border-t border-[#6f7f95]/30 px-3 py-3 text-[11px] text-[#aebbd0]">
              <div className="mb-2 font-bold text-[#d8e3f3]">Table Summary</div>
              {isEditing ? (
                <textarea
                  value={draftSummary}
                  onChange={(event) => setDraftSummary(event.target.value)}
                  rows={3}
                  className="w-full resize-y rounded-md border border-[#31415f] bg-[#0a1220] px-2 py-2 text-xs text-white outline-none focus:border-[#78a9ff]"
                />
              ) : (
                <div>{draftSummary}</div>
              )}
            </div>
          )}
        </div>
      )}

      <div className="border-t border-[#1f6b64]/35 bg-[#0f1728] px-4 py-3 text-[11px] text-[#91a4cb]">
        Edits are local to this review screen. Approve/reject submits the gate decision only; edited semantic values are not persisted to backend.
      </div>

      <div className="flex flex-col gap-3 border-t border-[#1f6b64]/35 bg-[#0f1728] px-4 py-4 md:flex-row md:items-center md:justify-between">
        <input
          value={reason}
          onChange={(event) => setReason(event.target.value)}
          placeholder="Rejection reason..."
          className="h-10 flex-1 rounded-[8px] border border-[#31415f] bg-[#0a1220] px-3 text-xs text-white outline-none placeholder:text-[#64748b] focus:border-[#78a9ff]"
        />
        <div className="flex gap-2">
          <button
            type="button"
            onClick={() => onReject(id, reason || 'Rejected by reviewer')}
            className="inline-flex h-10 items-center gap-2 rounded-[8px] border border-red-500/30 bg-[#2b1724] px-3 text-xs font-bold text-red-300 transition-colors hover:bg-red-500/10"
          >
            <X size={14} />
            Reject
          </button>
          <button
            type="button"
            onClick={() => onApprove(id)}
            className="inline-flex h-10 items-center gap-2 rounded-[8px] border border-emerald-500/30 bg-[#123027] px-3 text-xs font-bold text-emerald-300 transition-colors hover:bg-emerald-500/10"
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
