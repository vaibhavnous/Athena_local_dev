// @ts-nocheck
import React, { useEffect, useMemo, useState } from 'react'
import { Check, CheckCircle, ChevronDown, ChevronRight, Clock3, Database, Layers3, Pencil, RotateCcw, X, XCircle } from 'lucide-react'

const TYPE_STYLES = {
  MEASURE: 'border-[#188461]/40 bg-[#0f3f37] text-[#4ee3ad]',
  DIMENSION: 'border-[#237c86]/40 bg-[#0d3740] text-[#74d6e7]',
  ID: 'border-[#237c86]/40 bg-[#0d3740] text-[#74d6e7]',
  SURROGATE_KEY: 'border-[#237c86]/40 bg-[#0d3740] text-[#74d6e7]',
  DATE: 'border-[#8b7430]/45 bg-[#3a3218] text-[#f4cf65]',
  AUDIT_TIMESTAMP: 'border-[#8b7430]/45 bg-[#3a3218] text-[#f4cf65]',
  PII: 'border-[#8a3148]/45 bg-[#2f1722] text-[#ff7d98]',
  FLAG: 'border-[#426a76]/45 bg-[#132f36] text-[#a5c7d0]',
  UNKNOWN: 'border-[#426a76]/45 bg-[#132f36] text-[#a5c7d0]',
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
    pii_type: column?.pii_type || column?.pii_category || '-',
  }))
}

function formatDateTime(value) {
  if (!value) return '-'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return String(value)
  return date.toLocaleString()
}

function BooleanCell({ value, editable, onChange }) {
  if (!editable) {
    return value
      ? <Check size={13} strokeWidth={2.5} className="text-[#27d3a0]" />
      : <span className="text-[#79969a]">—</span>
  }

  return (
    <input
      type="checkbox"
      checked={Boolean(value)}
      onChange={(event) => onChange(event.target.checked)}
      className="h-4 w-4 rounded border-[#3f756f] bg-[#071c24] accent-[#20d39b]"
      title={value ? 'Enabled' : 'Disabled'}
    />
  )
}

function SemanticReviewCard({ item, localDecision, rejectionReason, onApprove, onReject, onDraftChange }) {
  const [reason, setReason] = useState(rejectionReason || '')
  const [expanded, setExpanded] = useState(false)
  const [editing, setEditing] = useState(false)
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
  const isDirty = JSON.stringify(draftColumns) !== JSON.stringify(initialColumns) || draftSummary !== initialSummary

  useEffect(() => {
    setDraftColumns(initialColumns)
    setDraftSummary(initialSummary)
    setReason(rejectionReason || '')
    setEditing(false)
    onDraftChange?.(id, { table_name: tableName, table_summary: initialSummary, columns: initialColumns })
    // Reset the local edit surface only when the review item changes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id, initialColumns, initialSummary])

  useEffect(() => {
    setReason(rejectionReason || '')
  }, [rejectionReason])

  const updateColumn = (index, field, value) => {
    setDraftColumns((prev) => {
      const next = prev.map((column, columnIndex) => (
        columnIndex === index ? { ...column, [field]: value } : column
      ))
      onDraftChange?.(id, { table_name: tableName, table_summary: draftSummary, columns: next })
      return next
    })
  }

  const updateSummary = (value) => {
    setDraftSummary(value)
    onDraftChange?.(id, { table_name: tableName, table_summary: value, columns: draftColumns })
  }

  const resetDraftColumns = () => {
    setDraftColumns(initialColumns)
    setDraftSummary(initialSummary)
    onDraftChange?.(id, { table_name: tableName, table_summary: initialSummary, columns: initialColumns })
  }

  const toggleExpanded = () => {
    setExpanded((value) => {
      if (value) setEditing(false)
      return !value
    })
  }

  return (
    <div className={`overflow-hidden rounded-[14px] border transition-colors ${
      isApproved
        ? 'border-[#17735f] bg-[#092e2f]'
        : isRejected
        ? 'border-[#723148] bg-[#2a1622]'
        : 'border-[#155f5a] bg-[#092e2f]'
    }`}>
      <div className="px-4 pb-3 pt-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <div className="flex min-w-0 items-center gap-2">
              <Database size={14} className="shrink-0 text-[#42c9a5]" />
              <h3 className="truncate text-[15px] font-extrabold text-white">{tableName}</h3>
              <span className="rounded border border-[#2f615d] bg-[#0b222b] px-2 py-0.5 text-[9px] font-bold uppercase text-[#9fb7bd]">
                Enrichment
              </span>
            </div>
            <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px] text-[#87a7aa]">
              <span className="inline-flex items-center gap-1"><Layers3 size={11} /> {draftColumns.length} column{draftColumns.length !== 1 ? 's' : ''}</span>
              <span className="inline-flex items-center gap-1 text-[#4fc8ff]"><CheckCircle size={11} /> {llmColumns} LLM-enriched</span>
              <span className="inline-flex items-center gap-1"><Clock3 size={11} /> Queued: {formatDateTime(queuedAt)}</span>
              <span className="inline-flex items-center gap-1"><Clock3 size={11} /> Decided: {formatDateTime(decidedAt)}</span>
            </div>
          </div>

          <div className="flex items-center gap-2">
            {decision && (
              <span className={`inline-flex items-center gap-1 rounded-full px-2.5 py-1 text-[11px] font-bold ${
                isApproved ? 'text-[#45daa9]' : 'text-red-300'
              }`}>
                {isApproved ? <CheckCircle size={13} /> : <XCircle size={13} />}
                {isApproved ? 'Approved' : 'Rejected'}
              </span>
            )}
            {expanded && (
              <div className="flex items-center gap-2">
                {editing && isDirty && (
                  <button
                    type="button"
                    onClick={resetDraftColumns}
                    className="inline-flex h-8 items-center gap-1.5 rounded-md border border-[#2f615d] bg-[#0b222b] px-2.5 text-[11px] font-semibold text-[#a8c0c4] transition-all hover:border-[#45a391] hover:text-white"
                  >
                    <RotateCcw size={12} />
                    Reset
                  </button>
                )}
                <button
                  type="button"
                  onClick={() => setEditing((value) => !value)}
                  className={`inline-flex h-8 items-center gap-1.5 rounded-md border px-2.5 text-[11px] font-semibold transition-all ${
                    editing
                      ? 'border-[#28a989] bg-[#123b35] text-[#58e2b6]'
                      : 'border-[#2f615d] bg-[#0b222b] text-[#a8c0c4] hover:border-[#45a391] hover:text-white'
                  }`}
                >
                  {editing ? <Check size={12} /> : <Pencil size={12} />}
                  {editing ? 'Finish editing' : 'Edit columns'}
                </button>
              </div>
            )}
          </div>
        </div>

        <button
          type="button"
          onClick={toggleExpanded}
          aria-expanded={expanded}
          className="mt-3 inline-flex items-center gap-1 text-[11px] font-semibold text-[#b7c9cc] transition-colors hover:text-white"
        >
          {expanded ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
          {expanded ? `Hide columns (${draftColumns.length})` : `Show columns (${draftColumns.length})`}
        </button>
      </div>

      <div className={`mx-4 mb-4 overflow-hidden transition-all duration-200 ${expanded ? 'max-h-[min(72vh,760px)] opacity-100' : 'max-h-0 opacity-0'}`}>
        <div className="max-h-[min(68vh,720px)] overflow-auto rounded-md border border-[#2c6a63]/60 bg-[#071c24]">
          <div className="grid min-w-[900px] grid-cols-[1.05fr_1.15fr_1fr_2fr_0.75fr_0.28fr_0.28fr_0.28fr] gap-3 border-b border-[#2c6a63]/60 bg-[#061923] px-3 py-3 text-[10px] font-bold uppercase tracking-wide text-[#88a5aa]">
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
            <div className="px-3 py-5 text-sm text-[#a8c0c4]">No enriched columns returned for this item.</div>
          ) : (
            <div className="divide-y divide-[#2c6a63]/60">
              {draftColumns.map((column, index) => (
                <div
                  key={`${column.column_name || index}`}
                  className="grid min-w-[900px] grid-cols-[1.05fr_1.15fr_1fr_2fr_0.75fr_0.28fr_0.28fr_0.28fr] items-center gap-3 px-3 py-2.5 text-xs text-[#c8dde0]"
                >
                  <span className="min-w-0 truncate font-bold text-white" title={column.column_name}>
                    {column.column_name || '-'}
                  </span>

                  {editing ? (
                    <input
                      value={column.suggested_display_name}
                      onChange={(event) => updateColumn(index, 'suggested_display_name', event.target.value)}
                      className="h-9 min-w-0 rounded-md border border-[#2f615d] bg-[#061923] px-2.5 text-xs text-white outline-none focus:border-[#45c7a5]"
                    />
                  ) : (
                    <span className="min-w-0 truncate text-[#c4d3d6]" title={column.suggested_display_name}>{column.suggested_display_name || '—'}</span>
                  )}

                  {editing ? (
                    <select
                      value={column.semantic_type || 'UNKNOWN'}
                      onChange={(event) => updateColumn(index, 'semantic_type', event.target.value)}
                      className={`h-9 min-w-0 rounded-md border px-2 text-[10px] font-semibold outline-none focus:border-[#45c7a5] ${semanticTypeClass(column.semantic_type)}`}
                    >
                      {SEMANTIC_TYPE_OPTIONS.map((option) => (
                        <option key={option} value={option}>{option}</option>
                      ))}
                    </select>
                  ) : (
                    <span className={`inline-flex w-fit rounded border px-2 py-1 text-[9px] font-bold ${semanticTypeClass(column.semantic_type)}`}>
                      {column.semantic_type || 'UNKNOWN'}
                    </span>
                  )}

                  {editing ? (
                    <textarea
                      value={column.business_description}
                      onChange={(event) => updateColumn(index, 'business_description', event.target.value)}
                      rows={2}
                      className="min-h-[48px] w-full resize-y rounded-md border border-[#2f615d] bg-[#061923] px-2.5 py-2 text-xs text-white outline-none focus:border-[#45c7a5]"
                    />
                  ) : (
                    <span className="min-w-0 truncate text-[#c4d3d6]" title={column.business_description}>{column.business_description || '—'}</span>
                  )}

                  <span className="inline-flex w-fit rounded bg-[#172633] px-2 py-1 text-[10px] font-semibold text-[#9fb7bd]">
                    {column.enrichment_source || '-'}
                  </span>
                  <BooleanCell value={column.is_measure} editable={editing} onChange={(value) => updateColumn(index, 'is_measure', value)} />
                  <BooleanCell value={column.is_dimension} editable={editing} onChange={(value) => updateColumn(index, 'is_dimension', value)} />
                  <div className="flex min-w-0 flex-col items-start gap-1">
                    <BooleanCell value={column.is_pii_candidate} editable={editing} onChange={(value) => updateColumn(index, 'is_pii_candidate', value)} />
                    {editing && column.is_pii_candidate && (
                      <input
                        value={column.pii_type === '-' ? '' : column.pii_type}
                        onChange={(event) => updateColumn(index, 'pii_type', event.target.value)}
                        placeholder="type"
                        className="h-7 w-20 rounded border border-[#2f615d] bg-[#061923] px-1 text-[9px] text-white outline-none focus:border-[#45c7a5]"
                      />
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}

          {draftColumns.length > 0 && (
            <div className="border-t border-[#2c6a63]/60 px-3 py-3 text-[11px] text-[#9fb7bd]">
              <div className="flex items-start gap-2">
                <span className="shrink-0 font-bold text-[#d5e7e8]">Table Summary:</span>
                {editing ? (
                  <textarea
                    value={draftSummary}
                    onChange={(event) => updateSummary(event.target.value)}
                    rows={2}
                    className="w-full resize-y rounded-md border border-[#2f615d] bg-[#061923] px-2 py-2 text-xs text-white outline-none focus:border-[#45c7a5]"
                  />
                ) : (
                  <p className="leading-relaxed text-[#9fb7bd]">{draftSummary}</p>
                )}
              </div>
            </div>
          )}
        </div>
      </div>

      {expanded && (
        <>
          <div className="border-t border-[#155f5a] bg-[#071c24] px-4 py-3 text-[11px] text-[#8ea9ad]">
            Use Edit columns to change semantic values. Approved edits are persisted to the Gate 3 enrichment artifact.
          </div>

          <div className="flex flex-col gap-3 border-t border-[#155f5a] bg-[#071c24] px-4 py-4 md:flex-row md:items-center md:justify-between">
            <input
              value={reason}
              onChange={(event) => setReason(event.target.value)}
              placeholder="Rejection reason..."
              className="h-10 flex-1 rounded-md border border-[#2f615d] bg-[#061923] px-3 text-xs text-white outline-none placeholder:text-[#638287] focus:border-[#45c7a5]"
            />
            <div className="flex gap-2">
              <button
                type="button"
                onClick={() => onReject(id, reason || 'Rejected by reviewer')}
                className="inline-flex h-10 items-center gap-2 rounded-md border border-red-500/30 bg-[#2b1724] px-3 text-xs font-bold text-red-300 transition-colors hover:bg-red-500/10"
              >
                <X size={14} />
                Reject
              </button>
              <button
                type="button"
                onClick={() => onApprove(id, { table_name: tableName, table_summary: draftSummary, columns: draftColumns })}
                className="inline-flex h-10 items-center gap-2 rounded-md border border-[#14856d] bg-[#103533] px-3 text-xs font-bold text-[#31d49f] transition-colors hover:bg-[#15413d]"
              >
                <CheckCircle size={14} />
                Approve
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  )
}

export default SemanticReviewCard
