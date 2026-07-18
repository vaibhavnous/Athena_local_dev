// @ts-nocheck
import React, { useEffect, useMemo, useState } from 'react'
import { createPortal } from 'react-dom'
import { Check, CheckCircle2, ChevronDown, ChevronRight, Clock3, Database, Layers3, Pencil, Save, Sparkles, X } from 'lucide-react'

const SEMANTIC_TYPE_OPTIONS = ['MEASURE', 'DIMENSION', 'ID', 'SURROGATE_KEY', 'DATE', 'AUDIT_TIMESTAMP', 'PII', 'FLAG', 'UNKNOWN']
const EMPTY_COLUMNS = []

function itemId(item) {
  return item?.queue_id || item?.id || item?.item_id
}

function normalizeColumns(columns = []) {
  return (columns || []).map((column) => ({
    ...column,
    suggested_display_name: column?.suggested_display_name || column?.display_name || column?.column_name || '',
    semantic_type: String(column?.semantic_type || 'UNKNOWN').toUpperCase(),
    business_description: column?.business_description || column?.description || '',
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

function ColumnCheck({ checked, onChange, label }) {
  return (
    <label className="flex items-center justify-center" title={label}>
      <input
        type="checkbox"
        aria-label={label}
        checked={Boolean(checked)}
        onChange={(event) => onChange(event.target.checked)}
        className="h-5 w-5 rounded border-[#52617a] bg-[#07101f] accent-[#4388ff]"
      />
    </label>
  )
}

function SemanticEditModal({ tableName, tableSummary, columns, onClose, onSave }) {
  const [draftColumns, setDraftColumns] = useState(() => normalizeColumns(columns))

  useEffect(() => {
    const handleKeyDown = (event) => {
      if (event.key === 'Escape') onClose()
    }
    document.addEventListener('keydown', handleKeyDown)
    const previousOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    return () => {
      document.removeEventListener('keydown', handleKeyDown)
      document.body.style.overflow = previousOverflow
    }
  }, [onClose])

  const updateColumn = (index, field, value) => {
    setDraftColumns((current) => current.map((column, columnIndex) => (
      columnIndex === index ? { ...column, [field]: value } : column
    )))
  }

  return createPortal(
    <div className="fixed inset-0 z-[100] flex items-center justify-center bg-black/80 p-3 backdrop-blur-[2px] sm:p-5" role="dialog" aria-modal="true" aria-label={`Edit ${tableName} semantic enrichment`}>
      <div className="flex max-h-[94vh] w-full max-w-[1560px] flex-col overflow-hidden rounded-[22px] border border-[#26334a] bg-[#111a2b] shadow-[0_35px_120px_rgba(0,0,0,0.7)]">
        <div className="flex shrink-0 items-start justify-between border-b border-[#26334a] px-5 py-5 sm:px-8">
          <div>
            <h2 className="text-xl font-extrabold text-white">Edit Semantic Enrichment</h2>
            <p className="mt-1 text-sm text-[#9ca8bc]">Update column metadata for this table before submitting the HITL decision.</p>
          </div>
          <button type="button" onClick={onClose} aria-label="Close editor" className="flex h-10 w-10 items-center justify-center rounded-lg text-[#a5afc0] transition-colors hover:bg-white/5 hover:text-white">
            <X size={21} />
          </button>
        </div>

        <div className="min-h-0 flex-1 overflow-auto px-5 py-4 sm:px-8">
          <div className="mb-5 rounded-xl border border-[#26334a] bg-[#081120] px-4 py-3 font-mono text-sm font-semibold text-[#e5e9f2]">
            {tableName}
          </div>

          <div className="overflow-x-auto rounded-xl border border-[#26334a] bg-[#0d1627]">
            <div className="min-w-[1180px]">
              <div className="grid grid-cols-[1.1fr_1.15fr_1.05fr_1.65fr_52px_52px_52px_1fr] gap-4 border-b border-[#26334a] bg-[#081120] px-4 py-3 text-xs font-bold uppercase tracking-[0.05em] text-[#9ea9bc]">
                <span>Column</span>
                <span>Display Name</span>
                <span>Semantic Type</span>
                <span>Description</span>
                <span className="text-center">M</span>
                <span className="text-center">D</span>
                <span className="text-center">PII</span>
                <span>PII Type</span>
              </div>

              {draftColumns.length === 0 ? (
                <div className="px-4 py-12 text-center text-sm text-[#9ca8bc]">No enriched columns were returned for this table.</div>
              ) : draftColumns.map((column, index) => (
                <div key={`${column.column_name || index}`} className="grid grid-cols-[1.1fr_1.15fr_1.05fr_1.65fr_52px_52px_52px_1fr] items-center gap-4 border-b border-[#26334a] px-4 py-3 last:border-b-0">
                  <span className="truncate font-mono text-sm font-semibold text-white" title={column.column_name}>{column.column_name || '-'}</span>
                  <input
                    value={column.suggested_display_name}
                    onChange={(event) => updateColumn(index, 'suggested_display_name', event.target.value)}
                    className="h-11 min-w-0 rounded-xl border border-[#26334a] bg-[#081120] px-3 text-sm text-white outline-none transition-colors focus:border-[#4388ff]"
                  />
                  <select
                    value={column.semantic_type}
                    onChange={(event) => updateColumn(index, 'semantic_type', event.target.value)}
                    className="h-11 min-w-0 rounded-xl border border-[#26334a] bg-[#081120] px-3 text-sm text-white outline-none transition-colors focus:border-[#4388ff]"
                  >
                    {SEMANTIC_TYPE_OPTIONS.map((option) => <option key={option} value={option}>{option}</option>)}
                  </select>
                  <textarea
                    value={column.business_description}
                    onChange={(event) => updateColumn(index, 'business_description', event.target.value)}
                    rows={2}
                    className="min-h-[58px] w-full resize-y rounded-xl border border-[#26334a] bg-[#081120] px-3 py-2.5 text-sm leading-snug text-white outline-none transition-colors focus:border-[#4388ff]"
                  />
                  <ColumnCheck label={`${column.column_name} measure`} checked={column.is_measure} onChange={(value) => updateColumn(index, 'is_measure', value)} />
                  <ColumnCheck label={`${column.column_name} dimension`} checked={column.is_dimension} onChange={(value) => updateColumn(index, 'is_dimension', value)} />
                  <ColumnCheck label={`${column.column_name} PII`} checked={column.is_pii_candidate} onChange={(value) => updateColumn(index, 'is_pii_candidate', value)} />
                  <input
                    value={column.pii_type === '-' ? '' : column.pii_type}
                    onChange={(event) => updateColumn(index, 'pii_type', event.target.value || '-')}
                    disabled={!column.is_pii_candidate}
                    placeholder="-"
                    className="h-11 min-w-0 rounded-xl border border-[#26334a] bg-[#081120] px-3 text-sm text-white outline-none transition-colors placeholder:text-[#738096] focus:border-[#4388ff] disabled:cursor-not-allowed disabled:opacity-55"
                  />
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className="grid shrink-0 grid-cols-2 gap-4 border-t border-[#26334a] bg-[#0d1627] px-5 py-4 sm:px-8 sm:py-5">
          <button type="button" onClick={onClose} className="h-12 rounded-xl bg-[#202c40] text-sm font-bold text-[#d0d6e1] transition-colors hover:bg-[#29364b]">Cancel</button>
          <button
            type="button"
            onClick={() => onSave({ table_name: tableName, table_summary: tableSummary, columns: draftColumns })}
            className="inline-flex h-12 items-center justify-center gap-2 rounded-xl bg-[#4388ff] text-sm font-bold text-white transition-colors hover:bg-[#5595ff]"
          >
            <Save size={16} />
            Save Changes
          </button>
        </div>
      </div>
    </div>,
    document.body
  )
}

function SemanticReviewCard({ item, localDecision, rejectionReason, onApprove, onReject, onDraftChange }) {
  const id = itemId(item)
  const tableName = item?.item_detail?.table_name || item?.item_id || 'Semantic Review'
  const sourceColumns = item?.item_detail?.columns || EMPTY_COLUMNS
  const initialColumns = useMemo(() => normalizeColumns(sourceColumns), [sourceColumns])
  const initialSummary = item?.item_detail?.table_summary || item?.summary || 'Review enriched semantic labels and business descriptions before continuing.'
  const [columns, setColumns] = useState(() => initialColumns)
  const [summary, setSummary] = useState(initialSummary)
  const [columnsOpen, setColumnsOpen] = useState(false)
  const [editorOpen, setEditorOpen] = useState(false)
  const [reason, setReason] = useState(rejectionReason || '')
  const decision = localDecision || item?.decision
  const llmColumns = columns.filter((column) => String(column.enrichment_source || '').toLowerCase().includes('llm')).length

  useEffect(() => {
    setColumns(initialColumns)
    setSummary(initialSummary)
    setReason(rejectionReason || '')
    onDraftChange?.(id, { table_name: tableName, table_summary: initialSummary, columns: initialColumns })
    // The item id identifies the review draft session.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id, initialColumns, initialSummary])

  const saveDraft = (draft) => {
    setColumns(draft.columns)
    setSummary(draft.table_summary)
    onDraftChange?.(id, draft)
    setEditorOpen(false)
  }

  return (
    <article className={`rounded-[18px] border bg-[#10192a] p-5 transition-colors ${decision === 'APPROVED' ? 'border-[#14745e]' : decision === 'REJECTED' ? 'border-[#7d2e43]' : 'border-[#26334a]'}`}>
      <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
        <div className="min-w-0">
          <div className="flex min-w-0 flex-wrap items-center gap-2.5">
            <Database size={17} className="shrink-0 text-[#4388ff]" />
            <h3 className="truncate font-mono text-lg font-bold text-white">{tableName}</h3>
            <span className="rounded-full bg-[#202b3d] px-3 py-1 text-[10px] font-extrabold uppercase text-[#b9c2d1]">Enrichment</span>
          </div>
          <div className="mt-5 flex flex-wrap items-center gap-x-5 gap-y-2 text-xs text-[#98a5ba]">
            <span className="inline-flex items-center gap-1.5"><Layers3 size={13} /> {columns.length} columns</span>
            <span className="inline-flex items-center gap-1.5 text-[#4388ff]"><Sparkles size={13} /> {llmColumns} LLM-enriched</span>
            <span className="inline-flex items-center gap-1.5"><Clock3 size={13} /> Queued: {formatDateTime(item?.queued_at || item?.created_at)}</span>
            <span className="inline-flex items-center gap-1.5"><CheckCircle2 size={13} /> Decided: {formatDateTime(item?.decided_at)}</span>
          </div>
        </div>

        <button type="button" onClick={() => setEditorOpen(true)} className="inline-flex h-12 shrink-0 items-center justify-center gap-2 rounded-xl bg-[#202b3d] px-5 text-sm font-bold text-[#d0d6e1] transition-colors hover:bg-[#29374d] hover:text-white">
          <Pencil size={15} className="text-[#4388ff]" />
          Edit
        </button>
      </div>

      <button type="button" onClick={() => setColumnsOpen((open) => !open)} aria-expanded={columnsOpen} className="mt-5 inline-flex items-center gap-2 text-xs font-bold text-[#c4ccda] transition-colors hover:text-white">
        {columnsOpen ? <ChevronDown size={15} /> : <ChevronRight size={15} />}
        {columnsOpen ? `Hide columns (${columns.length})` : `Show columns (${columns.length})`}
      </button>

      {columnsOpen && (
        <div className="mt-3 max-h-56 overflow-auto rounded-xl border border-[#26334a] bg-[#091221]">
          {columns.map((column) => (
            <div key={column.column_name} className="grid grid-cols-[minmax(150px,1fr)_minmax(120px,0.8fr)_2fr] gap-3 border-b border-[#202c40] px-4 py-3 text-xs last:border-b-0">
              <span className="truncate font-mono font-semibold text-white">{column.column_name}</span>
              <span className="text-[#77a8ff]">{column.semantic_type}</span>
              <span className="truncate text-[#9ca8bc]">{column.business_description || '-'}</span>
            </div>
          ))}
        </div>
      )}

      {decision === 'REJECTED' && (
        <input value={reason} onChange={(event) => setReason(event.target.value)} placeholder="Rejection reason" className="mt-4 h-11 w-full rounded-xl border border-[#673044] bg-[#1d1420] px-3 text-sm text-white outline-none placeholder:text-[#886b76] focus:border-[#b23d5d]" />
      )}

      <div className="mt-5 grid gap-3 sm:grid-cols-2">
        <button type="button" onClick={() => onApprove(id, { table_name: tableName, table_summary: summary, columns })} className={`inline-flex h-12 items-center justify-center gap-2 rounded-xl border text-sm font-bold transition-colors ${decision === 'APPROVED' ? 'border-[#13a47d] bg-[#103f36] text-[#26d5a3]' : 'border-[#12634f] bg-[#0d302d] text-[#22c99a] hover:bg-[#104039]'}`}>
          <Check size={17} /> Approve
        </button>
        <button type="button" onClick={() => onReject(id, reason || 'Rejected by reviewer')} className={`inline-flex h-12 items-center justify-center gap-2 rounded-xl border text-sm font-bold transition-colors ${decision === 'REJECTED' ? 'border-[#b43b5c] bg-[#3a1928] text-[#ff506e]' : 'border-[#6d2d42] bg-[#281722] text-[#ff4d68] hover:bg-[#351a28]'}`}>
          <X size={17} /> Reject
        </button>
      </div>

      {editorOpen && (
        <SemanticEditModal tableName={tableName} tableSummary={summary} columns={columns} onClose={() => setEditorOpen(false)} onSave={saveDraft} />
      )}
    </article>
  )
}

export default SemanticReviewCard
