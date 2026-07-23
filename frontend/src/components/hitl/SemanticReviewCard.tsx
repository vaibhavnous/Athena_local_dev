// @ts-nocheck
import React, { useEffect, useMemo, useState } from 'react'
import { createPortal } from 'react-dom'
import { Check, CheckCircle2, ChevronDown, ChevronRight, Clock3, Database, Info, Layers3, Pencil, Save, Sparkles, X } from 'lucide-react'

const SEMANTIC_TYPE_OPTIONS = ['MEASURE', 'DIMENSION', 'ID', 'SURROGATE_KEY', 'DATE', 'AUDIT_TIMESTAMP', 'PII', 'FLAG', 'UNKNOWN']
const EMPTY_COLUMNS = []
const SEMANTIC_TYPE_COLORS = {
  ID: 'border-violet-500/20 bg-violet-500/10 text-violet-400',
  DIMENSION: 'border-accent-blue/20 bg-accent-blue/10 text-accent-blue',
  MEASURE: 'border-emerald-500/20 bg-emerald-500/10 text-emerald-400',
  DATE: 'border-amber-500/20 bg-amber-500/10 text-amber-400',
  AUDIT_TIMESTAMP: 'border-gray-500/20 bg-gray-500/10 text-gray-400',
  FLAG: 'border-pink-500/20 bg-pink-500/10 text-pink-400',
}

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
        className="h-4 w-4 rounded border-[#52617a] bg-[#07101f] accent-[#4388ff]"
      />
    </label>
  )
}

function SemanticEditModal({ tableName, tableSummary, columns, onClose, onSave }) {
  const [draftColumns, setDraftColumns] = useState(() => normalizeColumns(columns).filter((column) => column.column_name !== '__TABLE_SUMMARY__'))

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

  const updateSemanticType = (index, semanticType) => {
    setDraftColumns((current) => current.map((column, columnIndex) => columnIndex === index ? {
      ...column,
      semantic_type: semanticType,
      is_measure: semanticType === 'MEASURE',
      is_dimension: semanticType === 'DIMENSION',
      is_pii_candidate: semanticType === 'PII',
      pii_type: semanticType === 'PII' ? column.pii_type : '-',
    } : column))
  }

  const canSave = draftColumns.length > 0 && draftColumns.every((column) =>
    column.column_name && column.suggested_display_name.trim() && column.business_description.trim()
  )

  return createPortal(
    <div className="fixed inset-0 z-[100] flex items-center justify-center bg-black/80 p-3 backdrop-blur-[2px] sm:p-5" role="dialog" aria-modal="true" aria-label={`Edit ${tableName} semantic enrichment`}>
      <div className="flex max-h-[90vh] w-full max-w-6xl flex-col overflow-hidden rounded-2xl border border-bg-border bg-bg-card shadow-2xl">
        <div className="flex shrink-0 items-start justify-between border-b border-bg-border px-6 py-4">
          <div>
            <h2 className="text-base font-bold text-text-primary">Edit Semantic Enrichment</h2>
            <p className="mt-0.5 text-xs text-text-tertiary">Update column metadata before submitting the HITL decision.</p>
          </div>
          <button type="button" onClick={onClose} aria-label="Close editor" className="flex h-10 w-10 items-center justify-center rounded-lg text-[#a5afc0] transition-colors hover:bg-white/5 hover:text-white">
            <X size={21} />
          </button>
        </div>

        <div className="max-h-[70vh] space-y-4 overflow-y-auto px-6 py-5">
          <div>
            <div className="mb-1.5 text-xs font-medium text-text-secondary">Table Name</div>
            <div className="input-field cursor-not-allowed bg-bg-base/60 font-mono text-xs text-text-secondary">{tableName || '-'}</div>
          </div>

          <div className="overflow-x-auto rounded-lg border border-bg-border">
            <table className="w-full min-w-[1180px] text-xs">
              <thead>
                <tr className="border-b border-bg-border bg-bg-base text-left text-[11px] font-medium uppercase tracking-wider text-text-tertiary">
                  <th className="min-w-[190px] px-3 py-2">Column</th>
                  <th className="min-w-[180px] px-3 py-2">Display Name</th>
                  <th className="min-w-[165px] px-3 py-2">Semantic Type</th>
                  <th className="min-w-[270px] px-3 py-2">Description</th>
                  <th className="w-12 px-3 py-2 text-center">M</th>
                  <th className="w-12 px-3 py-2 text-center">D</th>
                  <th className="w-12 px-3 py-2 text-center">PII</th>
                  <th className="min-w-[150px] px-3 py-2">PII Type</th>
                </tr>
              </thead>
              <tbody>
                {draftColumns.length === 0 ? (
                  <tr><td colSpan={8} className="px-4 py-12 text-center text-sm text-[#9ca8bc]">No enriched columns were returned for this table.</td></tr>
                ) : draftColumns.map((column, index) => (
                  <tr key={`${column.column_name || index}`} className="border-b border-bg-border/50 align-top last:border-b-0">
                    <td className="px-3 py-2 font-mono text-xs font-semibold text-white">{column.column_name || '-'}</td>
                    <td className="px-3 py-2"><input value={column.suggested_display_name} onChange={(event) => updateColumn(index, 'suggested_display_name', event.target.value)} className="h-9 w-full rounded-lg border border-[#26334a] bg-[#081120] px-2.5 text-xs text-white outline-none focus:border-[#4388ff]" /></td>
                    <td className="px-3 py-2"><select value={column.semantic_type} onChange={(event) => updateSemanticType(index, event.target.value)} className="h-9 w-full rounded-lg border border-[#26334a] bg-[#081120] px-2.5 text-xs text-white outline-none focus:border-[#4388ff]">{SEMANTIC_TYPE_OPTIONS.map((option) => <option key={option} value={option}>{option}</option>)}</select></td>
                    <td className="px-3 py-2"><textarea value={column.business_description} onChange={(event) => updateColumn(index, 'business_description', event.target.value)} rows={2} className="min-h-[48px] w-full resize-y rounded-lg border border-[#26334a] bg-[#081120] px-2.5 py-2 text-xs leading-snug text-white outline-none focus:border-[#4388ff]" /></td>
                    <td className="px-3 py-3"><ColumnCheck label={`${column.column_name} measure`} checked={column.is_measure} onChange={(value) => updateColumn(index, 'is_measure', value)} /></td>
                    <td className="px-3 py-3"><ColumnCheck label={`${column.column_name} dimension`} checked={column.is_dimension} onChange={(value) => updateColumn(index, 'is_dimension', value)} /></td>
                    <td className="px-3 py-3"><ColumnCheck label={`${column.column_name} PII`} checked={column.is_pii_candidate} onChange={(value) => updateColumn(index, 'is_pii_candidate', value)} /></td>
                    <td className="px-3 py-2"><input value={column.pii_type === '-' ? '' : column.pii_type} onChange={(event) => updateColumn(index, 'pii_type', event.target.value || '-')} disabled={!column.is_pii_candidate} placeholder="-" className="h-9 w-full rounded-lg border border-[#26334a] bg-[#081120] px-2.5 text-xs text-white outline-none placeholder:text-[#738096] focus:border-[#4388ff] disabled:cursor-not-allowed disabled:opacity-55" /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="grid shrink-0 grid-cols-2 gap-3 border-t border-bg-border px-6 py-4">
          <button type="button" onClick={onClose} className="btn-secondary">Cancel</button>
          <button
            type="button"
            onClick={() => onSave({ table_name: tableName, table_summary: tableSummary, columns: draftColumns })}
            disabled={!canSave}
            className="btn-primary inline-flex items-center justify-center gap-2 disabled:cursor-not-allowed disabled:opacity-45"
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

function SemanticReviewCard({ item, localDecision, rejectionReason, onApprove, onReject, onClearDecision, onDraftChange }) {
  const id = itemId(item)
  const tableName = item?.item_detail?.table_name || item?.item_id || 'Semantic Review'
  const sourceColumns = item?.item_detail?.columns || EMPTY_COLUMNS
  const initialColumns = useMemo(() => normalizeColumns(sourceColumns), [sourceColumns])
  const initialSummary = item?.item_detail?.table_summary || item?.summary || 'Review enriched semantic labels and business descriptions before continuing.'
  const [columns, setColumns] = useState(() => initialColumns)
  const [summary, setSummary] = useState(initialSummary)
  const [columnsOpen, setColumnsOpen] = useState(false)
  const [editorOpen, setEditorOpen] = useState(false)
  const [showRejectInput, setShowRejectInput] = useState(false)
  const [reason, setReason] = useState(rejectionReason || '')
  const decision = localDecision || item?.decision
  const displayColumns = columns.filter((column) => column.column_name !== '__TABLE_SUMMARY__')
  const summaryColumn = columns.find((column) => column.column_name === '__TABLE_SUMMARY__')
  const displayedSummary = summaryColumn?.business_description || summary
  const llmColumns = displayColumns.filter((column) => String(column.enrichment_source || '').toLowerCase().includes('llm')).length

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
    <article className={`relative rounded-xl border p-5 transition-colors ${decision === 'APPROVED' ? 'border-[#1f6658] bg-[#103033]' : decision === 'REJECTED' ? 'border-[#803348] bg-[#301c29]' : 'border-bg-border bg-bg-card'}`}>
      <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
        <div className="min-w-0">
          <div className="flex min-w-0 flex-wrap items-center gap-2.5">
            <Database size={17} className="shrink-0 text-[#4388ff]" />
            <h3 className="truncate font-mono text-base font-bold text-text-primary">{tableName}</h3>
            <span className="rounded-full border border-bg-border bg-bg-border px-2 py-0.5 text-[11px] font-medium uppercase text-text-secondary">Enrichment</span>
          </div>
          <div className="mt-3 flex flex-wrap items-center gap-4 text-xs text-text-tertiary">
            <span className="inline-flex items-center gap-1.5"><Layers3 size={13} /> {displayColumns.length} columns</span>
            <span className="inline-flex items-center gap-1.5 text-[#4388ff]"><Sparkles size={13} /> {llmColumns} LLM-enriched</span>
            <span className="inline-flex items-center gap-1.5"><Clock3 size={13} /> Queued: {formatDateTime(item?.queued_at || item?.created_at)}</span>
            <span className="inline-flex items-center gap-1.5"><CheckCircle2 size={13} /> Decided: {formatDateTime(item?.decided_at)}</span>
          </div>
        </div>

        <div className="flex shrink-0 items-center gap-3">
          <button type="button" onClick={() => setEditorOpen(true)} className="btn-secondary inline-flex items-center gap-2 text-sm">
            <Pencil size={12} className="text-accent-blue" /> Edit
          </button>
          {decision && <span className={`inline-flex items-center gap-1.5 text-xs font-semibold ${decision === 'APPROVED' ? 'text-accent-green' : 'text-accent-red'}`}><CheckCircle2 size={13} />{decision === 'APPROVED' ? 'Approved' : 'Rejected'}</span>}
        </div>
      </div>

      {displayColumns.length > 0 && <button type="button" onClick={() => setColumnsOpen((open) => !open)} aria-expanded={columnsOpen} className="mt-3 inline-flex items-center gap-1.5 text-xs font-medium text-text-secondary transition-colors hover:text-text-primary">
        {columnsOpen ? <ChevronDown size={15} /> : <ChevronRight size={15} />}
        {columnsOpen ? `Hide columns (${displayColumns.length})` : `Show columns (${displayColumns.length})`}
      </button>}

      {columnsOpen && (
        <div className="mt-3 max-h-64 overflow-auto rounded-xl border border-[#26334a] bg-[#091221]">
          <table className="w-full min-w-[980px] text-xs">
            <thead><tr className="border-b border-[#26334a] bg-[#080f1e] text-left text-[11px] font-bold uppercase tracking-[0.04em] text-[#9ea9bc]">
              <th className="px-3 py-2">Column</th><th className="px-3 py-2">Display Name</th><th className="px-3 py-2">Semantic Type</th><th className="px-3 py-2">Description</th><th className="px-3 py-2">Source</th><th className="px-3 py-2 text-center">M</th><th className="px-3 py-2 text-center">D</th><th className="px-3 py-2 text-center">PII</th>
            </tr></thead>
            <tbody>{displayColumns.map((column, index) => {
              const source = String(column.enrichment_source || '-').toLowerCase().includes('llm') ? 'LLM' : (column.enrichment_source || '-')
              return <tr key={column.column_name} className={`border-b border-[#202c40] ${index % 2 ? 'bg-white/[0.015]' : ''}`}>
                <td className="max-w-[180px] truncate px-3 py-2 font-mono font-semibold text-white" title={column.column_name}>{column.column_name || '-'}</td>
                <td className="max-w-[170px] truncate px-3 py-2 text-[#d2d8e2]" title={column.suggested_display_name}>{column.suggested_display_name || '-'}</td>
                <td className="px-3 py-2"><span className={`inline-flex rounded border px-2 py-0.5 text-[10px] font-semibold ${SEMANTIC_TYPE_COLORS[column.semantic_type] || 'border-bg-border bg-bg-border text-text-secondary'}`}>{column.semantic_type || 'UNKNOWN'}</span></td>
                <td className="max-w-[260px] truncate px-3 py-2 text-[#b6c0cf]" title={column.business_description}>{column.business_description || '-'}</td>
                <td className="px-3 py-2"><span className={`inline-flex rounded px-2 py-0.5 text-[10px] ${source === 'LLM' ? 'border border-accent-blue/20 bg-accent-blue/10 text-accent-blue' : 'bg-[#202b3d] text-[#9ea9bc]'}`}>{source}</span></td>
                <td className="px-3 py-2 text-center text-accent-green">{column.is_measure ? '✓' : <span className="text-text-tertiary">—</span>}</td>
                <td className="px-3 py-2 text-center text-accent-blue">{column.is_dimension ? '✓' : <span className="text-text-tertiary">—</span>}</td>
                <td className="px-3 py-2 text-center text-accent-red">{column.is_pii_candidate ? '✓' : <span className="text-text-tertiary">—</span>}</td>
              </tr>
            })}</tbody>
          </table>
          <div className="flex items-start gap-2 px-3 py-2 text-xs text-[#98a5ba]"><Info size={13} className="mt-0.5 shrink-0" /><span><strong className="text-[#c4ccda]">Table Summary:</strong> {displayedSummary || '-'}</span></div>
        </div>
      )}

      {!decision && !showRejectInput && <div className="mt-3 grid gap-2 sm:grid-cols-2">
        <button type="button" onClick={() => onApprove(id, { table_name: tableName, table_summary: summary, columns })} className={`inline-flex h-12 items-center justify-center gap-2 rounded-xl border text-sm font-bold transition-colors ${decision === 'APPROVED' ? 'border-[#13a47d] bg-[#103f36] text-[#26d5a3]' : 'border-[#12634f] bg-[#0d302d] text-[#22c99a] hover:bg-[#104039]'}`}>
          <Check size={17} /> Approve
        </button>
        <button type="button" onClick={() => { setReason(''); setShowRejectInput(true) }} className="inline-flex h-12 items-center justify-center gap-2 rounded-xl border border-[#6d2d42] bg-[#281722] text-sm font-bold text-[#ff4d68] transition-colors hover:bg-[#351a28]">
          <X size={17} /> Reject
        </button>
      </div>}

      {!decision && showRejectInput && (
        <div className="mt-4 space-y-3">
          <label className="block text-xs font-semibold text-[#ff647f]">Rejection Reason *</label>
          <textarea autoFocus rows={2} value={reason} onChange={(event) => setReason(event.target.value)} placeholder="Describe why this enrichment is being rejected..." className="w-full resize-none rounded-xl border border-[#ff445f] bg-[#081120] px-4 py-3 text-sm text-white outline-none placeholder:text-[#738096]" />
          <div className="grid gap-3 sm:grid-cols-2">
            <button type="button" onClick={() => { setReason(''); setShowRejectInput(false) }} className="btn-secondary">Cancel</button>
            <button type="button" disabled={!reason.trim()} onClick={() => { onReject(id, reason.trim()); setShowRejectInput(false) }} className="inline-flex items-center justify-center gap-2 rounded-xl border border-[#8a3148] bg-[#3a1928] px-4 py-3 text-sm font-semibold text-[#ff647f] disabled:cursor-not-allowed disabled:opacity-40"><X size={14} /> Confirm Reject</button>
          </div>
        </div>
      )}

      {decision === 'REJECTED' && (rejectionReason || reason) && <div className="mt-4 rounded-lg border border-accent-red/20 bg-accent-red/5 p-3">
        <p className="text-[11px] font-semibold text-accent-red">Rejection Reason</p>
        <p className="mt-1 text-xs text-text-secondary">{rejectionReason || reason}</p>
      </div>}

      {decision && onClearDecision && (
        <button type="button" onClick={() => onClearDecision(id)} className="mt-3 text-xs text-text-tertiary transition-colors hover:text-text-primary">← Change decision</button>
      )}

      {editorOpen && (
        <SemanticEditModal tableName={tableName} tableSummary={summary} columns={columns} onClose={() => setEditorOpen(false)} onSave={saveDraft} />
      )}
    </article>
  )
}

export default SemanticReviewCard
