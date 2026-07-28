// @ts-nocheck
import React, { useEffect, useMemo, useState } from 'react'
import { motion } from 'framer-motion'
import { useSearchParams } from 'react-router-dom'
import {
  AlertCircle,
  ArrowRightLeft,
  ArrowRight,
  Box,
  CheckCircle2,
  Database,
  GitBranch,
  KeyRound,
  Layers3,
  Loader2,
  Network,
  Search,
  Sparkles,
  Table2,
} from 'lucide-react'
import { getRunLineage, getRuns } from '../api/athenaApi'

const LAYER_ORDER = ['source', 'bronze', 'silver', 'gold']
const LAYER_TITLES = {
  source: 'Source',
  bronze: 'Bronze',
  silver: 'Silver',
  gold: 'Gold',
}

const LAYER_STYLES = {
  source: {
    border: 'border-sky-500/30',
    bg: 'from-sky-500/15 to-cyan-500/10',
    dot: 'bg-sky-400',
    pill: 'bg-sky-500/15 text-sky-100 border-sky-400/25',
    text: 'text-sky-100',
    rail: 'from-sky-400/80 to-cyan-300/80',
  },
  bronze: {
    border: 'border-amber-500/30',
    bg: 'from-amber-500/15 to-orange-500/10',
    dot: 'bg-amber-400',
    pill: 'bg-amber-500/15 text-amber-100 border-amber-400/25',
    text: 'text-amber-100',
    rail: 'from-amber-400/80 to-orange-300/80',
  },
  silver: {
    border: 'border-slate-400/35',
    bg: 'from-slate-300/12 to-slate-500/8',
    dot: 'bg-slate-300',
    pill: 'bg-slate-300/12 text-slate-100 border-slate-300/20',
    text: 'text-slate-100',
    rail: 'from-slate-200/80 to-slate-400/80',
  },
  gold: {
    border: 'border-emerald-500/30',
    bg: 'from-emerald-500/16 to-yellow-500/10',
    dot: 'bg-emerald-400',
    pill: 'bg-emerald-500/15 text-emerald-50 border-emerald-400/20',
    text: 'text-emerald-100',
    rail: 'from-emerald-400/80 to-lime-300/80',
  },
}

const LAYER_DESCRIPTIONS = {
  source: 'Operational insurance records',
  bronze: 'Raw Delta landing and audit capture',
  silver: 'Curated conformed tables with merge keys',
  gold: 'Metric-ready KPI fact outputs',
}

const OPERATION_LABELS = {
  bronze_ingest: 'Raw ingest',
  silver_transform: 'Cast, dedupe, merge key',
  gold_aggregation: 'KPI aggregation',
}

function displayRunLabel(run) {
  return run?.display_name || run?.brd_filename || run?.run_id || run?.id || 'Unknown run'
}

function nodeLabel(node) {
  return String(node?.label || node?.name || '').split('.').slice(-1)[0] || 'Unnamed'
}

function layerIcon(layer, size = 16) {
  if (layer === 'source') return <Database size={size} />
  if (layer === 'bronze') return <Box size={size} />
  if (layer === 'silver') return <KeyRound size={size} />
  if (layer === 'gold') return <Sparkles size={size} />
  return <Table2 size={size} />
}

function nodeMetaLines(node) {
  const layer = node?.layer
  if (layer === 'source') {
    return [
      node.role,
      `${Number(node.column_count || 0)} columns`,
      `${Number(node.sample_row_count || 0).toLocaleString()} sampled rows`,
    ].filter(Boolean)
  }
  if (layer === 'bronze') {
    return [
      node.role || 'Raw landing table',
      node.source_table ? `Source: ${node.source_table}` : null,
      `${Number(node.column_count || 0)} columns with audit fields`,
    ].filter(Boolean)
  }
  if (layer === 'silver') {
    const mergeKey = Array.isArray(node.merge_key) ? node.merge_key.join(' + ') : node.merge_key
    return [
      node.role || 'Curated table',
      mergeKey ? `Merge key: ${mergeKey}` : null,
      node.source_table ? `From: ${node.source_table}` : null,
    ].filter(Boolean)
  }
  if (layer === 'gold') {
    return [
      node.kpi_name ? `KPI: ${node.kpi_name}` : null,
      node.source_table ? `From: ${node.source_table}` : null,
      node.target_table ? `Target: ${node.target_table}` : null,
    ].filter(Boolean)
  }
  return [node?.role].filter(Boolean)
}

function nodeMetricChips(node) {
  const layer = node?.layer
  const chips = []
  const columnCount = Number(node?.column_count || 0)
  if (columnCount > 0) chips.push(`${columnCount} columns`)

  if (layer === 'source') {
    const sampleRows = Number(node?.sample_row_count || 0)
    if (sampleRows > 0) chips.push(`${sampleRows.toLocaleString()} rows`)
    if (node?.schema) chips.push(node.schema)
  }

  if (layer === 'bronze') {
    chips.push('audit fields')
    chips.push('source fidelity')
  }

  if (layer === 'silver') {
    const mergeKeyCount = Array.isArray(node?.merge_key) ? node.merge_key.length : node?.merge_key ? 1 : 0
    if (mergeKeyCount > 0) chips.push(`${mergeKeyCount} key fields`)
    chips.push('dedupe')
  }

  if (layer === 'gold') {
    const dimensionCount = Number(node?.dimension_count || 0)
    const joinCount = Number(node?.join_count || 0)
    if (dimensionCount > 0) chips.push(`${dimensionCount} dimensions`)
    if (joinCount > 0) chips.push(`${joinCount} joins`)
    if (node?.time_grain) chips.push(String(node.time_grain))
  }

  return [...new Set(chips)].slice(0, 4)
}

function operationLabel(edge) {
  return OPERATION_LABELS[edge?.operation] || String(edge?.operation || edge?.type || 'lineage').replaceAll('_', ' ')
}

function groupNodesByLayer(lineage) {
  const grouped = {
    source: [],
    bronze: [],
    silver: [],
    gold: [],
  }
  ;(lineage?.nodes || []).forEach((node) => {
    if (grouped[node.layer]) grouped[node.layer].push(node)
  })
  for (const key of Object.keys(grouped)) {
    grouped[key].sort((a, b) => String(a.label || a.name).localeCompare(String(b.label || b.name)))
  }
  return grouped
}

function buildColumnNodeIds(groups) {
  return new Set(
    LAYER_ORDER.flatMap((layer) => (groups[layer] || []).map((node) => node.id))
  )
}

function buildPipelineEdges(lineage, columnNodeIds) {
  return (lineage?.edges || []).filter((edge) =>
    edge?.type === 'pipeline' &&
    columnNodeIds.has(edge.source) &&
    columnNodeIds.has(edge.target)
  )
}

function buildRelationshipEdges(lineage) {
  return (lineage?.edges || []).filter((edge) => edge?.type === 'fk' || edge?.type === 'heuristic')
}

function computeNodeLayout(groups) {
  const positions = {}
  const columnWidth = 270
  const columnGap = 80
  const topOffset = 76
  const cardHeight = 196
  const cardGap = 30

  LAYER_ORDER.forEach((layer, columnIndex) => {
    const nodes = groups[layer] || []
    nodes.forEach((node, rowIndex) => {
      positions[node.id] = {
        x: columnIndex * (columnWidth + columnGap),
        y: topOffset + rowIndex * (cardHeight + cardGap),
        width: columnWidth,
        height: cardHeight,
      }
    })
  })

  const maxRows = Math.max(...LAYER_ORDER.map((layer) => (groups[layer] || []).length), 1)
  return {
    positions,
    width: LAYER_ORDER.length * columnWidth + (LAYER_ORDER.length - 1) * columnGap,
    height: topOffset + maxRows * (cardHeight + cardGap) + 40,
    columnWidth,
  }
}

function edgePath(sourceBox, targetBox) {
  const x1 = sourceBox.x + sourceBox.width
  const y1 = sourceBox.y + sourceBox.height / 2
  const x2 = targetBox.x
  const y2 = targetBox.y + targetBox.height / 2
  const mid = (x1 + x2) / 2
  return `M ${x1} ${y1} C ${mid} ${y1}, ${mid} ${y2}, ${x2} ${y2}`
}

function edgeLabelPosition(sourceBox, targetBox) {
  const x1 = sourceBox.x + sourceBox.width
  const y1 = sourceBox.y + sourceBox.height / 2
  const x2 = targetBox.x
  const y2 = targetBox.y + targetBox.height / 2
  return {
    left: (x1 + x2) / 2 - 62,
    top: (y1 + y2) / 2 - 14,
  }
}

function relationshipConfidence(edge) {
  const value = Number(edge?.confidence || 0)
  if (!Number.isFinite(value) || value <= 0) return null
  return Math.round(Math.min(value, 1) * 100)
}

function MetricCard({ label, value, tone }) {
  const toneClass =
    tone === 'amber' ? 'border-amber-500/25 bg-amber-500/10' :
    tone === 'emerald' ? 'border-emerald-500/25 bg-emerald-500/10' :
    tone === 'sky' ? 'border-sky-500/25 bg-sky-500/10' :
    'border-bg-border bg-bg-card'

  return (
    <div className={`rounded-2xl border px-4 py-4 ${toneClass}`}>
      <div className="text-[11px] uppercase tracking-[0.18em] text-text-tertiary">{label}</div>
      <div className="mt-2 text-2xl font-semibold text-text-primary">{value}</div>
    </div>
  )
}

function FlowStep({ label, count, tone, detail }) {
  const toneClass =
    tone === 'sky' ? 'border-sky-400/30 bg-sky-500/10 text-sky-100' :
    tone === 'amber' ? 'border-amber-400/30 bg-amber-500/10 text-amber-100' :
    tone === 'slate' ? 'border-slate-300/25 bg-slate-300/10 text-slate-100' :
    'border-emerald-400/30 bg-emerald-500/10 text-emerald-100'
  return (
    <div className={`min-w-[150px] rounded-2xl border px-4 py-3 ${toneClass}`}>
      <div className="flex items-center justify-between gap-3">
        <div className="text-[11px] uppercase tracking-[0.16em] opacity-75">{label}</div>
        <div className="opacity-80">{layerIcon(String(label).toLowerCase().split(' ')[0], 14)}</div>
      </div>
      <div className="mt-1 text-2xl font-semibold">{count}</div>
      {detail && <div className="mt-1 text-[11px] leading-4 opacity-70">{detail}</div>}
    </div>
  )
}

function FlowArrow() {
  return <div className="hidden h-px flex-1 bg-gradient-to-r from-white/10 via-white/40 to-white/10 md:block" />
}

function ModelingRail({ summary }) {
  const counts = {
    source: summary?.source_count ?? 0,
    bronze: summary?.bronze_count ?? 0,
    silver: summary?.silver_count ?? 0,
    gold: summary?.gold_count ?? 0,
  }

  return (
    <div className="rounded-[24px] border border-bg-border bg-bg-card p-5 shadow-card">
      <div className="flex items-center gap-2 text-sm font-medium text-text-secondary">
        <Network size={16} />
        Model Blueprint
      </div>
      <div className="mt-5 space-y-4">
        {LAYER_ORDER.map((layer, index) => {
          const style = LAYER_STYLES[layer]
          return (
            <div key={layer} className="relative pl-9">
              {index < LAYER_ORDER.length - 1 && (
                <div className={`absolute left-[13px] top-9 h-[calc(100%+16px)] w-px bg-gradient-to-b ${style.rail}`} />
              )}
              <div className={`absolute left-0 top-0 flex h-7 w-7 items-center justify-center rounded-full border ${style.border} bg-[#0f172a] ${style.text}`}>
                {layerIcon(layer, 14)}
              </div>
              <div className="rounded-2xl border border-white/5 bg-white/[0.03] px-3 py-3">
                <div className="flex items-center justify-between gap-2">
                  <div className="text-sm font-semibold text-text-primary">{LAYER_TITLES[layer]}</div>
                  <span className={`rounded-full border px-2 py-0.5 text-[10px] uppercase tracking-[0.12em] ${style.pill}`}>
                    {counts[layer]} nodes
                  </span>
                </div>
                <div className="mt-1 text-xs leading-5 text-text-tertiary">{LAYER_DESCRIPTIONS[layer]}</div>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

function DataLineagePage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const requestedRunId = searchParams.get('runId') || ''
  const [runs, setRuns] = useState([])
  const [selectedRunId, setSelectedRunId] = useState(requestedRunId)
  const [lineage, setLineage] = useState(null)
  const [filter, setFilter] = useState('')
  const [loadingRuns, setLoadingRuns] = useState(true)
  const [loadingLineage, setLoadingLineage] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    let cancelled = false
    const loadRuns = async () => {
      try {
        setLoadingRuns(true)
        const data = await getRuns()
        if (cancelled) return
        const nextRuns = Array.isArray(data) ? data : []
        setRuns(nextRuns)
        const requestedExists = requestedRunId
          ? nextRuns.some((run) => String(run.id || run.run_id || '') === requestedRunId)
          : false
        if (requestedExists) {
          setSelectedRunId(requestedRunId)
        } else if (!selectedRunId && nextRuns.length > 0) {
          setSelectedRunId(String(nextRuns[0].id || nextRuns[0].run_id || ''))
        }
      } catch (loadError) {
        if (!cancelled) setError(loadError.message || 'Failed to load runs')
      } finally {
        if (!cancelled) setLoadingRuns(false)
      }
    }
    loadRuns()
    return () => { cancelled = true }
  }, [requestedRunId, selectedRunId])

  useEffect(() => {
    if (!requestedRunId || requestedRunId === selectedRunId) return
    setSelectedRunId(requestedRunId)
  }, [requestedRunId, selectedRunId])

  useEffect(() => {
    if (!selectedRunId) return
    let cancelled = false
    const loadLineage = async () => {
      try {
        setLoadingLineage(true)
        setError('')
        const payload = await getRunLineage(selectedRunId)
        if (!cancelled) setLineage(payload)
      } catch (loadError) {
        if (!cancelled) {
          setLineage(null)
          setError(loadError.message || 'Failed to load lineage')
        }
      } finally {
        if (!cancelled) setLoadingLineage(false)
      }
    }
    loadLineage()
    return () => { cancelled = true }
  }, [selectedRunId])

  const selectedRun = useMemo(
    () => runs.find((run) => String(run.id || run.run_id || '') === selectedRunId) || null,
    [runs, selectedRunId]
  )

  const grouped = useMemo(() => {
    const groups = groupNodesByLayer(lineage)
    if (!filter.trim()) return groups
    const q = filter.trim().toLowerCase()
    const filtered = {}
    for (const layer of LAYER_ORDER) {
      filtered[layer] = (groups[layer] || []).filter((node) =>
        [node.label, node.name].join(' ').toLowerCase().includes(q)
      )
    }
    return filtered
  }, [lineage, filter])

  const layout = useMemo(() => computeNodeLayout(grouped), [grouped])
  const columnNodeIds = useMemo(() => buildColumnNodeIds(grouped), [grouped])
  const pipelineEdges = useMemo(() => buildPipelineEdges(lineage, columnNodeIds), [lineage, columnNodeIds])
  const relationshipEdges = useMemo(() => buildRelationshipEdges(lineage), [lineage])

  return (
    <div className="min-h-screen bg-bg-base px-6 py-6">
      <div className="mx-auto flex w-full max-w-[1440px] flex-col gap-6">
        <section className="overflow-hidden rounded-[28px] border border-[#253044] bg-[radial-gradient(circle_at_top_left,_rgba(56,189,248,0.18),_transparent_34%),radial-gradient(circle_at_top_right,_rgba(251,191,36,0.16),_transparent_30%),linear-gradient(135deg,_#0f172a,_#111827_55%,_#0b1325)] p-6 shadow-[0_24px_80px_rgba(3,7,18,0.35)]">
          <div className="flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
            <div className="max-w-3xl">
              <div className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-3 py-1 text-[11px] uppercase tracking-[0.18em] text-slate-200">
                <ArrowRightLeft size={14} />
                Databricks Medallion View
              </div>
              <h1 className="mt-4 text-3xl font-semibold tracking-[-0.04em] text-white sm:text-4xl">
                Data Lineage Studio
              </h1>
              <p className="mt-3 max-w-2xl text-sm leading-6 text-slate-300 sm:text-base">
                Production-focused lineage for Astra Data runs. Solid paths show medallion pipeline movement, certified FK relationships are tracked separately, and heuristics remain visible without pretending they are truth.
              </p>
            </div>

            <div className="grid gap-3 sm:grid-cols-2 lg:min-w-[420px]">
              <MetricCard label="Sources" value={lineage?.summary?.source_count ?? 0} tone="sky" />
              <MetricCard label="Bronze/Silver/Gold" value={`${lineage?.summary?.bronze_count ?? 0}/${lineage?.summary?.silver_count ?? 0}/${lineage?.summary?.gold_count ?? 0}`} tone="amber" />
              <MetricCard label="FK Edges" value={lineage?.summary?.fk_edge_count ?? 0} tone="emerald" />
              <MetricCard label="Heuristic Edges" value={lineage?.summary?.heuristic_edge_count ?? 0} />
            </div>
          </div>
        </section>

        <section className="grid gap-6 xl:grid-cols-[340px_minmax(0,1fr)]">
          <aside className="space-y-6">
            <div className="rounded-[24px] border border-bg-border bg-bg-card p-5 shadow-card">
              <div className="flex items-center gap-2 text-sm font-medium text-text-secondary">
                <Database size={16} />
                Run Selector
              </div>
              <div className="mt-4">
                <label className="mb-2 block text-[11px] font-semibold uppercase tracking-[0.16em] text-text-tertiary">
                  Active Run
                </label>
                <select
                  className="input-field"
                  value={selectedRunId}
                  onChange={(event) => {
                    const nextRunId = event.target.value
                    setSelectedRunId(nextRunId)
                    setSearchParams(nextRunId ? { runId: nextRunId } : {})
                  }}
                  disabled={loadingRuns || runs.length === 0}
                >
                  {runs.length === 0 && <option value="">No runs available</option>}
                  {runs.map((run) => {
                    const runId = String(run.id || run.run_id || '')
                    return (
                      <option key={runId} value={runId}>
                        {displayRunLabel(run)}
                      </option>
                    )
                  })}
                </select>
              </div>

              <div className="mt-5">
                <label className="mb-2 block text-[11px] font-semibold uppercase tracking-[0.16em] text-text-tertiary">
                  Filter Nodes
                </label>
                <div className="relative">
                  <Search size={15} className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-text-tertiary" />
                  <input
                    className="input-field pl-9"
                    placeholder="claims, policy, silver..."
                    value={filter}
                    onChange={(event) => setFilter(event.target.value)}
                  />
                </div>
              </div>

              <div className="mt-5 rounded-2xl border border-white/5 bg-white/[0.03] p-4">
                <div className="text-[11px] uppercase tracking-[0.16em] text-text-tertiary">Selected Run</div>
                <div className="mt-2 text-sm font-medium text-text-primary">{displayRunLabel(selectedRun)}</div>
                <div className="mt-1 text-xs text-text-tertiary">Run ID: {selectedRunId || '-'}</div>
                <div className="mt-1 text-xs text-text-tertiary">Status: {selectedRun?.status || 'Unknown'}</div>
              </div>
            </div>

            <div className="rounded-[24px] border border-bg-border bg-bg-card p-5 shadow-card">
              <div className="flex items-center gap-2 text-sm font-medium text-text-secondary">
                <GitBranch size={16} />
                Relationship Truth
              </div>
              <div className="mt-4 space-y-3">
                <LegendRow label="Pipeline lineage" styleClass="bg-emerald-400" detail="Source to Bronze to Silver to Gold" />
                <LegendRow label="Certified FK" styleClass="bg-sky-400" detail="Metadata-backed relationship" />
                <LegendRow label="Heuristic join" styleClass="bg-amber-400" detail="Suggestion only, not trusted as truth" />
              </div>
            </div>

            <ModelingRail summary={lineage?.summary} />
          </aside>

          <div className="space-y-6">
            <section className="rounded-[28px] border border-bg-border bg-bg-card p-5 shadow-card">
              <div className="flex items-center justify-between gap-4">
                <div>
                  <h2 className="text-lg font-semibold tracking-[-0.03em] text-text-primary">Medallion Flow</h2>
                  <p className="mt-1 text-sm text-text-tertiary">
                    Visual lineage view for the selected run. FK and heuristic joins are summarized below to keep the core flow readable.
                  </p>
                </div>
                {(loadingRuns || loadingLineage) && (
                  <div className="inline-flex items-center gap-2 rounded-full border border-accent-blue/20 bg-accent-blue/10 px-3 py-1 text-xs text-accent-blue">
                    <Loader2 size={14} className="animate-spin" />
                    Loading lineage
                  </div>
                )}
              </div>

              {lineage?.summary?.fallback && !error && (
                <div className="mt-5 rounded-2xl border border-amber-400/25 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                  Showing checkpoint lineage for this BRD/database run because generated script artifacts are not available yet. Certified tables and run metadata are still used to preserve the Source to Bronze to Silver to Gold flow.
                </div>
              )}

              {error ? (
                <div className="mt-5 flex items-start gap-3 rounded-2xl border border-red-500/25 bg-red-500/10 p-4 text-sm text-red-100">
                  <AlertCircle size={18} className="mt-0.5 shrink-0" />
                  <span>{error}</span>
                </div>
              ) : (
                <div className="mt-6 rounded-[24px] border border-white/5 bg-[linear-gradient(180deg,rgba(15,23,42,0.45),rgba(17,24,39,0.2))] p-4">
                  <div className="mb-5 flex flex-col gap-3 md:flex-row md:items-center">
                    <FlowStep label="Source" count={lineage?.summary?.source_count ?? 0} tone="sky" detail="insurance.dbo" />
                    <FlowArrow />
                    <FlowStep label="Bronze" count={lineage?.summary?.bronze_count ?? 0} tone="amber" detail="raw + audit" />
                    <FlowArrow />
                    <FlowStep label="Silver" count={lineage?.summary?.silver_count ?? 0} tone="slate" detail="clean + keyed" />
                    <FlowArrow />
                    <FlowStep label="Gold KPIs" count={lineage?.summary?.gold_count ?? 0} tone="emerald" detail="analytics facts" />
                  </div>
                  <div className="overflow-x-auto overflow-y-hidden">
                  <div className="relative" style={{ width: layout.width, minHeight: layout.height }}>
                    <svg className="absolute left-0 top-0" width={layout.width} height={layout.height}>
                      <defs>
                        <linearGradient id="pipelineGradient" x1="0%" y1="0%" x2="100%" y2="0%">
                          <stop offset="0%" stopColor="#38bdf8" />
                          <stop offset="50%" stopColor="#c084fc" />
                          <stop offset="100%" stopColor="#f59e0b" />
                        </linearGradient>
                      </defs>
                      {pipelineEdges.map((edge) => {
                        const sourceBox = layout.positions[edge.source]
                        const targetBox = layout.positions[edge.target]
                        if (!sourceBox || !targetBox) return null
                        return (
                          <path
                            key={edge.id}
                            d={edgePath(sourceBox, targetBox)}
                            fill="none"
                            stroke="url(#pipelineGradient)"
                            strokeWidth="3"
                            strokeLinecap="round"
                            opacity="0.88"
                          />
                        )
                      })}
                    </svg>

                    {pipelineEdges.map((edge) => {
                      const sourceBox = layout.positions[edge.source]
                      const targetBox = layout.positions[edge.target]
                      if (!sourceBox || !targetBox) return null
                      const position = edgeLabelPosition(sourceBox, targetBox)
                      return (
                        <div
                          key={`${edge.id}:label`}
                          className="pointer-events-none absolute z-20 w-[124px] rounded-full border border-white/10 bg-[#111827]/95 px-2 py-1 text-center text-[10px] font-medium uppercase tracking-[0.12em] text-slate-200 shadow-[0_12px_28px_rgba(2,6,23,0.4)]"
                          style={{ left: position.left, top: position.top }}
                        >
                          {operationLabel(edge)}
                        </div>
                      )
                    })}

                    <div className="absolute left-0 top-0 flex gap-20">
                      {LAYER_ORDER.map((layer) => {
                        const style = LAYER_STYLES[layer]
                        return (
                          <div key={layer} className="w-[270px]">
                            <div className={`sticky top-0 z-10 rounded-2xl border ${style.border} bg-gradient-to-r ${style.bg} px-4 py-4 backdrop-blur`}>
                              <div className="flex items-center justify-between">
                                <div className="flex items-center gap-2 text-sm font-semibold text-white">
                                  <span className={`flex h-8 w-8 items-center justify-center rounded-xl border ${style.border} bg-white/5 ${style.text}`}>
                                    {layerIcon(layer, 15)}
                                  </span>
                                  <span>{LAYER_TITLES[layer]}</span>
                                </div>
                                <span className={`inline-flex items-center gap-2 rounded-full border px-2 py-1 text-[11px] ${style.pill}`}>
                                  <span className={`h-2 w-2 rounded-full ${style.dot}`} />
                                  {(grouped[layer] || []).length}
                                </span>
                              </div>
                              <div className="mt-2 text-[11px] leading-4 text-slate-300">{LAYER_DESCRIPTIONS[layer]}</div>
                            </div>
                            <div className="mt-6 space-y-6">
                              {(grouped[layer] || []).map((node, index) => (
                                <motion.div
                                  key={node.id}
                                  initial={{ opacity: 0, y: 12 }}
                                  animate={{ opacity: 1, y: 0 }}
                                  transition={{ duration: 0.28, delay: index * 0.03 }}
                                  className={`rounded-[22px] border ${style.border} bg-gradient-to-br ${style.bg} bg-[#0f172a]/90 p-4 shadow-[0_18px_40px_rgba(2,6,23,0.28)]`}
                                  style={{ minHeight: 196 }}
                                >
                                  <div className="flex items-start justify-between gap-3">
                                    <div className="min-w-0">
                                      <div className="text-sm font-semibold text-white">{nodeLabel(node)}</div>
                                      <div className="mt-1 break-words text-[11px] leading-4 text-slate-400">{node.name}</div>
                                    </div>
                                    <span className={`inline-flex shrink-0 items-center gap-1 rounded-full border px-2 py-1 text-[10px] uppercase tracking-[0.12em] ${style.pill}`}>
                                      {layerIcon(layer, 11)}
                                      {layer}
                                    </span>
                                  </div>
                                  <div className="mt-3 flex flex-wrap gap-1.5">
                                    {nodeMetricChips(node).map((chip) => (
                                      <span key={`${node.id}:chip:${chip}`} className="rounded-full border border-white/10 bg-black/15 px-2 py-1 text-[10px] text-slate-200">
                                        {chip}
                                      </span>
                                    ))}
                                  </div>
                                  <div className="mt-3 space-y-1.5">
                                    {nodeMetaLines(node).slice(0, 3).map((line) => (
                                      <div key={`${node.id}:${line}`} className="rounded-lg border border-white/5 bg-white/[0.03] px-2 py-1 text-[11px] leading-4 text-slate-300">
                                        {line}
                                      </div>
                                    ))}
                                  </div>
                                  <div className="mt-3 flex items-center gap-2 text-[11px] text-slate-400">
                                    <CheckCircle2 size={13} className={style.text} />
                                    <span>{layer === 'gold' ? 'Analytics output ready' : 'Mapped in medallion flow'}</span>
                                  </div>
                                </motion.div>
                              ))}
                            </div>
                          </div>
                        )
                      })}
                    </div>
                  </div>
                  </div>
                </div>
              )}
            </section>

            <section className="grid gap-6 xl:grid-cols-2">
              <EdgePanel
                title="Certified FK Relationships"
                description="Relationships promoted from metadata discovery and safe for downstream lineage."
                edges={relationshipEdges.filter((edge) => edge.type === 'fk')}
                emptyText="No FK-backed relationships are available for this run yet."
                pillClass="border-sky-400/30 bg-sky-500/12 text-sky-100"
              />
              <EdgePanel
                title="Heuristic Join Candidates"
                description="Visible for investigation, but intentionally not treated as certified truth."
                edges={relationshipEdges.filter((edge) => edge.type === 'heuristic')}
                emptyText="No heuristic joins were proposed for this run."
                pillClass="border-amber-400/30 bg-amber-500/12 text-amber-100"
              />
            </section>
          </div>
        </section>
      </div>
    </div>
  )
}

function LegendRow({ label, detail, styleClass }) {
  return (
    <div className="flex items-start gap-3">
      <span className={`mt-1 h-2.5 w-2.5 rounded-full ${styleClass}`} />
      <div>
        <div className="text-sm text-text-primary">{label}</div>
        <div className="text-xs text-text-tertiary">{detail}</div>
      </div>
    </div>
  )
}

function EdgePanel({ title, description, edges, emptyText, pillClass }) {
  return (
    <div className="rounded-[24px] border border-bg-border bg-bg-card p-5 shadow-card">
      <div className="flex items-center gap-2 text-sm font-medium text-text-secondary">
        <Layers3 size={16} />
        {title}
      </div>
      <p className="mt-2 text-sm text-text-tertiary">{description}</p>
      <div className="mt-5 space-y-3">
        {edges.length === 0 && (
          <div className="rounded-2xl border border-white/5 bg-white/[0.03] px-4 py-4 text-sm text-text-tertiary">
            {emptyText}
          </div>
        )}
        {edges.map((edge) => (
          <div key={edge.id} className="rounded-2xl border border-white/5 bg-white/[0.03] px-4 py-4">
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0 flex-1">
                <div className="flex flex-wrap items-center gap-2 text-sm font-medium text-text-primary">
                  <span className="break-all">{edge.source.split(':').slice(-1)[0]}</span>
                  <ArrowRight size={14} className="shrink-0 text-text-tertiary" />
                  <span className="break-all">{edge.target.split(':').slice(-1)[0]}</span>
                </div>
                <div className="mt-1 text-xs text-text-tertiary">
                  {edge.source_column || '-'} {'->'} {edge.target_column || '-'}
                </div>
                {edge.constraint_name && (
                  <div className="mt-1 text-[11px] text-text-tertiary">Constraint: {edge.constraint_name}</div>
                )}
                {edge.description && (
                  <div className="mt-2 text-xs leading-5 text-text-secondary">{edge.description}</div>
                )}
                {relationshipConfidence(edge) !== null && (
                  <div className="mt-3">
                    <div className="mb-1 flex items-center justify-between text-[10px] uppercase tracking-[0.12em] text-text-tertiary">
                      <span>Confidence</span>
                      <span>{relationshipConfidence(edge)}%</span>
                    </div>
                    <div className="h-1.5 overflow-hidden rounded-full bg-white/10">
                      <div
                        className={`h-full rounded-full ${edge.type === 'fk' ? 'bg-sky-400' : 'bg-amber-400'}`}
                        style={{ width: `${relationshipConfidence(edge)}%` }}
                      />
                    </div>
                  </div>
                )}
              </div>
              <span className={`inline-flex shrink-0 rounded-full border px-2 py-1 text-[10px] uppercase tracking-[0.12em] ${pillClass}`}>
                {edge.type}
              </span>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

export default DataLineagePage
