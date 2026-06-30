// @ts-nocheck
import React, { useEffect, useMemo, useState } from 'react'
import { motion } from 'framer-motion'
import { useSearchParams } from 'react-router-dom'
import { AlertCircle, Database, GitBranch, Layers3, Loader2, Search, Sparkles } from 'lucide-react'
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
  },
  bronze: {
    border: 'border-amber-500/30',
    bg: 'from-amber-500/15 to-orange-500/10',
    dot: 'bg-amber-400',
    pill: 'bg-amber-500/15 text-amber-100 border-amber-400/25',
  },
  silver: {
    border: 'border-slate-400/35',
    bg: 'from-slate-300/12 to-slate-500/8',
    dot: 'bg-slate-300',
    pill: 'bg-slate-300/12 text-slate-100 border-slate-300/20',
  },
  gold: {
    border: 'border-emerald-500/30',
    bg: 'from-emerald-500/16 to-yellow-500/10',
    dot: 'bg-emerald-400',
    pill: 'bg-emerald-500/15 text-emerald-50 border-emerald-400/20',
  },
}

function displayRunLabel(run) {
  return run?.display_name || run?.brd_filename || run?.run_id || run?.id || 'Unknown run'
}

function nodeLabel(node) {
  return String(node?.label || node?.name || '').split('.').slice(-1)[0] || 'Unnamed'
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
  const cardHeight = 92
  const cardGap = 24

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
                <Sparkles size={14} />
                Databricks Medallion View
              </div>
              <h1 className="mt-4 text-3xl font-semibold tracking-[-0.04em] text-white sm:text-4xl">
                Data Lineage Studio
              </h1>
              <p className="mt-3 max-w-2xl text-sm leading-6 text-slate-300 sm:text-base">
                Production-focused lineage for Athena runs. Solid paths show medallion pipeline movement, certified FK relationships are tracked separately, and heuristics remain visible without pretending they are truth.
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
                  Showing checkpoint fallback lineage for this BRD/database run because generated script artifacts are not available yet. Certified tables and run metadata are still used to preserve the Source to Bronze to Silver to Gold demo flow.
                </div>
              )}

              {error ? (
                <div className="mt-5 flex items-start gap-3 rounded-2xl border border-red-500/25 bg-red-500/10 p-4 text-sm text-red-100">
                  <AlertCircle size={18} className="mt-0.5 shrink-0" />
                  <span>{error}</span>
                </div>
              ) : (
                <div className="mt-6 overflow-x-auto overflow-y-hidden rounded-[24px] border border-white/5 bg-[linear-gradient(180deg,rgba(15,23,42,0.45),rgba(17,24,39,0.2))] p-4">
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

                    <div className="absolute left-0 top-0 flex gap-20">
                      {LAYER_ORDER.map((layer) => {
                        const style = LAYER_STYLES[layer]
                        return (
                          <div key={layer} className="w-[270px]">
                            <div className={`sticky top-0 z-10 rounded-2xl border ${style.border} bg-gradient-to-r ${style.bg} px-4 py-4 backdrop-blur`}>
                              <div className="flex items-center justify-between">
                                <div className="text-sm font-semibold text-white">{LAYER_TITLES[layer]}</div>
                                <span className={`inline-flex items-center gap-2 rounded-full border px-2 py-1 text-[11px] ${style.pill}`}>
                                  <span className={`h-2 w-2 rounded-full ${style.dot}`} />
                                  {(grouped[layer] || []).length}
                                </span>
                              </div>
                            </div>
                            <div className="mt-6 space-y-6">
                              {(grouped[layer] || []).map((node, index) => (
                                <motion.div
                                  key={node.id}
                                  initial={{ opacity: 0, y: 12 }}
                                  animate={{ opacity: 1, y: 0 }}
                                  transition={{ duration: 0.28, delay: index * 0.03 }}
                                  className={`rounded-[22px] border ${style.border} bg-[#0f172a]/80 p-4 shadow-[0_18px_40px_rgba(2,6,23,0.28)]`}
                                  style={{ minHeight: 92 }}
                                >
                                  <div className="flex items-start justify-between gap-3">
                                    <div>
                                      <div className="text-sm font-semibold text-white">{nodeLabel(node)}</div>
                                      <div className="mt-1 text-[11px] text-slate-400">{node.name}</div>
                                    </div>
                                    <span className={`inline-flex rounded-full border px-2 py-1 text-[10px] uppercase tracking-[0.12em] ${style.pill}`}>
                                      {layer}
                                    </span>
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
              <div>
                <div className="text-sm font-medium text-text-primary">
                  {edge.source.split(':').slice(-1)[0]} {'->'} {edge.target.split(':').slice(-1)[0]}
                </div>
                <div className="mt-1 text-xs text-text-tertiary">
                  {edge.source_column || '-'} {'->'} {edge.target_column || '-'}
                </div>
                {edge.constraint_name && (
                  <div className="mt-1 text-[11px] text-text-tertiary">Constraint: {edge.constraint_name}</div>
                )}
              </div>
              <span className={`inline-flex rounded-full border px-2 py-1 text-[10px] uppercase tracking-[0.12em] ${pillClass}`}>
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
