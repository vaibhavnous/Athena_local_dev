// @ts-nocheck
import React, { useEffect, useMemo, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import { ArrowLeft, ChevronDown, ChevronUp, CheckCircle2, Loader2, ShieldCheck, XCircle, Code2 } from 'lucide-react'
import useAthenaStore from '../store/useAthenaStore'
import PipelineDag from '../components/pipeline/PipelineDag'
import StatusBadge from '../components/shared/StatusBadge'
import ConfidenceBar from '../components/shared/ConfidenceBar'
import CopyableId from '../components/shared/CopyableId'
import JsonViewer from '../components/shared/JsonViewer'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts'
import { getRun, submitEnrichmentReview, submitTableReviews } from '../api/athenaApi'

const TABS = ['Overview', 'Requirements', 'KPIs', 'Scripts', 'HITL Decisions', 'Cost Log']

function RunDetail() {
  const { runId } = useParams()
  const navigate = useNavigate()
  const getRunById = useAthenaStore((s) => s.getRunById)
  const updateRun = useAthenaStore((s) => s.updateRun)
  const addNotification = useAthenaStore((s) => s.addNotification)
  const storeRun = getRunById(runId)
  const [backendRun, setBackendRun] = useState(null)
  const [activeTab, setActiveTab] = useState('Overview')
  const run = backendRun || storeRun

  useEffect(() => {
    if (!runId) return
    let cancelled = false

    const loadRun = async () => {
      try {
        const data = await getRun(runId)
        if (cancelled) return
        setBackendRun(data)
        updateRun(runId, data)
      } catch (error) {
        if (cancelled) return
        console.warn('[RunDetail] Failed to load backend run detail', error)
      }
    }

    loadRun()
    const timer = window.setInterval(loadRun, 5000)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [runId, updateRun])

  if (!run) {
    return (
      <div className="flex flex-col items-center justify-center h-64 gap-3">
        <p className="text-gray-400">Run not found: <span className="font-mono text-accent-red">{runId}</span></p>
        <button onClick={() => navigate('/app/data-discovery')} className="btn-secondary text-sm">← Back to Monitor</button>
      </div>
    )
  }

  return (
    <div className="flex flex-col gap-4">
      {/* Header */}
      <div className="flex items-center gap-3">
        <button
          onClick={() => navigate('/app/data-discovery')}
          className="w-8 h-8 rounded-lg bg-bg-card border border-bg-border flex items-center justify-center text-gray-400 hover:text-white hover:bg-bg-border transition-colors"
        >
          <ArrowLeft size={15} />
        </button>
        <div className="flex items-center gap-3">
          <CopyableId id={run.id} chars={14} />
          <StatusBadge status={run.status} />
          <span className="text-sm text-gray-500">{run.brd_filename}</span>
        </div>
      </div>

      {/* Tab bar */}
      <div className="flex gap-1 border-b border-bg-border overflow-x-auto">
        {TABS.map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            className={`
              px-4 py-2.5 text-sm font-medium whitespace-nowrap transition-colors border-b-2 -mb-px
              ${activeTab === tab
                ? 'border-accent-blue text-accent-blue'
                : 'border-transparent text-gray-500 hover:text-gray-300'
              }
            `}
          >
            {tab}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <AnimatePresence mode="wait">
        <motion.div
          key={activeTab}
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -8 }}
          transition={{ duration: 0.2 }}
        >
          {activeTab === 'Overview' && <OverviewTab run={run} onRunRefresh={setBackendRun} addNotification={addNotification} />}
          {activeTab === 'Requirements' && <RequirementsTab run={run} />}
          {activeTab === 'KPIs' && <KpisTab run={run} />}
          {activeTab === 'Scripts' && <ScriptsTab run={run} />}
          {activeTab === 'HITL Decisions' && <HitlDecisionsTab run={run} />}
          {activeTab === 'Cost Log' && <CostLogTab run={run} />}
        </motion.div>
      </AnimatePresence>
    </div>
  )
}

/** Overview tab */
function OverviewTab({ run, onRunRefresh, addNotification }) {
  const [selectedTables, setSelectedTables] = useState({})
  const [submittingGate2, setSubmittingGate2] = useState(false)
  const [submittingGate3, setSubmittingGate3] = useState(false)
  const currentStatus = (run.status || '').toUpperCase()
  const reviewableRun = currentStatus === 'HITL_WAIT' || currentStatus === 'PAUSED_FOR_HITL'

  useEffect(() => {
    const nominated = run.nominated_tables || []
    if (!nominated.length) return
    setSelectedTables((prev) => {
      const next = { ...prev }
      for (const table of nominated) {
        const key = tableReviewKey(table)
        if (!(key in next)) next[key] = true
      }
      return next
    })
  }, [run.nominated_tables])

  // Build Gantt data
  const ganttData = (run.stages || []).map((s) => {
    let duration = Number(s.duration_seconds || 0);
    if (!duration && s.started_at) {
      const start = new Date(s.started_at).getTime()
      const end = s.completed_at ? new Date(s.completed_at).getTime() : Date.now()
      duration = Math.max(0, Math.round((end - start) / 1000))
    }
    return {
      name: s.name.replace(/Stage \d+ — /, ''),
      duration,
      status: s.status
    }
  })

  const statusColors = {
    COMPLETED: '#10b981',
    RUNNING: '#3b82f6',
    FAILED: '#ef4444',
    HITL_WAIT: '#f59e0b',
    PENDING: '#374151'
  }

  const handleToggleTable = (key) => {
    setSelectedTables((prev) => ({ ...prev, [key]: !prev[key] }))
  }

  const handleSubmitGate2 = async () => {
    const approvedTables = (run.nominated_tables || [])
      .map((table) => tableReviewKey(table))
      .filter((key) => selectedTables[key])

    if (!approvedTables.length) {
      addNotification({
        type: 'amber',
        title: 'No Tables Selected',
        message: 'Select at least one nominated table before submitting Gate 2.',
        duration: 4000
      })
      return
    }

    setSubmittingGate2(true)
    try {
      await submitTableReviews(run.id, approvedTables)
      const refreshed = await getRun(run.id)
      onRunRefresh(refreshed)
      addNotification({
        type: 'success',
        title: 'Gate 2 Submitted',
        message: 'Approved tables were submitted. Metadata discovery and profiling are resuming.',
        duration: 5000
      })
    } catch (error) {
      addNotification({
        type: 'error',
        title: 'Gate 2 Failed',
        message: error.message || 'Unable to submit approved tables.',
        duration: 5000
      })
    } finally {
      setSubmittingGate2(false)
    }
  }

  const handleSubmitGate3 = async (approve) => {
    setSubmittingGate3(true)
    try {
      await submitEnrichmentReview(run.id, approve)
      const refreshed = await getRun(run.id)
      onRunRefresh(refreshed)
      addNotification({
        type: 'success',
        title: approve ? 'Gate 3 Approved' : 'Gate 3 Rejected',
        message: approve
          ? 'Enrichment was approved. Bronze, Silver, and Gold generation are resuming.'
          : 'Enrichment was rejected and the run remains paused for rework.',
        duration: 5000
      })
    } catch (error) {
      addNotification({
        type: 'error',
        title: 'Gate 3 Failed',
        message: error.message || 'Unable to submit enrichment review.',
        duration: 5000
      })
    } finally {
      setSubmittingGate3(false)
    }
  }

  return (
    <div className="grid grid-cols-2 gap-4">
      {/* Left: DAG */}
      <div className="card p-4">
        <h3 className="text-sm font-semibold text-gray-300 mb-3">Pipeline Flow</h3>
        <PipelineDag stages={run.stages || []} compact={false} layout="vertical" />
      </div>

      {/* Right: Metadata + Gantt */}
      <div className="flex flex-col gap-4">
        {/* Metadata table */}
        <div className="card p-4">
          <h3 className="text-sm font-semibold text-gray-300 mb-3">Run Metadata</h3>
          <div className="space-y-2">
            {[
              { label: 'Run ID', value: <CopyableId id={run.id} chars={20} /> },
              { label: 'BRD File', value: run.brd_filename },
              { label: 'Provider', value: run.provider },
              { label: 'Deployment', value: run.deployment || '—' },
              { label: 'Cache Hit', value: run.cache_hit || 'NONE' },
              { label: 'Cache Score', value: run.cache_score ? run.cache_score.toFixed(3) : '—' },
              { label: 'Total Tokens', value: run.total_tokens?.toLocaleString() || '—' },
              { label: 'Total Cost', value: run.total_cost ? `$${run.total_cost.toFixed(4)}` : '—' },
              { label: 'Started', value: run.started_at ? new Date(run.started_at).toLocaleString() : '—' },
              { label: 'Completed', value: run.completed_at ? new Date(run.completed_at).toLocaleString() : 'In progress' }
            ].map(({ label, value }) => (
              <div key={label} className="flex justify-between items-center py-1.5 border-b border-bg-border last:border-0">
                <span className="text-xs text-gray-500">{label}</span>
                <span className="text-xs text-gray-300 font-mono">{value}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Gantt */}
        {ganttData.length > 0 && (
          <div className="card p-4">
            <h3 className="text-sm font-semibold text-gray-300 mb-3">Stage Timing (seconds)</h3>
            <ResponsiveContainer width="100%" height={Math.max(180, ganttData.length * 35)}>
              <BarChart data={ganttData} layout="vertical" margin={{ left: 10, right: 10, top: 0, bottom: 0 }}>
                <XAxis type="number" tick={{ fill: '#6b7280', fontSize: 10 }} axisLine={false} tickLine={false} />
                <YAxis type="category" dataKey="name" tick={{ fill: '#9ca3af', fontSize: 10 }} axisLine={false} tickLine={false} width={130} />
                <Tooltip
                  formatter={(val) => [`${val}s`, 'Duration']}
                  contentStyle={{ background: '#111827', border: '1px solid #1f2937', borderRadius: 8, fontSize: 12 }}
                  labelStyle={{ color: '#d1d5db' }}
                />
                <Bar dataKey="duration" radius={[0, 4, 4, 0]}>
                  {ganttData.map((entry, i) => (
                    <Cell key={i} fill={statusColors[entry.status] || '#374151'} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {reviewableRun && run.next_gate === 2 && (run.nominated_tables || []).length > 0 && (
          <div className="card p-4">
            <div className="flex items-start justify-between gap-3 mb-3">
              <div>
                <h3 className="text-sm font-semibold text-gray-300">Gate 2 Table Review</h3>
                <p className="text-xs text-gray-500 mt-1">{run.resume_message || 'Review and certify nominated tables.'}</p>
              </div>
              <StatusBadge status="PENDING" size="sm" />
            </div>

            <div className="space-y-2 max-h-72 overflow-y-auto pr-1">
              {(run.nominated_tables || []).map((table) => {
                const key = tableReviewKey(table)
                return (
                  <label key={key} className="flex items-start gap-3 p-3 rounded-lg border border-bg-border hover:border-gray-600 transition-colors cursor-pointer">
                    <input
                      type="checkbox"
                      checked={!!selectedTables[key]}
                      onChange={() => handleToggleTable(key)}
                      className="mt-1 accent-accent-blue"
                    />
                    <div className="min-w-0 flex-1">
                      <div className="text-sm text-gray-200 font-medium break-all">{key}</div>
                      <div className="flex gap-3 flex-wrap mt-1 text-[11px] text-gray-500">
                        <span>Confidence: {Number(table.confidence_score || 0).toFixed(3)}</span>
                        <span>Lexical: {Number(table.lexical_score || 0).toFixed(3)}</span>
                        <span>Semantic: {Number(table.semantic_score || 0).toFixed(3)}</span>
                        {(table.matched_columns || []).length > 0 && (
                          <span>Matched columns: {(table.matched_columns || []).length}</span>
                        )}
                      </div>
                      {table.nomination_reason && (
                        <p className="text-xs text-gray-400 mt-1">{table.nomination_reason}</p>
                      )}
                    </div>
                  </label>
                )
              })}
            </div>

            <div className="flex items-center justify-between mt-4 gap-3">
              <p className="text-xs text-gray-500">
                {(run.nominated_tables || []).filter((table) => selectedTables[tableReviewKey(table)]).length} of {(run.nominated_tables || []).length} selected
              </p>
              <button
                onClick={handleSubmitGate2}
                disabled={submittingGate2}
                className="flex items-center gap-2 px-4 py-2 bg-accent-blue hover:bg-blue-600 disabled:opacity-50 disabled:cursor-not-allowed text-white text-sm font-semibold rounded-lg transition-colors"
              >
                {submittingGate2 ? <Loader2 size={14} className="animate-spin" /> : <CheckCircle2 size={14} />}
                Certify Tables
              </button>
            </div>
          </div>
        )}

        {reviewableRun && run.next_gate === 3 && (
          <div className="card p-4">
            <div className="flex items-start justify-between gap-3 mb-3">
              <div>
                <h3 className="text-sm font-semibold text-gray-300">Gate 3 Enrichment Review</h3>
                <p className="text-xs text-gray-500 mt-1">{run.resume_message || 'Approve enrichment to continue script generation.'}</p>
              </div>
              <StatusBadge status="PENDING" size="sm" />
            </div>

            <div className="grid grid-cols-3 gap-2 mb-4">
              <StatTile label="Columns" value={(run.enriched_columns || []).length} />
              <StatTile label="Joins" value={(run.enriched_joins || []).length} />
              <StatTile label="PII" value={(run.pii_columns || []).length} />
            </div>

            <div className="flex items-center justify-end gap-2">
              <button
                onClick={() => handleSubmitGate3(false)}
                disabled={submittingGate3}
                className="flex items-center gap-2 px-4 py-2 border border-accent-red/25 text-accent-red hover:bg-accent-red/10 disabled:opacity-50 disabled:cursor-not-allowed text-sm font-semibold rounded-lg transition-colors"
              >
                {submittingGate3 ? <Loader2 size={14} className="animate-spin" /> : <XCircle size={14} />}
                Reject
              </button>
              <button
                onClick={() => handleSubmitGate3(true)}
                disabled={submittingGate3}
                className="flex items-center gap-2 px-4 py-2 bg-accent-blue hover:bg-blue-600 disabled:opacity-50 disabled:cursor-not-allowed text-white text-sm font-semibold rounded-lg transition-colors"
              >
                {submittingGate3 ? <Loader2 size={14} className="animate-spin" /> : <ShieldCheck size={14} />}
                Approve and Generate
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

function tableReviewKey(table) {
  return [table.database_name, table.schema_name, table.table_name].filter(Boolean).join('.')
}

function StatTile({ label, value }) {
  return (
    <div className="rounded-lg border border-bg-border bg-bg-base px-3 py-3">
      <div className="text-xs text-text-tertiary">{label}</div>
      <div className="text-lg font-bold text-text-primary mt-1">{value}</div>
    </div>
  )
}

/** Requirements tab */
function RequirementsTab({ run }) {
  const req = run.requirements
  if (!req) {
    return <EmptyState message="Requirements not extracted for this run." />
  }

  return (
    <div className="space-y-4 max-w-3xl">
      <div className="card p-5">
        <div className="flex items-center justify-between mb-2">
          <h3 className="text-sm font-semibold text-gray-300">Objective</h3>
          <div className="flex items-center gap-2">
            <span className="text-xs text-gray-500">Faithfulness</span>
            <span className={`text-xs font-mono font-bold ${req.faithfulness_score >= 0.9 ? 'text-accent-green' : req.faithfulness_score >= 0.7 ? 'text-accent-amber' : 'text-accent-red'}`}>
              {req.faithfulness_score?.toFixed(3)}
            </span>
            {req.retry_count > 0 && (
              <span className="text-xs bg-amber-500/10 text-accent-amber border border-accent-amber/20 px-2 py-0.5 rounded-full">
                {req.retry_count} retries
              </span>
            )}
          </div>
        </div>
        <p className="text-sm text-gray-300 leading-relaxed">{req.objective}</p>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="card p-4">
          <h4 className="text-xs uppercase tracking-wider text-gray-500 mb-3">Data Domains</h4>
          <div className="flex flex-wrap gap-2">
            {(req.data_domains || []).map((d) => (
              <span key={d} className="px-2.5 py-1 bg-accent-blue/10 text-accent-blue border border-accent-blue/20 rounded-full text-xs font-medium">
                {d}
              </span>
            ))}
          </div>
        </div>

        <div className="card p-4">
          <h4 className="text-xs uppercase tracking-wider text-gray-500 mb-3">Reporting Frequency</h4>
          <p className="text-sm text-gray-300">{req.reporting_frequency || '—'}</p>
          <h4 className="text-xs uppercase tracking-wider text-gray-500 mt-3 mb-1">Target Audience</h4>
          <p className="text-sm text-gray-300">{req.target_audience || '—'}</p>
        </div>
      </div>

      <div className="card p-4">
        <h4 className="text-xs uppercase tracking-wider text-gray-500 mb-3">Constraints</h4>
        <ul className="space-y-2">
          {(req.constraints || []).map((c, i) => (
            <li key={i} className="flex items-start gap-2 text-sm text-gray-300">
              <span className="w-1.5 h-1.5 rounded-full bg-accent-amber mt-1.5 flex-shrink-0" />
              {c}
            </li>
          ))}
        </ul>
      </div>
    </div>
  )
}

/** KPIs tab */
function KpisTab({ run }) {
  const [filter, setFilter] = useState('All')
  const [statusFilter, setStatusFilter] = useState('All')
  const [confThreshold, setConfThreshold] = useState(0)
  const [expandedId, setExpandedId] = useState(null)
  const [sortBy, setSortBy] = useState('confidence')
  const [sortDir, setSortDir] = useState('desc')

  const kpis = useMemo(() => run.kpis || [], [run.kpis])

  const filtered = useMemo(() => {
    let list = [...kpis]
    if (filter === 'Explicit') list = list.filter((k) => k.explicit)
    if (filter === 'Implicit') list = list.filter((k) => !k.explicit)
    if (statusFilter !== 'All') list = list.filter((k) => k.status === statusFilter || k.decision === statusFilter)
    list = list.filter((k) => (k.confidence || 0) >= confThreshold)
    list.sort((a, b) => {
      const av = a[sortBy] || 0, bv = b[sortBy] || 0
      return sortDir === 'desc' ? (bv > av ? 1 : -1) : (av > bv ? 1 : -1)
    })
    return list
  }, [kpis, filter, statusFilter, confThreshold, sortBy, sortDir])

  const toggleSort = (col) => {
    if (sortBy === col) setSortDir((d) => d === 'desc' ? 'asc' : 'desc')
    else { setSortBy(col); setSortDir('desc') }
  }

  const SortIcon = ({ col }) => sortBy === col
    ? (sortDir === 'desc' ? <ChevronDown size={12} /> : <ChevronUp size={12} />)
    : null

  if (kpis.length === 0) {
    return <EmptyState message="No KPIs extracted for this run." />
  }

  return (
    <div className="space-y-3">
      {/* Filter bar */}
      <div className="flex items-center gap-3 flex-wrap">
        <div className="flex rounded-lg border border-bg-border overflow-hidden">
          {['All', 'Explicit', 'Implicit'].map((f) => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={`px-3 py-1.5 text-xs font-medium transition-colors ${filter === f ? 'bg-accent-blue text-white' : 'text-gray-400 hover:text-white hover:bg-bg-border'}`}
            >
              {f}
            </button>
          ))}
        </div>

        <select
          value={statusFilter}
          onChange={(e) => setStatusFilter(e.target.value)}
          className="input-field w-auto text-xs py-1.5 cursor-pointer"
        >
          <option value="All">All Status</option>
          <option value="PENDING_REVIEW">Pending Review</option>
          <option value="APPROVED">Approved</option>
          <option value="EDITED">Edited</option>
          <option value="REJECTED">Rejected</option>
        </select>

        <div className="flex items-center gap-2 text-xs text-gray-500">
          <span>Min confidence:</span>
          <input
            type="range" min="0" max="1" step="0.1"
            value={confThreshold}
            onChange={(e) => setConfThreshold(parseFloat(e.target.value))}
            className="w-20 accent-accent-blue"
          />
          <span className="font-mono w-8">{confThreshold.toFixed(1)}</span>
        </div>

        <span className="text-xs text-gray-600 ml-auto">{filtered.length} KPIs</span>
      </div>

      {/* Table */}
      <div className="card overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-bg-border">
              {[
                { label: 'KPI Name', col: 'name' },
                { label: 'Category', col: 'category' },
                { label: 'Confidence', col: 'confidence' },
                { label: 'Status', col: 'status' },
                { label: 'Grounded', col: 'grounded' }
              ].map(({ label, col }) => (
                <th
                  key={col}
                  onClick={() => toggleSort(col)}
                  className="text-left px-4 py-3 text-xs uppercase tracking-wider text-gray-500 font-medium cursor-pointer hover:text-gray-300 transition-colors select-none"
                >
                  <span className="flex items-center gap-1">
                    {label}
                    <SortIcon col={col} />
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filtered.map((kpi) => (
              <React.Fragment key={kpi.id}>
                <tr
                  onClick={() => setExpandedId(expandedId === kpi.id ? null : kpi.id)}
                  className="border-b border-bg-border hover:bg-white/2 cursor-pointer transition-colors"
                >
                  <td className="px-4 py-3">
                    <span className="text-gray-200 font-medium">{kpi.name}</span>
                  </td>
                  <td className="px-4 py-3">
                    <span className="text-xs px-2 py-1 rounded-full bg-bg-border text-gray-400">{kpi.category}</span>
                  </td>
                  <td className="px-4 py-3 w-40">
                    <ConfidenceBar score={kpi.confidence} showLabel={false} compact={true} />
                    <span className="text-[10px] font-mono text-gray-500 ml-1">{kpi.confidence?.toFixed(3)}</span>
                  </td>
                  <td className="px-4 py-3">
                    <StatusBadge status={kpi.status || kpi.decision || 'PENDING'} size="sm" />
                  </td>
                  <td className="px-4 py-3">
                    <span className={`text-xs ${kpi.grounded ? 'text-accent-green' : 'text-gray-600'}`}>
                      {kpi.grounded ? '✓ Yes' : '✗ No'}
                    </span>
                  </td>
                </tr>
                {/* Expanded row */}
                {expandedId === kpi.id && (
                  <tr>
                    <td colSpan={5} className="px-6 py-4 bg-bg-base border-b border-bg-border">
                      <div className="space-y-2">
                        <p className="text-sm text-gray-300 leading-relaxed">{kpi.definition}</p>
                        {kpi.evidence && (
                          <div className="pl-3 border-l-2 border-accent-blue/30">
                            <p className="text-xs italic text-gray-400">{kpi.evidence}</p>
                          </div>
                        )}
                      </div>
                    </td>
                  </tr>
                )}
              </React.Fragment>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function ScriptsTab({ run }) {
  const [layer, setLayer] = useState('gold')
  const scripts = useMemo(() => {
    const rows = []
    for (const [layerName, bundle] of Object.entries({
      bronze: run.bronze,
      silver: run.silver,
      gold: run.gold
    })) {
      for (const script of bundle?.scripts || []) {
        rows.push({
          ...script,
          layer: layerName,
          title:
            script.table ||
            script.kpi_name ||
            script.target_table ||
            script.script_path?.split(/[\\/]/).pop() ||
            `${layerName} script`,
          body: script.script_body || ''
        })
        if (layerName === 'gold' && script.dimension_script_body) {
          rows.push({
            ...script,
            layer: 'gold',
            title: `${script.kpi_name || script.target_table || 'KPI'} dimensions`,
            target_table: script.target_table,
            script_path: script.dimension_script_path,
            body: script.dimension_script_body,
            status: script.status
          })
        }
      }
    }
    return rows
  }, [run.bronze, run.silver, run.gold])

  const filtered = scripts.filter((script) => script.layer === layer)
  const [selectedPath, setSelectedPath] = useState('')

  useEffect(() => {
    if (!filtered.length) {
      setSelectedPath('')
      return
    }
    if (!filtered.some((script) => script.script_path === selectedPath)) {
      setSelectedPath(filtered[0].script_path || filtered[0].title)
    }
  }, [layer, filtered, selectedPath])

  const selected =
    filtered.find((script) => (script.script_path || script.title) === selectedPath) ||
    filtered[0]

  const counts = {
    bronze: scripts.filter((script) => script.layer === 'bronze').length,
    silver: scripts.filter((script) => script.layer === 'silver').length,
    gold: scripts.filter((script) => script.layer === 'gold').length
  }

  if (!scripts.length) {
    return <EmptyState message="No generated scripts are available for this run yet." />
  }

  return (
    <div className="grid grid-cols-[320px_1fr] gap-4 min-h-[640px]">
      <div className="card p-4 flex flex-col min-h-0">
        <div className="flex items-center gap-2 mb-3">
          <Code2 size={15} className="text-accent-blue" />
          <h3 className="text-sm font-semibold text-gray-300">Generated Scripts</h3>
        </div>

        <div className="grid grid-cols-3 gap-1 mb-3">
          {['bronze', 'silver', 'gold'].map((item) => (
            <button
              key={item}
              onClick={() => setLayer(item)}
              className={`px-2 py-2 rounded-lg border text-xs font-semibold capitalize transition-colors ${
                layer === item
                  ? 'bg-accent-blue text-white border-accent-blue'
                  : 'border-bg-border text-gray-400 hover:text-gray-200 hover:border-gray-600'
              }`}
            >
              {item} {counts[item] > 0 ? `(${counts[item]})` : ''}
            </button>
          ))}
        </div>

        <div className="space-y-2 overflow-y-auto pr-1">
          {filtered.length === 0 ? (
            <p className="text-xs text-gray-600 py-6 text-center">No {layer} scripts yet.</p>
          ) : (
            filtered.map((script) => {
              const key = script.script_path || script.title
              const active = key === selectedPath
              return (
                <button
                  key={key}
                  onClick={() => setSelectedPath(key)}
                  className={`w-full text-left p-3 rounded-lg border transition-colors ${
                    active ? 'border-accent-blue/50 bg-accent-blue/10' : 'border-bg-border hover:border-gray-600'
                  }`}
                >
                  <div className="text-sm text-gray-200 font-medium break-words">{script.title}</div>
                  <div className="text-[11px] text-gray-500 mt-1 break-all">{script.target_table || script.source_table || '-'}</div>
                  {script.status && <div className="text-[10px] text-gray-600 mt-1">{script.status}</div>}
                </button>
              )
            })
          )}
        </div>
      </div>

      <div className="card p-4 min-w-0 flex flex-col">
        {selected ? (
          <>
            <div className="flex items-start justify-between gap-3 mb-3">
              <div className="min-w-0">
                <h3 className="text-sm font-semibold text-gray-200 break-words">{selected.title}</h3>
                <p className="text-xs text-gray-500 break-all mt-1">{selected.script_path || selected.target_table}</p>
              </div>
              <StatusBadge status={selected.status || 'GENERATED'} size="sm" />
            </div>
            <pre className="flex-1 min-h-[560px] overflow-auto rounded-lg border border-bg-border bg-bg-base p-4 text-xs leading-relaxed text-gray-300">
              <code>{selected.body || '# Script body is not available.'}</code>
            </pre>
          </>
        ) : (
          <EmptyState message={`No ${layer} script selected.`} />
        )}
      </div>
    </div>
  )
}

/** HITL Decisions tab */
function HitlDecisionsTab({ run }) {
  const decisions = run.hitl_decisions || (run.kpis || []).filter((k) => k.decision)

  if (decisions.length === 0) {
    return <EmptyState message="No HITL decisions recorded for this run." />
  }

  return (
    <div className="space-y-3 max-w-3xl">
      {decisions.map((decision) => (
        <div key={decision.id} className="card p-4">
          <div className="flex items-start justify-between gap-3">
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2 mb-1">
                {decision.gate && (
                  <span className="text-[10px] uppercase tracking-wider text-accent-blue font-semibold">
                    {decision.gate}
                  </span>
                )}
                {decision.type && (
                  <span className="text-[10px] uppercase tracking-wider text-gray-500">
                    {decision.type}
                  </span>
                )}
              </div>
              <p className="text-sm font-semibold text-gray-200 mb-1">{decision.name}</p>
              <p className="text-xs text-gray-400 leading-relaxed">{decision.definition}</p>
              {decision.rejection_reason && (
                <p className="text-xs text-accent-red mt-2">{decision.rejection_reason}</p>
              )}
            </div>
            <StatusBadge status={decision.decision} size="sm" />
          </div>
          <div className="flex items-center gap-4 mt-3 text-xs text-gray-500">
            <span>Reviewer: <span className="text-gray-300">{decision.reviewer || 'Athena reviewer'}</span></span>
            {decision.reviewed_at && (
              <span>{new Date(decision.reviewed_at).toLocaleString()}</span>
            )}
          </div>
        </div>
      ))}
    </div>
  )
}

/** Cost Log tab */
function CostLogTab({ run }) {
  const stages = run.stages || []

  return (
    <div className="space-y-4 max-w-3xl">
      <div className="card overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-bg-border">
              {['Stage', 'Tokens', 'Cost', 'Attempts', 'Duration'].map((h) => (
                <th key={h} className="text-left px-4 py-3 text-xs uppercase tracking-wider text-gray-500 font-medium">
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {stages.map((s) => {
              const dur = s.duration_seconds != null
                ? `${Number(s.duration_seconds).toFixed(1)}s`
                : s.started_at && s.completed_at
                ? ((new Date(s.completed_at) - new Date(s.started_at)) / 1000).toFixed(1) + 's'
                : '—'
              return (
                <tr key={s.id} className="border-b border-bg-border hover:bg-white/2">
                  <td className="px-4 py-3 text-gray-300 text-xs">{s.name}</td>
                  <td className="px-4 py-3 font-mono text-xs text-gray-400">{s.tokens ? s.tokens.toLocaleString() : '—'}</td>
                  <td className="px-4 py-3 font-mono text-xs text-accent-green">{s.cost ? `$${s.cost.toFixed(4)}` : '—'}</td>
                  <td className="px-4 py-3 text-xs text-gray-400">{s.attempts || '—'}</td>
                  <td className="px-4 py-3 font-mono text-xs text-gray-400">{dur}</td>
                </tr>
              )
            })}
            <tr className="bg-bg-border/20 font-semibold">
              <td className="px-4 py-2 text-xs text-gray-300">Total</td>
              <td className="px-4 py-2 font-mono text-xs text-gray-300">{run.total_tokens?.toLocaleString() || '—'}</td>
              <td className="px-4 py-2 font-mono text-xs text-accent-green">{run.total_cost ? `$${run.total_cost.toFixed(4)}` : '—'}</td>
              <td colSpan={2} />
            </tr>
          </tbody>
        </table>
      </div>

      {/* Prompt metadata per stage */}
      {stages.filter((s) => s.prompt_metadata).map((s) => (
        <div key={s.id} className="card p-4">
          <h4 className="text-xs uppercase tracking-wider text-gray-500 mb-2">{s.name} — Prompt Config</h4>
          <JsonViewer data={s.prompt_metadata} maxHeight={160} />
        </div>
      ))}
    </div>
  )
}

function EmptyState({ message }) {
  return (
    <div className="flex items-center justify-center h-40 text-gray-600 text-sm">
      {message}
    </div>
  )
}

export default RunDetail

