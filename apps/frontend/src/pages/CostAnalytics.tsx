// @ts-nocheck
import React, { useMemo } from 'react'
import { DollarSign, Zap, TrendingUp, Activity } from 'lucide-react'
import useAthenaStore from '../store/useAthenaStore'
import MetricCard from '../components/shared/MetricCard'
import CostLineChart from '../components/analytics/CostLineChart'
import TokenBarChart from '../components/analytics/TokenBarChart'
import ProviderDonutChart from '../components/analytics/ProviderDonutChart'
import StatusBadge from '../components/shared/StatusBadge'
import CopyableId from '../components/shared/CopyableId'

function CostAnalytics() {
  const { costData, runs } = useAthenaStore()

  // Derived metrics
  const metrics = useMemo(() => {
    const totalSpend = runs.reduce((s, r) => s + (r.total_cost || 0), 0)
    const completedRuns = runs.filter((r) => r.status === 'COMPLETED' || r.status === 'HITL_WAIT')
    const avgPerRun = completedRuns.length > 0 ? totalSpend / completedRuns.length : 0
    const totalTokens = runs.reduce((s, r) => s + (r.total_tokens || 0), 0)

    // Find peak stage
    const stageCosts = {}
    runs.forEach((r) => {
      (r.stages || []).forEach((s) => {
        stageCosts[s.name] = (stageCosts[s.name] || 0) + (s.cost || 0)
      })
    })
    const peakStage = Object.entries(stageCosts).sort((a, b) => b[1] - a[1])[0]

    return { totalSpend, avgPerRun, totalTokens, peakStage }
  }, [runs])

  // Token bar chart data from runs
  const tokenData = useMemo(() => {
    return runs.slice(0, 10).map((r) => {
      const inputTokens = Math.round((r.total_tokens || 0) * 0.65)
      const outputTokens = Math.round((r.total_tokens || 0) * 0.35)
      return { run_id: r.id, inputTokens, outputTokens }
    })
  }, [runs])

  // Provider donut data
  const providerData = useMemo(() => {
    const map = {}
    runs.forEach((r) => {
      if (!map[r.provider]) map[r.provider] = 0
      map[r.provider] += r.total_cost || 0
    })
    return Object.entries(map).map(([provider, cost]) => ({ provider, cost: parseFloat(cost.toFixed(4)) }))
  }, [runs])

  // Per-run cost table
  const runCostTable = useMemo(() => {
    return [...runs].sort((a, b) => new Date(b.started_at) - new Date(a.started_at))
  }, [runs])

  return (
    <div className="flex flex-col gap-6">
      {/* Header */}
      <div>
        <h1 className="text-lg font-bold text-white">Cost Analytics</h1>
        <p className="text-sm text-gray-500 mt-0.5">LLM spend tracking across all pipeline runs</p>
      </div>

      {/* Top metrics */}
      <div className="grid grid-cols-4 gap-4">
        <MetricCard
          title="Total Spend"
          value={`$${metrics.totalSpend.toFixed(3)}`}
          subtitle={`${runs.length} total runs`}
          icon={DollarSign}
          accentColor="#10b981"
          trend={{ value: '+12% vs last week', direction: 'up', positive: false }}
        />
        <MetricCard
          title="Avg per Run"
          value={`$${metrics.avgPerRun.toFixed(3)}`}
          subtitle="Completed runs only"
          icon={Activity}
          accentColor="#3b82f6"
          trend={{ value: '-8% vs last week', direction: 'down', positive: true }}
        />
        <MetricCard
          title="Total Tokens"
          value={metrics.totalTokens > 1000 ? `${(metrics.totalTokens / 1000).toFixed(1)}k` : String(metrics.totalTokens)}
          subtitle="Input + output combined"
          icon={Zap}
          accentColor="#8b5cf6"
        />
        <MetricCard
          title="Peak Stage"
          value={metrics.peakStage?.[0]?.split('—')[1]?.trim() || 'N/A'}
          subtitle={metrics.peakStage ? `$${metrics.peakStage[1].toFixed(3)} total` : ''}
          icon={TrendingUp}
          accentColor="#f59e0b"
        />
      </div>

      {/* Charts row */}
      <div className="grid grid-cols-2 gap-4">
        {/* Cost line chart */}
        <div className="card p-5">
          <h3 className="text-sm font-semibold text-gray-300 mb-4">30-Day Cost Trend</h3>
          <CostLineChart data={costData} />
        </div>

        {/* Provider donut */}
        <div className="card p-5">
          <h3 className="text-sm font-semibold text-gray-300 mb-4">Cost by Provider</h3>
          <ProviderDonutChart data={providerData} />
        </div>
      </div>

      {/* Token bar chart */}
      <div className="card p-5">
        <h3 className="text-sm font-semibold text-gray-300 mb-4">Token Usage by Run (last 10)</h3>
        <TokenBarChart data={tokenData} />
      </div>

      {/* Per-run cost table */}
      <div className="card overflow-hidden">
        <div className="px-5 py-4 border-b border-bg-border">
          <h3 className="text-sm font-semibold text-gray-300">Per-Run Cost Breakdown</h3>
        </div>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-bg-border">
              {['Run ID', 'BRD File', 'Stage 02', 'Stage 03', 'Total', 'Tokens', 'Date', 'Status'].map((h) => (
                <th key={h} className="text-left px-4 py-3 text-xs uppercase tracking-wider text-gray-500 font-medium">
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {runCostTable.map((run) => {
              const s2 = (run.stages || []).find((s) => s.id === 'stage_02')
              const s3 = (run.stages || []).find((s) => s.id === 'stage_03')
              return (
                <tr key={run.id} className="border-b border-bg-border hover:bg-white/2 transition-colors">
                  <td className="px-4 py-3"><CopyableId id={run.id} chars={12} /></td>
                  <td className="px-4 py-3 text-xs text-gray-400 max-w-32 truncate">{run.brd_filename}</td>
                  <td className="px-4 py-3 font-mono text-xs text-accent-purple">
                    {s2?.cost ? `$${s2.cost.toFixed(4)}` : '—'}
                  </td>
                  <td className="px-4 py-3 font-mono text-xs text-accent-blue">
                    {s3?.cost ? `$${s3.cost.toFixed(4)}` : '—'}
                  </td>
                  <td className="px-4 py-3 font-mono text-xs text-accent-green font-semibold">
                    {run.total_cost ? `$${run.total_cost.toFixed(4)}` : '—'}
                  </td>
                  <td className="px-4 py-3 font-mono text-xs text-gray-400">
                    {run.total_tokens ? run.total_tokens.toLocaleString() : '—'}
                  </td>
                  <td className="px-4 py-3 text-xs font-mono text-gray-500">
                    {run.started_at ? new Date(run.started_at).toLocaleDateString() : '—'}
                  </td>
                  <td className="px-4 py-3">
                    <StatusBadge status={run.status} size="sm" />
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export default CostAnalytics

