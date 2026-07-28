// @ts-nocheck
import React from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
} from 'recharts'

/**
 * TokenBarChart — stacked bar chart of input/output tokens per run.
 * @param {{ data: Array<{ run_id: string, inputTokens: number, outputTokens: number }> }} props
 */
function TokenBarChart({ data = [] }) {
  const CustomTooltip = ({ active, payload, label }) => {
    if (!active || !payload?.length) return null
    const total = payload.reduce((s, e) => s + (e.value || 0), 0)
    return (
      <div className="bg-bg-card border border-bg-border rounded-xl p-3 shadow-2xl text-xs">
        <p className="text-text-tertiary mb-2 font-mono">{label}</p>
        {payload.map((entry) => (
          <div key={entry.dataKey} className="flex items-center gap-2 mb-1">
            <span className="w-2 h-2 rounded-sm" style={{ background: entry.fill }} />
            <span className="text-text-secondary">{entry.name}:</span>
            <span className="font-mono font-semibold" style={{ color: entry.fill }}>
              {(entry.value || 0).toLocaleString()}
            </span>
          </div>
        ))}
        <div className="border-t border-bg-border mt-2 pt-2 flex justify-between">
          <span className="text-text-tertiary">Total</span>
          <span className="font-mono font-semibold text-text-secondary">{total.toLocaleString()}</span>
        </div>
      </div>
    )
  }

  const formatK = (v) => {
    if (v >= 1000) return `${(v / 1000).toFixed(0)}k`
    return v
  }

  return (
    <ResponsiveContainer width="100%" height={260}>
      <BarChart data={data} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" horizontal={true} vertical={false} />
        <XAxis
          dataKey="run_id"
          tickFormatter={(id) => (id || '').slice(0, 8)}
          tick={{ fill: '#6b7280', fontSize: 10, fontFamily: 'monospace' }}
          axisLine={{ stroke: '#1f2937' }}
          tickLine={false}
        />
        <YAxis
          tickFormatter={formatK}
          tick={{ fill: '#6b7280', fontSize: 11 }}
          axisLine={false}
          tickLine={false}
          width={44}
        />
        <Tooltip content={<CustomTooltip />} />
        <Legend
          wrapperStyle={{ paddingTop: 12, fontSize: 12 }}
          formatter={(value) => <span style={{ color: '#9ca3af' }}>{value}</span>}
        />
        <Bar dataKey="inputTokens" name="Input Tokens" stackId="a" fill="#3b82f6" radius={[0, 0, 0, 0]} />
        <Bar dataKey="outputTokens" name="Output Tokens" stackId="a" fill="#8b5cf6" radius={[3, 3, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  )
}

export default TokenBarChart

