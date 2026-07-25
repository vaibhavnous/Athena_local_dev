// @ts-nocheck
import React from 'react'
import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  ResponsiveContainer,
  Legend
} from 'recharts'

const PROVIDER_COLORS = {
  'Azure OpenAI': '#3b82f6',
  'OpenAI': '#10b981',
  'Anthropic': '#8b5cf6',
  'azure_openai': '#3b82f6',
  'openai': '#10b981',
  'anthropic': '#8b5cf6'
}

const PROVIDER_LABELS = {
  azure_openai: 'Azure OpenAI',
  openai: 'OpenAI',
  anthropic: 'Anthropic'
}

/**
 * ProviderDonutChart — donut chart of cost by LLM provider.
 * @param {{ data: Array<{ provider: string, cost: number }> }} props
 */
function ProviderDonutChart({ data = [] }) {
  const normalized = data.map((d) => ({
    ...d,
    name: PROVIDER_LABELS[d.provider] || d.provider,
    color: PROVIDER_COLORS[d.provider] || '#6b7280'
  }))

  const total = normalized.reduce((s, d) => s + d.cost, 0)

  const CustomTooltip = ({ active, payload }) => {
    if (!active || !payload?.length) return null
    const entry = payload[0]
    return (
      <div className="bg-bg-card border border-bg-border rounded-xl p-3 shadow-2xl text-xs">
        <div className="flex items-center gap-2 mb-1">
          <span className="w-2.5 h-2.5 rounded-full" style={{ background: entry.payload.color }} />
          <span className="text-text-primary font-medium">{entry.name}</span>
        </div>
        <div className="flex justify-between gap-4 text-text-secondary">
          <span>Cost:</span>
          <span className="font-mono font-semibold">${(entry.value || 0).toFixed(3)}</span>
        </div>
        <div className="flex justify-between gap-4 text-gray-500">
          <span>Share:</span>
          <span className="font-mono">{total > 0 ? ((entry.value / total) * 100).toFixed(1) : 0}%</span>
        </div>
      </div>
    )
  }

  const CustomLabel = ({ cx, cy }) => (
    <text x={cx} y={cy} textAnchor="middle" dominantBaseline="central" fill="#f3f4f6">
      <tspan x={cx} dy="-8" fontSize={20} fontWeight="700" fontFamily="monospace">
        ${total.toFixed(2)}
      </tspan>
      <tspan x={cx} dy="20" fontSize={11} fill="#6b7280" fontFamily="Inter">
        total
      </tspan>
    </text>
  )

  if (normalized.length === 0) {
    return (
      <div className="flex items-center justify-center h-48 text-gray-600 text-sm">
        No cost data available
      </div>
    )
  }

  return (
    <ResponsiveContainer width="100%" height={260}>
      <PieChart>
        <Pie
          data={normalized}
          cx="50%"
          cy="50%"
          innerRadius={72}
          outerRadius={100}
          paddingAngle={3}
          dataKey="cost"
          labelLine={false}
          label={CustomLabel}
        >
          {normalized.map((entry, index) => (
            <Cell key={index} fill={entry.color} stroke="transparent" />
          ))}
        </Pie>
        <Tooltip content={<CustomTooltip />} />
        <Legend
          formatter={(value) => <span style={{ color: '#9ca3af', fontSize: 12 }}>{value}</span>}
          iconType="circle"
          iconSize={8}
        />
      </PieChart>
    </ResponsiveContainer>
  )
}

export default ProviderDonutChart

