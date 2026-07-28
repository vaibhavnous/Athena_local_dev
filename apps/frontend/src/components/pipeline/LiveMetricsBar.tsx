// @ts-nocheck
import React from 'react'
import { Zap, DollarSign, Database, GitBranch } from 'lucide-react'

/**
 * LiveMetricsBar — bottom strip showing live run metrics.
 * @param {{ run: object }} props
 */
function LiveMetricsBar({ run }) {
  if (!run) return null

  const cacheHitConfig = getCacheHitConfig(run.cache_hit)
  const pathConfig = getPathConfig(run.extraction_path)

  return (
    <div className="flex items-stretch gap-px bg-bg-border rounded-xl overflow-hidden border border-bg-border">
      <MetricCell
        icon={Zap}
        label="Total Tokens"
        value={run.total_tokens ? run.total_tokens.toLocaleString() : '—'}
        valueClass="text-accent-blue"
        iconColor="#3b82f6"
      />

      <div className="w-px bg-bg-border" />

      <MetricCell
        icon={DollarSign}
        label="Total Cost"
        value={run.total_cost ? `$${run.total_cost.toFixed(4)}` : '—'}
        valueClass="text-accent-green"
        iconColor="#10b981"
      />

      <div className="w-px bg-bg-border" />

      <MetricCell
        icon={Database}
        label="Cache Hit"
        value={
          <span
            className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-semibold border ${cacheHitConfig.classes}`}
          >
            {cacheHitConfig.label}
          </span>
        }
        iconColor="#14b8a6"
      />

      <div className="w-px bg-bg-border" />

      <MetricCell
        icon={GitBranch}
        label="Extraction Path"
        value={
          <span
            className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-semibold border ${pathConfig.classes}`}
          >
            {pathConfig.label}
          </span>
        }
        iconColor="#8b5cf6"
      />
    </div>
  )
}

function MetricCell({ icon: Icon, label, value, valueClass, iconColor }) {
  return (
    <div className="flex-1 flex items-center gap-3 px-4 py-3 bg-bg-card hover:bg-bg-border/30 transition-colors">
      <div
        className="w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0"
        style={{ background: `${iconColor}15`, border: `1px solid ${iconColor}25` }}
      >
        <Icon size={14} style={{ color: iconColor }} />
      </div>
      <div>
        <p className="text-[10px] uppercase tracking-wider text-text-tertiary font-medium">{label}</p>
        {typeof value === 'string' ? (
          <p className={`text-sm font-semibold font-mono mt-0.5 ${valueClass || 'text-text-secondary'}`}>
            {value}
          </p>
        ) : (
          <div className="mt-0.5">{value}</div>
        )}
      </div>
    </div>
  )
}

function getCacheHitConfig(cacheHit) {
  switch (cacheHit) {
    case 'L1_EXACT':
      return {
        label: 'L1 EXACT',
        classes: 'bg-teal-500/10 text-accent-teal border-accent-teal/30'
      }
    case 'L2_FUZZY':
      return {
        label: 'L2 FUZZY',
        classes: 'bg-blue-500/10 text-accent-blue border-accent-blue/30'
      }
    case 'NONE':
    default:
      return {
        label: 'NO HIT',
        classes: 'bg-gray-500/10 text-gray-500 border-gray-600'
      }
  }
}

function getPathConfig(path) {
  switch (path) {
    case 'CACHED_L1':
      return {
        label: 'CACHED L1',
        classes: 'bg-teal-500/10 text-accent-teal border-accent-teal/30'
      }
    case 'CACHED_L2':
      return {
        label: 'CACHED L2',
        classes: 'bg-blue-500/10 text-accent-blue border-accent-blue/30'
      }
    case 'FULL_EXTRACTION':
    default:
      return {
        label: 'FULL EXTRACT',
        classes: 'bg-purple-500/10 text-accent-purple border-accent-purple/30'
      }
  }
}

export default LiveMetricsBar

