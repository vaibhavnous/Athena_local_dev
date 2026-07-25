// @ts-nocheck
import React from 'react'
import { TrendingUp, TrendingDown, Minus } from 'lucide-react'

/**
 * MetricCard — a stat card with title, value, optional trend, icon, accent.
 * @param {{
 *   title: string,
 *   value: string | number,
 *   subtitle?: string,
 *   trend?: { value: string, direction: 'up' | 'down' | 'neutral', positive?: boolean },
 *   icon?: React.ComponentType,
 *   accentColor?: string,
 *   loading?: boolean
 * }} props
 */
function MetricCard({ title, value, subtitle, trend, icon: Icon, accentColor = '#3b82f6', loading = false }) {
  const trendColor = trend
    ? trend.direction === 'neutral'
      ? 'text-gray-400'
      : trend.positive === false
      ? trend.direction === 'up' ? 'text-accent-red' : 'text-accent-green'
      : trend.direction === 'up' ? 'text-accent-green' : 'text-accent-red'
    : ''

  return (
    <div
      className="card p-5 relative overflow-hidden"
      style={{ borderLeft: `3px solid ${accentColor}` }}
    >
      {/* Background glow */}
      <div
        className="absolute top-0 left-0 w-24 h-24 rounded-full opacity-5 pointer-events-none"
        style={{ background: accentColor, transform: 'translate(-30%, -30%)' }}
      />

      <div className="flex items-start justify-between">
        <div className="flex-1 min-w-0">
          <p className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1.5">
            {title}
          </p>

          {loading ? (
            <div className="h-8 w-24 bg-bg-border animate-pulse rounded mb-1" />
          ) : (
            <div className="text-2xl font-bold text-gray-100 leading-none mb-1 truncate">
              {value}
            </div>
          )}

          {subtitle && (
            <p className="text-xs text-gray-500 mt-1">{subtitle}</p>
          )}

          {trend && !loading && (
            <div className={`flex items-center gap-1 mt-2 ${trendColor}`}>
              {trend.direction === 'up' && <TrendingUp size={12} />}
              {trend.direction === 'down' && <TrendingDown size={12} />}
              {trend.direction === 'neutral' && <Minus size={12} />}
              <span className="text-xs font-medium">{trend.value}</span>
            </div>
          )}
        </div>

        {Icon && (
          <div
            className="w-10 h-10 rounded-xl flex items-center justify-center flex-shrink-0 ml-3"
            style={{ background: `${accentColor}15`, border: `1px solid ${accentColor}30` }}
          >
            <Icon size={18} style={{ color: accentColor }} />
          </div>
        )}
      </div>
    </div>
  )
}

export default MetricCard

