// @ts-nocheck
import React from 'react'

/**
 * ConfidenceBar — visual bar showing confidence score 0–1.
 * Green ≥ 0.7, Amber 0.4–0.69, Red < 0.4
 * @param {{ score: number, showLabel?: boolean, compact?: boolean }} props
 */
function ConfidenceBar({ score = 0, showLabel = true, compact = false }) {
  const pct = Math.round((score || 0) * 100)
  const width = `${Math.max(0, Math.min(100, pct))}%`

  let barColor, textColor, bgColor
  if (score >= 0.7) {
    barColor = 'bg-accent-green'
    textColor = 'text-accent-green'
    bgColor = 'bg-accent-green/10'
  } else if (score >= 0.4) {
    barColor = 'bg-accent-amber'
    textColor = 'text-accent-amber'
    bgColor = 'bg-accent-amber/10'
  } else {
    barColor = 'bg-accent-red'
    textColor = 'text-accent-red'
    bgColor = 'bg-accent-red/10'
  }

  if (compact) {
    return (
      <div className="flex items-center gap-2">
        <div className={`flex-1 h-1 rounded-full ${bgColor}`} style={{ minWidth: 40 }}>
          <div
            className={`h-full rounded-full ${barColor} transition-all duration-500`}
            style={{ width }}
          />
        </div>
        {showLabel && (
          <span className={`text-xs font-mono font-medium ${textColor} w-8 text-right`}>
            {pct}%
          </span>
        )}
      </div>
    )
  }

  return (
    <div className="space-y-1">
      {showLabel && (
        <div className="flex items-center justify-between">
          <span className="text-xs text-text-tertiary">Confidence</span>
          <span className={`text-xs font-mono font-semibold ${textColor}`}>
            {score.toFixed(3)}
          </span>
        </div>
      )}
      <div className={`w-full h-1.5 rounded-full bg-bg-border`}>
        <div
          className={`h-full rounded-full ${barColor} transition-all duration-700 ease-out`}
          style={{ width }}
        />
      </div>
    </div>
  )
}

export default ConfidenceBar

