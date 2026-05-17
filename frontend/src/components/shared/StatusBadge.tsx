// @ts-nocheck
import React from 'react'

/**
 * StatusBadge — renders a colored pill for pipeline/KPI status.
 * @param {{ status: string, size?: 'sm' | 'md' }} props
 */
function StatusBadge({ status, size = 'md' }) {
  const config = getStatusConfig(status)
  const sizeClass = size === 'sm' ? 'text-[10px] px-2 py-0.5 gap-1' : 'text-xs px-2.5 py-1 gap-1.5'

  return (
    <span
      className={`inline-flex items-center rounded-full font-medium border ${sizeClass} ${config.classes}`}
      title={config.label}
    >
      <span className={`relative flex-shrink-0 ${config.dotSize || 'w-1.5 h-1.5'}`}>
        <span className={`rounded-full block w-full h-full ${config.dotColor}`} />
        {config.pulse && (
          <span className={`absolute inset-0 rounded-full animate-ping ${config.dotColor} opacity-75`} />
        )}
      </span>
      <span className={config.strikethrough ? 'line-through opacity-60' : ''}>
        {config.label}
      </span>
    </span>
  )
}

function getStatusConfig(status) {
  const s = (status || '').toUpperCase()

  switch (s) {
    case 'RUNNING':
      return {
        label: 'Running',
        classes: 'bg-blue-500/10 text-accent-blue border-accent-blue/25',
        dotColor: 'bg-accent-blue',
        pulse: true,
        dotSize: 'w-1.5 h-1.5'
      }
    case 'COMPLETED':
      return {
        label: 'Completed',
        classes: 'bg-emerald-500/10 text-accent-green border-accent-green/25',
        dotColor: 'bg-accent-green',
        pulse: false
      }
    case 'FAILED':
      return {
        label: 'Failed',
        classes: 'bg-red-500/10 text-accent-red border-accent-red/25',
        dotColor: 'bg-accent-red',
        pulse: false
      }
    case 'HITL_WAIT':
    case 'PAUSED_FOR_HITL':
    case 'PENDING_REVIEW':
      return {
        label: 'Awaiting Review',
        classes: 'bg-amber-500/10 text-accent-amber border-accent-amber/25',
        dotColor: 'bg-accent-amber',
        pulse: true
      }
    case 'PENDING':
      return {
        label: 'Pending',
        classes: 'bg-gray-500/10 text-text-secondary border-bg-border',
        dotColor: 'bg-text-tertiary',
        pulse: false
      }
    case 'AUTO_SUPPRESSED':
      return {
        label: 'Suppressed',
        classes: 'bg-gray-500/10 text-text-tertiary border-bg-border',
        dotColor: 'bg-text-tertiary',
        pulse: false,
        strikethrough: true
      }
    case 'APPROVED':
      return {
        label: 'Approved',
        classes: 'bg-emerald-500/10 text-accent-green border-accent-green/25',
        dotColor: 'bg-accent-green',
        pulse: false
      }
    case 'REJECTED':
      return {
        label: 'Rejected',
        classes: 'bg-red-500/10 text-accent-red border-accent-red/25',
        dotColor: 'bg-accent-red',
        pulse: false,
        strikethrough: true
      }
    case 'EDITED':
      return {
        label: 'Edited',
        classes: 'bg-purple-500/10 text-accent-purple border-accent-purple/25',
        dotColor: 'bg-accent-purple',
        pulse: false
      }
    case 'ABORTED':
      return {
        label: 'Aborted',
        classes: 'bg-orange-500/10 text-orange-400 border-orange-500/25',
        dotColor: 'bg-orange-500',
        pulse: false
      }
    default:
      return {
        label: status || 'Unknown',
        classes: 'bg-gray-500/10 text-gray-400 border-gray-600',
        dotColor: 'bg-gray-500',
        pulse: false
      }
  }
}

export default StatusBadge

