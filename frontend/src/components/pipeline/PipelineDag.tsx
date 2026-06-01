// @ts-nocheck
import React from 'react'
import { motion } from 'framer-motion'
import { Loader2, AlertCircle, Users, Clock, CheckCircle2 } from 'lucide-react'

/**
 * PipelineDag — renders a DAG of pipeline stages with animated connectors.
 * @param {{
 *   stages: Array,
 *   onStageClick?: (stage: object) => void,
 *   compact?: boolean,
 *   layout?: 'horizontal' | 'vertical'
 * }} props
 */
function PipelineDag({ stages = [], onStageClick, compact = false, layout = 'horizontal' }) {
  if (!stages || stages.length === 0) {
    return (
      <div className="flex items-center justify-center h-20 text-text-tertiary text-sm">
        No stages available
      </div>
    )
  }

  if (layout === 'vertical') {
    return <VerticalPipelineDag stages={stages} onStageClick={onStageClick} compact={compact} />
  }

  return (
    <div className="w-full border border-bg-border rounded-lg bg-bg-card/50 overflow-x-auto relative z-0 px-4 py-5">
      <div className="flex items-start w-max min-w-full gap-3">
        {stages.map((stage, index) => (
          <React.Fragment key={stage.id}>
            {/* Stage Column - Bubble + Name */}
            <div className="flex flex-col items-center" style={{ flex: '0 0 auto', zIndex: 0, gap: '8px' }}>
              {/* Bubble */}
              <HorizontalBubbleNode
                stage={stage}
                index={index}
                onClick={onStageClick ? () => onStageClick(stage) : undefined}
                compact={compact}
              />
              
              {/* Stage Name - Centered under bubble */}
              <div style={{ width: compact ? '84px' : '112px' }} className="text-center">
                <p style={{ fontSize: compact ? '10px' : '11px' }} className="font-medium text-text-secondary leading-snug break-words">
                  {stage.name.replace(/Stage \d+ — /, '')}
                </p>
              </div>
            </div>

            {/* Connector between stages - Flex to fill space between bubbles */}
            {index < stages.length - 1 && (
              <div style={{ flex: '1 1 auto', display: 'flex', alignItems: 'center', justifyContent: 'center', height: '32px', minWidth: compact ? '18px' : '28px' }}>
                <HorizontalConnector
                  upstream={stage}
                  downstream={stages[index + 1]}
                  compact={compact}
                />
              </div>
            )}
          </React.Fragment>
        ))}
      </div>
    </div>
  )
}

/**
 * VerticalPipelineDag — renders stages vertically for detail views
 */
function VerticalPipelineDag({ stages = [], onStageClick, compact = false }) {
  return (
    <div className="w-full border border-bg-border rounded-lg bg-bg-card/50 relative z-0 p-4">
      <div className="flex flex-col items-start w-full">
        {stages.map((stage, index) => (
          <React.Fragment key={stage.id}>
            {/* Stage Row - Bubble + Name */}
            <div className="flex items-start w-full" style={{ gap: '12.8px', paddingTop: '9.6px', paddingBottom: '9.6px' }}>
              {/* Bubble */}
              <div className="flex-shrink-0">
                <HorizontalBubbleNode
                  stage={stage}
                  index={index}
                  onClick={onStageClick ? () => onStageClick(stage) : undefined}
                  compact={true}
                />
              </div>
              
              {/* Stage Name - Left aligned */}
              <div className="flex-1">
                <p style={{ fontSize: '11px' }} className="font-medium text-text-secondary leading-snug break-words">
                  {stage.name.replace(/Stage \d+ — /, '')}
                </p>
                {stage.status && (
                  <p style={{ fontSize: '9.6px', marginTop: '3.2px' }} className="text-text-tertiary">
                    Status: <span className="text-text-secondary">{stage.status}</span>
                  </p>
                )}
              </div>
            </div>

            {/* Connector between stages - Vertical line */}
            {index < stages.length - 1 && (
              <div className="flex justify-center w-full relative" style={{ minHeight: '20px' }}>
                <VerticalConnector
                  upstream={stage}
                  downstream={stages[index + 1]}
                />
              </div>
            )}
          </React.Fragment>
        ))}
      </div>
    </div>
  )
}

/**
 * HorizontalBubbleNode — a circular bubble representing a single pipeline stage.
 */
function HorizontalBubbleNode({ stage, index, onClick, compact = false }) {
  const status = String(stage.status || stage.state || '').toUpperCase()
  const isRunning = status === 'RUNNING' || status === 'SUBMITTED'
  const isHitlWait = status === 'HITL_WAIT' || status === 'PAUSED_FOR_HITL'
  const isFailed = status === 'FAILED'
  const isCompleted = status === 'COMPLETED' || status === 'SUCCESS'
  const isPending = status === 'PENDING'

  let bubbleClass = 'border-2 border-bg-border bg-bg-card'
  let iconColor = 'text-text-tertiary'
  let glowClass = ''

  if (isRunning) {
    bubbleClass = 'border-2 border-accent-blue/50 bg-blue-950/30'
    iconColor = 'text-accent-blue'
    glowClass = 'glow-blue'
  } else if (isHitlWait) {
    bubbleClass = 'border-2 border-accent-amber/50 bg-amber-950/20'
    iconColor = 'text-accent-amber'
    glowClass = 'glow-amber'
  } else if (isFailed) {
    bubbleClass = 'border-2 border-accent-red/50 bg-red-950/20'
    iconColor = 'text-accent-red'
    glowClass = 'glow-red'
  } else if (isCompleted) {
    bubbleClass = 'border-2 border-accent-green/50 bg-accent-green/20'
    iconColor = 'text-text-primary'
  }

  const bubbleSize = compact ? 'w-8 h-8' : 'w-9.6 h-9.6'

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.8 }}
      animate={{ opacity: 1, scale: 1 }}
      transition={{ duration: 0.3 }}
      onClick={onClick}
      style={{ width: compact ? '32px' : '38.4px', height: compact ? '32px' : '38.4px' }}
      className={`
        relative flex-shrink-0 flex items-center justify-center rounded-full transition-all duration-300
        ${bubbleClass} ${glowClass}
        ${onClick ? 'cursor-pointer hover:scale-110' : ''}
        ${isPending ? 'opacity-50' : 'opacity-100'}
      `}
      title={stage.name}
      whileHover={onClick ? { scale: 1.1 } : {}}
    >
      {/* Running pulse border animation */}
      {isRunning && (
        <motion.div
          className="absolute inset-0 rounded-full border-2 border-accent-blue/40 pointer-events-none"
          animate={{ opacity: [0.4, 1, 0.4], scale: [1, 1.1, 1] }}
          transition={{ duration: 2, repeat: Infinity, ease: 'easeInOut' }}
        />
      )}

      {/* HITL pulse */}
      {isHitlWait && (
        <motion.div
          className="absolute inset-0 rounded-full border-2 border-accent-amber/40 pointer-events-none"
          animate={{ opacity: [0.3, 0.8, 0.3], scale: [1, 1.1, 1] }}
          transition={{ duration: 1.8, repeat: Infinity, ease: 'easeInOut' }}
        />
      )}

      <div className="flex items-center justify-center z-10">
        {isRunning ? (
          <Loader2 size={compact ? 14 : 18} className={`animate-spin ${iconColor}`} />
        ) : isFailed ? (
          <AlertCircle size={compact ? 14 : 18} className={iconColor} />
        ) : isHitlWait ? (
          <Users size={compact ? 14 : 18} className={iconColor} />
        ) : isPending ? (
          <Clock size={compact ? 14 : 18} className={iconColor} />
        ) : isCompleted ? (
          <CheckCircle2 size={compact ? 14 : 18} className={iconColor} />
        ) : (
          <span style={{ fontSize: '13.6px' }} className={`font-semibold ${iconColor}`}>
            {index + 1}
          </span>
        )}
      </div>
    </motion.div>
  )
}

/**
 * HorizontalConnector — horizontal line connecting two stage bubbles.
 */
function HorizontalConnector({ upstream, downstream, compact }) {
  const upstreamStatus = String(upstream.status || '').toUpperCase()
  const isRunning = upstreamStatus === 'RUNNING'
  const isHitlWait = upstreamStatus === 'HITL_WAIT' || upstreamStatus === 'PAUSED_FOR_HITL'
  const isActive = upstreamStatus === 'COMPLETED' || upstreamStatus === 'RUNNING'

  return (
    <div 
      className="flex items-center justify-center" 
      style={{ width: '100%', height: '1.6px', overflow: 'hidden', position: 'relative' }}
    >
      {isRunning ? (
        // Animated dashed connector
        <svg width="100%" height="1.6" className="overflow-visible" style={{ display: 'block', position: 'absolute', left: 0, top: 0 }} preserveAspectRatio="none">
          <line
            x1="0"
            y1="0.8"
            x2="100%"
            y2="0.8"
            stroke="#3b82f6"
            strokeWidth="1.6"
            strokeDasharray="3.2 2.4"
            strokeLinecap="round"
            className="dag-connector-animated"
          />
        </svg>
      ) : isHitlWait ? (
        <svg width="100%" height="1.6" className="overflow-visible" style={{ display: 'block', position: 'absolute', left: 0, top: 0 }} preserveAspectRatio="none">
          <line
            x1="0"
            y1="0.8"
            x2="100%"
            y2="0.8"
            stroke="#f59e0b"
            strokeWidth="1.6"
            strokeDasharray="2.4 2.4"
            strokeLinecap="round"
            className="dag-connector-animated"
          />
        </svg>
      ) : isActive ? (
        // Solid connector with gradient
        <div style={{ height: '1.6px' }} className="w-full bg-gradient-to-r from-accent-green/60 via-accent-green/40 to-bg-border" />
      ) : (
        // Inactive connector
        <div style={{ height: '1.6px' }} className="w-full bg-gray-600" />
      )}
    </div>
  )
}

/**
 * VerticalConnector — vertical line connecting two stage bubbles in vertical layout.
 */
function VerticalConnector({ upstream, downstream }) {
  const upstreamStatus = String(upstream.status || '').toUpperCase()
  const isRunning = upstreamStatus === 'RUNNING'
  const isHitlWait = upstreamStatus === 'HITL_WAIT' || upstreamStatus === 'PAUSED_FOR_HITL'
  const isActive = upstreamStatus === 'COMPLETED' || upstreamStatus === 'RUNNING'

  return (
    <div 
      className="flex items-center justify-center" 
      style={{ width: '1.6px', height: '100%', overflow: 'hidden', position: 'relative', margin: '0 auto' }}
    >
      {isRunning ? (
        // Animated dashed connector
        <svg width="1.6" height="100%" className="overflow-visible" style={{ display: 'block', position: 'absolute', top: 0, left: 0 }} preserveAspectRatio="none">
          <line
            x1="0.8"
            y1="0"
            x2="0.8"
            y2="100%"
            stroke="#3b82f6"
            strokeWidth="1.6"
            strokeDasharray="3.2 2.4"
            strokeLinecap="round"
            className="dag-connector-animated"
          />
        </svg>
      ) : isHitlWait ? (
        <svg width="1.6" height="100%" className="overflow-visible" style={{ display: 'block', position: 'absolute', top: 0, left: 0 }} preserveAspectRatio="none">
          <line
            x1="0.8"
            y1="0"
            x2="0.8"
            y2="100%"
            stroke="#f59e0b"
            strokeWidth="1.6"
            strokeDasharray="2.4 2.4"
            strokeLinecap="round"
            className="dag-connector-animated"
          />
        </svg>
      ) : isActive ? (
        // Solid connector with gradient
        <div style={{ width: '1.6px', margin: '0 auto' }} className="h-full bg-gradient-to-b from-accent-green/60 via-accent-green/40 to-bg-border" />
      ) : (
        // Inactive connector
        <div style={{ width: '1.6px', margin: '0 auto' }} className="h-full bg-gray-600" />
      )}
    </div>
  )
}

export default PipelineDag

