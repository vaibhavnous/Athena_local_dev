import React, { useState, useEffect, useRef } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { AlertCircle, X } from 'lucide-react'
import { usePipelineLogs, PipelineLog } from '../../hooks/usePipelineLogs'
import useAthenaStore from '../../store/useAthenaStore'

const LOG_LEVELS = ['ALL', 'INFO', 'WARNING', 'ERROR', 'DEBUG'] as const

function levelBadgeClass(level: string) {
  switch (level) {
    case 'ERROR':   return 'bg-red-500/20 text-red-300'
    case 'WARNING': return 'bg-yellow-500/20 text-yellow-300'
    case 'DEBUG':   return 'bg-purple-500/20 text-purple-300'
    default:        return 'bg-blue-500/20 text-blue-300'
  }
}

function eventBadgeClass(eventType?: string | null) {
  if (eventType === 'stage_start') return 'bg-emerald-500/15 text-emerald-300'
  if (eventType === 'stage_end') return 'bg-cyan-500/15 text-cyan-300'
  return 'bg-gray-500/15 text-gray-300'
}

function eventLabel(eventType?: string | null) {
  if (eventType === 'stage_start') return 'START'
  if (eventType === 'stage_end') return 'END'
  return null
}

function formatStageLabel(stage?: string | null) {
  const value = String(stage || '').trim()
  if (!value) return 'General'
  const normalized = value.replace(/_/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase())
  if (normalized === 'Sftp Source Ingestion') return 'SFTP Source Ingestion'
  if (normalized === 'Sftp Feed Discovery') return 'SFTP Feed Discovery'
  if (normalized === 'Sftp Gate1') return 'SFTP Gate 1'
  if (normalized === 'Sftp Gate2') return 'SFTP Gate 2'
  return normalized
}

interface Props {
  runId?: string | null
  isActive?: boolean
}

export default function PipelineLogsPanel({ runId, isActive = true }: Props) {
  const getRunById = useAthenaStore((s) => s.getRunById)
  const run = getRunById(runId || '')
  const { discoveredRunId, isDiscovering, discoveryError, logs, isLoadingLogs, logsError } =
    usePipelineLogs(runId, isActive)

  const [filterLevel, setFilterLevel] = useState('ALL')
  const [filterStage, setFilterStage] = useState('ALL')
  const [searchQuery, setSearchQuery] = useState('')
  const [selectedLog, setSelectedLog] = useState<PipelineLog | null>(null)

  const logsEndRef = useRef<HTMLDivElement>(null)

  // Auto-scroll on new logs
  useEffect(() => {
    if (discoveredRunId && logs.length > 0) {
      logsEndRef.current?.scrollIntoView({ behavior: 'smooth' })
    }
  }, [logs, discoveredRunId])

  // Derived data
  const stageSet = new Set(logs.map((l) => formatStageLabel(l.stage)).filter(Boolean))
  const uniqueStages = ['ALL', ...Array.from(stageSet as Set<string>)]

  const filteredLogs = logs.filter((log) => {
    if (filterLevel !== 'ALL' && log.log_level !== filterLevel) return false
    if (filterStage !== 'ALL' && formatStageLabel(log.stage) !== filterStage) return false
    if (
      searchQuery &&
      !log.message?.toLowerCase().includes(searchQuery.toLowerCase()) &&
      !log.step_name?.toLowerCase().includes(searchQuery.toLowerCase()) &&
      !formatStageLabel(log.stage).toLowerCase().includes(searchQuery.toLowerCase())
    ) {
      return false
    }
    return true
  })

  if (!isActive) return null

  return (
    <div className="w-full flex flex-col">
      {/* Panel */}
      <motion.div
        initial={{ opacity: 0, y: 16 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.2 }}
        className="w-full flex flex-col bg-bg-card border border-bg-border rounded-lg overflow-hidden flex-shrink-0"
        style={{ height: '350px', minHeight: '350px' }}
      >
        {/* Header */}
        <div className="px-4 py-3 border-b border-bg-border flex items-center justify-between bg-gradient-to-r from-bg-card to-bg-card/50">
          <div className="flex flex-col gap-1">
            <h4 className="text-sm font-semibold text-gray-100">Execution Logs</h4>
            <p className="text-xs text-gray-500">
              {run?.source === 'sftp' || run?.source === 'adls_gen2'
                ? `Live file execution for ${run?.brd_filename || runId}`
                : 'Real-time pipeline execution monitoring'}
            </p>
          </div>
          <div className="flex items-center gap-2">
            {isDiscovering && (
              <StatusBadge color="blue" pulse label="Loading..." />
            )}
            {discoveredRunId && !isDiscovering && (
              <StatusBadge color="green" label="Live" />
            )}
            {discoveryError && (
              <StatusBadge color="red" label="Error" />
            )}
            {!isDiscovering && !discoveryError && !discoveredRunId && (
              <StatusBadge color="yellow" pulse label="Waiting" />
            )}
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto flex flex-col bg-bg-card/30">
          {/* Discovering */}
          {isDiscovering && (
            <div className="flex-1 flex flex-col items-center justify-center p-8">
              <div className="flex flex-col items-center gap-4">
                <div className="relative w-12 h-12">
                  <div className="absolute inset-0 border-2 border-blue-500/20 rounded-full animate-spin border-t-blue-400" />
                </div>
                <div className="text-center">
                  <p className="text-sm font-medium text-gray-300">Discovering Run ID</p>
                  <p className="text-xs text-gray-500 mt-1">Connecting to execution logs…</p>
                </div>
              </div>
            </div>
          )}

          {/* Discovery error */}
          {discoveryError && !isDiscovering && (
            <div className="flex-1 flex flex-col items-center justify-center p-8">
              <div className="w-full max-w-md bg-red-500/10 border border-red-500/25 rounded-lg p-4">
                <div className="flex gap-3">
                  <AlertCircle size={20} className="text-red-400 flex-shrink-0 mt-0.5" />
                  <div>
                    <p className="text-sm font-semibold text-red-300">Discovery Failed</p>
                    <p className="text-xs text-red-300/70 mt-1">{discoveryError}</p>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Logs view */}
          {discoveredRunId && !isDiscovering && !discoveryError && (
            <div className="flex flex-col h-full">
              {/* Filter bar */}
              <div className="sticky top-0 px-4 py-3 border-b border-bg-border/50 bg-bg-card/50 backdrop-blur-sm">
                <div className="flex flex-wrap gap-2 items-center">
                  <input
                    type="text"
                    placeholder="Search logs..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="px-3 py-1.5 text-xs bg-bg-card/80 border border-bg-border rounded-md placeholder-gray-600 text-gray-300 focus:outline-none focus:border-blue-500/50 focus:ring-1 focus:ring-blue-500/20 flex-1 min-w-[200px]"
                  />
                  <select
                    value={filterLevel}
                    onChange={(e) => setFilterLevel(e.target.value)}
                    className="px-2.5 py-1.5 text-xs bg-bg-card/80 border border-bg-border rounded-md text-gray-300 focus:outline-none focus:border-blue-500/50"
                  >
                    {LOG_LEVELS.map((l) => (
                      <option key={l} value={l}>{l === 'ALL' ? 'All Levels' : l}</option>
                    ))}
                  </select>
                  <select
                    value={filterStage}
                    onChange={(e) => setFilterStage(e.target.value)}
                    className="px-2.5 py-1.5 text-xs bg-bg-card/80 border border-bg-border rounded-md text-gray-300 focus:outline-none focus:border-blue-500/50"
                  >
                    {uniqueStages.map((s) => (
                      <option key={s} value={s}>{s === 'ALL' ? 'All Stages' : s}</option>
                    ))}
                  </select>
                  <div className="flex items-center gap-1 text-xs text-gray-400">
                    <span className="font-medium">{filteredLogs.length}</span>
                    <span>/</span>
                    <span className="font-medium">{logs.length}</span>
                  </div>
                </div>
              </div>

              {/* Logs list */}
              <div className="flex-1 overflow-y-auto">
                {isLoadingLogs && (
                  <div className="flex items-center gap-2 px-4 py-3 bg-blue-500/10 border-b border-blue-500/20">
                    <span className="w-1.5 h-1.5 rounded-full bg-blue-400 animate-pulse" />
                    <p className="text-xs text-blue-300">Fetching logs…</p>
                  </div>
                )}

                {logsError && (
                  <div className="m-4 p-3 bg-red-500/10 border border-red-500/25 rounded-lg">
                    <p className="text-xs font-semibold text-red-300">Error loading logs</p>
                    <p className="text-xs text-red-300/70 mt-1">{logsError}</p>
                  </div>
                )}

                {logs.length === 0 && !isLoadingLogs && !logsError && (
                  <div className="flex-1 flex flex-col items-center justify-center p-8 text-center">
                    <p className="text-sm text-gray-400 font-medium">No logs yet</p>
                    <p className="text-xs text-gray-500 mt-1">Logs will appear as the pipeline executes</p>
                  </div>
                )}

                {filteredLogs.length === 0 && logs.length > 0 && !isLoadingLogs && (
                  <div className="flex-1 flex flex-col items-center justify-center p-8">
                    <p className="text-sm text-gray-400">No logs match your filters</p>
                  </div>
                )}

                {filteredLogs.length > 0 && (
                  <div className="divide-y divide-bg-border/30">
                    {filteredLogs.map((log, idx) => (
                      <div
                        key={log.log_id || idx}
                        onClick={() => setSelectedLog(log)}
                        className="px-4 py-2.5 hover:bg-bg-border/10 cursor-pointer transition-colors border-l-2 border-transparent hover:border-l-blue-500/50"
                      >
                        <div className="flex items-center gap-2 text-xs">
                          <span className="text-gray-500 font-mono flex-shrink-0">
                            {new Date(log.logged_at).toLocaleTimeString()}
                          </span>
                          <span className={`px-2 py-0.5 rounded text-xs font-semibold flex-shrink-0 ${levelBadgeClass(log.log_level)}`}>
                            {log.log_level || 'INFO'}
                          </span>
                          {eventLabel(log.event_type) && (
                            <span className={`px-2 py-0.5 rounded text-xs font-semibold flex-shrink-0 ${eventBadgeClass(log.event_type)}`}>
                              {eventLabel(log.event_type)}
                            </span>
                          )}
                          {log.stage && <span className="text-gray-300">{formatStageLabel(log.stage)}</span>}
                          {log.duration_seconds != null && (
                            <span className="text-gray-500 font-mono">{log.duration_seconds.toFixed(2)}s</span>
                          )}
                          <span className="text-gray-500">-</span>
                          <span className="text-gray-300 truncate">{log.message}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              <div ref={logsEndRef} />
            </div>
          )}
        </div>
      </motion.div>

      {/* Log detail modal */}
      <AnimatePresence>
        {selectedLog && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            onClick={() => setSelectedLog(null)}
            className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center p-4 z-50"
          >
            <motion.div
              initial={{ scale: 0.95, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0.95, opacity: 0 }}
              onClick={(e) => e.stopPropagation()}
              className="w-full max-w-2xl max-h-[80vh] overflow-y-auto bg-bg-card border border-bg-border rounded-lg shadow-2xl"
            >
              <div className="sticky top-0 px-6 py-4 border-b border-bg-border bg-gradient-to-r from-bg-card to-bg-card/50 flex items-center justify-between">
                <h2 className="text-base font-semibold text-gray-100">Log Details</h2>
                <button
                  onClick={() => setSelectedLog(null)}
                  className="p-1 hover:bg-bg-border/30 rounded-md transition-colors"
                >
                  <X size={20} className="text-gray-400" />
                </button>
              </div>

              <div className="p-6 space-y-4">
                <DetailRow label="Timestamp">
                  <span className="text-sm text-gray-200 font-mono">
                    {new Date(selectedLog.logged_at).toLocaleString()}
                  </span>
                </DetailRow>

                <DetailRow label="Level">
                  <span className={`inline-block px-3 py-1 rounded text-sm font-semibold ${levelBadgeClass(selectedLog.log_level)}`}>
                    {selectedLog.log_level || 'INFO'}
                  </span>
                </DetailRow>

                {selectedLog.stage && (
                  <DetailRow label="Stage">
                    <span className="text-sm text-gray-200">{formatStageLabel(selectedLog.stage)}</span>
                  </DetailRow>
                )}

                {selectedLog.step_name && (
                  <DetailRow label="Step">
                    <span className="text-sm text-gray-200 font-mono">{selectedLog.step_name}</span>
                  </DetailRow>
                )}

                {eventLabel(selectedLog.event_type) && (
                  <DetailRow label="Event">
                    <span className={`inline-block px-3 py-1 rounded text-sm font-semibold ${eventBadgeClass(selectedLog.event_type)}`}>
                      {eventLabel(selectedLog.event_type)}
                    </span>
                  </DetailRow>
                )}

                {selectedLog.duration_seconds != null && (
                  <DetailRow label="Duration">
                    <span className="text-sm text-gray-200 font-mono">
                      {selectedLog.duration_seconds.toFixed(2)}s
                    </span>
                  </DetailRow>
                )}

                <DetailRow label="Message">
                  <pre className="text-sm text-gray-200 whitespace-pre-wrap font-mono leading-relaxed">
                    {selectedLog.message}
                  </pre>
                </DetailRow>

                {selectedLog.notebook_name && (
                  <DetailRow label="Notebook">
                    <span className="text-sm text-gray-400 font-mono">{selectedLog.notebook_name}</span>
                  </DetailRow>
                )}
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

// ── Small helpers ─────────────────────────────────────────────────────────────

function StatusBadge({
  color,
  label,
  pulse = false,
}: {
  color: 'blue' | 'green' | 'red' | 'yellow'
  label: string
  pulse?: boolean
}) {
  const bg = {
    blue:   'bg-blue-500/15 border-blue-500/30',
    green:  'bg-green-500/15 border-green-500/30',
    red:    'bg-red-500/15 border-red-500/30',
    yellow: 'bg-yellow-500/15 border-yellow-500/30',
  }[color]
  const dot = {
    blue:   'bg-blue-400',
    green:  'bg-green-400',
    red:    'bg-red-400',
    yellow: 'bg-yellow-400',
  }[color]
  const text = {
    blue:   'text-blue-300',
    green:  'text-green-300',
    red:    'text-red-300',
    yellow: 'text-yellow-300',
  }[color]
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1.5 ${bg} border rounded-md`}>
      <span className={`w-1.5 h-1.5 rounded-full ${dot} ${pulse ? 'animate-pulse' : ''}`} />
      <span className={`text-xs font-medium ${text}`}>{label}</span>
    </span>
  )
}

function DetailRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-1">{label}</p>
      {children}
    </div>
  )
}
