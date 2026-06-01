// @ts-nocheck
import React, { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import { X, ArrowRight, Play, Table2, ShieldCheck } from 'lucide-react'
import useAthenaStore from '../store/useAthenaStore'
import PipelineDag from '../components/pipeline/PipelineDag'
import LiveMetricsBar from '../components/pipeline/LiveMetricsBar'
import StageNode from '../components/pipeline/StageNode'
import PipelineLogsPanel from '../components/pipeline/PipelineLogsPanel'

function PipelineMonitor() {
  const navigate = useNavigate()
  const {
    runs,
    activeRunId,
    setActiveRun
  } = useAthenaStore()

  const [selectedStage, setSelectedStage] = useState(null)
  const activeRun = runs.find((r) => r.id === activeRunId) || runs[0] || null
  const activeStatus = (activeRun?.status || '').toUpperCase()
  const reviewableRun = activeStatus === 'HITL_WAIT' || activeStatus === 'PAUSED_FOR_HITL'

  const handleStageClick = (stage) => {
    setSelectedStage(stage)
  }

  const describeRun = (run) => {
    if (!run) return 'Unknown run'
    if (run.source === 'sftp' || run.source === 'adls_gen2') {
      const rows = run.source_row_count ? ` · ${run.source_row_count} rows` : ''
      return `${run.brd_filename}${rows}`
    }
    return run.brd_filename || 'athena_brd.txt'
  }

  return (
    <div className="flex flex-col gap-4 h-full">
      <div className="flex gap-4 flex-1 min-h-0">
        <div className="flex-1 min-w-0 flex flex-col gap-4">
          {activeRun ? (
            <>
              <div className="flex items-center justify-between mb-1">
                <div className="min-w-0">
                  <h2 className="text-xs uppercase tracking-widest text-gray-500 font-medium">
                    Pipeline - {describeRun(activeRun)}
                  </h2>
                  <div className="mt-2 max-w-[420px]">
                    <select
                      value={activeRun.id}
                      onChange={(event) => setActiveRun(event.target.value)}
                      className="input-field text-xs"
                    >
                      {runs.map((run) => (
                        <option key={run.id} value={run.id}>
                          {run.id} | {describeRun(run)} | {run.status}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>
                <button
                  onClick={() => navigate(`/app/runs/${activeRun.id}`)}
                  className="text-xs text-accent-blue hover:underline flex items-center gap-1"
                >
                  Full Detail <ArrowRight size={11} />
                </button>
              </div>

              <div className="h-48 overflow-y-auto pr-1">
                <PipelineDag
                  stages={activeRun.stages || []}
                  onStageClick={handleStageClick}
                />
              </div>

              {reviewableRun && (activeRun?.next_gate === 2 || activeRun?.next_gate === 3) && (
                <div className="rounded-xl border border-accent-blue/30 bg-accent-blue/10 px-4 py-3 flex items-center justify-between gap-4">
                  <div className="min-w-0">
                    <div className="flex items-center gap-2 text-sm font-semibold text-white">
                      {activeRun.next_gate === 2 ? (
                        <Table2 size={15} className="text-accent-blue" />
                      ) : (
                        <ShieldCheck size={15} className="text-accent-blue" />
                      )}
                      Gate {activeRun.next_gate} ready
                    </div>
                    <p className="text-xs text-gray-300 mt-1">
                      {activeRun.resume_message || 'Review the pending gate to continue the pipeline.'}
                    </p>
                  </div>
                  <button
                    onClick={() => navigate('/app/hitl')}
                    className="flex items-center gap-2 px-3 py-2 bg-accent-blue hover:bg-blue-600 text-white text-xs font-semibold rounded-lg transition-colors"
                  >
                    Resume Review
                    <ArrowRight size={12} />
                  </button>
                </div>
              )}

              {/* Logs Panel - Below Pipeline Stages */}
              <div className="flex-1 min-h-0">
                <PipelineLogsPanel
                  runId={activeRun.run_id || activeRun.id}
                  isActive={!!activeRun}
                />
              </div>
            </>
          ) : (
            <div className="flex-1 flex flex-col items-center justify-center gap-4 text-center">
              <div className="w-16 h-16 rounded-2xl bg-bg-card border border-bg-border flex items-center justify-center">
                <Play size={24} className="text-gray-600" />
              </div>
              <div>
                <p className="text-gray-400 font-medium mb-1">No active pipeline</p>
                <p className="text-gray-600 text-sm">Start a new run using the button above.</p>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Metrics Bar */}
      {activeRun && (
        <div className="flex-shrink-0">
          <LiveMetricsBar run={activeRun} />
        </div>
      )}

      <AnimatePresence>
        {selectedStage && (
          <StageDrawer stage={selectedStage} onClose={() => setSelectedStage(null)} />
        )}
      </AnimatePresence>
    </div>
  )
}

function StageDrawer({ stage, onClose }) {
  return (
    <>
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        className="fixed inset-0 z-30"
        onClick={onClose}
      />
      <motion.div
        initial={{ x: '100%' }}
        animate={{ x: 0 }}
        exit={{ x: '100%' }}
        transition={{ type: 'spring', stiffness: 350, damping: 30 }}
        className="fixed right-0 top-0 h-full w-80 bg-bg-card border-l border-bg-border z-40 p-5 overflow-y-auto shadow-2xl"
      >
        <div className="flex items-center justify-between mb-4">
          <h3 className="font-bold text-white text-sm">Stage Details</h3>
          <button onClick={onClose} className="text-gray-500 hover:text-gray-300 transition-colors">
            <X size={16} />
          </button>
        </div>
        <StageNode stage={stage} compact={false} />

        {stage.prompt_metadata && (
          <div className="mt-4">
            <p className="text-xs uppercase tracking-wider text-gray-500 mb-2">Prompt Config</p>
            <div className="space-y-2">
              {Object.entries(stage.prompt_metadata).map(([k, v]) => (
                <div key={k} className="flex justify-between text-xs">
                  <span className="text-gray-500">{k}</span>
                  <span className="font-mono text-gray-300">{String(v)}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {stage.error && (
          <div className="mt-4 p-3 bg-red-950/30 border border-accent-red/20 rounded-lg">
            <p className="text-xs uppercase tracking-wider text-accent-red mb-1">Error</p>
            <p className="text-xs font-mono text-red-300 leading-relaxed whitespace-pre-wrap">{stage.error}</p>
          </div>
        )}
      </motion.div>
    </>
  )
}

export default PipelineMonitor

