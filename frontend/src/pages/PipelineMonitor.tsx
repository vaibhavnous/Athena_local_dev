// @ts-nocheck
import React, { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { X, Play, Square } from 'lucide-react'
import useAthenaStore from '../store/useAthenaStore'
import PhasedPipelineDag from '../components/pipeline/PhasedPipelineDag'
import StageNode from '../components/pipeline/StageNode'
import PipelineLogsPanel from '../components/pipeline/PipelineLogsPanel'
import { abortRun, retryFailedStage } from '../api/athenaApi'
import { PageHeader } from '../components/shared/DashboardLayout'

function PipelineMonitor() {
  const {
    runs,
    activeRunId,
    updateRun,
    addNotification,
  } = useAthenaStore()

  const [selectedStage, setSelectedStage] = useState(null)
  const [cancelling, setCancelling] = useState(false)
  const [cancelError, setCancelError] = useState(null)
  const handleCancel = async () => {
    if (!activeRun?.id) return
    setCancelling(true)
    setCancelError(null)
    try {
      const result = await abortRun(activeRun.id)
      updateRun(activeRun.id, result?.run || result || { status: 'CANCELLED' })
      addNotification({ type: 'info', title: 'Run Cancelled', message: `Pipeline run cancelled.`, duration: 4000 })
    } catch (err) {
      setCancelError(err.message || 'Failed to cancel run')
    } finally {
      setCancelling(false)
    }
  }
  const activeRun = activeRunId
    ? runs.find((r) => r.id === activeRunId) || null
    : null
  const activeBrdFilename = activeRun?.brd_filename || activeRun?.form_params?.fileName || ''
  const activeDisplayRunId = activeRun
    ? activeRun.databricks_run_id || activeRun.run_id || activeRun.id
    : ''
  const headerDescription = activeRun ? (
    <>
      {activeBrdFilename && (
        <>
          BRD: <span className="font-semibold text-text-secondary">{activeBrdFilename}</span>
        </>
      )}
      {activeBrdFilename && activeDisplayRunId && ' '}
      {activeDisplayRunId && (
        <>
          Run ID: <span className="font-semibold text-text-secondary">{activeDisplayRunId}</span>
        </>
      )}
    </>
  ) : 'Start a new run to monitor pipeline stages and streaming logs.'

  const handleStageClick = (stage) => {
    setSelectedStage(stage)
  }

  const handleRetryStage = async () => {
    if (!activeRun?.id) return
    try {
      const result = await retryFailedStage(activeRun.id)
      updateRun(activeRun.id, result?.run || result)
      addNotification({ type: 'success', title: 'Stage retry started', message: 'The failed stage is running again.', duration: 4000 })
    } catch (error) {
      addNotification({ type: 'error', title: 'Retry failed', message: error?.message || 'Unable to retry the stage.', duration: 4000 })
    }
  }

  return (
    <div className="flex h-full flex-col gap-3">
      <PageHeader
        eyebrow="Data Discovery"
        title="Live pipeline monitor."
        description={headerDescription}
        // icon={Play}
        actions={activeRun ? (
          <div className="flex flex-wrap items-center justify-end gap-2">
            {activeRun.status === 'RUNNING' && (
              <button
                onClick={handleCancel}
                disabled={cancelling}
                className="flex items-center gap-1.5 rounded-lg border border-accent-red/30 bg-accent-red/10 px-2.5 py-1 text-xs font-medium text-accent-red transition-colors hover:bg-accent-red/20 disabled:cursor-not-allowed disabled:opacity-50"
              >
                <Square size={11} className={cancelling ? 'animate-pulse' : ''} />
                {cancelling ? 'Cancelling...' : 'Cancel Run'}
              </button>
            )}
            {cancelError && (
              <span className="text-[11px] text-accent-red">{cancelError}</span>
            )}
          </div>
        ) : null}
        compact
      />
      <div className="flex gap-4 flex-1 min-h-0">
        <div className="flex-1 min-w-0 flex flex-col gap-4">
          {activeRun ? (
            <>
              {/* Pipeline phases + Logs side by side */}
              <div className="flex gap-4 flex-1 min-h-0">
                {/* Phases — 1/3 width, fills height */}
                <div className="w-1/3 flex-shrink-0 min-h-0">
                  <PhasedPipelineDag
                    key={activeRun.id}
                    stages={activeRun.stages || []}
                    connectionType={
                      activeRun.connectionType ||
                      activeRun.connection_type ||
                      activeRun.form_params?.connectionType
                    }
                    target={
                      activeRun.target ||
                      activeRun.target_platform ||
                      activeRun.targetPlatform ||
                      activeRun.form_params?.target ||
                      activeRun.form_params?.target_platform ||
                      activeRun.form_params?.targetPlatform
                    }
                    onStageClick={handleStageClick}
                    onRetry={handleRetryStage}
                  />
                </div>

                {/* Logs — remaining 2/3 */}
                <div className="flex-1 min-w-0 min-h-0">
                  <PipelineLogsPanel
                    runId={activeRun.run_id || activeRun.id}
                    isActive={activeRun.status === 'RUNNING'}
                  />
                </div>
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

      <AnimatePresence>
        {selectedStage && (
          <StageDrawer
            stage={selectedStage}
            onClose={() => setSelectedStage(null)}
          />
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
          <div className="flex items-center gap-2">
            <button onClick={onClose} className="text-gray-500 hover:text-gray-300 transition-colors">
              <X size={16} />
            </button>
          </div>
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
