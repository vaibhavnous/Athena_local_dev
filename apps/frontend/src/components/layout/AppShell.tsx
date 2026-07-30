// @ts-nocheck
import React, { useEffect, useLayoutEffect, useRef, useState } from 'react'
import { Outlet, useLocation } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import Sidebar from './Sidebar'
import Topbar from './Topbar'
import StageGateDialog from '../pipeline/StageGateDialog'
import useAthenaStore from '../../store/useAthenaStore'
import usePipelineSocket from '../../hooks/usePipelineSocket'
import { abortRun, continueStage, getRuns } from '../../api/athenaApi'
import { ENABLE_DEMO_FALLBACKS, getDemoRuns, isDemoFallbackRun } from '../../utils/demoFallbacks'
import { isSnowflakeDbtRun, normalizeState } from '../../utils/pipelinePhases'
import { isTransientReadError } from '../../utils/apiErrors'

const RUNS_POLL_SUCCESS_MS = 10000
const RUNS_POLL_ERROR_BASE_MS = 15000
const RUNS_POLL_ERROR_MAX_MS = 60000

/**
 * Root application shell — Topbar + Sidebar + main content area.
 * Manages the notification toast stack.
 */
function AppShell() {
  const location = useLocation()
  const {
    runs,
    sidebarCollapsed,
    toggleSidebar,
    notifications,
    removeNotification,
    setRuns,
    setActiveRun,
    setServerOnline,
    activeRunId,
    addNotification
  } = useAthenaStore()
  usePipelineSocket()

  const runsRequestInFlightRef = useRef(false)
  const runsHydrationFailuresRef = useRef(0)
  const latestRunsRef = useRef(runs)
  const latestActiveRunIdRef = useRef(activeRunId)
  const demoRunsSeededRef = useRef(false)
  const demoRunsNotifiedRef = useRef(false)
  const consumedStageGatesRef = useRef(new Set())
  const mainScrollRef = useRef(null)
  const [mobileSidebarOpen, setMobileSidebarOpen] = useState(false)
  const [stageGateBusy, setStageGateBusy] = useState(false)

  useLayoutEffect(() => {
    mainScrollRef.current?.scrollTo({ top: 0, left: 0, behavior: 'auto' })
    setMobileSidebarOpen(false)
  }, [location.pathname])

  useEffect(() => {
    if (!mobileSidebarOpen) return undefined

    const closeOnEscape = (event) => {
      if (event.key === 'Escape') setMobileSidebarOpen(false)
    }

    window.addEventListener('keydown', closeOnEscape)
    return () => window.removeEventListener('keydown', closeOnEscape)
  }, [mobileSidebarOpen])

  useEffect(() => {
    latestRunsRef.current = runs
  }, [runs])

  useEffect(() => {
    latestActiveRunIdRef.current = activeRunId
  }, [activeRunId])

  useEffect(() => {
    let cancelled = false
    let timer: number | null = null

    const scheduleNext = (delay = RUNS_POLL_SUCCESS_MS) => {
      if (!cancelled) {
        timer = window.setTimeout(loadRuns, delay)
      }
    }

    const loadRuns = async () => {
      if (runsRequestInFlightRef.current) {
        scheduleNext()
        return
      }

      runsRequestInFlightRef.current = true
      let nextPollDelay = RUNS_POLL_SUCCESS_MS
      try {
        const backendRuns = await getRuns()
        if (cancelled || !Array.isArray(backendRuns)) return
        setServerOnline(true)
        runsHydrationFailuresRef.current = 0
        demoRunsSeededRef.current = false

        const currentActiveRun = latestRunsRef.current.find((run) => run.id === latestActiveRunIdRef.current)
        if (isDemoFallbackRun(currentActiveRun) && backendRuns.length > 0) {
          setActiveRun(null)
        }

        setRuns(backendRuns)
      } catch (error) {
        if (!cancelled) {
          runsHydrationFailuresRef.current += 1
          const failureCount = runsHydrationFailuresRef.current
          nextPollDelay = Math.min(
            RUNS_POLL_ERROR_MAX_MS,
            RUNS_POLL_ERROR_BASE_MS * Math.max(1, failureCount)
          )
          const transient = isTransientReadError(error)
          if (!transient) setServerOnline(false)
          if (!transient && failureCount >= 2) {
            console.warn('[AppShell] Failed to hydrate backend runs; keeping last known UI state', error)
          }

          const hasAnyRuns = latestRunsRef.current.length > 0
          const hasOnlyFallbackRuns =
            hasAnyRuns && latestRunsRef.current.every((run) => isDemoFallbackRun(run))

          if (!transient && ENABLE_DEMO_FALLBACKS && hasOnlyFallbackRuns) {
            const demoRuns = getDemoRuns()
            setRuns(demoRuns)
          } else if (!transient && ENABLE_DEMO_FALLBACKS && !hasAnyRuns && !demoRunsSeededRef.current) {
            const demoRuns = getDemoRuns()
            setRuns(demoRuns)
            demoRunsSeededRef.current = true
            if (!demoRunsNotifiedRef.current) {
              demoRunsNotifiedRef.current = true
              addNotification({
                type: 'amber',
                title: 'Demo data loaded',
                message: 'Backend run hydration timed out. Showing the saved demo pipeline so the UI stays usable.',
                duration: 7000,
              })
            }
          }
        }
      } finally {
        runsRequestInFlightRef.current = false
        scheduleNext(nextPollDelay)
      }
    }

    loadRuns()
    return () => {
      cancelled = true
      if (timer !== null) window.clearTimeout(timer)
    }
  }, [activeRunId, addNotification, setRuns, setActiveRun, setServerOnline])

  const activeRun = activeRunId ? runs.find((run) => run.id === activeRunId) : null
  const stageConfirmation = activeRun?.stage_confirmation
  const executionReady = Boolean(
    stageConfirmation?.awaiting_confirmation &&
    String(stageConfirmation?.last_completed_stage_key || '').toLowerCase() === 'gold_review' &&
    ['bronze_code_execution', 'gold_code_execution'].includes(
      String(stageConfirmation?.next_stage_key || '').toLowerCase()
    )
  )
  const dbtExecutionReady = executionReady && isSnowflakeDbtRun(activeRun)
  const stageGateKey = [
    activeRun?.id,
    stageConfirmation?.last_completed_stage_key,
    stageConfirmation?.next_stage_key,
  ].filter(Boolean).join(':')
  const stageGateOpen = Boolean(
    activeRun &&
    location.pathname !== '/app/hitl' &&
    !hasReviewGate(activeRun) &&
    normalizeState(activeRun.status) === 'PAUSED_FOR_STAGE_CONFIRMATION' &&
    stageConfirmation?.awaiting_confirmation &&
    !consumedStageGatesRef.current.has(stageGateKey)
  )

  const handleStageGateContinue = async (autoAdvance) => {
    if (!activeRunId) return
    setStageGateBusy(true)
    try {
      await continueStage(activeRunId, autoAdvance)
      consumedStageGatesRef.current.add(stageGateKey)
      useAthenaStore.getState().updateRun(activeRunId, {
        status: 'PROCESSING',
        background_stage: stageConfirmation?.next_stage_key,
        stage_confirmation: null,
        resume_message: `${stageConfirmation?.next_stage_label || 'Next stage'} is starting.`,
      })
    } catch (error) {
      addNotification({ type: 'error', title: 'Unable to continue', message: error.message || 'The next stage could not be started.', duration: 5000 })
    } finally {
      setStageGateBusy(false)
    }
  }

  const handleStageGateCancel = async () => {
    if (!activeRunId) return
    setStageGateBusy(true)
    try {
      await abortRun(activeRunId)
      useAthenaStore.getState().updateRun(activeRunId, { status: 'ABORTED', stage_confirmation: null })
    } catch (error) {
      addNotification({ type: 'error', title: 'Unable to cancel', message: error.message || 'The run could not be cancelled.', duration: 5000 })
    } finally {
      setStageGateBusy(false)
    }
  }

  return (
    <div className="flex h-[100dvh] w-screen overflow-hidden bg-[#080e1d] text-text-primary">
      <div className="hidden h-full md:flex">
        <Sidebar collapsed={sidebarCollapsed} onToggle={toggleSidebar} />
      </div>

      <AnimatePresence>
        {mobileSidebarOpen && (
          <>
            <motion.button
              type="button"
              aria-label="Close navigation"
              className="fixed inset-0 z-30 bg-black/60 backdrop-blur-[2px] md:hidden"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setMobileSidebarOpen(false)}
            />
            <motion.div
              className="fixed inset-y-0 left-0 z-40 md:hidden"
              initial={{ x: '-100%' }}
              animate={{ x: 0 }}
              exit={{ x: '-100%' }}
              transition={{ type: 'spring', stiffness: 360, damping: 34 }}
            >
              <Sidebar
                collapsed={false}
                mobile
                onToggle={() => setMobileSidebarOpen(false)}
                onNavigate={() => setMobileSidebarOpen(false)}
              />
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Main area */}
      <motion.div
        className="flex flex-col flex-1 min-w-0 overflow-hidden"
        animate={{ marginLeft: 0 }}
        transition={{ type: 'spring', stiffness: 300, damping: 30 }}
      >
        <Topbar onOpenNavigation={() => setMobileSidebarOpen(true)} />
        <main ref={mainScrollRef} className="flex-1 overflow-auto bg-[#080e1d] p-3 sm:p-4">
          <Outlet />
        </main>
      </motion.div>

      <StageGateDialog
        isOpen={stageGateOpen}
        completedStage={{ name: stageConfirmation?.last_completed_stage_label }}
        nextStage={{
          name: executionReady
            ? dbtExecutionReady
              ? 'Snowflake dbt Deployment & Build'
              : 'Bronze → Silver → Gold Target Execution'
            : stageConfirmation?.next_stage_label,
        }}
        onContinue={handleStageGateContinue}
        onCancel={handleStageGateCancel}
        busy={stageGateBusy}
        title={executionReady ? 'Code Generation Complete' : 'Stage Completed'}
        prompt={executionReady
          ? dbtExecutionReady
            ? 'All dbt models are reviewed and frozen. Start source landing, deployment, and dbt build now?'
            : 'All generated code has been reviewed. Start ordered target execution now?'
          : 'Do you want to proceed to the next stage?'}
        continueLabel={executionReady ? (dbtExecutionReady ? 'Start Deployment & Build' : 'Start Execution') : 'Continue'}
        showAutoAdvance={!executionReady}
      />

      {/* Toast notification stack */}
      <div className="pointer-events-none fixed inset-x-3 bottom-3 z-50 flex flex-col gap-2 sm:inset-x-auto sm:bottom-6 sm:right-6 sm:w-full sm:max-w-[380px]">
        <AnimatePresence initial={false}>
          {notifications.map((notif) => (
            <motion.div
              key={notif.id}
              initial={{ opacity: 0, x: 60, scale: 0.95 }}
              animate={{ opacity: 1, x: 0, scale: 1 }}
              exit={{ opacity: 0, x: 60, scale: 0.95 }}
              transition={{ type: 'spring', stiffness: 400, damping: 30 }}
              className="pointer-events-auto"
            >
              <ToastCard notif={notif} onClose={() => removeNotification(notif.id)} />
            </motion.div>
          ))}
        </AnimatePresence>
      </div>
    </div>
  )
}

function hasReviewGate(run) {
  if (run?.next_review_key) return true
  const gate = Number(run?.next_gate || 0)
  return gate >= 1 && gate <= 5
}

/** Individual toast card */
function ToastCard({ notif, onClose }) {
  const colorMap = {
    info: 'border-accent-blue bg-blue-950/80',
    success: 'border-accent-green bg-emerald-950/80',
    error: 'border-accent-red bg-red-950/80',
    amber: 'border-accent-amber bg-amber-950/80',
    warning: 'border-accent-amber bg-amber-950/80'
  }

  const iconMap = {
    info: (
      <svg className="w-4 h-4 text-accent-blue flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
        <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
      </svg>
    ),
    success: (
      <svg className="w-4 h-4 text-accent-green flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
        <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
      </svg>
    ),
    error: (
      <svg className="w-4 h-4 text-accent-red flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
        <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
      </svg>
    ),
    amber: (
      <svg className="w-4 h-4 text-accent-amber flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
        <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
      </svg>
    )
  }

  const colorClass = colorMap[notif.type] || colorMap.info
  const icon = iconMap[notif.type] || iconMap.info

  return (
    <div className={`rounded-xl border backdrop-blur-sm p-4 shadow-2xl ${colorClass}`}>
      <div className="flex items-start gap-3">
        <div className="mt-0.5">{icon}</div>
        <div className="flex-1 min-w-0">
          {notif.title && (
            <p className="text-sm font-semibold text-text-primary mb-0.5">{notif.title}</p>
          )}
          <p className="text-xs text-text-secondary leading-relaxed">{notif.message}</p>
          {notif.action && (
            <a
              href={notif.action.href}
              className="text-xs font-medium text-accent-blue hover:underline mt-1 inline-block"
            >
              {notif.action.label} →
            </a>
          )}
        </div>
        <button
          onClick={onClose}
          className="text-text-tertiary hover:text-text-secondary transition-colors ml-1 flex-shrink-0"
        >
          <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z" clipRule="evenodd" />
          </svg>
        </button>
      </div>
    </div>
  )
}

export default AppShell

