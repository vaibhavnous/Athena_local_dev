// @ts-nocheck
import React, { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { Outlet, useLocation, useNavigate } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import { AlertTriangle, Clock3, PlayCircle, X } from 'lucide-react'
import Sidebar from './Sidebar'
import Topbar from './Topbar'
import StageGateDialog from '../pipeline/StageGateDialog'
import useAthenaStore from '../../store/useAthenaStore'
import usePipelineSocket from '../../hooks/usePipelineSocket'
import { abortRun, continueStage, getRun, getRuns } from '../../api/athenaApi'
import { ENABLE_DEMO_FALLBACKS, getDemoRuns, isDemoFallbackRun } from '../../utils/demoFallbacks'
import { getGateDisplayName, getPhaseGroups, normalizeState, summarizeRunSource } from '../../utils/pipelinePhases'

const PAUSED_BANNER_DISMISSALS_KEY = 'athena.pausedBannerDismissals'
const PAUSED_BANNER_DELAY_MS = 2500
const REVIEW_READY_NOTIFICATIONS_KEY = 'athena.reviewReadyNotifications'
const REVIEW_READY_NOTIFICATION_DELAY_MS = 3000
const RUNS_POLL_SUCCESS_MS = 10000
const RUNS_POLL_ERROR_BASE_MS = 15000
const RUNS_POLL_ERROR_MAX_MS = 60000
const RUNS_HYDRATION_WARN_INTERVAL_MS = 60000

function loadJsonMap(key) {
  if (typeof window === 'undefined') return {}
  try {
    const raw = window.localStorage.getItem(key)
    const parsed = raw ? JSON.parse(raw) : {}
    return parsed && typeof parsed === 'object' ? parsed : {}
  } catch {
    return {}
  }
}

function persistJsonMap(key, value) {
  if (typeof window === 'undefined') return
  try {
    window.localStorage.setItem(key, JSON.stringify(value))
  } catch {
    // ignore localStorage errors
  }
}

/**
 * Root application shell — Topbar + Sidebar + main content area.
 * Manages the notification toast stack.
 */
function AppShell() {
  const navigate = useNavigate()
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
  const lastRunsHydrationWarningRef = useRef(0)
  const latestRunsRef = useRef(runs)
  const latestActiveRunIdRef = useRef(activeRunId)
  const demoRunsSeededRef = useRef(false)
  const demoRunsNotifiedRef = useRef(false)
  const pausedDetailKeyRef = useRef<string | null>(null)
  const reviewAutoOpenSessionRef = useRef({})
  const mainScrollRef = useRef(null)
  const [mobileSidebarOpen, setMobileSidebarOpen] = useState(false)
  const [dismissedPausedBanners, setDismissedPausedBanners] = useState(() => loadJsonMap(PAUSED_BANNER_DISMISSALS_KEY))
  const [reviewReadyNotifications, setReviewReadyNotifications] = useState(() => loadJsonMap(REVIEW_READY_NOTIFICATIONS_KEY))
  const [pausedRunDetail, setPausedRunDetail] = useState(null)
  const [readyPausedBannerKey, setReadyPausedBannerKey] = useState<string | null>(null)
  const [verifiedPausedBannerKey, setVerifiedPausedBannerKey] = useState<string | null>(null)
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
          setServerOnline(false)
          const now = Date.now()
          if (now - lastRunsHydrationWarningRef.current > RUNS_HYDRATION_WARN_INTERVAL_MS) {
            lastRunsHydrationWarningRef.current = now
            console.warn('[AppShell] Failed to hydrate backend runs; keeping last known UI state', error)
          }

          const hasAnyRuns = latestRunsRef.current.length > 0
          const hasOnlyFallbackRuns =
            hasAnyRuns && latestRunsRef.current.every((run) => isDemoFallbackRun(run))

          if (ENABLE_DEMO_FALLBACKS && hasOnlyFallbackRuns) {
            const demoRuns = getDemoRuns()
            setRuns(demoRuns)
          } else if (ENABLE_DEMO_FALLBACKS && !hasAnyRuns && !demoRunsSeededRef.current) {
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

  const pausedRun = useMemo(() => {
    if (!activeRunId) return null
    const activeRun = activeRunId ? (runs || []).find((run) => run.id === activeRunId) : null
    if (isReviewPausedRun(activeRun)) return activeRun
    return null
  }, [activeRunId, runs])

  const pausedRunId = pausedRun?.id || null
  const pausedRunGate = Number(pausedRun?.next_gate || 0)
  const pausedRunReviewKey = pausedRun?.next_review_key || ''
  const pausedBannerKey = pausedRunId && (pausedRunGate || pausedRunReviewKey)
    ? `${pausedRunId}:${pausedRunReviewKey || pausedRunGate}`
    : null

  useEffect(() => {
    if (!pausedRunId || !pausedBannerKey) {
      pausedDetailKeyRef.current = null
      setPausedRunDetail(null)
      setReadyPausedBannerKey(null)
      setVerifiedPausedBannerKey(null)
      return
    }

    let cancelled = false
    if (pausedDetailKeyRef.current !== pausedBannerKey) {
      pausedDetailKeyRef.current = pausedBannerKey
      setPausedRunDetail(null)
      setVerifiedPausedBannerKey(null)
    }

    setPausedRunDetail(pausedRun)
    if (isDemoFallbackRun(pausedRun)) {
      setVerifiedPausedBannerKey(pausedBannerKey)
      return
    }

    const hydratePausedRun = async () => {
      try {
        const detail = await getRun(pausedRunId)
        if (cancelled) return

        const detailGate = Number(detail?.next_gate || 0)
        const detailReviewKey = detail?.next_review_key || ''
        const expectedGate = pausedRunGate
        const expectedReviewKey = pausedRunReviewKey
        const expectedGateKey = expectedReviewKey === 'gold_review' ? 'gold_code_execution' : expectedReviewKey || (
          expectedGate === 1 ? 'gate1' :
          expectedGate === 2 ? 'gate2' :
          expectedGate === 3 ? 'gate3' :
          expectedGate === 4 ? 'gate4' :
          expectedGate === 5 ? 'gate5' :
          null
        )

        const detailSteps = [
          ...(detail?.pipeline_steps || []),
          ...(detail?.stages || []).map((stage) => ({
            key: stage?.key,
            state: stage?.state || stage?.status,
          })),
        ]
        const status = normalizeState(detail?.status)
        const resolvedReviewKey = detailReviewKey || expectedReviewKey
        const resolvedGate = detailGate || expectedGate
        const gateStepReady = expectedGateKey
          ? detailSteps.some(
              (step) => step?.key === expectedGateKey && ['HITL_WAIT', 'PAUSED_FOR_HITL', 'PENDING_REVIEW'].includes(normalizeState(step?.state))
            ) || (
              resolvedGate === expectedGate &&
              ['HITL_WAIT', 'PAUSED_FOR_HITL', 'PENDING_REVIEW'].includes(status)
            )
          : false

        const reviewPaused = isReviewPausedRun(detail) || (
          ['HITL_WAIT', 'PAUSED_FOR_HITL', 'PENDING_REVIEW'].includes(status) &&
          Boolean(resolvedReviewKey || resolvedGate)
        )
        const gateMatches = expectedReviewKey
          ? resolvedReviewKey === expectedReviewKey
          : resolvedGate === expectedGate

        if (gateMatches && reviewPaused && gateStepReady) {
          setPausedRunDetail(detail)
          setVerifiedPausedBannerKey(pausedBannerKey)
        }
      } catch (error) {
        if (!cancelled) {
          console.warn('[AppShell] Failed to hydrate paused run detail', error)
        }
      }
    }

    hydratePausedRun()
    return () => {
      cancelled = true
    }
  }, [pausedBannerKey, pausedRun, pausedRunGate, pausedRunId, pausedRunReviewKey])

  useEffect(() => {
    if (!pausedRunDetail || !pausedBannerKey || verifiedPausedBannerKey !== pausedBannerKey) {
      setReadyPausedBannerKey(null)
      return
    }

    const timer = window.setTimeout(() => {
      setReadyPausedBannerKey(pausedBannerKey)
    }, PAUSED_BANNER_DELAY_MS)

    return () => {
      window.clearTimeout(timer)
    }
  }, [pausedBannerKey, pausedRunDetail, verifiedPausedBannerKey])

  const pausedRunSummary = useMemo(() => {
    const bannerRun = pausedRunDetail || pausedRun
    if (!bannerRun) return null
    const gate = Number(bannerRun.next_gate || 0)
    const reviewKey = bannerRun.next_review_key || ''
    const phases = getPhaseGroups(bannerRun)
    const total = phases.reduce((sum, phase) => sum + (phase.total || 0), 0)
    const completed = phases.reduce((sum, phase) => sum + (phase.completed || 0), 0)
    return {
      gate,
      reviewKey,
      gateLabel: reviewKey === 'silver_merge_key_review'
        ? 'Silver Merge Key Review'
        : reviewKey === 'compliance_review'
          ? 'Compliance Review'
        : reviewKey === 'gold_review'
          ? 'Gold Code Review'
          : getGateDisplayName(gate, bannerRun.source),
      progressLabel: total > 0 ? `${completed}/${total} stages done` : 'Pipeline paused',
      timeAgo: formatTimeAgo(bannerRun.updated_at || bannerRun.started_at || bannerRun.created_at),
      resumeMessage: bannerRun.resume_message || 'Pipeline progress is saved. Resume review when you are ready.',
    }
  }, [pausedRun, pausedRunDetail])
  const pausedGateLabel = pausedRunSummary?.gateLabel || ''
  const pausedResumeMessage = pausedRunSummary?.resumeMessage || ''
  const isPausedBannerStillCurrent = useCallback((key) => {
    if (!key) return false
    return latestRunsRef.current.some((run) => {
      if (!isReviewPausedRun(run)) return false
      return `${run.id}:${run.next_review_key || Number(run.next_gate || 0)}` === key
    })
  }, [])

  useEffect(() => {
    const pausedKeys = (runs || [])
      .filter(isReviewPausedRun)
      .map((run) => `${run.id}:${run.next_review_key || Number(run.next_gate || 0)}`)
    if (!pausedKeys.length) return
    setDismissedPausedBanners((current) => {
      const activeKeys = new Set(pausedKeys)
      const next = Object.fromEntries(Object.entries(current).filter(([key]) => activeKeys.has(key)))
      const changed = Object.keys(next).length !== Object.keys(current).length
      if (changed) persistJsonMap(PAUSED_BANNER_DISMISSALS_KEY, next)
      return changed ? next : current
    })
    const activeKeys = new Set(pausedKeys)
    reviewAutoOpenSessionRef.current = Object.fromEntries(
      Object.entries(reviewAutoOpenSessionRef.current || {}).filter(([key]) => activeKeys.has(key))
    )
    setReviewReadyNotifications((current) => {
      const next = Object.fromEntries(Object.entries(current).filter(([key]) => activeKeys.has(key)))
      const changed = Object.keys(next).length !== Object.keys(current).length
      if (changed) persistJsonMap(REVIEW_READY_NOTIFICATIONS_KEY, next)
      return changed ? next : current
    })
  }, [runs])

  useEffect(() => {
    if (!pausedRun || !pausedRunDetail || !pausedBannerKey || !pausedRunSummary) return
    if (verifiedPausedBannerKey !== pausedBannerKey) return
    if (reviewAutoOpenSessionRef.current?.[pausedBannerKey]) return

    const timer = window.setTimeout(() => {
      if (!isPausedBannerStillCurrent(pausedBannerKey)) return
      const reviewRun = pausedRunDetail || pausedRun
      setActiveRun(reviewRun.id || reviewRun.run_id)
      navigate(reviewPathForPausedRun(reviewRun))
      addNotification({
        type: 'amber',
        title: `${pausedGateLabel} opened automatically`,
        message: pausedResumeMessage,
        duration: 5000,
      })
      reviewAutoOpenSessionRef.current = {
        ...(reviewAutoOpenSessionRef.current || {}),
        [pausedBannerKey]: true,
      }
    }, 800)

    return () => window.clearTimeout(timer)
  }, [
    addNotification,
    isPausedBannerStillCurrent,
    navigate,
    pausedBannerKey,
    pausedGateLabel,
    pausedResumeMessage,
    pausedRun,
    pausedRunDetail,
    pausedRunSummary,
    setActiveRun,
    verifiedPausedBannerKey,
  ])

  useEffect(() => {
    if (!pausedRunDetail || !pausedBannerKey || !pausedRunSummary) return
    if (readyPausedBannerKey !== pausedBannerKey) return
    if (reviewReadyNotifications[pausedBannerKey]) return

    const timer = window.setTimeout(() => {
      if (!isPausedBannerStillCurrent(pausedBannerKey)) return
      addNotification({
        type: 'amber',
        title: `${pausedGateLabel} ready for review`,
        message: pausedResumeMessage,
        duration: 6000,
      })

      setReviewReadyNotifications((current) => {
        if (current[pausedBannerKey]) return current
        const next = { ...current, [pausedBannerKey]: true }
        persistJsonMap(REVIEW_READY_NOTIFICATIONS_KEY, next)
        return next
      })
    }, REVIEW_READY_NOTIFICATION_DELAY_MS)

    return () => window.clearTimeout(timer)
  }, [
    addNotification,
    isPausedBannerStillCurrent,
    pausedBannerKey,
    pausedRunDetail,
    pausedGateLabel,
    pausedResumeMessage,
    pausedRunSummary,
    readyPausedBannerKey,
    reviewReadyNotifications,
  ])

  const isPausedBannerVisible = Boolean(
    pausedRun &&
    pausedRunDetail &&
    pausedBannerKey &&
    readyPausedBannerKey === pausedBannerKey &&
    !dismissedPausedBanners[pausedBannerKey]
  )

  const dismissPausedBanner = () => {
    if (!pausedBannerKey) return
    setDismissedPausedBanners((current) => {
      const next = { ...current, [pausedBannerKey]: true }
      persistJsonMap(PAUSED_BANNER_DISMISSALS_KEY, next)
      return next
    })
  }

  const handleResumePausedRun = () => {
    if (!pausedRun) return
    setActiveRun(pausedRun.id)
    navigate(reviewPathForPausedRun(pausedRun))
  }

  const activeRun = activeRunId ? runs.find((run) => run.id === activeRunId) : null
  const stageConfirmation = activeRun?.stage_confirmation
  const stageGateOpen = Boolean(
    activeRun &&
    normalizeState(activeRun.status) === 'PAUSED_FOR_STAGE_CONFIRMATION' &&
    stageConfirmation?.awaiting_confirmation
  )

  const handleStageGateContinue = async (autoAdvance) => {
    if (!activeRunId) return
    setStageGateBusy(true)
    try {
      await continueStage(activeRunId, autoAdvance)
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
          {isPausedBannerVisible && pausedRun && pausedRunSummary && (
            <div className="mb-5 rounded-xl border border-amber-500/40 bg-[#19171d] px-4 py-3 shadow-[0_12px_40px_rgba(0,0,0,0.18)]">
              <div className="flex flex-wrap items-center justify-between gap-4">
                <div className="flex min-w-0 items-center gap-4">
                  <div className="flex h-9 w-9 flex-shrink-0 items-center justify-center rounded-lg border border-amber-500/30 bg-amber-500/10 text-amber-400">
                    <AlertTriangle size={16} />
                  </div>
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-3 text-sm">
                      <div className="truncate font-semibold text-white">
                        {summarizeRunSource(pausedRun)}
                      </div>
                      <span className="rounded-lg border border-amber-500/35 bg-amber-500/12 px-2.5 py-1 text-[11px] font-semibold text-amber-300">
                        {pausedRunSummary.gateLabel} Pending
                      </span>
                      <span className="text-[#d4d9e5]">{pausedRunSummary.progressLabel}</span>
                      <span className="flex items-center gap-1 text-[#9da7bb]">
                        <Clock3 size={13} />
                        {pausedRunSummary.timeAgo}
                      </span>
                    </div>
                    <div className="mt-1 text-xs text-slate-400">
                      {pausedRunSummary.resumeMessage} Progress is saved automatically.
                    </div>
                  </div>
                </div>

                <div className="flex flex-wrap items-center gap-3">
                  <button
                    onClick={handleResumePausedRun}
                    className="inline-flex h-10 items-center gap-2 rounded-lg border border-[#2f5fb2] bg-[#102144] px-4 text-sm font-semibold text-[#9fc0ff] transition-colors hover:bg-[#14305f]"
                  >
                    <PlayCircle size={15} />
                    Resume {pausedRunSummary.gateLabel}
                  </button>
                  <button
                    onClick={dismissPausedBanner}
                    className="inline-flex h-9 w-9 items-center justify-center rounded-lg text-slate-400 transition-colors hover:bg-white/5 hover:text-white"
                    title="Dismiss paused pipeline banner"
                  >
                    <X size={16} />
                  </button>
                </div>
              </div>
            </div>
          )}
          <Outlet />
        </main>
      </motion.div>

      <StageGateDialog
        isOpen={stageGateOpen}
        completedStage={{ name: stageConfirmation?.last_completed_stage_label }}
        nextStage={{ name: stageConfirmation?.next_stage_label }}
        onContinue={handleStageGateContinue}
        onCancel={handleStageGateCancel}
        busy={stageGateBusy}
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

function formatTimeAgo(value) {
  if (!value) return 'just now'
  const diff = Date.now() - new Date(value).getTime()
  const seconds = Math.max(0, Math.floor(diff / 1000))
  const minutes = Math.floor(seconds / 60)
  const hours = Math.floor(minutes / 60)
  const days = Math.floor(hours / 24)
  if (days > 0) return `${days}d ago`
  if (hours > 0) return `${hours}h ago`
  if (minutes > 0) return `${minutes}m ago`
  return 'just now'
}

function hasReviewGate(run) {
  if (run?.next_review_key) return true
  const gate = Number(run?.next_gate || 0)
  return gate >= 1 && gate <= 5
}

function hasRenderableRunDetail(run) {
  return Boolean(
    (Array.isArray(run?.stages) && run.stages.length > 0) ||
    (Array.isArray(run?.pipeline_steps) && run.pipeline_steps.length > 0) ||
    run?.stage_confirmation ||
    Number(run?.next_gate || 0) > 0 ||
    run?.next_review_key ||
    run?.bronze ||
    run?.silver ||
    run?.gold
  )
}

function isSuppressedInitialReviewRun(run) {
  const runId = String(run?.id || run?.run_id || '')
  return (
    Number(run?.next_gate || 0) === 1 &&
    (
      runId === 'run_a3f8c2' ||
      isDemoFallbackRun(run) ||
      Boolean(run?.demo_review_fallback) ||
      String(run?.review_fallback_reason || '').toLowerCase().includes('fallback')
    )
  )
}

function isReviewPausedRun(run) {
  if (isSuppressedInitialReviewRun(run)) return false
  const status = normalizeState(run?.status)
  const reviewStatuses = ['HITL_WAIT', 'PAUSED_FOR_HITL', 'PENDING_REVIEW']
  return (
    hasRenderableRunDetail(run) &&
    hasReviewGate(run) &&
    !run?.stage_confirmation?.awaiting_confirmation &&
    status !== 'PAUSED_FOR_STAGE_CONFIRMATION' &&
    reviewStatuses.includes(status)
  )
}

function reviewPathForPausedRun(run) {
  const runId = encodeURIComponent(run.id || run.run_id)
  if (run?.next_review_key === 'compliance_review') {
    return `/app/compliance-governance?runId=${runId}`
  }
  if (run?.next_review_key) {
    return `/app/hitl?runId=${runId}&review=${encodeURIComponent(run.next_review_key)}`
  }
  return `/app/hitl?runId=${runId}&gate=${Number(run.next_gate || 0)}`
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

