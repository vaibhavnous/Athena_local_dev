// @ts-nocheck
import React, { useEffect, useMemo, useRef, useState } from 'react'
import { Outlet } from 'react-router-dom'
import { useNavigate } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import { AlertTriangle, Clock3, PlayCircle, RotateCcw, X } from 'lucide-react'
import Sidebar from './Sidebar'
import Topbar from './Topbar'
import useAthenaStore from '../../store/useAthenaStore'
import usePipelineSocket from '../../hooks/usePipelineSocket'
import {
  fetchKpiReviews,
  getBronzeReview,
  getEnrichmentReviews,
  getPipelineKpis,
  getRun,
  getRuns,
  getSilverReview,
  getTableReviews
} from '../../api/athenaApi'
import { getGateDisplayName, getPhaseGroups } from '../../utils/pipelinePhases'

const PAUSED_BANNER_DISMISSALS_KEY = 'athena.pausedBannerDismissals'
const PAUSED_BANNER_DELAY_MS = 2500

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
  const pausedDetailKeyRef = useRef<string | null>(null)
  const announcedPausedBannerKeysRef = useRef<Record<string, true>>({})
  const [dismissedPausedBanners, setDismissedPausedBanners] = useState(() => loadJsonMap(PAUSED_BANNER_DISMISSALS_KEY))
  const [pausedRunDetail, setPausedRunDetail] = useState(null)
  const [readyPausedBannerKey, setReadyPausedBannerKey] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    let timer: number | null = null

    const scheduleNext = () => {
      if (!cancelled) {
        timer = window.setTimeout(loadRuns, 5000)
      }
    }

    const loadRuns = async () => {
      if (runsRequestInFlightRef.current) {
        scheduleNext()
        return
      }

      runsRequestInFlightRef.current = true
      try {
        const backendRuns = await getRuns()
        if (cancelled || !Array.isArray(backendRuns)) return
        setServerOnline(true)

        setRuns(backendRuns)
        if (!activeRunId && backendRuns.length > 0) {
          const resumable = backendRuns.find(isReviewPausedRun)
          setActiveRun((resumable || backendRuns[0]).id)
        }
      } catch (error) {
        if (!cancelled) {
          setServerOnline(false)
          console.warn('[AppShell] Failed to hydrate backend runs', error)
        }
      } finally {
        runsRequestInFlightRef.current = false
        scheduleNext()
      }
    }

    loadRuns()
    return () => {
      cancelled = true
      if (timer !== null) window.clearTimeout(timer)
    }
  }, [activeRunId, addNotification, setRuns, setActiveRun, setServerOnline])

  const pausedRun = useMemo(() => {
    const pausedRuns = (runs || []).filter(isReviewPausedRun)
    if (!pausedRuns.length) return null
    return pausedRuns.find((run) => run.id === activeRunId) || pausedRuns[0]
  }, [activeRunId, runs])

  const pausedRunId = pausedRun?.id || null
  const pausedRunGate = Number(pausedRun?.next_gate || 0)
  const pausedBannerKey = pausedRunId && pausedRunGate ? `${pausedRunId}:${pausedRunGate}` : null

  useEffect(() => {
    if (!pausedRunId || !pausedBannerKey) {
      pausedDetailKeyRef.current = null
      setPausedRunDetail(null)
      setReadyPausedBannerKey(null)
      return
    }

    let cancelled = false
    if (pausedDetailKeyRef.current !== pausedBannerKey) {
      pausedDetailKeyRef.current = pausedBannerKey
      setPausedRunDetail(null)
    }

    const hydratePausedRun = async () => {
      try {
        const detail = await getRun(pausedRunId)
        if (cancelled) return

        const detailGate = Number(detail?.next_gate || 0)
        const expectedGate = pausedRunGate
        const expectedGateKey =
          detailGate === 1 ? 'gate1' :
          detailGate === 2 ? 'gate2' :
          detailGate === 3 ? 'gate3' :
          detailGate === 4 ? 'gate4' :
          detailGate === 5 ? 'gate5' :
          null

        const detailSteps = [
          ...(detail?.pipeline_steps || []),
          ...(detail?.stages || []).map((stage) => ({
            key: stage?.key,
            state: stage?.state || stage?.status,
          })),
        ]
        const gateStepReady = expectedGateKey
          ? detailSteps.some(
              (step) => step?.key === expectedGateKey && String(step?.state || '').toUpperCase() === 'HITL_WAIT'
            )
          : false
        const reviewDataReady = isReviewPausedRun(detail) && gateStepReady
          ? await isReviewDataReadyForGate(detail.id || pausedRunId, detailGate, detail?.source)
          : false

        if (detailGate === expectedGate && gateStepReady && reviewDataReady) {
          setPausedRunDetail(detail)
        } else {
          setPausedRunDetail((current) => (current && `${current.id}:${Number(current.next_gate || 0)}` === pausedBannerKey ? current : null))
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
  }, [pausedBannerKey, pausedRunGate, pausedRunId])

  useEffect(() => {
    if (!pausedRunDetail || !pausedBannerKey) {
      setReadyPausedBannerKey(null)
      return
    }

    const timer = window.setTimeout(() => {
      setReadyPausedBannerKey(pausedBannerKey)
    }, PAUSED_BANNER_DELAY_MS)

    return () => {
      window.clearTimeout(timer)
    }
  }, [pausedBannerKey, pausedRunDetail])

  const pausedRunSummary = useMemo(() => {
    const bannerRun = pausedRunDetail || pausedRun
    if (!bannerRun) return null
    const gate = Number(bannerRun.next_gate || 0)
    const phases = getPhaseGroups(bannerRun)
    const total = phases.reduce((sum, phase) => sum + (phase.total || 0), 0)
    const completed = phases.reduce((sum, phase) => sum + (phase.completed || 0), 0)
    return {
      gate,
      gateLabel: getGateDisplayName(gate, bannerRun.source),
      progressLabel: total > 0 ? `${completed}/${total} stages done` : 'Pipeline paused',
      timeAgo: formatTimeAgo(bannerRun.updated_at || bannerRun.started_at || bannerRun.created_at),
      resumeMessage: bannerRun.resume_message || 'Pipeline progress is saved. Resume review when you are ready.',
    }
  }, [pausedRun, pausedRunDetail])

  useEffect(() => {
    const pausedKeys = (runs || [])
      .filter(isReviewPausedRun)
      .map((run) => `${run.id}:${Number(run.next_gate || 0)}`)
    if (!pausedKeys.length) return
    setDismissedPausedBanners((current) => {
      const activeKeys = new Set(pausedKeys)
      const next = Object.fromEntries(Object.entries(current).filter(([key]) => activeKeys.has(key)))
      const changed = Object.keys(next).length !== Object.keys(current).length
      if (changed) persistJsonMap(PAUSED_BANNER_DISMISSALS_KEY, next)
      return changed ? next : current
    })
  }, [runs])

  useEffect(() => {
    if (!pausedRun || !pausedRunSummary || !pausedBannerKey) return
    if (readyPausedBannerKey !== pausedBannerKey) return
    if (dismissedPausedBanners[pausedBannerKey]) return
    if (announcedPausedBannerKeysRef.current[pausedBannerKey]) return

    announcedPausedBannerKeysRef.current[pausedBannerKey] = true
    addNotification({
      type: 'amber',
      title: `${pausedRunSummary.gateLabel} Ready`,
      message: `${pausedRun.brd_filename || pausedRun.id} is waiting for review.`,
      duration: 4500,
    })
  }, [addNotification, dismissedPausedBanners, pausedBannerKey, pausedRun, pausedRunSummary, readyPausedBannerKey])

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
    navigate('/app/hitl')
  }

  const handleRestartPausedRun = () => {
    if (!pausedRun) return
    window.dispatchEvent(new CustomEvent('athena:new-run', { detail: { seedRun: pausedRun } }))
  }

  return (
    <div className="flex h-screen w-screen overflow-hidden bg-[#080e1d] text-text-primary">
      {/* Sidebar */}
      <Sidebar collapsed={sidebarCollapsed} onToggle={toggleSidebar} />

      {/* Main area */}
      <motion.div
        className="flex flex-col flex-1 min-w-0 overflow-hidden"
        animate={{ marginLeft: 0 }}
        transition={{ type: 'spring', stiffness: 300, damping: 30 }}
      >
        <Topbar />
        <main className="flex-1 overflow-auto bg-[#080e1d] px-7 py-4">
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
                        {pausedRun.brd_filename || pausedRun.id}
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
                    onClick={handleRestartPausedRun}
                    className="inline-flex h-10 items-center gap-2 rounded-lg border border-[#32435f] bg-[#0f172a] px-4 text-sm font-semibold text-slate-100 transition-colors hover:border-[#4b6aa1] hover:bg-[#121c31]"
                  >
                    <RotateCcw size={15} />
                    Restart
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

      {/* Toast notification stack */}
      <div className="fixed bottom-6 right-6 z-50 flex flex-col gap-2 pointer-events-none" style={{ maxWidth: 380 }}>
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
  const gate = Number(run?.next_gate || 0)
  return gate >= 1 && gate <= 5
}

function isReviewPausedRun(run) {
  const status = String(run?.status || '').toUpperCase()
  return (
    hasReviewGate(run) &&
    !run?.stage_confirmation?.awaiting_confirmation &&
    status !== 'PAUSED_FOR_STAGE_CONFIRMATION' &&
    ['HITL_WAIT', 'PAUSED_FOR_HITL'].includes(status)
  )
}

async function isReviewDataReadyForGate(runId, gate, source) {
  if (!runId || !gate) return false

  try {
    if (gate === 1) {
      const review = await fetchKpiReviews(runId)
      if (Array.isArray(review) && review.length > 0) return true
      if (Array.isArray(review?.kpis) && review.kpis.length > 0) return true

      const fallback = await getPipelineKpis(runId)
      if (Array.isArray(fallback) && fallback.length > 0) return true
      return Array.isArray(fallback?.kpis) && fallback.kpis.length > 0
    }

    if (gate === 2) {
      const review = await getTableReviews(runId)
      const isFileSource = source === 'sftp' || source === 'adls_gen2'
      if (isFileSource) {
        return Boolean(review?.candidate_feed) || Boolean((review?.candidate_feeds || []).length)
      }
      return Boolean((review?.nominated_tables || []).length)
    }

    if (gate === 3) {
      const review = await getEnrichmentReviews(runId)
      return Boolean(
        (review?.enriched_columns || []).length ||
        (review?.enriched_joins || []).length ||
        (review?.feed_semantic_summary || []).length ||
        Object.keys(review?.enriched_metadata || {}).length ||
        Object.keys(review?.semantic_counts || {}).length ||
        (review?.pii_columns || []).length ||
        (review?.join_key_columns || []).length ||
        (review?.measure_columns || []).length ||
        review?.resume_message ||
        Number(review?.next_gate || 0) === 3
      )
    }

    if (gate === 4) {
      const review = await getBronzeReview(runId)
      return Boolean((review?.bronze_review_artifact?.feeds || []).length)
    }

    if (gate === 5) {
      const review = await getSilverReview(runId)
      return Boolean((review?.silver_review_artifact?.items || []).length)
    }
  } catch (error) {
    console.warn('[AppShell] Review gate data is not ready yet', error)
  }

  return false
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

