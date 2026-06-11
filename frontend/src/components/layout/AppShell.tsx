// @ts-nocheck
import React, { useEffect, useRef } from 'react'
import { Outlet } from 'react-router-dom'
import { useNavigate } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import Sidebar from './Sidebar'
import Topbar from './Topbar'
import useAthenaStore from '../../store/useAthenaStore'
import usePipelineSocket from '../../hooks/usePipelineSocket'
import { getRuns } from '../../api/athenaApi'

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
    serverOnline,
    addNotification
  } = useAthenaStore()
  usePipelineSocket()

  const lastOnlineRef = useRef<boolean | null>(null)
  const runsRequestInFlightRef = useRef(false)
  const lastAutoOpenedGateRef = useRef<string | null>(null)

  useEffect(() => {
    if (lastOnlineRef.current === serverOnline) return
    lastOnlineRef.current = serverOnline

    if (!serverOnline) {
      addNotification({
        type: 'amber',
        title: 'FastAPI offline',
        message: 'Backend API at http://localhost:8000 is not reachable. Start the server to see live runs/logs.',
        duration: 6000
      })
    }
  }, [addNotification, serverOnline])

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
          const resumable = backendRuns.find((run) => [1, 2, 3, 4, 5].includes(Number(run?.next_gate || 0)))
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

  useEffect(() => {
    const readyRuns = (runs || []).filter((run) => [1, 2, 3, 4, 5].includes(Number(run?.next_gate || 0)))
    if (!readyRuns.length) return

    const preferredRun = readyRuns.find((run) => run.id === activeRunId) || readyRuns[0]
    const gate = Number(preferredRun?.next_gate || 0)
    if (!gate) return

    const gateKey = `${preferredRun.id}:${gate}`
    if (lastAutoOpenedGateRef.current === gateKey) return

    lastAutoOpenedGateRef.current = gateKey
    setActiveRun(preferredRun.id)
    navigate('/app/hitl')
  }, [activeRunId, navigate, runs, setActiveRun])

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
        <main className="flex-1 overflow-auto bg-[#080e1d] px-7 py-7">
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

