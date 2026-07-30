import { useState, useEffect, useRef, useCallback } from 'react'
import {
  getPipelineLogs,
  getPipelineLogsSinceWithLimit,
} from '../api/athenaApi'
import useAthenaStore from '../store/useAthenaStore'
import { isTransientReadError } from '../utils/apiErrors'

export interface PipelineLog {
  log_id: string
  run_id: string
  notebook_name: string | null
  stage: string | null
  step_name: string | null
  log_level: string
  message: string
  duration_seconds: number | null
  event_type?: string | null
  logged_at: string
}

type NewLogsHandler = (logs: PipelineLog[]) => void
const REFRESH_DELAYED_MESSAGE = 'Live refresh delayed — retrying.'

export function nextLogRefreshFailure(error: any, previousFailures = 0) {
  const failureCount = previousFailures + 1
  const transient = isTransientReadError(error)
  return {
    failureCount,
    error: transient ? null : (error?.message ?? 'Fetch error'),
    warning: transient && failureCount >= 2 ? REFRESH_DELAYED_MESSAGE : null,
  }
}

function stableLogKey(log: PipelineLog) {
  return [
    log.log_id,
    log.run_id,
    log.logged_at,
    log.log_level,
    log.stage || '',
    log.step_name || '',
    log.event_type || '',
    log.message || '',
  ].join('|')
}

export function isCurrentLogRequest(requestedRunId: string, activeRunId: string | null | undefined) {
  return requestedRunId === activeRunId
}

export function usePipelineLogs(
  runId: string | null | undefined,
  isActive = true,
  onNewLogs?: NewLogsHandler,
) {
  const serverOnline = useAthenaStore((s) => s.serverOnline)
  const logIdsRef = useRef(new Set<string>())
  const isFetchingRef = useRef(false)
  const refreshFailuresRef = useRef(0)
  const onNewLogsRef = useRef<NewLogsHandler | undefined>(onNewLogs)
  const lastLogTimestampRef = useRef<string | null>(null)
  const activeRunIdRef = useRef<string | null>(runId || null)
  activeRunIdRef.current = runId || null

  const [discoveredRunId, setDiscoveredRunId] = useState<string | null>(null)
  const [isDiscovering, setIsDiscovering] = useState(false)
  const [discoveryError, setDiscoveryError] = useState<string | null>(null)

  const [logs, setLogs] = useState<PipelineLog[]>([])
  const [isLoadingLogs, setIsLoadingLogs] = useState(false)
  const [isRefreshingLogs, setIsRefreshingLogs] = useState(false)
  const [logsError, setLogsError] = useState<string | null>(null)
  const [refreshWarning, setRefreshWarning] = useState<string | null>(null)
  const [lastLogTimestamp, setLastLogTimestamp] = useState<string | null>(null)
  const [terminalLogs] = useState<{ message: string; timestamp: string }[]>([])

  useEffect(() => {
    onNewLogsRef.current = onNewLogs
  }, [onNewLogs])

  const fetchLogs = useCallback(
    async (
      targetRunId: string,
      since?: string | null,
      initialLoad = false,
    ): Promise<PipelineLog[]> => {
      if (!targetRunId || isFetchingRef.current) return []

      isFetchingRef.current = true
      if (initialLoad) {
        setIsLoadingLogs(true)
        setLogsError(null)
      } else {
        setIsRefreshingLogs(true)
      }

      try {
        const data: any = since
          ? await getPipelineLogsSinceWithLimit(targetRunId, since, 300)
          : await getPipelineLogs(targetRunId, 300)
        if (!isCurrentLogRequest(targetRunId, activeRunIdRef.current)) return []
        refreshFailuresRef.current = 0
        setLogsError(null)
        setRefreshWarning(null)
        return Array.isArray(data?.logs) ? (data.logs as PipelineLog[]) : []
      } catch (error: any) {
        if (isCurrentLogRequest(targetRunId, activeRunIdRef.current)) {
          const failure = nextLogRefreshFailure(error, refreshFailuresRef.current)
          refreshFailuresRef.current = failure.failureCount
          setLogsError(failure.error)
          setRefreshWarning(failure.warning)
        }
        return []
      } finally {
        isFetchingRef.current = false
        if (initialLoad) {
          setIsLoadingLogs(false)
        } else {
          setIsRefreshingLogs(false)
        }
      }
    },
    []
  )

  const mergeLogs = useCallback((incoming: PipelineLog[]) => {
    const newestTimestamp = incoming[incoming.length - 1]?.logged_at || null
    if (newestTimestamp) {
      lastLogTimestampRef.current = newestTimestamp
      setLastLogTimestamp(newestTimestamp)
    }

    const unique = incoming.filter((log) => !logIdsRef.current.has(stableLogKey(log)))
    if (unique.length === 0) return
    unique.forEach((log) => logIdsRef.current.add(stableLogKey(log)))
    setLogs((prev) => [...prev, ...unique])
    onNewLogsRef.current?.(unique)
  }, [])

  const startLogsPolling = useCallback(
    async (targetRunId: string, since?: string | null, initialLoad = false) => {
      try {
        const incoming = await fetchLogs(targetRunId, since, initialLoad)
        mergeLogs(incoming)
      } catch (err: any) {
        setLogsError(`Failed to fetch logs: ${err?.message}`)
      }
    },
    [fetchLogs, mergeLogs]
  )

  const stopLogsPolling = useCallback(() => {}, [])

  const initiateDiscovery = useCallback(async () => {
    if (!runId || !isActive || !serverOnline) return

    setIsDiscovering(true)
    setDiscoveryError(null)

    try {
      setDiscoveredRunId(runId)
      await startLogsPolling(runId, null, true)
    } catch (error: any) {
      setDiscoveryError(error?.message ?? 'Failed to load logs')
    } finally {
      setIsDiscovering(false)
    }
  }, [runId, isActive, serverOnline, startLogsPolling])

  useEffect(() => {
    logIdsRef.current = new Set<string>()
    setDiscoveredRunId(null)
    setDiscoveryError(null)
    setLogs([])
    setLogsError(null)
    setRefreshWarning(null)
    refreshFailuresRef.current = 0
    lastLogTimestampRef.current = null
    setLastLogTimestamp(null)
  }, [runId])

  useEffect(() => {
    if (!runId || !isActive || !serverOnline) return

    initiateDiscovery()
  }, [runId, isActive, serverOnline, initiateDiscovery])

  useEffect(() => {
    if (!discoveredRunId || !isActive || !serverOnline) return

    let cancelled = false
    let timer: number | null = null

    const poll = async () => {
      if (cancelled || isFetchingRef.current) {
        if (!cancelled) {
          timer = window.setTimeout(poll, 2000)
        }
        return
      }

      await startLogsPolling(discoveredRunId, lastLogTimestampRef.current, false)
      if (!cancelled) {
        timer = window.setTimeout(poll, 2000)
      }
    }

    timer = window.setTimeout(poll, 2000)
    return () => {
      cancelled = true
      if (timer !== null) window.clearTimeout(timer)
    }
  }, [discoveredRunId, isActive, serverOnline, startLogsPolling])

  return {
    runId,
    discoveredRunId,
    isDiscovering,
    discoveryError,
    logs,
    isLoadingLogs,
    isRefreshingLogs,
    logsError,
    refreshWarning,
    lastLogTimestamp,
    terminalLogs,
    fetchLogs,
    startLogsPolling,
    stopLogsPolling,
    initiateDiscovery,
  }
}
