import { useState, useEffect, useRef, useCallback } from 'react'
import {
  getPipelineLogs,
  getPipelineLogsSinceWithLimit,
} from '../api/athenaApi'
import useAthenaStore from '../store/useAthenaStore'

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

export function usePipelineLogs(
  runId: string | null | undefined,
  isActive = true,
) {
  const serverOnline = useAthenaStore((s) => s.serverOnline)
  const logIdsRef = useRef(new Set<string>())
  const isFetchingRef = useRef(false)

  const [discoveredRunId, setDiscoveredRunId] = useState<string | null>(null)
  const [isDiscovering, setIsDiscovering] = useState(false)
  const [discoveryError, setDiscoveryError] = useState<string | null>(null)

  const [logs, setLogs] = useState<PipelineLog[]>([])
  const [isLoadingLogs, setIsLoadingLogs] = useState(false)
  const [isRefreshingLogs, setIsRefreshingLogs] = useState(false)
  const [logsError, setLogsError] = useState<string | null>(null)
  const [lastLogTimestamp, setLastLogTimestamp] = useState<string | null>(null)
  const [terminalLogs] = useState<{ message: string; timestamp: string }[]>([])

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
        return Array.isArray(data?.logs) ? (data.logs as PipelineLog[]) : []
      } catch (error: any) {
        setLogsError(error?.message ?? 'Fetch error')
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
    const unique = incoming.filter((log) => !logIdsRef.current.has(stableLogKey(log)))
    if (unique.length === 0) return
    unique.forEach((log) => logIdsRef.current.add(stableLogKey(log)))
    setLogs((prev) => [...prev, ...unique])
    setLastLogTimestamp(unique[unique.length - 1].logged_at)
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

      await startLogsPolling(discoveredRunId, lastLogTimestamp, false)
      if (!cancelled) {
        timer = window.setTimeout(poll, 2000)
      }
    }

    timer = window.setTimeout(poll, 2000)
    return () => {
      cancelled = true
      if (timer !== null) window.clearTimeout(timer)
    }
  }, [discoveredRunId, isActive, lastLogTimestamp, serverOnline, startLogsPolling])

  return {
    runId,
    discoveredRunId,
    isDiscovering,
    discoveryError,
    logs,
    isLoadingLogs,
    isRefreshingLogs,
    logsError,
    lastLogTimestamp,
    terminalLogs,
    fetchLogs,
    startLogsPolling,
    stopLogsPolling,
    initiateDiscovery,
  }
}
