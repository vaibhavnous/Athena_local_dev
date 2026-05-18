import { useState, useEffect, useRef, useCallback } from 'react'
import {
  getPipelineLogs,
  getPipelineLogsSince,
} from '../api/athenaApi'

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

export function usePipelineLogs(
  runId: string | null | undefined,
  isActive = true,
) {
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
          ? await getPipelineLogsSince(targetRunId, since)
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
    const unique = incoming.filter((log) => !logIdsRef.current.has(log.log_id))
    if (unique.length === 0) return
    unique.forEach((log) => logIdsRef.current.add(log.log_id))
    setLogs((prev) => [...prev, ...unique])
    setLastLogTimestamp(unique[unique.length - 1].logged_at)
  }, [])

  const startLogsPolling = useCallback(
    (targetRunId: string, since?: string | null, initialLoad = false) => {
      fetchLogs(targetRunId, since, initialLoad)
        .then(mergeLogs)
        .catch((err: any) => {
          setLogsError(`Failed to fetch logs: ${err?.message}`)
        })
    },
    [fetchLogs, mergeLogs]
  )

  const stopLogsPolling = useCallback(() => {}, [])

  const initiateDiscovery = useCallback(async () => {
    if (!runId || !isActive) return

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
  }, [runId, isActive, startLogsPolling])

  useEffect(() => {
    logIdsRef.current = new Set<string>()
    setDiscoveredRunId(null)
    setDiscoveryError(null)
    setLogs([])
    setLogsError(null)
    setLastLogTimestamp(null)
  }, [runId])

  useEffect(() => {
    if (!runId || !isActive) return

    initiateDiscovery()
  }, [runId, isActive, initiateDiscovery])

  useEffect(() => {
    if (!discoveredRunId || !isActive) return
    const interval = window.setInterval(() => {
      startLogsPolling(discoveredRunId, lastLogTimestamp, false)
    }, 750)
    return () => window.clearInterval(interval)
  }, [discoveredRunId, isActive, lastLogTimestamp, startLogsPolling])

  return {
    runId,
    discoveredRunId,
    isDiscovering,
    discoveryError,
    logs,
    isLoadingLogs,
    logsError,
    lastLogTimestamp,
    terminalLogs,
    fetchLogs,
    startLogsPolling,
    stopLogsPolling,
    initiateDiscovery,
  }
}
