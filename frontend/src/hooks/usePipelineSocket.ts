import { useEffect, useState } from 'react'
import useAthenaStore from '../store/useAthenaStore'

const API_BASE_URL = (process.env.REACT_APP_API_BASE_URL || 'http://localhost:8000').replace(/\/$/, '')

const usePipelineSocket = () => {
  const [connected, setConnected] = useState(false)
  const setServerOnline = useAthenaStore((s) => s.setServerOnline)

  useEffect(() => {
    let cancelled = false

    const checkHealth = async () => {
      try {
        const res = await fetch(`${API_BASE_URL}/health`)
        const online = res.ok
        if (!cancelled) {
          setConnected(online)
          setServerOnline(online)
        }
      } catch {
        if (!cancelled) {
          setConnected(false)
          setServerOnline(false)
        }
      }
    }

    checkHealth()
    const interval = window.setInterval(checkHealth, 10000)

    return () => {
      cancelled = true
      window.clearInterval(interval)
    }
  }, [setServerOnline])

  return { connected, socket: null }
}

export default usePipelineSocket
