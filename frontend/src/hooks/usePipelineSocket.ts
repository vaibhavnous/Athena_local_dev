import { useEffect, useState } from 'react'
import useAthenaStore from '../store/useAthenaStore'

// Use an explicit IPv4 loopback by default. Some Windows setups resolve
// `localhost` to IPv6 (::1) first, which will fail if the backend only binds IPv4.
const API_BASE_URL = (process.env.REACT_APP_API_BASE_URL || 'http://127.0.0.1:8000').replace(/\/$/, '')

const usePipelineSocket = () => {
  const [connected, setConnected] = useState(false)
  const [checkedAtLeastOnce, setCheckedAtLeastOnce] = useState(false)
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
          setCheckedAtLeastOnce(true)
        }
      } catch {
        if (!cancelled) {
          setConnected(false)
          setServerOnline(false)
          setCheckedAtLeastOnce(true)
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

  return { connected, checkedAtLeastOnce, socket: null }
}

export default usePipelineSocket
