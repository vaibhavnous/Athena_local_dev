import { useEffect, useState } from 'react'
import { getApiBaseUrl } from '../api/baseUrl'
import useAthenaStore from '../store/useAthenaStore'

const API_BASE_URL = getApiBaseUrl()

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
