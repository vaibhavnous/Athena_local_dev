import React, { createContext, useContext, useEffect, useMemo, useState } from 'react'
import { createDemoSession, login as loginRequest, refreshAuthSession } from '../api/athenaApi'
import {
  clearSession,
  readSession,
  writeSession,
  type AuthSession,
  type AuthUser,
} from '../auth/session'
import { isDemoAuthEnabled } from '../auth/demoAuth'

type AuthContextValue = {
  user: AuthUser | null
  isLoading: boolean
  login: (email: string, password: string) => Promise<void>
  logout: () => void
}

const AuthContext = createContext<AuthContextValue | null>(null)
const SESSION_REFRESH_BUFFER_MS = 5 * 60 * 1000
const SESSION_REFRESH_RETRY_MS = 60 * 1000

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const demoMode = isDemoAuthEnabled()
  const initialSession = useMemo(() => readSession(), [])
  const [user, setUser] = useState<AuthUser | null>(() => initialSession?.user ?? null)
  const [expiresAt, setExpiresAt] = useState(() => initialSession?.expiresAt ?? 0)
  const [isLoading, setIsLoading] = useState(() => demoMode || Boolean(initialSession?.accessToken))

  const storeSession = (response: Awaited<ReturnType<typeof refreshAuthSession>>) => {
    const session: AuthSession = {
      accessToken: response.access_token,
      user: response.user,
      expiresAt: Date.now() + response.expires_in * 1000,
    }
    writeSession(session)
    setUser(session.user)
    setExpiresAt(session.expiresAt || 0)
  }

  useEffect(() => {
    if (demoMode) {
      createDemoSession()
        .then(storeSession)
        .catch(() => {
          clearSession()
          setUser(null)
          setExpiresAt(0)
        })
        .finally(() => setIsLoading(false))
      return
    }

    const session = readSession()
    if (!session) {
      setIsLoading(false)
      return
    }

    refreshAuthSession()
      .then(storeSession)
      .catch((error) => {
        if (error?.status === 401) {
          clearSession()
          setUser(null)
          setExpiresAt(0)
        } else {
          setExpiresAt(Date.now() + SESSION_REFRESH_BUFFER_MS + SESSION_REFRESH_RETRY_MS)
        }
      })
      .finally(() => setIsLoading(false))
    // Session restoration only runs once; subsequent renewals are scheduled below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [demoMode])

  useEffect(() => {
    const handleUnauthorized = () => {
      setUser(null)
      setExpiresAt(0)
    }
    const handleSessionUpdated = () => {
      const session = readSession()
      if (!session) return
      setUser(session.user)
      setExpiresAt(session.expiresAt || 0)
    }
    window.addEventListener('astra:unauthorized', handleUnauthorized)
    window.addEventListener('astra:session-updated', handleSessionUpdated)
    return () => {
      window.removeEventListener('astra:unauthorized', handleUnauthorized)
      window.removeEventListener('astra:session-updated', handleSessionUpdated)
    }
  }, [])

  useEffect(() => {
    if (!user || !expiresAt) return

    const refresh = async () => {
      try {
        storeSession(await refreshAuthSession())
      } catch (error: any) {
        if (error?.status === 401) {
          clearSession()
          setUser(null)
          setExpiresAt(0)
        } else {
          setExpiresAt(Date.now() + SESSION_REFRESH_BUFFER_MS + SESSION_REFRESH_RETRY_MS)
        }
      }
    }
    const delay = Math.max(0, expiresAt - Date.now() - SESSION_REFRESH_BUFFER_MS)
    const timer = window.setTimeout(refresh, delay)
    return () => window.clearTimeout(timer)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [demoMode, expiresAt, user])

  const login = async (email: string, password: string) => {
    storeSession(await loginRequest({ email, password }))
  }

  const logout = () => {
    clearSession()
    if (demoMode) {
      setIsLoading(true)
      createDemoSession()
        .then(storeSession)
        .catch(() => {
          setUser(null)
          setExpiresAt(0)
        })
        .finally(() => setIsLoading(false))
      return
    }
    setUser(null)
    setExpiresAt(0)
  }

  const value = { user, isLoading, login, logout }
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth() {
  const context = useContext(AuthContext)
  if (!context) throw new Error('useAuth must be used inside AuthProvider')
  return context
}
