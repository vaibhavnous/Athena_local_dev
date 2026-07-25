import React, { createContext, useContext, useEffect, useMemo, useState } from 'react'
import { getCurrentUser, login as loginRequest } from '../api/athenaApi'
import {
  clearSession,
  readSession,
  writeSession,
  type AuthSession,
  type AuthUser,
} from '../auth/session'

type AuthContextValue = {
  user: AuthUser | null
  isLoading: boolean
  login: (email: string, password: string) => Promise<void>
  logout: () => void
}

const AuthContext = createContext<AuthContextValue | null>(null)

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<AuthUser | null>(() => readSession()?.user ?? null)
  const [isLoading, setIsLoading] = useState(() => Boolean(readSession()?.accessToken))

  useEffect(() => {
    const session = readSession()
    if (!session) {
      setIsLoading(false)
      return
    }

    getCurrentUser()
      .then((currentUser) => {
        writeSession({ ...session, user: currentUser })
        setUser(currentUser)
      })
      .catch(() => {
        clearSession()
        setUser(null)
      })
      .finally(() => setIsLoading(false))
  }, [])

  useEffect(() => {
    const handleUnauthorized = () => setUser(null)
    window.addEventListener('astra:unauthorized', handleUnauthorized)
    return () => window.removeEventListener('astra:unauthorized', handleUnauthorized)
  }, [])

  const login = async (email: string, password: string) => {
    const response = await loginRequest({ email, password })
    const session: AuthSession = { accessToken: response.access_token, user: response.user }
    writeSession(session)
    setUser(response.user)
  }

  const logout = () => {
    clearSession()
    setUser(null)
  }

  const value = useMemo(() => ({ user, isLoading, login, logout }), [user, isLoading])
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth() {
  const context = useContext(AuthContext)
  if (!context) throw new Error('useAuth must be used inside AuthProvider')
  return context
}
