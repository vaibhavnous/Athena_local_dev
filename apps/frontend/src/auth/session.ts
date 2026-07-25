export type UserType = 'Admin' | 'Client'

export interface AuthUser {
  uid: string
  username: string
  email: string
  userType: UserType
  isActive: boolean
  canManageAccounts: boolean
}

export interface AuthSession {
  accessToken: string
  user: AuthUser
}

const SESSION_KEY = 'astra.auth.session'

export function readSession(): AuthSession | null {
  try {
    const raw = window.localStorage.getItem(SESSION_KEY)
    if (!raw) return null
    const session = JSON.parse(raw) as AuthSession
    return session?.accessToken && session?.user?.uid ? session : null
  } catch {
    return null
  }
}

export function writeSession(session: AuthSession) {
  window.localStorage.setItem(SESSION_KEY, JSON.stringify(session))
}

export function clearSession() {
  window.localStorage.removeItem(SESSION_KEY)
}

export function getAccessToken() {
  return readSession()?.accessToken ?? null
}
