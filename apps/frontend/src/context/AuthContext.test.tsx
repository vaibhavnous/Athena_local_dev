import React from 'react'
import { act, render, screen } from '@testing-library/react'
import { createDemoSession, refreshAuthSession } from '../api/athenaApi'
import { AuthProvider, useAuth } from './AuthContext'

jest.mock('../api/athenaApi', () => ({
  login: jest.fn(),
  createDemoSession: jest.fn(),
  refreshAuthSession: jest.fn(),
}))

const user = {
  uid: 'user-1',
  username: 'Test User',
  email: 'test@example.com',
  userType: 'Admin',
  isActive: true,
  canManageAccounts: true,
}

function CurrentUser() {
  const { user: currentUser, isLoading } = useAuth()
  return <div>{isLoading ? 'loading' : currentUser?.email || 'signed out'}</div>
}

beforeEach(() => {
  jest.useFakeTimers()
  jest.setSystemTime(new Date('2026-07-23T10:00:00Z'))
  window.localStorage.setItem('astra.auth.session', JSON.stringify({
    accessToken: 'existing-token',
    user,
    expiresAt: Date.now() + 60 * 60 * 1000,
  }))
  ;(refreshAuthSession as jest.Mock).mockResolvedValue({
    access_token: 'renewed-token',
    expires_in: 3600,
    user,
  })
  ;(createDemoSession as jest.Mock).mockResolvedValue({
    access_token: 'demo-session-token',
    expires_in: 3600,
    user: { ...user, email: 'demo@astra.local', userType: 'Client', canManageAccounts: false },
  })
})

afterEach(() => {
  delete process.env.REACT_APP_AUTH_MODE
  delete process.env.REACT_APP_AUTH_DEMO_MODE
  jest.useRealTimers()
  window.localStorage.clear()
  jest.clearAllMocks()
})

test('replaces any cached identity with an expiring restricted backend demo session', async () => {
  process.env.REACT_APP_AUTH_MODE = 'demo'

  render(
    <AuthProvider>
      <CurrentUser />
    </AuthProvider>,
  )

  await act(async () => {
    await Promise.resolve()
    await Promise.resolve()
  })

  expect(screen.getByText('demo@astra.local')).toBeInTheDocument()
  expect(createDemoSession).toHaveBeenCalledTimes(1)
  expect(refreshAuthSession).not.toHaveBeenCalled()
  expect(window.localStorage.getItem('astra.auth.session')).toContain('demo-session-token')
})

test('renews an active session before its token expires', async () => {
  render(
    <AuthProvider>
      <CurrentUser />
    </AuthProvider>,
  )

  await act(async () => {
    await Promise.resolve()
    await Promise.resolve()
  })
  expect(refreshAuthSession).toHaveBeenCalledTimes(1)
  expect(screen.getByText(user.email)).toBeInTheDocument()

  await act(async () => {
    jest.advanceTimersByTime(55 * 60 * 1000)
    await Promise.resolve()
  })

  expect(refreshAuthSession).toHaveBeenCalledTimes(2)
})

test('keeps the cached session when restoration hits a temporary backend error', async () => {
  ;(refreshAuthSession as jest.Mock).mockRejectedValueOnce({ status: 503 })

  render(
    <AuthProvider>
      <CurrentUser />
    </AuthProvider>,
  )

  await act(async () => {
    await Promise.resolve()
    await Promise.resolve()
  })
  expect(screen.getByText(user.email)).toBeInTheDocument()
  expect(window.localStorage.getItem('astra.auth.session')).toContain('existing-token')

  await act(async () => {
    jest.advanceTimersByTime(60 * 1000 + 1)
    await Promise.resolve()
  })
  expect(refreshAuthSession).toHaveBeenCalledTimes(2)
})
