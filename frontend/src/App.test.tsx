import React from 'react'
import { render, screen } from '@testing-library/react'

let mockPathname = '/'

jest.mock('./pages/LandingPage', () => ({
  __esModule: true,
  default: () => <div>Landing Mock</div>,
}))

jest.mock('./components/layout/AppShell', () => ({
  __esModule: true,
  default: () => <div>App Shell Mock</div>,
}))

jest.mock('./context/AuthContext', () => ({
  __esModule: true,
  AuthProvider: ({ children }) => <>{children}</>,
  useAuth: () => ({
    user: {
      uid: 'test-user',
      username: 'Test User',
      email: 'test@example.com',
      userType: 'Admin',
      isActive: true,
      canManageAccounts: true,
    },
    isLoading: false,
    login: jest.fn(),
    logout: jest.fn(),
  }),
}))

jest.mock('react-router-dom', () => {
  const React = require('react')
  const Route = ({ path, element, children }) => {
    const current = mockPathname
    if (path === '/') return current === '/' ? <>{element || children}</> : null
    if (path === '/app') return current.startsWith('/app') ? <>{element || children}</> : null
    return null
  }
  return {
    __esModule: true,
    BrowserRouter: ({ children }) => <>{children}</>,
    Routes: ({ children }) => <>{children}</>,
    Route,
    Navigate: () => null,
    Outlet: () => null,
    Link: ({ children }) => <>{children}</>,
    NavLink: ({ children }) => <>{children}</>,
    useNavigate: () => jest.fn(),
    useLocation: () => ({ pathname: mockPathname }),
    useParams: () => ({}),
  }
}, { virtual: true })

import App from './App'

test('renders the landing page entry point', () => {
  mockPathname = '/'
  render(<App />)
  expect(screen.getByText('Landing Mock')).toBeInTheDocument()
})

test('renders the app shell route entry point', () => {
  mockPathname = '/app'
  render(<App />)
  expect(screen.getByText('App Shell Mock')).toBeInTheDocument()
})
