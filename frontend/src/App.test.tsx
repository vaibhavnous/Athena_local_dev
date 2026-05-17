import React from 'react'
import { render, screen } from '@testing-library/react'

jest.mock('react-router-dom', () => {
  const React = require('react')
  const Route = ({ path, element, children }) => {
    if (path && path !== '/') return null
    return <>{element || children}</>
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
    useLocation: () => ({ pathname: '/' }),
    useParams: () => ({}),
  }
}, { virtual: true })

import App from './App'

test('renders the landing page entry point', () => {
  render(<App />)
  expect(screen.getByText(/AI-Powered Data Engineering Platform/i)).toBeInTheDocument()
})
