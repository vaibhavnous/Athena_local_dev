import React, { useEffect } from 'react'
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import AppShell from './components/layout/AppShell'
import LandingPage from './pages/LandingPage'
import DashboardPage from './pages/DashboardPage'
import PipelineMonitor from './pages/PipelineMonitor'
import RunDetail from './pages/RunDetail'
import HitlQueue from './pages/HitlQueue'
import Settings from './pages/Settings'
import DatabaseConfig from './pages/DatabaseConfig'
import useThemeStore from './store/useThemeStore'

function App() {
  const theme = useThemeStore((s) => s.theme)

  useEffect(() => {
    const root = document.documentElement
    if (theme === 'light') {
      root.classList.add('light')
    } else {
      root.classList.remove('light')
    }
  }, [theme])

  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<LandingPage />} />
        <Route path="/app" element={<AppShell />}>
          <Route index element={<DashboardPage />} />
          <Route path="data-discovery" element={<PipelineMonitor />} />
          <Route path="runs/:runId" element={<RunDetail />} />
          <Route path="hitl" element={<HitlQueue />} />
          <Route path="settings" element={<Settings />} />
          <Route path="db-config" element={<DatabaseConfig />} />
          <Route path="*" element={<Navigate to="/app" replace />} />
        </Route>
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </BrowserRouter>
  )
}

export default App
