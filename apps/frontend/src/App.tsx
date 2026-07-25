import React, { useEffect } from 'react'
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import AppShell from './components/layout/AppShell'
import ProtectedRoute from './components/layout/ProtectedRoute'
import LandingPage from './pages/LandingPage'
import LoginPage from './pages/LoginPage'
import AccountManagementPage from './pages/AccountManagementPage'
import DashboardPage from './pages/DashboardPage'
import PipelineMonitor from './pages/PipelineMonitor'
import RunDetail from './pages/RunDetail'
import HitlQueue from './pages/HitlQueue'
import Settings from './pages/Settings'
import DatabaseConfig from './pages/DatabaseConfig'
import RunHistoryPage from './pages/RunHistoryPage'
import ProjectInitiation from './pages/ProjectInitiation'
import ProjectDetailsPage from './pages/ProjectDetailsPage'
import DataQuality from './pages/DataQuality'
import DataMigration from './pages/DataMigration'
import NewRunPage from './pages/NewRunPage'
import ComplianceGovernance from './pages/ComplianceGovernance'
import useThemeStore from './store/useThemeStore'
import { AuthProvider } from './context/AuthContext'

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
      <AuthProvider>
      <Routes>
        <Route path="/" element={<LandingPage />} />
        <Route path="/login" element={<LoginPage />} />
        <Route path="/app" element={<ProtectedRoute><AppShell /></ProtectedRoute>}>
          <Route index element={<DashboardPage />} />
          <Route path="data-discovery" element={<PipelineMonitor />} />
          <Route path="run-history" element={<RunHistoryPage />} />
          <Route path="runs/:runId" element={<RunDetail />} />
          <Route path="hitl" element={<HitlQueue />} />
          <Route path="project" element={<ProjectInitiation />} />
          <Route path="project/:projectId" element={<ProjectDetailsPage />} />
          <Route path="project/:projectId/new-run" element={<NewRunPage />} />
          <Route path="data-quality" element={<DataQuality />} />
          <Route path="data-migration" element={<DataMigration />} />
          <Route path="compliance-governance" element={<ComplianceGovernance />} />
          <Route path="settings" element={<Settings />} />
          <Route path="db-config" element={<ProtectedRoute requireAdmin><DatabaseConfig /></ProtectedRoute>} />
          <Route path="accounts" element={<ProtectedRoute requireAccountManager><AccountManagementPage /></ProtectedRoute>} />
          <Route path="*" element={<Navigate to="/app" replace />} />
        </Route>
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
      </AuthProvider>
    </BrowserRouter>
  )
}

export default App
