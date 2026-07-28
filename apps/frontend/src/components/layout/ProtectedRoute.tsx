import React from 'react'
import { Navigate, useLocation } from 'react-router-dom'
import { useAuth } from '../../context/AuthContext'

export default function ProtectedRoute({
  children,
  requireAdmin = false,
  requireAccountManager = false,
}: {
  children: React.ReactNode
  requireAdmin?: boolean
  requireAccountManager?: boolean
}) {
  const { user, isLoading } = useAuth()
  const location = useLocation()

  if (isLoading) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-[#080e1d] text-sm text-slate-400">
        Restoring your session...
      </div>
    )
  }
  if (!user) {
    const next = `${location.pathname}${location.search}`
    return <Navigate to={`/login?next=${encodeURIComponent(next)}`} replace />
  }
  if (requireAccountManager && !user.canManageAccounts) {
    return <Navigate to="/app" replace />
  }
  if (requireAdmin && user.userType !== 'Admin') {
    return <Navigate to="/app" replace />
  }
  return <>{children}</>
}
