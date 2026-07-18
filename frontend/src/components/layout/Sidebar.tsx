// @ts-nocheck
import React from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import {
  ArrowRightLeft,
  ChevronLeft,
  ChevronRight,
  Clock3,
  Database,
  Folder,
  LayoutDashboard,
  Shield,
  Sparkles,
  LogOut,
  UserPlus,
  X,
} from 'lucide-react'
import useAthenaStore from '../../store/useAthenaStore'
import { useNavigate } from 'react-router-dom'
import { useAuth } from '../../context/AuthContext'

const ASTRA_WORDMARK_SRC = `${process.env.PUBLIC_URL}/astra-wordmark-white.png`

function Sidebar({ collapsed, onToggle, onNavigate, mobile = false }) {
  const userRole = useAthenaStore((s) => s.userRole)
  const { user, logout } = useAuth()
  const navigate = useNavigate()

  const mainItems = [
    { to: '/app', icon: LayoutDashboard, label: 'Dashboard', exact: true },
    { to: '/app/project', icon: Folder, label: 'Projects' },
    { to: '/app/data-discovery', icon: Sparkles, label: 'Data Discovery' },
    { to: '/app/run-history', icon: Clock3, label: 'Run History' },
    { to: '/app/data-quality', icon: Shield, label: 'Data Quality' },
    { to: '/app/compliance-governance', icon: Shield, label: 'Compliance' },
    { to: '/app/data-migration', icon: ArrowRightLeft, label: 'Data Migration' },
  ]

  const secondaryItems = [
    ...(user?.userType === 'Admin' ? [{ to: '/app/db-config', icon: Database, label: 'Configuration' }] : []),
    ...(user?.canManageAccounts ? [{ to: '/app/accounts', icon: UserPlus, label: 'Accounts' }] : []),
  ]

  return (
    <aside
      role={mobile ? 'dialog' : undefined}
      aria-modal={mobile || undefined}
      aria-label={mobile ? 'Navigation' : undefined}
      className={`relative flex h-full flex-shrink-0 flex-col overflow-hidden border-r border-[#253044] bg-[#131c2c] transition-[width] duration-300 ${mobile ? 'w-[min(86vw,300px)] shadow-2xl' : collapsed ? 'w-16' : 'w-[240px]'}`}
    >
      <div className="flex h-[51.2px] flex-shrink-0 items-center justify-between border-b border-[#253044] px-[9.6px]">
        {collapsed ? (
          <button
            onClick={onToggle}
            className="flex w-full items-center justify-center rounded-md py-1 transition-colors hover:bg-white/5"
            title="Expand sidebar"
            aria-label="Expand sidebar"
          >
            <AstraWordmark collapsed />
          </button>
        ) : (
          <>
            <div className="flex min-w-0 items-center overflow-hidden">
              <AstraWordmark collapsed={false} />
            </div>
            <button
              onClick={onToggle}
              className="flex h-[25.6px] w-[25.6px] items-center justify-center rounded-md text-slate-400 transition-colors hover:bg-white/5 hover:text-white"
              title={mobile ? 'Close navigation' : 'Collapse sidebar'}
              aria-label={mobile ? 'Close navigation' : 'Collapse sidebar'}
            >
              {mobile ? <X size={14.4} /> : <ChevronLeft size={14.4} />}
            </button>
          </>
        )}
      </div>

      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto">
        <div>
          {!collapsed && (
            <div className="px-[12.8px] py-[9.6px]">
              <SidebarShowcase title="Databricks Edition" subtitle="Unity Catalog, Delta Lake" />
            </div>
          )}

          <nav className="py-[12.8px]" aria-label="Primary navigation">
            {mainItems.map((item) => (
              <NavItem key={item.to} item={item} collapsed={collapsed} onNavigate={onNavigate} />
            ))}
          </nav>
        </div>

        <div className="mt-auto border-t border-[#253044] px-[12.8px] py-[9.6px]">
          {!collapsed && (
            <>
              <SidebarShowcase title="Data Engineer" subtitle="Pipeline Designer & Builder" compact />

            <nav className="mt-[9.6px]" aria-label="Secondary navigation">
              {secondaryItems.map((item) => (
                <NavItem key={item.to} item={item} collapsed={collapsed} compact onNavigate={onNavigate} />
              ))}
            </nav>
            </>
          )}

          {collapsed && (
            <nav aria-label="Secondary navigation">
              {secondaryItems.map((item) => (
                <NavItem key={item.to} item={item} collapsed={collapsed} compact onNavigate={onNavigate} />
              ))}
            </nav>
          )}

          <div className={`mt-[9.6px] border-t border-[#253044] px-2 pt-[9.6px] ${collapsed ? 'hidden' : 'block'}`}>
            <div className="flex items-center justify-between gap-3">
              <div className="min-w-0">
                <div className="truncate text-[9.6px] font-semibold text-white">{user?.username}</div>
                <div className="truncate text-[8px] text-slate-400">{user?.userType || userRole || 'Pipeline Designer & Builder'}</div>
              </div>
              <button onClick={() => { logout(); navigate('/login', { replace: true }) }} className="flex h-[25.6px] w-[25.6px] items-center justify-center rounded-md text-red-300 hover:bg-red-500/10" title="Sign out" aria-label="Sign out">
                <LogOut size={11.2} />
              </button>
            </div>
          </div>
        </div>
      </div>
    </aside>
  )
}

function SidebarShowcase({ title, subtitle }) {
  return (
    <div className="rounded-lg px-2 py-1.5 transition-colors hover:bg-white/[0.04]">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-[11.2px] font-semibold leading-tight text-white">{title}</div>
          <div className="text-[8px] leading-tight text-slate-400">{subtitle}</div>
        </div>
        <ChevronRight size={11.2} className="text-slate-400" />
      </div>
    </div>
  )
}

function NavItem({ item, collapsed, compact = false, onNavigate }) {
  const location = useLocation()
  const isActive = item.exact ? location.pathname === item.to : location.pathname.startsWith(item.to)
  const Icon = item.icon

  return (
    <div className={`mb-[3.2px] ${compact ? 'px-0' : 'px-2'}`}>
      <NavLink
        to={item.to}
        onClick={onNavigate}
        title={collapsed ? item.label : undefined}
        className={`group flex h-[30.4px] items-center gap-[9.6px] rounded-lg border px-[9.6px] text-[11.2px] font-medium transition-colors ${
          isActive
            ? 'border-[#2f5fb2] bg-[#1f325d] text-[#3f82ff]'
            : 'border-transparent text-slate-100 hover:bg-white/[0.05] hover:text-white'
        } ${collapsed ? 'justify-center px-0' : ''}`}
      >
        <div className="flex h-[14.4px] w-[14.4px] items-center justify-center">
          <Icon size={14.4} strokeWidth={1.5} />
        </div>
        {!collapsed && <span className="truncate">{item.label}</span>}
      </NavLink>
    </div>
  )
}

function AstraWordmark({ collapsed }) {
  if (collapsed) {
    return (
      <span className="relative block h-8 w-8 overflow-hidden">
        <img
          src={ASTRA_WORDMARK_SRC}
          alt="Astra"
          className="absolute left-0 top-1/2 h-auto w-[145px] max-w-none -translate-y-1/2"
        />
      </span>
    )
  }

  return (
    <img
      src={ASTRA_WORDMARK_SRC}
      alt="Astra"
      className="h-auto w-[132px] flex-shrink-0 object-contain"
    />
  )
}

export default Sidebar
