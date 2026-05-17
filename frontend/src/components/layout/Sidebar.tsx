// @ts-nocheck
import React from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import {
  Brain,
  ChevronLeft,
  ChevronRight,
  Database,
  LayoutDashboard,
  PlayCircle,
  Settings,
  ShieldCheck,
} from 'lucide-react'
import useAthenaStore from '../../store/useAthenaStore'

function Sidebar({ collapsed, onToggle }) {
  const pendingCount = useAthenaStore((s) => s.getPendingHitlCount())

  const navItems = [
    { to: '/app', icon: LayoutDashboard, label: 'Dashboard', exact: true },
    { to: '/app/data-discovery', icon: PlayCircle, label: 'Pipeline Monitor' },
    { to: '/app/hitl', icon: ShieldCheck, label: 'Gate Review', badge: pendingCount > 0 ? pendingCount : null },
    { to: '/app/db-config', icon: Database, label: 'Database Config' },
    { to: '/app/settings', icon: Settings, label: 'Settings' },
  ]

  return (
    <motion.aside
      animate={{ width: collapsed ? 68 : 240 }}
      transition={{ type: 'spring', stiffness: 300, damping: 30 }}
      className="relative flex h-full flex-shrink-0 flex-col overflow-hidden border-r border-bg-border bg-bg-card"
      style={{ minHeight: 0 }}
    >
      <div
        className="flex flex-shrink-0 items-center border-b border-bg-border"
        style={{ height: '51.2px', paddingLeft: '9.6px', paddingRight: '9.6px' }}
      >
        <button
          onClick={onToggle}
          className="flex h-8 w-8 items-center justify-center rounded-lg hover:bg-bg-hover transition-colors"
          title={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        >
          <div className="flex items-center justify-center rounded-lg bg-accent-blue p-1.5">
            <Brain className="text-white" size={14} strokeWidth={2.2} />
          </div>
        </button>

        <AnimatePresence>
          {!collapsed && (
            <motion.div
              initial={{ opacity: 0, width: 0 }}
              animate={{ opacity: 1, width: 'auto' }}
              exit={{ opacity: 0, width: 0 }}
              transition={{ duration: 0.2 }}
              className="ml-3 flex flex-1 items-center justify-between overflow-hidden"
            >
              <span className="whitespace-nowrap text-sm font-bold text-text-primary">
                Athena
              </span>
              <button
                onClick={onToggle}
                className="flex h-7 w-7 items-center justify-center rounded-lg text-text-tertiary hover:bg-bg-hover hover:text-text-secondary transition-colors"
              >
                <ChevronLeft size={14} />
              </button>
            </motion.div>
          )}
        </AnimatePresence>

        {collapsed && (
          <button
            onClick={onToggle}
            className="absolute right-1 top-1/2 flex h-7 w-7 -translate-y-1/2 items-center justify-center rounded-lg text-text-tertiary opacity-0 transition-opacity hover:bg-bg-hover hover:text-text-secondary group-hover:opacity-100"
          >
            <ChevronRight size={14} />
          </button>
        )}
      </div>

      <nav className="overflow-hidden py-4">
        {navItems.map((item) => (
          <NavItem key={item.to} item={item} collapsed={collapsed} />
        ))}
      </nav>

      <div className="flex-1" />
      <SidebarFooter collapsed={collapsed} />
    </motion.aside>
  )
}

function NavItem({ item, collapsed }) {
  const location = useLocation()
  const isActive = item.exact
    ? location.pathname === item.to
    : location.pathname.startsWith(item.to)

  const Icon = item.icon

  return (
    <div className="relative group" style={{ paddingLeft: '8px', paddingRight: '8px', marginBottom: '4px' }}>
      <NavLink
        to={item.to}
        className={`flex items-center rounded-lg border font-medium transition-all duration-200 ${
          isActive
            ? 'border-accent-blue/20 bg-accent-blue/15 text-accent-blue'
            : 'border-transparent text-text-secondary hover:bg-accent-blue/10 hover:text-accent-blue'
        }`}
        style={{
          gap: '9.6px',
          paddingLeft: '9.6px',
          paddingRight: '9.6px',
          paddingTop: '8px',
          paddingBottom: '8px',
          fontSize: '11.2px',
          justifyContent: collapsed ? 'center' : 'flex-start',
          height: '32px',
          alignItems: 'center',
        }}
      >
        <div className="relative flex h-[14.4px] w-[14.4px] flex-shrink-0 items-center justify-center">
          <Icon size={14.4} strokeWidth={1.5} />
          {item.badge && (
            <span className="absolute -right-1.5 -top-1.5 flex h-[12.8px] min-w-[12.8px] items-center justify-center rounded-full bg-accent-amber px-[2px] text-[8px] font-bold text-gray-900">
              {item.badge > 9 ? '9+' : item.badge}
            </span>
          )}
        </div>

        <AnimatePresence>
          {!collapsed && (
            <motion.div
              initial={{ opacity: 0, width: 0 }}
              animate={{ opacity: 1, width: 'auto' }}
              exit={{ opacity: 0, width: 0 }}
              transition={{ duration: 0.2 }}
              className="flex flex-1 items-center justify-between overflow-hidden"
            >
              <span className="whitespace-nowrap">{item.label}</span>
              {item.badge && (
                <span className="ml-2 flex h-4 min-w-4 items-center justify-center rounded-full bg-accent-amber px-1 text-[8px] font-bold text-gray-900">
                  {item.badge > 9 ? '9+' : item.badge}
                </span>
              )}
            </motion.div>
          )}
        </AnimatePresence>
      </NavLink>
    </div>
  )
}

function SidebarFooter({ collapsed }) {
  const serverOnline = useAthenaStore((s) => s.serverOnline)

  return (
    <div className="flex-shrink-0 border-t border-bg-border p-[9.6px]">
      <div className={`flex items-center ${collapsed ? 'justify-center' : ''}`} style={{ gap: '9px' }}>
        <div
          className={`h-2 w-2 rounded-full flex-shrink-0 ${
            serverOnline ? 'bg-accent-green' : 'bg-gray-600'
          }`}
        />
        <AnimatePresence>
          {!collapsed && (
            <motion.span
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              style={{ fontSize: '9.6px' }}
              className="whitespace-nowrap text-gray-500"
            >
              {serverOnline ? 'FastAPI connected' : 'FastAPI offline'}
            </motion.span>
          )}
        </AnimatePresence>
      </div>
    </div>
  )
}

export default Sidebar
