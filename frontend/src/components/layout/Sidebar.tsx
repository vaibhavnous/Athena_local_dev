// @ts-nocheck
import React from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import {
  ArrowRightLeft,
  ChevronLeft,
  ChevronRight,
  Database,
  FolderKanban,
  LayoutDashboard,
  PlayCircle,
  Settings,
  ShieldCheck,
  Sparkles,
  Trophy,
} from 'lucide-react'
import useAthenaStore from '../../store/useAthenaStore'

const ATHENA_LOGO_SRC = `${process.env.PUBLIC_URL}/Athena_logo.png`

function Sidebar({ collapsed, onToggle }) {
  const userRole = useAthenaStore((s) => s.userRole)

  const mainItems = [
    { to: '/app', icon: LayoutDashboard, label: 'Dashboard', exact: true },
    { to: '/app/data-discovery', icon: Sparkles, label: 'Data Discovery' },
    { to: '/app/run-history', icon: PlayCircle, label: 'Run History' },
    { to: '/app/project', icon: FolderKanban, label: 'Project' },
    { to: '/app/data-quality', icon: ShieldCheck, label: 'Data Quality' },
    { to: '/app/data-migration', icon: ArrowRightLeft, label: 'Data Lineage' },
  ]

  const secondaryItems = [
    { to: '/app/db-config', icon: Database, label: 'Configuration' },
    { to: '/app/settings', icon: Settings, label: 'Settings' },
  ]

  return (
    <aside className={`relative flex h-full flex-shrink-0 flex-col overflow-hidden border-r border-[#253044] bg-[#131c2c] transition-[width] duration-300 ${collapsed ? 'w-[78px]' : 'w-[280px]'}`}>
      <div className="flex h-[60px] items-center justify-between border-b border-[#253044] px-4">
        <div className="flex items-center gap-3 overflow-hidden">
          <AthenaMark />
          {!collapsed && <div className="text-[18px] font-semibold text-white">Athena</div>}
        </div>
        <button
          onClick={onToggle}
          className="flex h-8 w-8 items-center justify-center rounded-md text-slate-400 transition-colors hover:bg-white/5 hover:text-white"
          title={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        >
          {collapsed ? <ChevronRight size={16} /> : <ChevronLeft size={16} />}
        </button>
      </div>

      <div className="px-3 py-4">
        {!collapsed && (
          <SidebarShowcase title="Databricks Edition" subtitle="Unity Catalog, Delta Lake" />
        )}

        <nav className="mt-9 space-y-2">
          {mainItems.map((item) => (
            <NavItem key={item.to} item={item} collapsed={collapsed} />
          ))}
        </nav>
      </div>

      <div className="mt-auto border-t border-[#253044] px-3 py-4">
        {!collapsed && (
          <>
            <SidebarShowcase title="Data Engineer" subtitle="Pipeline Designer & Builder" compact />

            <div className="mt-6 px-2">
              <div className="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-400">
                Databricks Integrations
              </div>
              <div className="mt-4 space-y-2.5">
                <IntegrationItem label="Unity Catalog" tone="emerald" />
                <IntegrationItem label="Delta Live Tables" tone="emerald" />
                <IntegrationItem label="MosaicAI" tone="amber" />
              </div>
            </div>

            <nav className="mt-7 space-y-2">
              {secondaryItems.map((item) => (
                <NavItem key={item.to} item={item} collapsed={collapsed} />
              ))}
            </nav>
          </>
        )}

        {collapsed && (
          <nav className="space-y-2">
            {secondaryItems.map((item) => (
              <NavItem key={item.to} item={item} collapsed={collapsed} />
            ))}
          </nav>
        )}

        <div className={`mt-5 border-t border-[#253044] px-3 pt-4 ${collapsed ? 'hidden' : 'block'}`}>
          <div className="flex items-center justify-between gap-3">
            <div className="min-w-0">
              <div className="truncate text-[12px] font-semibold text-white">Jayaprakasha Sarma C</div>
              <div className="truncate text-[10px] text-slate-400">{userRole || 'Pipeline Designer & Builder'}</div>
            </div>
            <button className="flex h-7 w-7 items-center justify-center rounded-md text-red-300" title="Profile action">
              <Trophy size={14} />
            </button>
          </div>
        </div>
      </div>
    </aside>
  )
}

function SidebarShowcase({ title, subtitle, compact = false }) {
  return (
    <div className={`rounded-lg ${compact ? 'bg-transparent px-2 py-2' : 'border border-[#2f5fb2] bg-[#182a47] px-3 py-3'}`}>
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-[12px] font-semibold text-white">{title}</div>
          <div className="mt-0.5 text-[10px] text-slate-300">{subtitle}</div>
        </div>
        <ChevronRight size={14} className="text-slate-300" />
      </div>
    </div>
  )
}

function IntegrationItem({ label, tone }) {
  const dotClass = tone === 'amber' ? 'bg-amber-400' : 'bg-emerald-400'
  return (
    <div className="flex items-center gap-2 text-[12px] text-white">
      <span className={`h-2 w-2 rounded-full ${dotClass}`} />
      {label}
    </div>
  )
}

function NavItem({ item, collapsed }) {
  const location = useLocation()
  const isActive = item.exact ? location.pathname === item.to : location.pathname.startsWith(item.to)
  const Icon = item.icon

  return (
    <NavLink
      to={item.to}
      className={`group flex items-center gap-3 rounded-lg border px-3 py-2.5 text-[14px] font-semibold transition-colors ${
        isActive
          ? 'border-[#2f5fb2] bg-[#1f325d] text-[#3f82ff]'
          : 'border-transparent text-slate-100 hover:bg-white/[0.05] hover:text-white'
      } ${collapsed ? 'justify-center px-0' : ''}`}
    >
      <div className="flex h-5 w-5 items-center justify-center">
        <Icon size={18} strokeWidth={1.8} />
      </div>
      {!collapsed && <span className="truncate">{item.label}</span>}
    </NavLink>
  )
}

function AthenaMark() {
  return (
    <img src={ATHENA_LOGO_SRC} alt="Athena" className="h-11 w-11 flex-shrink-0 object-contain" />
  )
}

export default Sidebar
