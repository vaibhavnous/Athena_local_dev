// @ts-nocheck
import React from 'react'
import { Menu, Moon, Sun } from 'lucide-react'
import useThemeStore from '../../store/useThemeStore'

// Runs are deliberately project-scoped, so the global header only owns navigation and theme controls.
export default function Topbar({ onOpenNavigation }) {
  const { theme, toggleTheme } = useThemeStore()
  return <header className="flex h-[51.2px] flex-shrink-0 items-center justify-between border-b border-[#253044] bg-[#131c2c] px-3 md:justify-end md:px-[19.2px]">
    <button onClick={onOpenNavigation} className="flex h-8 w-8 items-center justify-center rounded-lg border border-[#253044] bg-[#0b1120] text-slate-300 md:hidden" aria-label="Open navigation"><Menu size={16}/></button>
    <button onClick={toggleTheme} className="flex h-8 w-8 items-center justify-center rounded-lg border border-[#253044] bg-[#0b1120] text-slate-300" title={theme === 'dark' ? 'Switch to Light Mode' : 'Switch to Dark Mode'}>{theme === 'dark' ? <Sun size={16}/> : <Moon size={16}/>}</button>
  </header>
}
