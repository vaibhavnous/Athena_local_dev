// @ts-nocheck
import React, { useEffect, useState } from 'react'
import { Menu, Moon, Plus, Sun } from 'lucide-react'
import useThemeStore from '../../store/useThemeStore'
import NewRunModal from '../shared/NewRunModal'

export default function Topbar({ onOpenNavigation }) {
  const { theme, toggleTheme } = useThemeStore()
  const [showNewRunModal, setShowNewRunModal] = useState(false)
  const [seedRun, setSeedRun] = useState(null)

  useEffect(() => {
    const handleOpenNewRun = (event) => {
      setSeedRun(event?.detail?.seedRun || null)
      setShowNewRunModal(true)
    }

    window.addEventListener('athena:new-run', handleOpenNewRun)
    return () => window.removeEventListener('athena:new-run', handleOpenNewRun)
  }, [])

  const handleOpenFreshRun = () => {
    setSeedRun(null)
    setShowNewRunModal(true)
  }

  const handleCloseModal = () => {
    setShowNewRunModal(false)
    setSeedRun(null)
  }

  return (
    <>
      <header className="flex h-[51.2px] flex-shrink-0 items-center justify-between border-b border-[#253044] bg-[#131c2c] px-3 md:justify-end md:px-[19.2px]">
        <button onClick={onOpenNavigation} className="flex h-8 w-8 items-center justify-center rounded-lg border border-[#253044] bg-[#0b1120] text-slate-300 md:hidden" aria-label="Open navigation"><Menu size={16}/></button>
        <div className="flex items-center gap-2">
          <button onClick={toggleTheme} className="flex h-8 w-8 items-center justify-center rounded-lg border border-[#253044] bg-[#0b1120] text-slate-300" title={theme === 'dark' ? 'Switch to Light Mode' : 'Switch to Dark Mode'}>{theme === 'dark' ? <Sun size={16}/> : <Moon size={16}/>}</button>
          <button
            onClick={handleOpenFreshRun}
            className="inline-flex h-8 items-center gap-1.5 rounded-lg bg-[#3f82ff] px-3 text-xs font-semibold text-white transition-colors hover:bg-[#5a93f7]"
          >
            <Plus size={14} />
            New Run
          </button>
        </div>
      </header>
      <NewRunModal isOpen={showNewRunModal} onClose={handleCloseModal} initialSeedRun={seedRun} />
    </>
  )
}
