// @ts-nocheck
import React, { useEffect, useState } from 'react'
import { Menu, Plus, Sun, Moon } from 'lucide-react'
import useThemeStore from '../../store/useThemeStore'
import NewRunModal from '../shared/NewRunModal'

function Topbar({ onOpenNavigation, onCreateRun = () => {} }) {
  const { theme, toggleTheme } = useThemeStore()
  const [showNewRunModal, setShowNewRunModal] = useState(false)
  const [seedRun, setSeedRun] = useState(null)

  useEffect(() => {
    const handleOpenNewRun = (event) => {
      setSeedRun(event?.detail?.seedRun || event?.detail?.project || null)
      setShowNewRunModal(true)
    }

    window.addEventListener('athena:new-run', handleOpenNewRun)
    return () => window.removeEventListener('athena:new-run', handleOpenNewRun)
  }, [])

  const handleOpenFreshRun = () => {
    onCreateRun()
  }

  const handleCloseModal = () => {
    setShowNewRunModal(false)
    setSeedRun(null)
  }

  return (
    <>
      <header className="flex h-[51.2px] flex-shrink-0 items-center justify-between border-b border-[#253044] bg-[#131c2c] px-3 md:justify-end md:px-[19.2px]">
        <button
          onClick={onOpenNavigation}
          className="flex h-8 w-8 items-center justify-center rounded-lg border border-[#253044] bg-[#0b1120] text-slate-300 transition-colors hover:bg-white/[0.05] hover:text-white md:hidden"
          title="Open navigation"
          aria-label="Open navigation"
        >
          <Menu size={16} />
        </button>
        <div className="flex items-center gap-[9.6px]">
          <button
            onClick={toggleTheme}
            className="flex h-8 w-8 items-center justify-center rounded-lg border border-[#253044] bg-[#0b1120] text-slate-300 transition-colors hover:bg-white/[0.05] hover:text-white"
            title={theme === 'dark' ? 'Switch to Light Mode' : 'Switch to Dark Mode'}
          >
            {theme === 'dark' ? <Sun size={16} /> : <Moon size={16} />}
          </button>

          <button
            onClick={handleOpenFreshRun}
            className="inline-flex h-8 items-center gap-1.5 rounded-lg bg-[#3f82ff] px-3 text-[11.2px] font-semibold text-white transition-colors hover:bg-[#5a93f7]"
            title="Create a new run"
          >
            <Plus size={14.4} />
            <span className="hidden sm:inline">New Run</span>
          </button>
        </div>
      </header>

      <NewRunModal isOpen={showNewRunModal} onClose={handleCloseModal} initialSeedRun={seedRun} />
    </>
  )
}

export default Topbar
