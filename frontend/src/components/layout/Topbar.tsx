// @ts-nocheck
import React, { useState } from 'react'
import { Plus, Sun, Moon } from 'lucide-react'
import useThemeStore from '../../store/useThemeStore'
import NewRunModal from '../shared/NewRunModal'

function Topbar() {
  const { theme, toggleTheme } = useThemeStore()
  const [showNewRunModal, setShowNewRunModal] = useState(false)

  return (
    <>
      <header className="flex h-[60px] items-center justify-end border-b border-[#253044] bg-[#131c2c] px-6">
        <div className="flex items-center gap-3">
          <button
            onClick={toggleTheme}
            className="flex h-10 w-10 items-center justify-center rounded-lg border border-[#253044] bg-[#0b1120] text-slate-300 transition-colors hover:bg-white/[0.05] hover:text-white"
            title={theme === 'dark' ? 'Switch to Light Mode' : 'Switch to Dark Mode'}
          >
            {theme === 'dark' ? <Sun size={20} /> : <Moon size={20} />}
          </button>

          <button
            onClick={() => setShowNewRunModal(true)}
            className="inline-flex h-10 items-center gap-2 rounded-lg bg-[#3f82ff] px-5 text-[14px] font-semibold text-white transition-colors hover:bg-[#5a93f7]"
          >
            <Plus size={18} />
            New Run
          </button>
        </div>
      </header>

      <NewRunModal isOpen={showNewRunModal} onClose={() => setShowNewRunModal(false)} />
    </>
  )
}

export default Topbar
