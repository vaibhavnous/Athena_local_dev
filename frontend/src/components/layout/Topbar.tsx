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
      <header className="border-b border-bg-border bg-bg-card flex items-center justify-end flex-shrink-0 transition-colors duration-200" style={{ height: '51.2px', paddingLeft: '19.2px', paddingRight: '19.2px', gap: '12.8px' }}>
        {/* Right: Theme toggle + Provider badge + New Run button */}
        <div className="flex items-center" style={{ gap: '9.6px' }}>
          {/* Theme Toggle Button */}
          <button
            onClick={toggleTheme}
            className="p-2 rounded-lg border border-bg-border bg-bg-base hover:bg-bg-hover text-text-secondary 
                       hover:text-text-primary transition-colors duration-150"
            title={theme === 'dark' ? 'Switch to Light Mode' : 'Switch to Dark Mode'}
          >
            {theme === 'dark' ? (
              <Sun size={16} strokeWidth={2} />
            ) : (
              <Moon size={16} strokeWidth={2} />
            )}
          </button>

          <button
            onClick={() => setShowNewRunModal(true)}
            className="flex items-center bg-accent-blue hover:bg-blue-600 active:bg-blue-700 text-white font-semibold rounded-lg transition-colors duration-150 shadow-lg shadow-blue-900/30"
            style={{ gap: '8px', paddingLeft: '16px', paddingRight: '16px', paddingTop: '8px', paddingBottom: '8px', fontSize: '11.2px' }}
          >
            <Plus size={12} strokeWidth={2.5} />
            New Run
          </button>
        </div>
      </header>

      <NewRunModal isOpen={showNewRunModal} onClose={() => setShowNewRunModal(false)} />
    </>
  )
}

export default Topbar
