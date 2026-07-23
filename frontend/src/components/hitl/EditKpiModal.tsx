// @ts-nocheck
import React, { useState, useEffect } from 'react'
import { createPortal } from 'react-dom'
import { X, Save } from 'lucide-react'

/**
 * EditKpiModal — modal for editing a KPI before approval.
 */
function EditKpiModal({ kpi, isOpen, onClose, onSave }) {
  const [name, setName] = useState('')
  const [definition, setDefinition] = useState('')

  useEffect(() => {
    if (kpi) {
      setName(kpi.name || '')
      setDefinition(kpi.definition || '')
    }
  }, [kpi])

  if (!isOpen || !kpi) return null

  const handleSave = () => {
    if (!name.trim() || !definition.trim()) return
    onSave(kpi.id, {
      name: name.trim(),
      definition: definition.trim(),
      reviewer: 'analyst',
      notes: ''
    })
    onClose()
  }

  return createPortal(
    <>
      {/* Backdrop */}
      <div
        style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.75)', zIndex: 9998 }}
        onClick={onClose}
      />

      {/* Dialog */}
      <div
        style={{
          position: 'fixed',
          inset: 0,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          zIndex: 9999,
          padding: '1rem',
          pointerEvents: 'none'
        }}
      >
        <div
          style={{ pointerEvents: 'auto', width: '100%', maxWidth: '48rem' }}
          className="overflow-hidden rounded-2xl border border-[#24344d] bg-[#141d2d] shadow-[0_28px_90px_rgba(0,0,0,0.46)]"
          onClick={(e) => e.stopPropagation()}
        >
          {/* Header */}
          <div className="flex items-center justify-between border-b border-[#22304b] px-7 py-5">
            <div>
              <h3 className="text-[18px] font-bold text-white">Edit KPI</h3>
              <p className="mt-1 text-sm text-[#9eb0ca]">Update the KPI details before approving</p>
            </div>
            <button
              onClick={onClose}
              className="flex h-9 w-9 items-center justify-center rounded-lg text-[#8b98ad] transition-colors hover:bg-[#1d2940] hover:text-white"
            >
              <X size={16} />
            </button>
          </div>

          {/* Body */}
          <div className="space-y-5 px-7 py-6">
            <div>
              <label className="mb-2 block text-xs font-semibold uppercase tracking-wide text-[#c8d2e5]">KPI Name</label>
              <input
                autoFocus
                type="text"
                className="h-14 w-full rounded-[10px] border-2 border-[#4388ff] bg-[#0b1322] px-4 text-base text-white outline-none transition-colors placeholder:text-[#73829f] focus:border-[#4a81e8]"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="KPI name..."
              />
            </div>

            <div>
              <label className="mb-2 block text-xs font-semibold uppercase tracking-wide text-[#c8d2e5]">Description</label>
              <textarea
                className="w-full resize-none rounded-[10px] border border-[#24344d] bg-[#0b1322] px-4 py-3 text-base leading-7 text-white outline-none transition-colors placeholder:text-[#73829f] focus:border-[#4a81e8]"
                rows={5}
                value={definition}
                onChange={(e) => setDefinition(e.target.value)}
                placeholder="Clear, measurable description..."
              />
            </div>
          </div>

          {/* Footer */}
          <div className="flex gap-3 border-t border-[#22304b] px-7 py-5">
            <button
              onClick={onClose}
              className="flex-1 rounded-[10px] bg-[#273244] px-4 py-3 text-base font-semibold text-white transition-colors hover:bg-[#313d52]"
            >
              Cancel
            </button>
            <button
              onClick={handleSave}
              disabled={!name.trim() || !definition.trim()}
              className="flex-1 inline-flex items-center justify-center gap-2 rounded-[10px] bg-[#4a81e8] px-4 py-3 text-base font-semibold text-white transition-colors hover:bg-[#5a8df0] disabled:cursor-not-allowed disabled:opacity-50"
            >
              <Save size={14} />
              Save Changes
            </button>
          </div>
        </div>
      </div>
    </>,
    document.body
  )
}

export default EditKpiModal


