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
  const [reviewer, setReviewer] = useState('')
  const [notes, setNotes] = useState('')

  useEffect(() => {
    if (kpi) {
      setName(kpi.name || '')
      setDefinition(kpi.definition || '')
      setReviewer('')
      setNotes('')
    }
  }, [kpi])

  if (!isOpen || !kpi) return null

  const handleSave = () => {
    if (!name.trim() || !definition.trim()) return
    onSave(kpi.id, {
      name: name.trim(),
      definition: definition.trim(),
      reviewer: reviewer.trim() || 'analyst',
      notes: notes.trim()
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
          style={{ pointerEvents: 'auto', width: '100%', maxWidth: '32rem' }}
          className="bg-bg-card border border-bg-border rounded-2xl shadow-2xl overflow-hidden"
          onClick={(e) => e.stopPropagation()}
        >
          {/* Header */}
          <div className="flex items-center justify-between px-6 py-4 border-b border-bg-border">
            <div>
              <h3 className="text-base font-bold text-text-primary">Edit KPI</h3>
              <p className="text-xs text-text-tertiary mt-0.5">Modify before approving to the pipeline</p>
            </div>
            <button
              onClick={onClose}
              className="w-8 h-8 rounded-lg flex items-center justify-center text-text-tertiary hover:text-text-secondary hover:bg-bg-hover transition-colors"
            >
              <X size={16} />
            </button>
          </div>

          {/* Body */}
          <div className="px-6 py-5 space-y-4">
            <div>
              <label className="label">KPI Name</label>
              <input
                autoFocus
                type="text"
                className="input-field"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="KPI name..."
              />
            </div>

            <div>
              <label className="label">Definition</label>
              <textarea
                className="input-field resize-none"
                rows={4}
                value={definition}
                onChange={(e) => setDefinition(e.target.value)}
                placeholder="Clear, measurable definition..."
              />
            </div>

            <div>
              <label className="label">Reviewer ID</label>
              <input
                type="text"
                className="input-field"
                value={reviewer}
                onChange={(e) => setReviewer(e.target.value)}
                placeholder="e.g. analyst_01"
              />
            </div>

            <div>
              <label className="label">Notes (optional)</label>
              <input
                type="text"
                className="input-field"
                value={notes}
                onChange={(e) => setNotes(e.target.value)}
                placeholder="Reason for edit..."
              />
            </div>
          </div>

          {/* Footer */}
          <div className="px-6 py-4 border-t border-bg-border flex gap-3">
            <button onClick={onClose} className="flex-1 btn-secondary">
              Cancel
            </button>
            <button
              onClick={handleSave}
              disabled={!name.trim() || !definition.trim()}
              className="flex-1 flex items-center justify-center gap-2 btn-amber disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <Save size={14} />
              Save Edit
            </button>
          </div>
        </div>
      </div>
    </>,
    document.body
  )
}

export default EditKpiModal


