// @ts-nocheck
import React, { useState, useEffect } from 'react'
import { createPortal } from 'react-dom'
import { X, XCircle } from 'lucide-react'

/**
 * RejectReasonModal — asks the reviewer for a reason before marking a KPI as rejected.
 */
function RejectReasonModal ({ kpi, isOpen, onClose, onConfirm }) {
  const [reason, setReason] = useState('')

  useEffect(() => {
    if (isOpen) setReason('')
  }, [isOpen])

  // Nothing to render until opened
  if (!isOpen || !kpi) return null

  const handleConfirm = () => {
    if (!reason.trim()) return
    onConfirm(kpi.id, reason.trim())
    onClose()
  }

  const handleKey = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleConfirm() }
    if (e.key === 'Escape') onClose()
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
          style={{ pointerEvents: 'auto', width: '100%', maxWidth: '28rem' }}
          className="bg-bg-card border border-bg-border rounded-2xl shadow-2xl overflow-hidden"
          onClick={e => e.stopPropagation()}
        >
          {/* Header */}
          <div className="flex items-center justify-between px-6 py-4 border-b border-bg-border">
            <div>
              <h3 className="text-base font-bold text-text-primary">Reject KPI</h3>
              <p className="text-xs text-text-tertiary mt-0.5 truncate max-w-xs">
                {kpi.name || kpi.kpi_name || kpi.id}
              </p>
            </div>
            <button
              onClick={onClose}
              className="w-8 h-8 rounded-lg flex items-center justify-center text-text-tertiary hover:text-text-secondary hover:bg-bg-hover transition-colors"
            >
              <X size={16} />
            </button>
          </div>

          {/* Body */}
          <div className="px-6 py-5">
            <label className="label">
              Rejection Reason <span className="text-accent-red">*</span>
            </label>
            <textarea
              autoFocus
              className="input-field resize-none mt-1"
              rows={3}
              value={reason}
              onChange={e => setReason(e.target.value)}
              onKeyDown={handleKey}
              placeholder="Describe why this KPI is being rejected…"
            />
            <p className="text-[10px] text-text-tertiary mt-1.5">
              This reason is saved to the HITL review queue.
            </p>
          </div>

          {/* Footer */}
          <div className="px-6 py-4 border-t border-bg-border flex gap-3">
            <button onClick={onClose} className="flex-1 btn-secondary">
              Cancel
            </button>
            <button
              onClick={handleConfirm}
              disabled={!reason.trim()}
              className="flex-1 flex items-center justify-center gap-2 px-4 py-2 bg-accent-red/10 hover:bg-accent-red/20 border border-accent-red/30 text-accent-red text-sm font-semibold rounded-lg transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
            >
              <XCircle size={14} />
              Confirm Reject
            </button>
          </div>
        </div>
      </div>
    </>,
    document.body
  )
}

export default RejectReasonModal


