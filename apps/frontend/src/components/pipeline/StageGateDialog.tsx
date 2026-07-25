// @ts-nocheck
import React, { useEffect, useState } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { CheckCircle2, ChevronRight, Loader2, XCircle } from 'lucide-react'

export default function StageGateDialog({ isOpen, completedStage, nextStage, onContinue, onCancel, busy = false }) {
  const [autoAdvance, setAutoAdvance] = useState(false)

  useEffect(() => {
    if (isOpen) setAutoAdvance(false)
  }, [isOpen, completedStage?.name, nextStage?.name])

  return (
    <AnimatePresence>
      {isOpen && (
        <>
          <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="fixed inset-0 z-[70] bg-black/65 backdrop-blur-sm" />
          <motion.div
            initial={{ opacity: 0, scale: 0.95, y: -12 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.95, y: -12 }}
            className="pointer-events-none fixed inset-0 z-[71] flex items-center justify-center p-4"
          >
            <div className="pointer-events-auto w-full max-w-md overflow-hidden rounded-2xl border border-bg-border bg-bg-card shadow-2xl">
              <div className="flex items-start gap-4 border-b border-bg-border/60 px-6 pb-4 pt-6">
                <div className="flex h-10 w-10 flex-shrink-0 items-center justify-center rounded-xl bg-accent-green/15">
                  <CheckCircle2 size={20} className="text-accent-green" />
                </div>
                <div>
                  <h3 className="text-base font-semibold text-text-primary">Stage Completed</h3>
                  <p className="mt-0.5 text-sm text-text-tertiary">
                    <span className="font-medium text-text-secondary">{completedStage?.name || 'Stage'}</span> finished successfully.
                  </p>
                </div>
              </div>

              <div className="px-6 py-4">
                <div className="flex items-center gap-3 rounded-xl border border-accent-blue/20 bg-accent-blue/5 p-3">
                  <ChevronRight size={16} className="flex-shrink-0 text-accent-blue" />
                  <div>
                    <p className="text-xs text-text-tertiary">Next stage</p>
                    <p className="mt-0.5 text-sm font-medium text-text-primary">{nextStage?.name || 'Pipeline completion'}</p>
                  </div>
                </div>
                <p className="mt-3 text-center text-xs text-text-tertiary">Do you want to proceed to the next stage?</p>
              </div>

              <div className="px-6 pb-4">
                <label className="flex cursor-pointer select-none items-center gap-2.5">
                  <input type="checkbox" checked={autoAdvance} onChange={(event) => setAutoAdvance(event.target.checked)} className="h-4 w-4 accent-accent-blue" />
                  <span className="text-xs text-text-tertiary">Don't ask again — auto-advance between stages</span>
                </label>
              </div>

              <div className="flex gap-3 px-6 pb-6">
                <button disabled={busy} onClick={onCancel} className="flex flex-1 items-center justify-center gap-2 rounded-xl border border-bg-border px-4 py-2.5 text-sm font-medium text-text-secondary transition-colors hover:bg-bg-hover hover:text-text-primary disabled:opacity-50">
                  <XCircle size={14} /> Cancel Run
                </button>
                <button disabled={busy} onClick={() => onContinue(autoAdvance)} className="flex flex-1 items-center justify-center gap-2 rounded-xl bg-accent-blue px-4 py-2.5 text-sm font-medium text-white transition-colors hover:bg-accent-blue/90 disabled:opacity-50">
                  {busy ? <Loader2 size={14} className="animate-spin" /> : <ChevronRight size={14} />} Continue
                </button>
              </div>
            </div>
          </motion.div>
        </>
      )}
    </AnimatePresence>
  )
}
