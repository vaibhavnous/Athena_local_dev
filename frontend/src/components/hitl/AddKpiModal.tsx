// @ts-nocheck
import React, { useEffect, useState } from 'react'
import { createPortal } from 'react-dom'
import { Plus, X } from 'lucide-react'

function AddKpiModal({ isOpen, submitting, onClose, onAdd }) {
  const [name, setName] = useState('')
  const [definition, setDefinition] = useState('')

  useEffect(() => {
    if (!isOpen) return
    setName('')
    setDefinition('')
  }, [isOpen])

  if (!isOpen) return null
  const valid = Boolean(name.trim() && definition.trim())

  return createPortal(
    <div className="fixed inset-0 z-[100] flex items-center justify-center bg-black/80 p-4 backdrop-blur-[2px]" role="dialog" aria-modal="true" aria-label="Add New KPI">
      <div className="w-full max-w-[690px] overflow-hidden rounded-[22px] border border-[#26334a] bg-[#111a2b] shadow-[0_35px_110px_rgba(0,0,0,0.7)]">
        <div className="flex items-start justify-between border-b border-[#26334a] px-7 py-6">
          <div><h2 className="text-xl font-extrabold text-white">Add New KPI</h2><p className="mt-1 text-sm text-[#9ca8bc]">Define a new KPI to include in the pipeline.</p></div>
          <button type="button" onClick={onClose} aria-label="Close add KPI" className="flex h-10 w-10 items-center justify-center rounded-lg text-[#9ca8bc] hover:bg-white/5 hover:text-white"><X size={20} /></button>
        </div>
        <div className="space-y-5 px-7 py-7">
          <div><label className="mb-2 block text-xs font-bold uppercase tracking-wide text-[#c8d0de]">KPI Name</label><input autoFocus value={name} onChange={(event) => setName(event.target.value)} placeholder="e.g. Customer Acquisition Cost" className="h-14 w-full rounded-xl border-2 border-[#4388ff] bg-[#081120] px-4 text-base text-white outline-none placeholder:text-[#7d899e]" /></div>
          <div><label className="mb-2 block text-xs font-bold uppercase tracking-wide text-[#c8d0de]">Description</label><textarea value={definition} onChange={(event) => setDefinition(event.target.value)} rows={5} placeholder="Clear, measurable description of what this KPI measures..." className="w-full resize-none rounded-xl border border-[#26334a] bg-[#081120] px-4 py-3 text-base leading-7 text-white outline-none placeholder:text-[#7d899e] focus:border-[#4388ff]" /></div>
        </div>
        <div className="grid grid-cols-2 gap-4 border-t border-[#26334a] bg-[#0d1627] px-7 py-5">
          <button type="button" onClick={onClose} className="h-12 rounded-xl bg-[#202c40] font-bold text-[#d0d6e1] hover:bg-[#29364b]">Cancel</button>
          <button type="button" disabled={!valid || submitting} onClick={() => onAdd({ name: name.trim(), definition: definition.trim() })} className="inline-flex h-12 items-center justify-center gap-2 rounded-xl bg-[#4388ff] font-bold text-white hover:bg-[#5595ff] disabled:cursor-not-allowed disabled:opacity-45"><Plus size={17} />{submitting ? 'Adding...' : 'Add KPI'}</button>
        </div>
      </div>
    </div>,
    document.body
  )
}

export default AddKpiModal
