// @ts-nocheck
import React, { useState } from 'react'
import { motion } from 'framer-motion'
import { Check, CheckCircle, Database, Pencil, X } from 'lucide-react'

function KpiReviewCard({ kpi, onApprove, onEdit, onReject, localDecision, rejectionReason }) {
  const [showRejectInput, setShowRejectInput] = useState(false)
  const [rejectText, setRejectText] = useState('')

  const kpiId = kpi.queue_id || kpi.id
  const detail = kpi.kpi_detail || {}
  const currentDecision = localDecision || kpi.decision
  const hasDecision = Boolean(currentDecision)
  const kpiName = kpi.name || detail.kpi_name || detail.name || kpi.item_id || 'Unnamed KPI'
  const kpiDefinition = kpi.definition || detail.kpi_description || detail.definition || detail.description || 'No KPI description provided.'

  const decisionConfig = {
    APPROVED: {
      border: 'border-[#1f6658] bg-[#103033]',
      color: 'text-[#32d29f]',
      label: 'Approved',
      icon: CheckCircle,
    },
    REJECTED: {
      border: 'border-[#803348] bg-[#301c29]',
      color: 'text-[#ff6b86]',
      label: 'Rejected',
      icon: X,
    },
    EDITED: {
      border: 'border-[#34547f] bg-[#14233a]',
      color: 'text-[#8eb9ff]',
      label: 'Edited',
      icon: Pencil,
    },
  }

  const decisionUi = currentDecision ? decisionConfig[currentDecision] : null

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.25 }}
      className={`rounded-[16px] border px-4 py-4 transition-colors ${
        decisionUi ? `${decisionUi.border}` : 'border-[#263247] bg-[#121a2b]'
      }`}
    >
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <Database size={13} className="shrink-0 text-[#93a5c3]" />
            <h3 className="truncate text-[15px] font-bold text-white">{kpiName}</h3>
            <span className="rounded-full border border-[#2e394d] bg-[#202938] px-2 py-0.5 text-[10px] font-semibold text-[#d5deec]">
              KPI
            </span>
          </div>
        </div>

        <div className="flex items-center gap-2 self-end sm:self-auto">
          <button
            type="button"
            onClick={() => onEdit(kpi)}
            className="inline-flex h-9 items-center gap-1.5 rounded-[10px] border border-[#2c3b54] bg-[#1a2536] px-3 text-xs font-semibold text-[#b8c6dd] transition-colors hover:bg-[#223149] hover:text-white"
          >
            <Pencil size={12} />
            Edit
          </button>
          {hasDecision && decisionUi && (
            <span className={`inline-flex items-center gap-1.5 text-xs font-semibold ${decisionUi.color}`}>
              <decisionUi.icon size={13} />
              {decisionUi.label}
            </span>
          )}
        </div>
      </div>

      <p className="mt-4 text-sm leading-7 text-[#c5cfde]">
        {kpiDefinition}
      </p>

      {!hasDecision && !showRejectInput && (
        <div className="mt-4 flex gap-2">
          <button
            type="button"
            onClick={() => onApprove(kpiId)}
            className="flex-1 rounded-[10px] border border-[#14856d] bg-[#103533] px-4 py-2.5 text-sm font-semibold text-[#31d49f] transition-colors hover:bg-[#15413d]"
          >
            <span className="inline-flex items-center gap-1.5">
              <Check size={14} strokeWidth={2.5} />
              Approve
            </span>
          </button>
          <button
            type="button"
            onClick={() => {
              setShowRejectInput(true)
              setRejectText('')
            }}
            className="flex-1 rounded-[10px] border border-[#8a3148] bg-[#2a1823] px-4 py-2.5 text-sm font-semibold text-[#ff647f] transition-colors hover:bg-[#351d29]"
          >
            <span className="inline-flex items-center gap-1.5">
              <X size={14} strokeWidth={2.5} />
              Reject
            </span>
          </button>
        </div>
      )}

      {!hasDecision && showRejectInput && (
        <div className="mt-4 space-y-2">
          <label className="text-xs font-semibold text-[#ff8aa0]">Rejection Reason</label>
          <textarea
            autoFocus
            rows={2}
            value={rejectText}
            onChange={(event) => setRejectText(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Escape') {
                setShowRejectInput(false)
                setRejectText('')
              }
              if (event.key === 'Enter' && !event.shiftKey && rejectText.trim()) {
                event.preventDefault()
                onReject(kpiId, rejectText.trim())
                setShowRejectInput(false)
                setRejectText('')
              }
            }}
            placeholder="Describe why this KPI is being rejected..."
            className="w-full resize-none rounded-[10px] border border-[#7a3346] bg-[#0d1524] px-3 py-2 text-xs text-white placeholder:text-[#73829f] focus:border-[#ff647f] focus:outline-none"
          />
          <div className="flex gap-2">
            <button
              type="button"
              onClick={() => {
                setShowRejectInput(false)
                setRejectText('')
              }}
              className="flex-1 rounded-[10px] bg-[#202b3a] px-3 py-2 text-xs font-semibold text-[#bcc8dc] transition-colors hover:bg-[#263449]"
            >
              Cancel
            </button>
            <button
              type="button"
              disabled={!rejectText.trim()}
              onClick={() => {
                onReject(kpiId, rejectText.trim())
                setShowRejectInput(false)
                setRejectText('')
              }}
              className="flex-1 rounded-[10px] border border-[#8a3148] bg-[#2a1823] px-3 py-2 text-xs font-semibold text-[#ff647f] transition-colors hover:bg-[#351d29] disabled:cursor-not-allowed disabled:opacity-40"
            >
              Confirm Reject
            </button>
          </div>
        </div>
      )}

      {currentDecision === 'REJECTED' && (rejectionReason || kpi.rejection_reason) && (
        <p className="mt-3 text-xs italic text-[#ff8aa0]">
          "{rejectionReason || kpi.rejection_reason}"
        </p>
      )}

      {hasDecision && decisionUi && (
        <div className="mt-4">
          <button
            type="button"
            onClick={() => {
              if (localDecision) onApprove(null)
            }}
            className="text-xs text-[#93a5c3] transition-colors hover:text-white"
          >
            ← Change decision
          </button>
        </div>
      )}
    </motion.div>
  )
}

export default KpiReviewCard
