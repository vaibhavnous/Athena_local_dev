// @ts-nocheck
import React, { useCallback, useEffect, useMemo, useState } from 'react'
import {
  Check,
  CheckCircle2,
  ChevronDown,
  ChevronUp,
  Clock3,
  Eye,
  FileSearch,
  Info,
  Loader2,
  RefreshCcw,
  Search,
  ShieldCheck,
  X,
  XCircle,
} from 'lucide-react'
import { getComplianceReview, getRun, submitComplianceReview } from '../api/athenaApi'
import useAthenaStore from '../store/useAthenaStore'

export const decisionKey = (item) => `${item.table_name || 'unknown'}.${item.column_name || 'unknown'}`
export const normalizeDecision = (value) => {
  const status = String(value || '').toLowerCase()
  if (status === 'approved') return 'Approved'
  if (status === 'rejected' || status === 'excluded') return 'Rejected'
  return 'Pending'
}

function ComplianceGovernance() {
  const requestedRunId = new URLSearchParams(window.location.search).get('runId') || ''
  const { runs, activeRunId, setActiveRun, addRun, updateRun, addNotification } = useAthenaStore()
  const complianceRuns = useMemo(
    () => runs.filter((run) => run.compliance_enabled || run.compliance_assessment_id || run.compliance_review_status),
    [runs]
  )
  const selectedRunId = requestedRunId || (activeRunId && complianceRuns.some((run) => run.id === activeRunId)
    ? activeRunId
    : complianceRuns[0]?.id)
  const selectedRun = runs.find((run) => run.id === selectedRunId) || null
  const [reviewPayload, setReviewPayload] = useState(null)
  const [loading, setLoading] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [decisions, setDecisions] = useState({})
  const [comments, setComments] = useState({})
  const [search, setSearch] = useState('')
  const [statusFilter, setStatusFilter] = useState('All')
  const [riskFilter, setRiskFilter] = useState('All')
  const [sensitivityFilter, setSensitivityFilter] = useState('All')
  const [expandedTables, setExpandedTables] = useState({})
  const [selectedEvidence, setSelectedEvidence] = useState(null)
  const [summaryOpen, setSummaryOpen] = useState(false)

  const review = reviewPayload?.review || selectedRun?.compliance_review || null
  const evidence = useMemo(
    () => Array.isArray(review?.column_evidence) ? review.column_evidence : [],
    [review]
  )
  const results = reviewPayload?.results || selectedRun?.compliance_results || {}
  const evidencePack = results?.compliance_evidence || {}

  useEffect(() => {
    if (!requestedRunId) return
    setActiveRun(requestedRunId)
    if (runs.some((run) => run.id === requestedRunId)) return

    let cancelled = false
    getRun(requestedRunId)
      .then((run) => {
        if (!cancelled && run?.id) addRun(run)
      })
      .catch((error) => {
        if (!cancelled) {
          addNotification({
            type: 'error',
            title: 'Compliance Run Unavailable',
            message: error?.message || 'Unable to load the selected compliance run.',
            duration: 5000,
          })
        }
      })
    return () => {
      cancelled = true
    }
  }, [addNotification, addRun, requestedRunId, runs, setActiveRun])

  useEffect(() => {
    if (!evidence.length) return
    setDecisions((current) => {
      const next = { ...current }
      for (const item of evidence) {
        const key = decisionKey(item)
        if (!next[key]) next[key] = normalizeDecision(item.review_status)
      }
      return next
    })
    setExpandedTables((current) => {
      if (Object.keys(current).length) return current
      return { [evidence[0]?.table_name || 'Unknown table']: true }
    })
  }, [evidence])

  const loadReview = useCallback(async (runId = selectedRunId, quiet = false) => {
    if (!runId) return
    if (!quiet) setLoading(true)
    try {
      const [reviewData, runData] = await Promise.all([
        getComplianceReview(runId),
        getRun(runId).catch(() => null),
      ])
      setReviewPayload(reviewData)
      if (runData) updateRun(runId, runData)
    } catch (error) {
      if (!quiet) {
        addNotification({
          type: 'error',
          title: 'Compliance Review Unavailable',
          message: error?.message || 'Unable to load compliance review.',
          duration: 5000,
        })
      }
    } finally {
      if (!quiet) setLoading(false)
    }
  }, [addNotification, selectedRunId, updateRun])

  useEffect(() => {
    setReviewPayload(null)
    setDecisions({})
    setComments({})
    setSelectedEvidence(null)
    if (selectedRunId) void loadReview(selectedRunId)
  }, [loadReview, selectedRunId])

  useEffect(() => {
    if (!selectedRunId || evidence.length || !selectedRun?.compliance_enabled) return
    const timer = window.setInterval(() => void loadReview(selectedRunId, true), 8000)
    return () => window.clearInterval(timer)
  }, [evidence.length, loadReview, selectedRun?.compliance_enabled, selectedRunId])

  const decisionFor = useCallback(
    (item) => decisions[decisionKey(item)] || normalizeDecision(item.review_status),
    [decisions]
  )
  const counts = useMemo(() => {
    const values = evidence.map(decisionFor)
    return {
      total: evidence.length,
      approved: values.filter((value) => value === 'Approved').length,
      pending: values.filter((value) => value === 'Pending').length,
      rejected: values.filter((value) => value === 'Rejected').length,
    }
  }, [decisionFor, evidence])

  const risks = useMemo(() => [...new Set(evidence.map((item) => String(item.risk || item.priority || 'Low')))], [evidence])
  const sensitivities = useMemo(() => [...new Set(evidence.map((item) => String(item.sensitivity_level || 'Unclassified')))], [evidence])
  const filteredEvidence = useMemo(() => evidence.filter((item) => {
    const query = search.trim().toLowerCase()
    const status = decisionFor(item)
    const risk = String(item.risk || item.priority || 'Low')
    const sensitivity = String(item.sensitivity_level || 'Unclassified')
    const matchesSearch = !query || [item.table_name, item.column_name, item.column_description, item.pii_type, item.regulation]
      .some((value) => String(value || '').toLowerCase().includes(query))
    return matchesSearch &&
      (statusFilter === 'All' || status === statusFilter) &&
      (riskFilter === 'All' || risk === riskFilter) &&
      (sensitivityFilter === 'All' || sensitivity === sensitivityFilter)
  }), [decisionFor, evidence, riskFilter, search, sensitivityFilter, statusFilter])

  const groupedEvidence = useMemo(() => filteredEvidence.reduce((groups, item) => {
    const table = item.table_name || 'Unknown table'
    if (!groups[table]) groups[table] = []
    groups[table].push(item)
    return groups
  }, {}), [filteredEvidence])

  const setDecision = (item, status) => {
    setDecisions((current) => ({ ...current, [decisionKey(item)]: status }))
    setSelectedEvidence((current) => current && decisionKey(current) === decisionKey(item) ? { ...current, review_status: status } : current)
  }

  const approveAll = () => {
    setDecisions(Object.fromEntries(evidence.map((item) => [decisionKey(item), 'Approved'])))
  }

  const submitReview = async () => {
    if (!selectedRunId || !evidence.length || counts.pending) return
    setSubmitting(true)
    try {
      const findings = evidence.map((item) => ({
        table_name: item.table_name,
        column_name: item.column_name,
        status: decisionFor(item),
        security_control: item.security_control || null,
        reviewer_comments: comments[decisionKey(item)] || item.reviewer_comments || null,
      }))
      await submitComplianceReview(
        selectedRunId,
        findings,
        `Compliance governance review completed: ${counts.approved} approved, ${counts.rejected} rejected.`
      )
      addNotification({
        type: 'success',
        title: 'Compliance Review Submitted',
        message: `${counts.approved} columns approved and ${counts.rejected} rejected.`,
        duration: 4000,
      })
      await loadReview(selectedRunId)
    } catch (error) {
      addNotification({
        type: 'error',
        title: 'Compliance Submit Failed',
        message: error?.message || 'Unable to submit compliance decisions.',
        duration: 5000,
      })
    } finally {
      setSubmitting(false)
    }
  }

  const assessmentStatus = reviewPayload?.assessment_status || selectedRun?.compliance_assessment_status || 'Waiting'
  const assessmentError = reviewPayload?.assessment_error || selectedRun?.compliance_assessment_error || reviewPayload?.review_error || selectedRun?.compliance_review_error || ''
  const assessmentSubmittedAt = Number(reviewPayload?.assessment_submitted_at || selectedRun?.compliance_assessment_submitted_at || 0)
  const waitingMinutes = assessmentSubmittedAt ? Math.floor((Date.now() / 1000 - assessmentSubmittedAt) / 60) : 0
  const waitingTooLong = waitingMinutes >= 5 || ['FAILED', 'TIMED_OUT'].includes(String(assessmentStatus || '').toUpperCase())
  const isWaiting = !evidence.length && selectedRun?.compliance_enabled

  return (
    <div className="min-h-full bg-[#080d15] text-[#d7deea]">
      <div className="mx-auto max-w-[1600px] space-y-5 px-5 py-6 lg:px-8">
        <header className="flex flex-col gap-5 xl:flex-row xl:items-start xl:justify-between">
          <div>
            <div className="flex flex-wrap items-center gap-3">
              <h1 className="font-mono text-2xl font-bold tracking-tight text-[#e2e8f2] lg:text-[30px]">
                {selectedRun?.brd_filename || selectedRunId || 'Compliance review'}
              </h1>
              <span className={`rounded-md px-3 py-1 text-xs font-black uppercase tracking-wide ${counts.pending ? 'bg-amber-400/15 text-amber-300' : 'bg-emerald-400/15 text-emerald-300'}`}>
                {counts.pending ? 'In progress' : evidence.length ? 'Ready' : assessmentStatus}
              </span>
            </div>
            <div className="mt-3 flex flex-wrap items-center gap-3 text-sm text-[#8492a8]">
              <span className="inline-flex items-center gap-2 rounded-md bg-[#121923] px-3 py-2"><ShieldCheck size={15} /> Insurance</span>
              <span className="inline-flex items-center gap-2 rounded-md bg-[#121923] px-3 py-2"><FileSearch size={15} /> US</span>
              <span className="font-mono text-xs">Run {selectedRunId || 'not selected'}</span>
            </div>
          </div>

          <div className="flex flex-col gap-3 lg:flex-row">
            <select
              value={selectedRunId || ''}
              onChange={(event) => setActiveRun(event.target.value)}
              className="h-11 min-w-[270px] rounded-lg border border-[#253143] bg-[#0f1620] px-3 text-sm font-semibold text-white outline-none focus:border-[#4d80ff]"
            >
              {!complianceRuns.length && <option value="">No compliance runs yet</option>}
              {complianceRuns.map((run) => <option key={run.id} value={run.id}>{run.brd_filename || run.id}</option>)}
            </select>
            <button type="button" onClick={() => loadReview()} disabled={!selectedRunId || loading} className="inline-flex h-11 items-center justify-center gap-2 rounded-lg border border-[#253143] bg-[#121923] px-4 text-sm font-bold hover:border-[#3b4c65] disabled:opacity-50">
              {loading ? <Loader2 size={16} className="animate-spin" /> : <RefreshCcw size={16} />} Refresh
            </button>
          </div>
        </header>

        <section className="overflow-hidden rounded-xl border border-[#222d3c] bg-[#0e141d]">
          <div className="grid grid-cols-2 divide-x divide-[#222d3c] md:grid-cols-4">
            <Stat label="Total" value={counts.total} tone="text-white" />
            <Stat label="Approved" value={counts.approved} tone="text-emerald-400" />
            <Stat label="Pending" value={counts.pending} tone="text-amber-300" />
            <Stat label="Rejected" value={counts.rejected} tone="text-red-400" />
          </div>
        </section>

        <section className="overflow-hidden rounded-xl border border-[#222d3c] bg-[#0e141d]">
          <button type="button" onClick={() => setSummaryOpen((value) => !value)} className="flex w-full items-center justify-between px-5 py-4 text-left">
            <span className="inline-flex items-center gap-3 font-semibold text-[#93a2b9]"><Info size={18} /> Compliance summaries</span>
            {summaryOpen ? <ChevronUp size={18} /> : <ChevronDown size={18} />}
          </button>
          {summaryOpen && (
            <div className="grid gap-px border-t border-[#222d3c] bg-[#222d3c] lg:grid-cols-3">
              <Summary title="Business" text={evidencePack.business_summary} />
              <Summary title="Regulatory" text={evidencePack.regulatory_summary} />
              <Summary title="Security" text={evidencePack.security_summary} />
            </div>
          )}
        </section>

        <section className="rounded-xl border border-[#222d3c] bg-[#0e141d] p-4">
          <div className="flex flex-col gap-3 xl:flex-row xl:items-center">
            <label className="flex h-11 min-w-0 flex-1 items-center gap-3 rounded-lg border border-[#283549] bg-[#0a111a] px-4 focus-within:border-[#4d80ff]">
              <Search size={18} className="text-[#718097]" />
              <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search columns, tables, PII or regulation..." className="w-full bg-transparent text-sm text-white outline-none placeholder:text-[#68768c]" />
            </label>
            <Filter label="Status" value={statusFilter} onChange={setStatusFilter} options={['All', 'Approved', 'Pending', 'Rejected']} />
            <Filter label="Risk" value={riskFilter} onChange={setRiskFilter} options={['All', ...risks]} />
            <Filter label="Sensitivity" value={sensitivityFilter} onChange={setSensitivityFilter} options={['All', ...sensitivities]} />
          </div>
        </section>

        {isWaiting && (
          <section className={`overflow-hidden rounded-xl border ${waitingTooLong ? 'border-amber-300/25 bg-amber-300/[0.07]' : 'border-blue-400/20 bg-blue-400/[0.06]'}`}>
            <div className="flex flex-col gap-5 px-6 py-7 lg:flex-row lg:items-start lg:justify-between">
              <div className="max-w-3xl">
                <div className={`inline-flex items-center gap-2 rounded-full border px-3 py-1 text-xs font-black uppercase tracking-wide ${waitingTooLong ? 'border-amber-300/25 bg-amber-300/10 text-amber-200' : 'border-blue-300/25 bg-blue-300/10 text-blue-200'}`}>
                  <Loader2 size={14} className="animate-spin" /> {waitingTooLong ? 'Still preparing' : 'Loading compliance review'}
                </div>
                <h2 className="mt-4 text-xl font-bold text-white">
                  {waitingTooLong ? 'Compliance is taking longer than expected' : 'Preparing your compliance review'}
                </h2>
                <p className="mt-2 text-sm leading-6 text-[#9cafc8]">
                  {waitingTooLong
                    ? `This usually finishes in a few minutes for about 50 columns. Current status: ${assessmentStatus || 'Waiting'}${waitingMinutes ? ` after ${waitingMinutes} minutes` : ''}.`
                    : 'Athena is checking which columns may contain sensitive data, matching them to relevant rules, and preparing recommended controls.'}
                </p>
                {assessmentError && <p className="mt-3 rounded-lg border border-amber-300/20 bg-amber-300/10 px-3 py-2 font-mono text-xs text-amber-100">{assessmentError}</p>}
                <p className="mt-3 text-sm leading-6 text-[#9cafc8]">The pipeline will wait here until you review and submit the compliance decisions.</p>
              </div>
              <div className="grid gap-3 text-left sm:grid-cols-3 lg:w-[620px]">
                <LoadingBrief title="Rules" text="Finding which privacy and data-handling rules may apply to the columns." />
                <LoadingBrief title="Review" text="Preparing clear evidence so you can approve or reject each recommendation." />
                <LoadingBrief title="Controls" text="Choosing protections such as masking, hashing, redaction, or anonymization." />
              </div>
            </div>
          </section>
        )}

        <div className="space-y-3">
          {Object.entries(groupedEvidence).map(([tableName, items]) => {
            const expanded = expandedTables[tableName]
            const approved = items.filter((item) => decisionFor(item) === 'Approved').length
            return (
              <section key={tableName} className="overflow-hidden rounded-xl border border-[#222d3c] bg-[#0e141d]">
                <button type="button" onClick={() => setExpandedTables((current) => ({ ...current, [tableName]: !current[tableName] }))} className="flex w-full items-center justify-between gap-4 px-5 py-4 text-left hover:bg-white/[0.015]">
                  <span className="flex min-w-0 flex-wrap items-center gap-3">
                    <span className="font-mono font-bold text-[#5c8cff]">{tableName}</span>
                    <span className="rounded-md bg-[#1a222d] px-2.5 py-1 font-mono text-[11px] font-bold text-[#b7c1cf]">{items.length} columns</span>
                    <span className="inline-flex items-center gap-1.5 text-sm text-[#7f8da2]"><CheckCircle2 size={16} className="text-emerald-500" /> {approved} approved</span>
                  </span>
                  {expanded ? <ChevronUp size={18} /> : <ChevronDown size={18} />}
                </button>
                {expanded && <EvidenceTable items={items} decisionFor={decisionFor} setDecision={setDecision} openEvidence={setSelectedEvidence} />}
              </section>
            )
          })}
        </div>

        {!!evidence.length && (
          <footer className="sticky bottom-4 z-20 flex flex-col gap-3 rounded-xl border border-[#2a374a] bg-[#111923]/95 p-4 shadow-[0_20px_70px_rgba(0,0,0,0.55)] backdrop-blur lg:flex-row lg:items-center lg:justify-between">
            <div>
              <p className="text-sm font-semibold text-white">{counts.pending ? `${counts.pending} columns still need a decision` : 'All columns reviewed'}</p>
              <p className="mt-1 text-xs text-[#7f8da2]">Review decisions are sent together as one compliance plan.</p>
            </div>
            <div className="flex gap-3">
              <button type="button" onClick={approveAll} className="h-10 rounded-lg border border-emerald-400/30 px-4 text-sm font-bold text-emerald-300 hover:bg-emerald-400/10">Approve all</button>
              <button type="button" onClick={submitReview} disabled={submitting || counts.pending > 0} className="inline-flex h-10 items-center gap-2 rounded-lg bg-[#4d80ff] px-5 text-sm font-black text-white hover:bg-[#6793ff] disabled:cursor-not-allowed disabled:opacity-40">
                {submitting ? <Loader2 size={16} className="animate-spin" /> : <ShieldCheck size={16} />} Submit compliance review
              </button>
            </div>
          </footer>
        )}
      </div>

      {selectedEvidence && (
        <EvidenceDrawer
          item={selectedEvidence}
          decision={decisionFor(selectedEvidence)}
          comment={comments[decisionKey(selectedEvidence)] || ''}
          onComment={(value) => setComments((current) => ({ ...current, [decisionKey(selectedEvidence)]: value }))}
          onDecision={(status) => setDecision(selectedEvidence, status)}
          onClose={() => setSelectedEvidence(null)}
        />
      )}
    </div>
  )
}

function Stat({ label, value, tone }) {
  return <div className="px-5 py-4 text-center"><div className="text-xs font-black uppercase tracking-wide text-[#7d8ba0]">{label}</div><div className={`mt-1 font-mono text-lg ${tone}`}>{value}</div></div>
}

function Summary({ title, text }) {
  return <div className="bg-[#0e141d] p-5"><div className="text-xs font-black uppercase tracking-wider text-[#6f85aa]">{title}</div><p className="mt-2 text-sm leading-6 text-[#a9b5c7]">{text || 'No summary returned yet.'}</p></div>
}

function LoadingBrief({ title, text }) {
  return (
    <div className="rounded-lg border border-[#26364d] bg-[#0b121c] p-4">
      <div className="text-xs font-black uppercase tracking-wider text-blue-200">{title}</div>
      <p className="mt-2 text-sm leading-5 text-[#9cafc8]">{text}</p>
    </div>
  )
}

function Filter({ label, value, onChange, options }) {
  const allLabel = label === 'Status' ? 'All statuses' : label === 'Risk' ? 'All risks' : 'All sensitivities'
  return (
    <label className="flex items-center gap-2 whitespace-nowrap text-xs font-black uppercase tracking-wide text-[#7d8ba0]">
      {label}
      <select value={value} onChange={(event) => onChange(event.target.value)} className="h-11 min-w-[150px] rounded-lg border border-[#283549] bg-[#101720] px-3 text-sm font-medium normal-case tracking-normal text-[#d3dae5] outline-none focus:border-[#4d80ff]">
        {options.map((option) => <option key={option} value={option}>{option === 'All' ? allLabel : option}</option>)}
      </select>
    </label>
  )
}

function EvidenceTable({ items, decisionFor, setDecision, openEvidence }) {
  return (
    <div className="overflow-x-auto border-t border-[#222d3c]">
      <div className="min-w-[1050px]">
        <div className="grid grid-cols-[1.5fr_1.05fr_.7fr_.85fr_.8fr_190px] border-b border-[#222d3c] px-5 py-3 text-xs font-black uppercase tracking-wider text-[#78879d]">
          <span>Column</span><span>Classification</span><span>Risk</span><span>Control</span><span>Status</span><span className="text-right">Action</span>
        </div>
        {items.map((item) => {
          const decision = decisionFor(item)
          return (
            <div key={decisionKey(item)} className="grid grid-cols-[1.5fr_1.05fr_.7fr_.85fr_.8fr_190px] items-center border-b border-[#1b2533] px-5 py-4 last:border-0 hover:bg-white/[0.018]">
              <div className="min-w-0 pr-4"><div className="truncate font-mono text-sm text-[#d8dee8]">{item.column_name}</div><div className="mt-1 truncate text-xs text-[#75849a]">{item.column_description || 'No description'}</div></div>
              <div><span className="rounded border border-[#263348] bg-[#111923] px-2 py-1 font-mono text-[11px] text-[#c9d2df]">{item.pii_type || item.entity_type || 'General'}</span><div className="mt-2 text-xs text-[#77869b]">{item.sensitivity_level || 'Unclassified'}</div></div>
              <RiskBadge risk={item.risk || item.priority} />
              <span className="text-sm text-[#9ba9bc]">{item.security_control || 'Review'}</span>
              <DecisionStatus status={decision} />
              <div className="flex justify-end gap-2">
                <button type="button" title="Approve column" onClick={() => setDecision(item, 'Approved')} className={`flex h-9 w-9 items-center justify-center rounded-lg border ${decision === 'Approved' ? 'border-emerald-400 bg-emerald-400/15 text-emerald-300' : 'border-[#293548] text-[#7f8da2] hover:border-emerald-400/50 hover:text-emerald-300'}`}><Check size={17} /></button>
                <button type="button" title="Reject column" onClick={() => setDecision(item, 'Rejected')} className={`flex h-9 w-9 items-center justify-center rounded-lg border ${decision === 'Rejected' ? 'border-red-400 bg-red-400/15 text-red-300' : 'border-[#293548] text-[#7f8da2] hover:border-red-400/50 hover:text-red-300'}`}><X size={17} /></button>
                <button type="button" onClick={() => openEvidence(item)} className="inline-flex h-9 items-center gap-2 rounded-lg bg-[#1a222e] px-3 text-xs font-bold text-[#d0d7e2] hover:bg-[#222d3c]"><Eye size={15} /> Review</button>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

function RiskBadge({ risk }) {
  const value = String(risk || 'Low')
  const style = ['Critical', 'High'].includes(value) ? 'bg-red-400/10 text-red-400' : value === 'Medium' ? 'bg-amber-400/10 text-amber-300' : 'bg-emerald-400/10 text-emerald-300'
  return <span className={`w-fit rounded px-2.5 py-1 text-xs font-black uppercase ${style}`}>{value}</span>
}

function DecisionStatus({ status }) {
  if (status === 'Approved') return <span className="inline-flex items-center gap-1.5 text-sm text-emerald-400"><CheckCircle2 size={16} /> Approved</span>
  if (status === 'Rejected') return <span className="inline-flex items-center gap-1.5 text-sm text-red-400"><XCircle size={16} /> Rejected</span>
  return <span className="inline-flex items-center gap-1.5 text-sm text-[#8795a9]"><Clock3 size={16} /> Pending</span>
}

function EvidenceDrawer({ item, decision, comment, onComment, onDecision, onClose }) {
  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-black/65 backdrop-blur-[2px]" onMouseDown={(event) => event.target === event.currentTarget && onClose()}>
      <aside className="flex h-full w-full max-w-[780px] flex-col border-l border-[#283548] bg-[#0b1119] shadow-[-30px_0_90px_rgba(0,0,0,0.55)]">
        <header className="border-b border-[#263143] px-6 py-5">
          <div className="flex items-start justify-between gap-4">
            <div><div className="font-mono text-sm text-[#7f8da2]">{item.table_name} /</div><div className="mt-3 flex flex-wrap items-center gap-3"><h2 className="font-mono text-2xl font-bold text-[#5c8cff]">{item.column_name}</h2><RiskBadge risk={item.risk || item.priority} /></div><p className="mt-3 text-sm text-[#8d9bb0]">{item.column_description || 'No column description returned.'}</p></div>
            <button type="button" onClick={onClose} className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg text-[#8997aa] hover:bg-white/5 hover:text-white"><X size={20} /></button>
          </div>
        </header>

        <div className="flex-1 overflow-y-auto px-6 py-6">
          <div className="grid gap-8 md:grid-cols-2">
            <DrawerSection title="Detection details">
              <DrawerField label="Data Type / PII" value={`${item.entity_type || 'Unknown'} / ${item.pii_type || 'Not detected'}`} />
              <DrawerField label="Sensitivity" value={item.sensitivity_level || 'Unclassified'} />
              <DrawerBox label="Detection reason" value={item.detection_reason || item.classification_justification || 'No detection reason returned.'} />
              <div><div className="text-sm text-[#7f8da2]">Sample values</div><div className="mt-2 flex flex-wrap gap-2">{(item.sample_values || []).slice(0, 8).map((sample, index) => <span key={`${sample}-${index}`} className="rounded border border-[#263348] bg-[#111923] px-2.5 py-1.5 font-mono text-xs text-[#d0d7e2]">{sample}</span>)}</div></div>
            </DrawerSection>
            <DrawerSection title="Regulatory obligation">
              <DrawerField label="Regulation & Article" value={[item.regulation, item.article_title].filter(Boolean).join(' - ') || 'Not mapped'} accent />
              <DrawerField label="Obligation" value={item.obligation || 'No obligation returned.'} />
              <DrawerBox label="Required action" value={item.required_action || 'No required action returned.'} accent />
              <DrawerField label="Security control" value={item.security_control || 'Not recommended'} />
              <DrawerField label="Security rationale" value={item.security_rationale || 'No rationale returned.'} />
            </DrawerSection>
          </div>
          <div className="mt-8 border-t border-[#263143] pt-6">
            <label className="text-xs font-black uppercase tracking-wider text-[#7e8ba0]">Reviewer comments</label>
            <textarea value={comment} onChange={(event) => onComment(event.target.value)} rows={4} placeholder="Record why this finding is approved or rejected..." className="mt-3 w-full resize-none rounded-lg border border-[#283548] bg-[#101720] p-3 text-sm text-white outline-none placeholder:text-[#5f6d80] focus:border-[#4d80ff]" />
          </div>
        </div>

        <footer className="flex items-center justify-between gap-3 border-t border-[#263143] bg-[#0e151f] px-6 py-4">
          <DecisionStatus status={decision} />
          <div className="flex gap-3">
            <button type="button" onClick={() => onDecision('Rejected')} className="inline-flex h-10 items-center gap-2 rounded-lg border border-red-400/35 px-4 text-sm font-bold text-red-300 hover:bg-red-400/10"><X size={16} /> Reject</button>
            <button type="button" onClick={() => onDecision('Approved')} className="inline-flex h-10 items-center gap-2 rounded-lg bg-emerald-500 px-4 text-sm font-black text-[#04130c] hover:bg-emerald-400"><Check size={16} /> Approve</button>
          </div>
        </footer>
      </aside>
    </div>
  )
}

function DrawerSection({ title, children }) {
  return <section><h3 className="border-b border-[#222d3c] pb-3 text-xs font-black uppercase tracking-wider text-[#7f8da2]">{title}</h3><div className="mt-4 space-y-5">{children}</div></section>
}

function DrawerField({ label, value, accent = false }) {
  return <div><div className="text-sm text-[#7f8da2]">{label}</div><div className={`mt-1 text-sm font-semibold leading-6 ${accent ? 'text-[#55a0ff]' : 'text-[#d5dce7]'}`}>{value}</div></div>
}

function DrawerBox({ label, value, accent = false }) {
  return <div><div className="text-sm text-[#7f8da2]">{label}</div><div className={`mt-2 rounded-lg border p-3 text-sm leading-6 ${accent ? 'border-[#34558b] bg-[#142038] text-[#d6e4ff]' : 'border-[#293548] bg-[#101720] text-[#d1d8e3]'}`}>{value}</div></div>
}

export default ComplianceGovernance
