// @ts-nocheck
import React, { useEffect } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import {
  BarChart3,
  CheckCircle2,
  Clock3,
  Columns3,
  Database,
  FileCheck2,
  KeyRound,
  ShieldCheck,
  Sparkles,
  Table2,
  Target,
  X,
} from 'lucide-react'

const displayValue = (value, fallback = '—') => {
  const text = String(value ?? '').trim()
  return text || fallback
}

const displayDate = (value) => {
  if (!value) return '—'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString()
}

export default function RunReportDialog({ isOpen, onClose, report }) {
  useEffect(() => {
    if (!isOpen) return undefined
    const close = (event) => {
      if (event.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', close)
    return () => window.removeEventListener('keydown', close)
  }, [isOpen, onClose])

  if (!report) return null

  const metrics = report.metrics || {}
  const run = report.run || {}
  const artifacts = Array.isArray(report.artifacts) ? report.artifacts : []
  const tables = Array.isArray(report.tables) ? report.tables : []
  const kpis = Array.isArray(report.kpis) ? report.kpis : []
  const reviews = Object.entries(report.reviews || {})
  const deployment = report.deployment || {}
  const summaryCards = [
    { label: 'Tables', value: metrics.tables ?? tables.length, icon: Table2, tone: 'text-cyan-300 bg-cyan-400/10 border-cyan-400/20' },
    { label: 'Columns', value: metrics.columns ?? 0, icon: Columns3, tone: 'text-violet-300 bg-violet-400/10 border-violet-400/20' },
    { label: 'KPIs', value: metrics.kpis ?? kpis.length, icon: BarChart3, tone: 'text-amber-300 bg-amber-400/10 border-amber-400/20' },
    { label: 'Artifacts', value: metrics.artifacts ?? artifacts.length, icon: FileCheck2, tone: 'text-emerald-300 bg-emerald-400/10 border-emerald-400/20' },
    { label: 'Key Columns', value: metrics.key_columns ?? 0, icon: KeyRound, tone: 'text-blue-300 bg-blue-400/10 border-blue-400/20' },
    { label: 'PII Columns', value: metrics.pii_columns ?? 0, icon: ShieldCheck, tone: 'text-rose-300 bg-rose-400/10 border-rose-400/20' },
  ]

  return (
    <AnimatePresence>
      {isOpen && (
        <>
          <motion.button
            type="button"
            aria-label="Close run report"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            onClick={onClose}
            className="fixed inset-0 z-50 bg-black/75 backdrop-blur-sm"
          />
          <motion.section
            role="dialog"
            aria-modal="true"
            aria-labelledby="run-report-title"
            initial={{ opacity: 0, scale: 0.97, y: 14 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.97, y: 14 }}
            className="fixed inset-x-3 bottom-[3%] top-[3%] z-50 mx-auto flex max-w-6xl flex-col overflow-hidden rounded-2xl border border-[#2a3c59] bg-[#0b1220] shadow-[0_28px_100px_rgba(0,0,0,0.55)] sm:inset-x-6"
          >
            <header className="relative overflow-hidden border-b border-[#263650] bg-gradient-to-br from-[#132443] via-[#101a2d] to-[#0b1220] px-5 py-5 sm:px-7">
              <div className="pointer-events-none absolute -right-20 -top-24 h-64 w-64 rounded-full bg-blue-500/15 blur-3xl" />
              <div className="relative flex items-start justify-between gap-5">
                <div className="flex min-w-0 items-start gap-4">
                  <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl border border-emerald-400/25 bg-emerald-400/10 text-emerald-300">
                    <Sparkles size={22} />
                  </div>
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <h2 id="run-report-title" className="text-lg font-semibold tracking-tight text-white">
                        {displayValue(report.title, 'Pipeline Run Report')}
                      </h2>
                      <span className="inline-flex items-center gap-1 rounded-full border border-emerald-400/25 bg-emerald-400/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.12em] text-emerald-300">
                        <CheckCircle2 size={11} />
                        Successful
                      </span>
                    </div>
                    <p className="mt-1 truncate text-sm text-[#a7b5cc]">
                      {displayValue(run.name, 'Completed pipeline run')}
                    </p>
                    <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 font-mono text-[10px] text-[#7184a3]">
                      <span>Run {displayValue(run.id)}</span>
                      <span className="inline-flex items-center gap-1"><Clock3 size={11} />Generated {displayDate(report.generated_at)}</span>
                    </div>
                  </div>
                </div>
                <button
                  type="button"
                  onClick={onClose}
                  aria-label="Close report"
                  className="rounded-lg border border-[#2b3a52] bg-[#111b2d] p-2 text-[#93a4be] transition-colors hover:bg-[#17243a] hover:text-white"
                >
                  <X size={16} />
                </button>
              </div>
            </header>

            <div className="min-h-0 flex-1 overflow-y-auto">
              <div className="space-y-6 px-5 py-6 sm:px-7">
                {report.warning && (
                  <div className="rounded-xl border border-amber-400/25 bg-amber-400/10 px-4 py-3 text-sm text-amber-200">
                    {report.warning}
                  </div>
                )}

                <section>
                  <div className="mb-3 text-[11px] font-semibold uppercase tracking-[0.16em] text-[#6f84a5]">Executive summary</div>
                  <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">
                    {summaryCards.map(({ label, value, icon: Icon, tone }) => (
                      <div key={label} className="rounded-xl border border-[#22314a] bg-[#0f1929] p-3.5">
                        <div className={`flex h-8 w-8 items-center justify-center rounded-lg border ${tone}`}>
                          <Icon size={15} />
                        </div>
                        <div className="mt-3 text-xl font-semibold text-white">{value}</div>
                        <div className="mt-0.5 text-[10px] font-medium uppercase tracking-[0.1em] text-[#8192ad]">{label}</div>
                      </div>
                    ))}
                  </div>
                </section>

                <section className="grid gap-3 lg:grid-cols-2">
                  <div className="rounded-xl border border-[#22314a] bg-[#0f1929] p-4">
                    <div className="mb-4 flex items-center gap-2 text-sm font-semibold text-white">
                      <Database size={15} className="text-[#74a7ff]" />
                      Run profile
                    </div>
                    <div className="grid grid-cols-2 gap-x-5 gap-y-3 text-xs">
                      {[
                        ['Source', `${displayValue(run.source, 'database')} · ${displayValue(run.source_database)}`],
                        ['Target', displayValue(run.target, 'snowflake')],
                        ['Engine', displayValue(run.execution_engine, 'dbt')],
                        ['Mode', displayValue(run.deployment_mode, 'generate_and_deploy').replaceAll('_', ' ')],
                        ['Started', displayDate(run.started_at)],
                        ['Completed', displayDate(run.completed_at)],
                      ].map(([label, value]) => (
                        <div key={label}>
                          <div className="text-[10px] uppercase tracking-[0.1em] text-[#7184a3]">{label}</div>
                          <div className="mt-1 break-words font-medium capitalize text-[#d8e0ed]">{value}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div className="rounded-xl border border-[#22314a] bg-[#0f1929] p-4">
                    <div className="mb-4 flex items-center gap-2 text-sm font-semibold text-white">
                      <Target size={15} className="text-emerald-300" />
                      Deployment assurance
                    </div>
                    <div className="grid grid-cols-2 gap-x-5 gap-y-3 text-xs">
                      {[
                        ['Deployment', deployment.status],
                        ['Validation', deployment.validation_status],
                        ['Completion mode', deployment.completion_mode],
                        ['Artifact set', deployment.artifact_set_hash],
                      ].map(([label, value]) => (
                        <div key={label}>
                          <div className="text-[10px] uppercase tracking-[0.1em] text-[#7184a3]">{label}</div>
                          <div className="mt-1 break-all font-mono text-[#d8e0ed]">{displayValue(value)}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                </section>

                <section className="rounded-xl border border-[#22314a] bg-[#0f1929]">
                  <div className="flex items-center justify-between border-b border-[#22314a] px-4 py-3">
                    <div className="flex items-center gap-2 text-sm font-semibold text-white">
                      <FileCheck2 size={15} className="text-emerald-300" />
                      Generated artifacts
                    </div>
                    <span className="text-[10px] text-[#7184a3]">{artifacts.length} approved models</span>
                  </div>
                  <div className="overflow-x-auto">
                    <table className="w-full min-w-[680px] text-left text-xs">
                      <thead className="bg-[#0b1423] text-[10px] uppercase tracking-[0.1em] text-[#7184a3]">
                        <tr><th className="px-4 py-2.5">Layer</th><th className="px-4 py-2.5">Model</th><th className="px-4 py-2.5">Target</th><th className="px-4 py-2.5">Format</th><th className="px-4 py-2.5">Status</th></tr>
                      </thead>
                      <tbody className="divide-y divide-[#1f2d43]">
                        {artifacts.map((artifact, index) => (
                          <tr key={`${artifact.layer}:${artifact.name}:${index}`} className="text-[#c9d3e3]">
                            <td className="px-4 py-3"><span className="rounded-md border border-[#31548b] bg-[#13294d] px-2 py-1 text-[10px] font-semibold uppercase text-[#78a9ff]">{displayValue(artifact.layer)}</span></td>
                            <td className="px-4 py-3 font-medium text-white">{displayValue(artifact.name)}</td>
                            <td className="px-4 py-3 font-mono text-[11px]">{displayValue(artifact.target)}</td>
                            <td className="px-4 py-3 uppercase">{displayValue(artifact.format)}</td>
                            <td className="px-4 py-3 text-emerald-300">{displayValue(artifact.status)}</td>
                          </tr>
                        ))}
                        {!artifacts.length && <tr><td colSpan={5} className="px-4 py-8 text-center text-[#7184a3]">No artifact metadata was recorded.</td></tr>}
                      </tbody>
                    </table>
                  </div>
                </section>

                <section>
                  <div className="mb-3 flex items-center justify-between">
                    <div className="flex items-center gap-2 text-sm font-semibold text-white">
                      <Table2 size={15} className="text-cyan-300" />
                      Tables and columns
                    </div>
                    <span className="text-[10px] text-[#7184a3]">{tables.length} governed tables</span>
                  </div>
                  <div className="grid gap-3 lg:grid-cols-2">
                    {tables.map((table, index) => (
                      <div key={`${table.name}:${index}`} className="overflow-hidden rounded-xl border border-[#22314a] bg-[#0f1929]">
                        <div className="flex items-center justify-between border-b border-[#22314a] px-4 py-3">
                          <div>
                            <div className="text-sm font-semibold text-white">{displayValue(table.name)}</div>
                            <div className="mt-0.5 font-mono text-[10px] text-[#7184a3]">{[table.database, table.schema].filter(Boolean).join('.') || 'Source metadata'}</div>
                          </div>
                          <span className="rounded-md bg-[#17243a] px-2 py-1 text-[10px] text-[#9eb0ca]">{table.columns?.length || 0} columns</span>
                        </div>
                        <div className="max-h-56 overflow-y-auto">
                          {(table.columns || []).map((column, columnIndex) => (
                            <div key={`${column.name}:${columnIndex}`} className="grid grid-cols-[minmax(0,1fr)_110px_95px] items-center gap-3 border-b border-[#1c293d] px-4 py-2.5 last:border-b-0">
                              <div className="flex min-w-0 items-center gap-2">
                                {column.is_key && <KeyRound size={11} className="shrink-0 text-amber-300" />}
                                <span className="truncate font-mono text-[11px] text-[#d6deeb]">{displayValue(column.name)}</span>
                                {column.is_pii && <span className="rounded bg-rose-400/10 px-1.5 py-0.5 text-[9px] text-rose-300">PII</span>}
                              </div>
                              <span className="truncate text-[10px] text-[#8fa0ba]">{displayValue(column.data_type)}</span>
                              <span className="truncate text-right text-[10px] text-[#74a7ff]">{displayValue(column.semantic_type)}</span>
                            </div>
                          ))}
                          {!table.columns?.length && <div className="px-4 py-5 text-center text-xs text-[#7184a3]">Column metadata was not recorded.</div>}
                        </div>
                      </div>
                    ))}
                    {!tables.length && <div className="rounded-xl border border-dashed border-[#2a3b56] px-4 py-8 text-center text-sm text-[#7184a3] lg:col-span-2">No table metadata was recorded.</div>}
                  </div>
                </section>

                <section className="grid gap-3 lg:grid-cols-[1.4fr_1fr]">
                  <div className="rounded-xl border border-[#22314a] bg-[#0f1929] p-4">
                    <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-white"><BarChart3 size={15} className="text-amber-300" />Business KPIs</div>
                    <div className="space-y-2">
                      {kpis.map((kpi, index) => (
                        <div key={`${kpi.name}:${index}`} className="rounded-lg border border-[#24344c] bg-[#0b1423] px-3 py-3">
                          <div className="font-medium text-white">{displayValue(kpi.name)}</div>
                          <div className="mt-1 text-xs leading-relaxed text-[#91a3be]">{displayValue(kpi.description, 'Business KPI approved for the Gold layer.')}</div>
                          {kpi.formula && <div className="mt-2 rounded bg-[#101d31] px-2.5 py-2 font-mono text-[10px] text-[#b9c8dd]">{kpi.formula}</div>}
                        </div>
                      ))}
                      {!kpis.length && <div className="py-6 text-center text-xs text-[#7184a3]">No KPI details were recorded.</div>}
                    </div>
                  </div>
                  <div className="rounded-xl border border-[#22314a] bg-[#0f1929] p-4">
                    <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-white"><ShieldCheck size={15} className="text-emerald-300" />Governance trail</div>
                    <div className="space-y-2">
                      {reviews.map(([name, status]) => (
                        <div key={name} className="flex items-center justify-between rounded-lg border border-[#24344c] bg-[#0b1423] px-3 py-2.5">
                          <span className="text-xs capitalize text-[#b8c5d8]">{name.replaceAll('_', ' ')}</span>
                          <span className="inline-flex items-center gap-1 text-[10px] font-semibold uppercase text-emerald-300"><CheckCircle2 size={11} />{displayValue(status)}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                </section>
              </div>
            </div>

            <footer className="flex items-center justify-between border-t border-[#263650] bg-[#0a111e] px-5 py-3 text-[10px] text-[#7184a3] sm:px-7">
              <span>Generated from the approved run checkpoint; no additional warehouse queries were performed.</span>
              <span className="font-mono">Report v{report.version || 1}</span>
            </footer>
          </motion.section>
        </>
      )}
    </AnimatePresence>
  )
}
