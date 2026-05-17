import React from 'react'
import { Link } from 'react-router-dom'
import { Activity, CheckCircle2, Clock3, ShieldCheck } from 'lucide-react'
import useAthenaStore from '../store/useAthenaStore'

function DashboardPage() {
  const runs = useAthenaStore((s) => s.runs)

  const running = runs.filter((run) => ['RUNNING', 'PROCESSING'].includes(run.status)).length
  const waiting = runs.filter((run) => run.status === 'HITL_WAIT' || run.next_gate).length
  const completed = runs.filter((run) => run.status === 'SUCCESS').length
  const failed = runs.filter((run) => run.status === 'FAILED').length

  return (
    <div className="flex flex-col gap-4">
      <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
        <MetricCard icon={Activity} label="Running" value={running} tone="blue" />
        <MetricCard icon={ShieldCheck} label="Awaiting Review" value={waiting} tone="amber" />
        <MetricCard icon={CheckCircle2} label="Completed" value={completed} tone="green" />
        <MetricCard icon={Clock3} label="Failed" value={failed} tone="red" />
      </div>

      <section className="rounded-xl border border-bg-border bg-bg-card p-5">
        <div className="flex items-center justify-between gap-3">
          <div>
            <h2 className="text-base font-semibold text-text-primary">Backend Pipeline Overview</h2>
            <p className="mt-1 text-sm text-text-secondary">
              This UI is now aligned to the FastAPI workflow only: run submission, status tracking, Gate 1/2/3 review, and generated script inspection.
            </p>
          </div>
          <Link
            to="/app/data-discovery"
            className="rounded-lg bg-accent-blue px-4 py-2 text-sm font-semibold text-white transition-colors hover:bg-blue-600"
          >
            Open Monitor
          </Link>
        </div>
      </section>

      <section className="rounded-xl border border-bg-border bg-bg-card p-5">
        <div className="mb-4 flex items-center justify-between gap-3">
          <div>
            <h2 className="text-base font-semibold text-text-primary">Recent Runs</h2>
            <p className="mt-1 text-sm text-text-secondary">Live run list from FastAPI.</p>
          </div>
        </div>

        {runs.length === 0 ? (
          <div className="rounded-lg border border-dashed border-bg-border px-4 py-10 text-center text-sm text-text-tertiary">
            No runs found. Start a pipeline from the top bar.
          </div>
        ) : (
          <div className="space-y-3">
            {runs.slice(0, 8).map((run) => (
              <Link
                key={run.id}
                to={`/app/runs/${run.id}`}
                className="flex items-center justify-between rounded-lg border border-bg-border px-4 py-3 transition-colors hover:border-accent-blue/30 hover:bg-bg-hover"
              >
                <div className="min-w-0">
                  <div className="truncate text-sm font-semibold text-text-primary">
                    {run.brd_filename || run.id}
                  </div>
                  <div className="mt-1 text-xs text-text-tertiary">
                    {run.id}
                  </div>
                </div>
                <div className="text-right">
                  <div className="text-xs font-semibold text-text-secondary">
                    {run.status}
                  </div>
                  <div className="mt-1 text-xs text-text-tertiary">
                    {(run.total_tokens || 0).toLocaleString()} tokens
                  </div>
                </div>
              </Link>
            ))}
          </div>
        )}
      </section>
    </div>
  )
}

function MetricCard({ icon: Icon, label, value, tone }) {
  const toneClass = {
    blue: 'text-accent-blue bg-accent-blue/10 border-accent-blue/20',
    amber: 'text-accent-amber bg-accent-amber/10 border-accent-amber/20',
    green: 'text-accent-green bg-accent-green/10 border-accent-green/20',
    red: 'text-accent-red bg-accent-red/10 border-accent-red/20',
  }[tone]

  return (
    <div className="rounded-xl border border-bg-border bg-bg-card p-4">
      <div className="flex items-center justify-between">
        <span className="text-sm text-text-secondary">{label}</span>
        <div className={`rounded-lg border p-2 ${toneClass}`}>
          <Icon size={16} />
        </div>
      </div>
      <div className="mt-4 text-2xl font-bold text-text-primary">{value}</div>
    </div>
  )
}

export default DashboardPage
