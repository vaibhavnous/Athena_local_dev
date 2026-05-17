// @ts-nocheck
import React, { useMemo, useState } from 'react'
import { ChevronDown, Search } from 'lucide-react'

const STATUS_STYLES = {
  Active: 'bg-emerald-500/10 text-emerald-300 border-emerald-500/20',
  Review: 'bg-amber-500/10 text-amber-300 border-amber-500/20',
  Healthy: 'bg-blue-500/10 text-blue-300 border-blue-500/20',
  Failed: 'bg-red-500/10 text-red-300 border-red-500/20'
}

function PipelinesTable({ pipelines }) {
  const [query, setQuery] = useState('')
  const [status, setStatus] = useState('All')

  const filteredPipelines = useMemo(() => {
    return pipelines.filter((pipeline) => {
      const matchesQuery = [pipeline.name, pipeline.type, pipeline.team]
        .join(' ')
        .toLowerCase()
        .includes(query.trim().toLowerCase())

      const matchesStatus = status === 'All' || pipeline.status === status
      return matchesQuery && matchesStatus
    })
  }, [pipelines, query, status])

  return (
    <section className="rounded-xl border border-bg-border bg-bg-card">
      <div className="flex flex-col gap-4 border-b border-bg-border px-6 py-5 md:flex-row md:items-center md:justify-between">
        <div>
          <h2 className="text-xl font-semibold text-text-primary">Active Pipelines</h2>
          <p className="mt-1 text-sm text-text-secondary">Track live pipeline programs across engineering teams.</p>
        </div>

        <div className="flex flex-col gap-3 sm:flex-row">
          <label className="relative">
            <Search size={16} className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-text-tertiary" />
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search pipelines"
              className="h-10 w-full rounded-lg border border-bg-border bg-bg-base pl-9 pr-3 text-sm text-text-primary outline-none transition-colors placeholder:text-text-tertiary focus:border-accent-blue sm:w-64"
            />
          </label>

          <label className="relative">
            <select
              value={status}
              onChange={(e) => setStatus(e.target.value)}
              className="h-10 appearance-none rounded-lg border border-bg-border bg-bg-base pl-3 pr-10 text-sm text-text-primary outline-none transition-colors focus:border-accent-blue"
            >
              <option>All</option>
              <option>Active</option>
              <option>Review</option>
              <option>Healthy</option>
              <option>Failed</option>
            </select>
            <ChevronDown size={16} className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-text-tertiary" />
          </label>
        </div>
      </div>

      {filteredPipelines.length === 0 ? (
        <div className="px-6 py-16 text-center">
          <p className="text-base text-text-secondary">No pipelines yet. Create your first pipeline.</p>
        </div>
      ) : (
        <>
          <div className="hidden grid-cols-[2fr_1.1fr_0.9fr_1fr_1fr] gap-4 px-6 py-3 text-xs font-medium uppercase tracking-[0.16em] text-text-tertiary md:grid">
            <span>Pipeline Name</span>
            <span>Type</span>
            <span>Status</span>
            <span>Last Updated</span>
            <span>Team</span>
          </div>

          <div className="divide-y divide-bg-border">
            {filteredPipelines.map((pipeline, index) => (
              <div
                key={pipeline.name}
                className={`grid gap-3 px-6 py-4 transition-colors hover:bg-bg-hover md:grid-cols-[2fr_1.1fr_0.9fr_1fr_1fr] ${
                  index % 2 === 0 ? 'bg-transparent' : 'bg-bg-hover'
                }`}
              >
                <Cell label="Pipeline Name">
                  <div>
                    <p className="text-sm font-medium text-text-primary">{pipeline.name}</p>
                    <p className="mt-1 text-xs text-text-tertiary">{pipeline.description}</p>
                  </div>
                </Cell>

                <Cell label="Type">
                  <span className="text-sm text-text-secondary">{pipeline.type}</span>
                </Cell>

                <Cell label="Status">
                  <span className={`inline-flex w-fit rounded-full border px-2.5 py-1 text-xs font-medium ${STATUS_STYLES[pipeline.status]}`}>
                    {pipeline.status}
                  </span>
                </Cell>

                <Cell label="Last Updated">
                  <span className="text-sm text-gray-400">{pipeline.updated}</span>
                </Cell>

                <Cell label="Team">
                  <span className="text-sm text-gray-300">{pipeline.team}</span>
                </Cell>
              </div>
            ))}
          </div>
        </>
      )}
    </section>
  )
}

function Cell({ label, children }) {
  return (
    <div className="space-y-1 md:space-y-0">
      <p className="text-[11px] font-medium uppercase tracking-[0.16em] text-gray-500 md:hidden">{label}</p>
      {children}
    </div>
  )
}

export default PipelinesTable

