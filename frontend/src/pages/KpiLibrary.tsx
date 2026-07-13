// @ts-nocheck
import React, { useState, useMemo } from 'react'
import { Search, Download, ChevronDown, ChevronUp, ChevronLeft, ChevronRight } from 'lucide-react'
import useAthenaStore from '../store/useAthenaStore'
import StatusBadge from '../components/shared/StatusBadge'
import ConfidenceBar from '../components/shared/ConfidenceBar'
import CopyableId from '../components/shared/CopyableId'

const PAGE_SIZE = 10

function KpiLibrary() {
  const kpiLibrary = useAthenaStore((s) => s.kpiLibrary)

  const [search, setSearch] = useState('')
  const [decisionFilter, setDecisionFilter] = useState('All')
  const [domainFilter, setDomainFilter] = useState('All')
  const [dateFrom, setDateFrom] = useState('')
  const [dateTo, setDateTo] = useState('')
  const [sortBy, setSortBy] = useState('confidence')
  const [sortDir, setSortDir] = useState('desc')
  const [page, setPage] = useState(1)
  const [hoveredRejection, setHoveredRejection] = useState(null)

  // All unique domains
  const domains = useMemo(() => {
    const set = new Set(kpiLibrary.map((k) => k.domain).filter(Boolean))
    return ['All', ...Array.from(set).sort()]
  }, [kpiLibrary])

  const filtered = useMemo(() => {
    let list = [...kpiLibrary]

    if (search) {
      const q = search.toLowerCase()
      list = list.filter(
        (k) =>
          k.name?.toLowerCase().includes(q) ||
          k.definition?.toLowerCase().includes(q) ||
          k.run_id?.toLowerCase().includes(q)
      )
    }

    if (decisionFilter !== 'All') {
      list = list.filter((k) => (k.decision || k.status) === decisionFilter)
    }

    if (domainFilter !== 'All') {
      list = list.filter((k) => k.domain === domainFilter)
    }

    if (dateFrom) {
      list = list.filter((k) => k.recorded_at && k.recorded_at >= dateFrom)
    }

    if (dateTo) {
      list = list.filter((k) => k.recorded_at && k.recorded_at <= dateTo + 'T23:59:59')
    }

    // Sort
    list.sort((a, b) => {
      let av = a[sortBy] ?? 0
      let bv = b[sortBy] ?? 0
      if (typeof av === 'string') av = av.toLowerCase()
      if (typeof bv === 'string') bv = bv.toLowerCase()
      if (sortDir === 'desc') return bv > av ? 1 : bv < av ? -1 : 0
      return av > bv ? 1 : av < bv ? -1 : 0
    })

    return list
  }, [kpiLibrary, search, decisionFilter, domainFilter, dateFrom, dateTo, sortBy, sortDir])

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE))
  const paginated = filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE)

  const toggleSort = (col) => {
    if (sortBy === col) setSortDir((d) => d === 'desc' ? 'asc' : 'desc')
    else { setSortBy(col); setSortDir('desc'); setPage(1) }
  }

  const handleSearch = (val) => {
    setSearch(val)
    setPage(1)
  }

  const handleExportCsv = () => {
    const headers = ['KPI Name', 'Domain', 'Category', 'Run ID', 'Confidence', 'Decision', 'Grounded', 'Recorded At', 'Rejection Reason']
    const rows = filtered.map((k) => [
      `"${(k.name || '').replace(/"/g, '""')}"`,
      `"${k.domain || ''}"`,
      `"${k.category || ''}"`,
      k.run_id || '',
      k.confidence?.toFixed(3) || '',
      k.decision || k.status || '',
      k.grounded ? 'Yes' : 'No',
      k.recorded_at || '',
      `"${(k.rejection_reason || '').replace(/"/g, '""')}"`
    ])
    const csv = [headers.join(','), ...rows.map((r) => r.join(','))].join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `astra_data_kpis_${new Date().toISOString().slice(0, 10)}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  const SortIcon = ({ col }) =>
    sortBy === col
      ? sortDir === 'desc' ? <ChevronDown size={12} /> : <ChevronUp size={12} />
      : null

  const COLUMNS = [
    { label: 'KPI Name', col: 'name' },
    { label: 'Domain', col: 'domain' },
    { label: 'Run ID', col: 'run_id' },
    { label: 'Confidence', col: 'confidence' },
    { label: 'Decision', col: 'decision' },
    { label: 'Recorded', col: 'recorded_at' }
  ]

  return (
    <div className="flex flex-col gap-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-bold text-white">KPI Library</h1>
          <p className="text-sm text-gray-500 mt-0.5">{kpiLibrary.length} KPIs across all runs</p>
        </div>
        <button
          onClick={handleExportCsv}
          className="flex items-center gap-2 btn-secondary"
        >
          <Download size={14} />
          Export CSV
        </button>
      </div>

      {/* Filters */}
      <div className="flex items-center gap-3 flex-wrap card px-4 py-3">
        {/* Search */}
        <div className="relative flex-1 min-w-48">
          <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500 pointer-events-none" />
          <input
            type="text"
            placeholder="Search KPIs, definitions, run IDs…"
            className="input-field pl-8 text-xs"
            value={search}
            onChange={(e) => handleSearch(e.target.value)}
          />
        </div>

        <select
          value={decisionFilter}
          onChange={(e) => { setDecisionFilter(e.target.value); setPage(1) }}
          className="input-field w-auto text-xs"
        >
          <option value="All">All Decisions</option>
          <option value="APPROVED">Approved</option>
          <option value="EDITED">Edited</option>
          <option value="REJECTED">Rejected</option>
          <option value="PENDING_REVIEW">Pending</option>
          <option value="AUTO_SUPPRESSED">Suppressed</option>
        </select>

        <select
          value={domainFilter}
          onChange={(e) => { setDomainFilter(e.target.value); setPage(1) }}
          className="input-field w-auto text-xs"
        >
          {domains.map((d) => (
            <option key={d} value={d}>{d}</option>
          ))}
        </select>

        <div className="flex items-center gap-2 text-xs text-gray-500">
          <span>From</span>
          <input
            type="date"
            className="input-field w-auto text-xs py-1.5"
            value={dateFrom}
            onChange={(e) => { setDateFrom(e.target.value); setPage(1) }}
          />
          <span>to</span>
          <input
            type="date"
            className="input-field w-auto text-xs py-1.5"
            value={dateTo}
            onChange={(e) => { setDateTo(e.target.value); setPage(1) }}
          />
        </div>

        <span className="text-xs text-gray-600 ml-auto">{filtered.length} results</span>
      </div>

      {/* Table */}
      <div className="card overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-bg-border">
              {COLUMNS.map(({ label, col }) => (
                <th
                  key={col}
                  onClick={() => toggleSort(col)}
                  className="text-left px-4 py-3 text-xs uppercase tracking-wider text-gray-500 font-medium cursor-pointer hover:text-gray-300 transition-colors select-none"
                >
                  <span className="flex items-center gap-1">
                    {label}
                    <SortIcon col={col} />
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {paginated.length === 0 ? (
              <tr>
                <td colSpan={6} className="px-4 py-8 text-center text-gray-600 text-sm">
                  No KPIs match the current filters.
                </td>
              </tr>
            ) : (
              paginated.map((kpi) => {
                const isRejected = kpi.decision === 'REJECTED' || kpi.status === 'REJECTED'
                const isSuppressed = kpi.decision === 'AUTO_SUPPRESSED' || kpi.status === 'AUTO_SUPPRESSED'

                return (
                  <tr
                    key={kpi.id}
                    className={`border-b border-bg-border transition-colors ${isRejected || isSuppressed ? 'opacity-50' : 'hover:bg-white/2'}`}
                  >
                    <td className="px-4 py-3">
                      <div
                        className="relative inline-block"
                        onMouseEnter={() => kpi.rejection_reason ? setHoveredRejection(kpi.id) : null}
                        onMouseLeave={() => setHoveredRejection(null)}
                      >
                        <span className={`text-sm font-medium ${isRejected || isSuppressed ? 'line-through text-gray-500' : 'text-gray-200'}`}>
                          {kpi.name}
                        </span>
                        {/* Rejection reason tooltip */}
                        {hoveredRejection === kpi.id && kpi.rejection_reason && (
                          <div className="absolute bottom-full left-0 mb-2 z-20 bg-gray-800 text-xs text-gray-200 px-3 py-2 rounded-lg shadow-xl border border-bg-border whitespace-nowrap max-w-64">
                            <span className="text-accent-red font-medium">Rejection reason: </span>
                            {kpi.rejection_reason}
                          </div>
                        )}
                      </div>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-xs px-2 py-1 rounded-full bg-bg-border text-gray-400">{kpi.domain || '—'}</span>
                    </td>
                    <td className="px-4 py-3">
                      <CopyableId id={kpi.run_id || ''} chars={10} />
                    </td>
                    <td className="px-4 py-3 w-40">
                      <ConfidenceBar score={kpi.confidence || 0} showLabel={false} compact={true} />
                      <span className="text-[10px] font-mono text-gray-500">{(kpi.confidence || 0).toFixed(3)}</span>
                    </td>
                    <td className="px-4 py-3">
                      <StatusBadge status={kpi.decision || kpi.status || 'PENDING'} size="sm" />
                    </td>
                    <td className="px-4 py-3 text-xs font-mono text-gray-500">
                      {kpi.recorded_at ? new Date(kpi.recorded_at).toLocaleDateString() : '—'}
                    </td>
                  </tr>
                )
              })
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <p className="text-xs text-gray-500">
            Showing {((page - 1) * PAGE_SIZE) + 1}–{Math.min(page * PAGE_SIZE, filtered.length)} of {filtered.length}
          </p>
          <div className="flex items-center gap-1">
            <button
              onClick={() => setPage(Math.max(1, page - 1))}
              disabled={page === 1}
              className="w-8 h-8 rounded-lg bg-bg-card border border-bg-border flex items-center justify-center text-gray-400 hover:text-white hover:bg-bg-border transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
            >
              <ChevronLeft size={14} />
            </button>
            {Array.from({ length: Math.min(7, totalPages) }, (_, i) => {
              const p = totalPages <= 7 ? i + 1 : Math.max(1, Math.min(totalPages - 6, page - 3)) + i
              return (
                <button
                  key={p}
                  onClick={() => setPage(p)}
                  className={`w-8 h-8 rounded-lg text-xs font-medium transition-colors ${p === page ? 'bg-accent-blue text-white' : 'text-gray-400 hover:text-white hover:bg-bg-border border border-bg-border bg-bg-card'}`}
                >
                  {p}
                </button>
              )
            })}
            <button
              onClick={() => setPage(Math.min(totalPages, page + 1))}
              disabled={page === totalPages}
              className="w-8 h-8 rounded-lg bg-bg-card border border-bg-border flex items-center justify-center text-gray-400 hover:text-white hover:bg-bg-border transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
            >
              <ChevronRight size={14} />
            </button>
          </div>
        </div>
      )}
    </div>
  )
}

export default KpiLibrary

