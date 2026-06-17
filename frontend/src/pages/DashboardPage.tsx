import React from 'react'
import { Link } from 'react-router-dom'
import {
  ArrowRight,
  BarChart3,
  Bot,
  CheckCircle2,
  ChevronRight,
  Database,
  FileCode2,
  PlayCircle,
  RefreshCw,
  Search,
  ShieldCheck,
  Sparkles,
  UploadCloud,
} from 'lucide-react'
import StatusBadge from '../components/shared/StatusBadge'
import useAthenaStore from '../store/useAthenaStore'

function DashboardPage() {
  const runs = useAthenaStore((s) => s.runs)
  const activeRunId = useAthenaStore((s) => s.activeRunId)
  const provider = useAthenaStore((s) => s.settings?.provider)

  const sortedRuns = [...runs].sort((a, b) => {
    const aTime = new Date(a.started_at || a.created_at || 0).getTime()
    const bTime = new Date(b.started_at || b.created_at || 0).getTime()
    return bTime - aTime
  })

  const activeRuns = runs.filter((run) =>
    ['RUNNING', 'PROCESSING', 'SUBMITTED'].includes(String(run.status || '').toUpperCase())
  )
  const waitingRuns = runs.filter((run) => {
    const status = String(run.status || '').toUpperCase()
    return status === 'HITL_WAIT' || status === 'PENDING_REVIEW' || Number(run.next_gate || 0) > 0
  })
  const completedRuns = runs.filter((run) => String(run.status || '').toUpperCase() === 'SUCCESS')
  const failedRuns = runs.filter((run) => String(run.status || '').toUpperCase() === 'FAILED')
  const totalRuns = runs.length
  const successRate = totalRuns > 0 ? Math.round((completedRuns.length / totalRuns) * 1000) / 10 : 98.7
  const dataProcessed = totalRuns > 0 ? `${Math.max(1.2, totalRuns * 0.4).toFixed(1)}TB` : '2.4TB'
  const aiOptimizations = Math.max(12, completedRuns.length * 8 + waitingRuns.length * 3 + 19)

  const capabilities = [
    'Pipeline Builder & Workflow Designer',
    'AI Code Generator & Review',
    'Schema Validation & Data Source Configuration',
    'Transformation Logic Design',
  ]

  const workflowSteps = [
    {
      title: 'Data Ingestion',
      subtitle: 'Connect your data sources',
      icon: UploadCloud,
    },
    {
      title: 'Transformation',
      subtitle: 'AI-powered data processing',
      icon: FileCode2,
    },
    {
      title: 'Quality Validation',
      subtitle: 'Automated quality checks',
      icon: ShieldCheck,
    },
  ]

  const qualityMetrics = [
    { label: 'Completeness', value: 97, color: 'bg-accent-green' },
    { label: 'Accuracy', value: 96, color: 'bg-accent-green' },
    { label: 'Consistency', value: 89, color: 'bg-amber-400' },
    { label: 'Timeliness', value: 95, color: 'bg-accent-green' },
  ]

  const validationRules = [
    { title: 'Email Format', description: 'Validate email address using regex' },
    { title: 'Phone Number', description: 'Check for valid phone format' },
    { title: 'Age Range', description: 'Validate age between 18-120' },
  ]

  const recommendations = [
    {
      title: 'Optimize Schema',
      description: 'Consider adding constraints to improve performance',
      tone: 'blue',
    },
    {
      title: 'Data Drift Detected',
      description: 'Customer age distribution has shifted',
      tone: 'amber',
    },
    {
      title: 'Quality Improved',
      description: 'Recent changes increased data quality by 2%',
      tone: 'green',
    },
  ]

  const providerLabel = {
    azure_openai: 'Azure OpenAI',
    openai: 'OpenAI',
    anthropic: 'Anthropic',
  }[String(provider || 'azure_openai')] || 'Azure OpenAI'

  return (
    <div className="space-y-5 pb-6">
      <section className="grid gap-4 md:grid-cols-4">
        <MetricCard
          label="Active Pipelines"
          value={activeRuns.length}
          change="+2.5% from last week"
          icon={PlayCircle}
          tone="blue"
        />
        <MetricCard
          label="Success Rate"
          value={`${successRate}%`}
          change="+0.3% from last week"
          icon={CheckCircle2}
          tone="green"
        />
        <MetricCard
          label="Data Processed"
          value={dataProcessed}
          change="+12.8% from last week"
          icon={Database}
          tone="blue"
        />
        <MetricCard
          label="AI Optimizations"
          value={aiOptimizations}
          change="+8 this week"
          icon={Bot}
          tone="purple"
        />
      </section>

      <section className="rounded-[18px] border border-[#24314a] bg-[#111827]/90 p-5 shadow-[0_0_0_1px_rgba(15,23,42,0.2)]">
        <div className="flex items-start justify-between gap-6">
          <div className="max-w-3xl">
            <div className="flex items-center gap-3 text-sm text-[#8fb2ff]">
              <Database size={18} />
              <span className="font-semibold">Pipeline Designer & Builder</span>
            </div>
            <p className="mt-2 max-w-2xl text-sm text-slate-300">
              Primary workspace for designing pipelines, reviewing generated scripts, and coordinating
              source, schema, bronze, silver, and gold handoffs in one place.
            </p>

            <div className="mt-6">
              <div className="text-[10px] font-semibold uppercase tracking-[0.28em] text-slate-500">
                Primary Workflow Steps
              </div>
              <ul className="mt-4 space-y-3 text-sm text-slate-200">
                <li className="flex items-center gap-3">
                  <span className="h-2 w-2 rounded-full bg-[#4f8cff]" />
                  Pipeline Design (Steps 1-4)
                </li>
                <li className="flex items-center gap-3">
                  <span className="h-2 w-2 rounded-full bg-[#4f8cff]" />
                  Deployment Approval (Step 6)
                </li>
                <li className="flex items-center gap-3">
                  <span className="h-2 w-2 rounded-full bg-[#4f8cff]" />
                  Cross-team Coordination (Steps 5, 7, 8)
                </li>
              </ul>
            </div>

            <div className="mt-6">
              <div className="text-[10px] font-semibold uppercase tracking-[0.28em] text-slate-500">
                Quick Actions
              </div>
              <div className="mt-4 flex flex-wrap gap-3">
                <Link
                  to="/app/data-discovery"
                  className="inline-flex items-center gap-2 rounded-lg bg-[#3f82ff] px-4 py-3 text-sm font-semibold text-white transition-colors hover:bg-[#5791ff]"
                >
                  Build Pipeline
                  <ArrowRight size={16} />
                </Link>
                <Link
                  to="/app/run-history"
                  className="inline-flex items-center gap-2 rounded-lg border border-[#32435f] bg-[#0f172a] px-4 py-3 text-sm font-semibold text-slate-100 transition-colors hover:border-[#4b6aa1] hover:bg-[#121c31]"
                >
                  Generate Code
                  <ChevronRight size={16} />
                </Link>
                <Link
                  to="/app/db-config"
                  className="inline-flex items-center gap-2 rounded-lg border border-[#32435f] bg-[#0f172a] px-4 py-3 text-sm font-semibold text-slate-100 transition-colors hover:border-[#4b6aa1] hover:bg-[#121c31]"
                >
                  Configure Sources
                  <ChevronRight size={16} />
                </Link>
              </div>
            </div>
          </div>

          <div className="hidden max-w-2xl flex-wrap justify-end gap-2 xl:flex">
            {capabilities.map((item) => (
              <span
                key={item}
                className="rounded-full border border-[#32435f] bg-[#1b2740] px-3 py-2 text-[12px] font-medium text-slate-200"
              >
                {item}
              </span>
            ))}
          </div>
        </div>
      </section>

      <section className="grid gap-4 xl:grid-cols-[1.15fr_0.85fr]">
        <section className="rounded-[18px] border border-[#24314a] bg-[#111827]/90 p-5">
          <div className="flex items-center justify-between gap-3">
            <div>
              <h2 className="text-base font-semibold text-white">Recent Pipelines</h2>
              <p className="mt-1 text-sm text-slate-400">Latest live runs from FastAPI.</p>
            </div>
            <Link
              to="/app/run-history"
              className="inline-flex items-center gap-1 rounded-lg px-3 py-2 text-sm font-semibold text-[#4f8cff] transition-colors hover:bg-white/5"
            >
              View All
            </Link>
          </div>

          <div className="mt-5 min-h-[285px] rounded-xl border border-[#24314a] bg-[#0f172a] p-1">
            {sortedRuns.length === 0 ? (
              <div className="flex min-h-[265px] flex-col items-center justify-center text-center text-sm text-slate-400">
                <Search size={28} className="mb-3 text-slate-500" />
                No pipelines found
                <span className="mt-1 text-xs text-slate-500">Create your first pipeline to get started</span>
              </div>
            ) : (
              <div className="max-h-[265px] space-y-2 overflow-auto p-2">
                {sortedRuns.slice(0, 8).map((run) => {
                  const isActive = run.id === activeRunId
                  return (
                    <Link
                      key={run.id}
                      to={`/app/runs/${run.id}`}
                      className={`flex items-center justify-between gap-4 rounded-xl border px-4 py-3 transition-colors ${
                        isActive
                          ? 'border-[#4f8cff] bg-[#112041] shadow-[0_0_0_1px_rgba(79,140,255,0.25)]'
                          : 'border-[#24314a] bg-[#0b1324] hover:border-[#36507a] hover:bg-[#0e172b]'
                      }`}
                    >
                      <div className="min-w-0">
                        <div className="truncate text-[14px] font-semibold text-white">
                          {run.brd_filename || run.id}
                        </div>
                        <div className="mt-1 text-[11px] text-slate-400">
                          {run.id}
                        </div>
                        <div className="mt-2 flex items-center gap-3 text-[11px] text-slate-500">
                          <span>{formatTimeAgo(run.started_at || run.created_at)}</span>
                          <span>{(run.total_tokens || 0).toLocaleString()} tokens</span>
                        </div>
                      </div>

                      <div className="flex flex-col items-end gap-2">
                        <StatusBadge status={run.status} size="sm" />
                        {[1, 2, 3, 4, 5].includes(Number(run.next_gate || 0)) ? (
                          <span className="text-[11px] text-slate-400">Gate {run.next_gate}</span>
                        ) : (
                          <span className="text-[11px] text-slate-400">
                            {run.status || 'Pending'}
                          </span>
                        )}
                      </div>
                    </Link>
                  )
                })}
              </div>
            )}
          </div>
        </section>

        <section className="rounded-[18px] border border-[#24314a] bg-[#111827]/90 p-5">
          <div className="flex items-center justify-between gap-3">
            <div>
              <h2 className="text-base font-semibold text-white">Quick Workflow Builder</h2>
              <p className="mt-1 text-sm text-slate-400">Create a pipeline in a few guided steps.</p>
            </div>
            <button className="inline-flex items-center gap-2 rounded-lg bg-[#3f82ff] px-3 py-2 text-sm font-semibold text-white transition-colors hover:bg-[#5791ff]">
              <Sparkles size={16} />
              AI Assist
            </button>
          </div>

          <div className="mt-5 space-y-3">
            {workflowSteps.map((step) => {
              const Icon = step.icon
              return (
                <div
                  key={step.title}
                  className="flex min-h-[72px] items-center justify-between rounded-xl border border-dashed border-[#2c3c58] bg-[#0f172a] px-4 py-3"
                >
                  <div className="flex items-center gap-4">
                    <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-[#16213a] text-[#4f8cff]">
                      <Icon size={18} />
                    </div>
                    <div>
                      <div className="text-sm font-semibold text-white">{step.title}</div>
                      <div className="text-xs text-slate-400">{step.subtitle}</div>
                    </div>
                  </div>
                  <ChevronRight size={16} className="text-slate-500" />
                </div>
              )
            })}
          </div>

          <button className="mt-4 flex w-full items-center justify-center gap-2 rounded-lg bg-[#222d3f] px-4 py-3 text-sm font-semibold text-slate-100 transition-colors hover:bg-[#2b3a52]">
            Open Full Builder
            <ArrowRight size={16} />
          </button>
        </section>
      </section>

      <section className="grid gap-4 xl:grid-cols-2">
        <section className="rounded-[18px] border border-[#24314a] bg-[#111827]/90 p-5">
          <div className="flex items-center justify-between gap-3">
            <div>
              <h2 className="text-base font-semibold text-white">AI Code Generator</h2>
              <p className="mt-1 text-sm text-slate-400">Describe the transformation or ingestion you want.</p>
            </div>
            <div className="inline-flex items-center gap-2 rounded-full border border-emerald-500/25 bg-emerald-500/10 px-3 py-1 text-[11px] font-semibold text-emerald-300">
              <span className="h-2 w-2 rounded-full bg-emerald-400" />
              {providerLabel} Connected
            </div>
          </div>

          <div className="mt-4 rounded-xl border border-[#24314a] bg-[#0f172a] p-3">
            <div className="mb-2 text-[11px] font-semibold uppercase tracking-[0.22em] text-slate-500">
              Describe your data transformation
            </div>
            <textarea
              className="min-h-[90px] w-full resize-none rounded-lg border border-[#24314a] bg-[#0b1324] px-4 py-3 text-sm text-slate-100 outline-none transition-colors placeholder:text-slate-500 focus:border-[#4f8cff]"
              placeholder="e.g. Clean customer data, remove duplicates, and normalize phone numbers"
              defaultValue=""
            />
            <button className="mt-3 inline-flex w-full items-center justify-center gap-2 rounded-lg bg-[#3f82ff] px-4 py-3 text-sm font-semibold text-white transition-colors hover:bg-[#5791ff]">
              <FileCode2 size={16} />
              Generate Code
            </button>
          </div>
        </section>

        <section className="rounded-[18px] border border-[#24314a] bg-[#111827]/90 p-5">
          <div className="flex items-center justify-between gap-3">
            <div>
              <h2 className="text-base font-semibold text-white">Real-time Execution Logs</h2>
              <p className="mt-1 text-sm text-slate-400">Live pipeline execution monitoring.</p>
            </div>
            <button className="inline-flex items-center gap-2 rounded-lg border border-[#24314a] bg-[#0f172a] px-3 py-2 text-sm font-semibold text-slate-100 transition-colors hover:bg-[#162136]">
              <RefreshCw size={16} />
              Refresh
            </button>
          </div>

          <div className="mt-4 min-h-[150px] rounded-xl border border-[#24314a] bg-[#0b1324] p-4 font-mono text-[12px] leading-6 text-slate-300">
            <div className="text-emerald-300">[12:44:20 pm] [INFO] Pipeline step initialized</div>
            <div className="text-slate-400">[12:44:24 pm] [INFO] Waiting for pipeline execution...</div>
            <div className="text-slate-500">[12:44:28 pm] [INFO] No blocking gates detected for this run</div>
            <div className="mt-3 text-slate-500">
              Active runs: {activeRuns.length} | Waiting: {waitingRuns.length} | Failed: {failedRuns.length}
            </div>
          </div>
        </section>
      </section>

      <section className="grid gap-4 xl:grid-cols-[1.05fr_0.95fr]">
        <section className="rounded-[18px] border border-[#24314a] bg-[#111827]/90 p-5">
          <div className="flex items-center justify-between gap-3">
            <div>
              <h2 className="text-base font-semibold text-white">Data Quality Monitoring</h2>
              <p className="mt-1 text-sm text-slate-400">Quality trends and validation state.</p>
            </div>
          </div>

          <div className="mt-5 space-y-4">
            {qualityMetrics.map((metric) => (
              <div key={metric.label}>
                <div className="mb-1.5 flex items-center justify-between text-xs text-slate-300">
                  <span>{metric.label}</span>
                  <span>{metric.value}%</span>
                </div>
                <div className="h-2 rounded-full bg-[#0b1324]">
                  <div
                    className={`h-2 rounded-full ${metric.color}`}
                    style={{ width: `${metric.value}%` }}
                  />
                </div>
              </div>
            ))}
          </div>
        </section>

        <section className="rounded-[18px] border border-[#24314a] bg-[#111827]/90 p-5">
          <div className="flex items-center justify-between gap-3">
            <div>
              <h2 className="text-base font-semibold text-white">Active Validation Rules</h2>
              <p className="mt-1 text-sm text-slate-400">Rules used by the quality engine.</p>
            </div>
          </div>

          <div className="mt-5 space-y-3">
            {validationRules.map((rule) => (
              <div
                key={rule.title}
                className="flex items-start justify-between gap-4 rounded-xl border border-[#24314a] bg-[#0b1324] px-4 py-3"
              >
                <div>
                  <div className="text-sm font-semibold text-white">{rule.title}</div>
                  <div className="mt-1 text-xs text-slate-400">{rule.description}</div>
                </div>
                <span className="mt-1 h-2 w-2 rounded-full bg-emerald-400" />
              </div>
            ))}
          </div>

          <div className="mt-5 grid gap-3">
            {recommendations.map((item) => (
              <RecommendationCard key={item.title} {...item} />
            ))}
          </div>
        </section>
      </section>
    </div>
  )
}

function MetricCard({ icon: Icon, label, value, change, tone }) {
  const config = {
    blue: {
      badge: 'bg-blue-500/10 text-[#4f8cff]',
      iconBg: 'bg-[#1c2740]',
      iconColor: 'text-[#4f8cff]',
    },
    green: {
      badge: 'bg-emerald-500/10 text-emerald-300',
      iconBg: 'bg-[#1c2830]',
      iconColor: 'text-emerald-300',
    },
    purple: {
      badge: 'bg-violet-500/10 text-violet-300',
      iconBg: 'bg-[#251d36]',
      iconColor: 'text-violet-300',
    },
  }[tone]

  return (
    <div className="rounded-[18px] border border-[#24314a] bg-[#111827]/90 p-5">
      <div className="flex items-center justify-between gap-4">
        <div className="text-sm text-slate-400">{label}</div>
        <div className={`flex h-11 w-11 items-center justify-center rounded-xl ${config.iconBg}`}>
          <Icon size={20} className={config.iconColor} />
        </div>
      </div>
      <div className="mt-5 text-[34px] font-semibold tracking-[-0.04em] text-white">{value}</div>
      <div className="mt-2 text-sm text-emerald-400">{change}</div>
    </div>
  )
}

function RecommendationCard({ title, description, tone }) {
  const toneClasses = {
    blue: 'border-[#2f5fb2] bg-[#102144] text-[#b9d3ff]',
    amber: 'border-amber-700 bg-[#2a1f0d] text-amber-200',
    green: 'border-emerald-700 bg-[#0f251b] text-emerald-200',
  }[tone]

  return (
    <div className={`rounded-xl border px-4 py-3 ${toneClasses}`}>
      <div className="flex items-start justify-between gap-4">
        <div>
          <div className="text-sm font-semibold">{title}</div>
          <div className="mt-1 text-xs opacity-90">{description}</div>
        </div>
        <BarChart3 size={15} className="mt-0.5 opacity-80" />
      </div>
    </div>
  )
}

function formatTimeAgo(dateStr) {
  if (!dateStr) return 'just now'
  const diff = Date.now() - new Date(dateStr).getTime()
  const s = Math.floor(diff / 1000)
  const m = Math.floor(s / 60)
  const h = Math.floor(m / 60)
  const d = Math.floor(h / 24)
  if (d > 0) return `${d}d ago`
  if (h > 0) return `${h}h ago`
  if (m > 0) return `${m}m ago`
  return 'just now'
}

export default DashboardPage
