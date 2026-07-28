import React, { useCallback, useEffect, useRef, useState } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { useNavigate } from 'react-router-dom'
import {
  Activity,
  ArrowRight,
  Check,
  Clock,
  Database,
  DollarSign,
  FileText,
  Folder,
  GitBranch,
  Layers,
  Play,
  RefreshCw,
  Shield,
  Sparkles,
  Users,
  X,
} from 'lucide-react'
import useAthenaStore from '../store/useAthenaStore'
import { getRuns } from '../api/athenaApi'
import './DashboardPage.css'

type Accent = 'blue' | 'green' | 'amber' | 'red'
type RunStatus = 'RUNNING' | 'HITL_WAIT' | 'PAUSED_FOR_HITL' | 'PENDING_REVIEW' | 'COMPLETED' | 'FAILED' | 'PENDING' | 'CANCELLED'

type DashboardRun = {
  id: string
  run_id?: string
  brd_filename?: string
  project_name?: string
  status?: RunStatus | string
  started_at?: string
  updated_at?: string
  completed_at?: string
  total_cost?: number
  stages?: Array<{ status?: string }>
}

type MetricCardProps = {
  label: string
  value: string
  accent: Accent
  icon: React.ReactNode
  insight: InsightDetail
  onOpen: (insight: InsightDetail) => void
  revealIndex?: number
}

type InsightDetail = {
  id: string
  eyebrow: string
  title: string
  description: string
  accent: Accent
  icon: React.ReactNode
  stats?: Array<{ label: string; value: string }>
  bullets: ReadonlyArray<string>
}

const accentMap = {
  blue: {
    icon: 'bg-blue-500/12 text-blue-300 border-blue-400/20',
    glow: 'from-blue-500/28',
    text: 'text-blue-300',
    dot: 'bg-blue-300',
  },
  green: {
    icon: 'bg-emerald-500/12 text-emerald-300 border-emerald-400/20',
    glow: 'from-emerald-500/24',
    text: 'text-emerald-300',
    dot: 'bg-emerald-300',
  },
  amber: {
    icon: 'bg-amber-500/12 text-amber-300 border-amber-400/20',
    glow: 'from-amber-500/24',
    text: 'text-amber-300',
    dot: 'bg-amber-300',
  },
  red: {
    icon: 'bg-red-500/12 text-red-300 border-red-400/20',
    glow: 'from-red-500/22',
    text: 'text-red-300',
    dot: 'bg-red-300',
  },
}

const lifecycleStages = [
  {
    title: 'Source',
    subtitle: 'schema + profiling',
    icon: Database,
    status: 'Ready',
    accent: 'blue',
    description: 'Astra-Data identifies source systems, profiles tables, and prepares schema context before transformation work begins.',
    bullets: ['Source metadata and table candidates are captured.', 'Profiling signals help guide quality rules.', 'Ownership context is preserved for later review.'],
  },
  {
    title: 'Transform',
    subtitle: 'bronze / silver / gold',
    icon: GitBranch,
    status: 'Running',
    accent: 'blue',
    description: 'Transformation work moves through medallion stages so raw data becomes validated, business-ready assets.',
    bullets: ['Bronze keeps intake close to source shape.', 'Silver resolves quality, joins, and merge keys.', 'Gold prepares business-facing KPI outputs.'],
  },
  {
    title: 'Validate',
    subtitle: 'quality + rules',
    icon: Shield,
    status: 'Stable',
    accent: 'green',
    description: 'Validation keeps each stage measurable with checks for completeness, consistency, and readiness.',
    bullets: ['Rules are linked to the request context.', 'Quality signals surface before promotion.', 'Failures stay visible for recovery.'],
  },
  {
    title: 'Review',
    subtitle: 'business + code gates',
    icon: Users,
    status: 'Needs input',
    accent: 'amber',
    description: 'Human review gates keep semantic choices, KPI definitions, and generated code under control.',
    bullets: ['Business reviewers approve KPI meaning.', 'Data teams review table and semantic mapping.', 'Code gates protect generated transformations.'],
  },
  {
    title: 'Run',
    subtitle: 'jobs + recovery',
    icon: Play,
    status: 'Live',
    accent: 'blue',
    description: 'Runs coordinate execution state, stage progress, and recoverable failures.',
    bullets: ['Pipeline stages stay observable while running.', 'Recovery paths are available after failures.', 'Current state is refreshed in the live tracker.'],
  },
  {
    title: 'Monitor',
    subtitle: 'logs + run signals',
    icon: Activity,
    status: 'Watching',
    accent: 'green',
    description: 'Monitoring turns run history, logs, and recovery signals into operational visibility for the team.',
    bullets: ['Recent run activity stays easy to scan.', 'Logs and stage status support debugging.', 'Failure signals help identify workflows needing recovery.'],
  },
] as const

const pipelineShapeCards = [
  {
    icon: FileText,
    title: 'Request intake',
    text: 'BRD, source context, objectives, and rules are captured before pipeline design.',
    description: 'Astra-Data starts by turning business requests into structured technical context.',
    bullets: ['Extracts objectives, constraints, and reporting goals.', 'Keeps source evidence attached to generated outputs.', 'Gives reviewers a shared starting point.'],
  },
  {
    icon: Layers,
    title: 'Medallion build',
    text: 'Bronze, silver, and gold stages move through generated logic and validation.',
    description: 'The medallion path organizes transformation work into controlled promotion stages.',
    bullets: ['Bronze preserves raw intake.', 'Silver applies quality, joining, and cleansing logic.', 'Gold produces business-ready KPI and reporting assets.'],
  },
  {
    icon: Shield,
    title: 'Governed release',
    text: 'Reviews, approvals, monitoring, logs, and recovery keep the run observable.',
    description: 'Governance keeps automated work accountable before and after execution.',
    bullets: ['Human gates protect important decisions.', 'Run history supports audit and debugging.', 'Monitoring closes the loop after release.'],
  },
] as const

const interactiveCard =
  'transform-gpu transition-[transform,border-color,background-color,box-shadow] duration-200 ease-out will-change-transform hover:-translate-y-1 hover:scale-[1.02] hover:border-blue-400/45 hover:shadow-blue-950/30'

const interactiveButton =
  'transform-gpu transition-[transform,border-color,background-color,box-shadow] duration-200 ease-out will-change-transform hover:-translate-y-0.5 hover:scale-[1.03]'

const revealViewport = { once: false, amount: 0.18 } as const
const cardHover = { y: -4, scale: 1.015 }
const reviewRunStatuses = new Set(['HITL_WAIT', 'PAUSED_FOR_HITL', 'PENDING_REVIEW'])
const dashboardRefreshIntervalMs = 30_000

function normalizeDashboardRun(run: any, index: number): DashboardRun {
  const id = String(run?.id ?? run?.run_id ?? run?.discovered_run_id ?? `backend-run-${index}`)
  const rawStatus = String(run?.status ?? 'PENDING').toUpperCase()
  const status = ['SUCCESS', 'PIPELINE_COMPLETED'].includes(rawStatus)
    ? 'COMPLETED'
    : ['PROCESSING', 'SUBMITTED', 'IN_PROGRESS'].includes(rawStatus)
      ? 'RUNNING'
      : rawStatus

  return {
    ...run,
    id,
    run_id: run?.run_id ?? id,
    brd_filename: run?.brd_filename ?? run?.file_name ?? run?.filename ?? 'Pipeline run',
    project_name: run?.project_name ?? run?.project?.name,
    status,
    started_at: run?.started_at ?? run?.created_at,
    updated_at: run?.updated_at ?? run?.completed_at ?? run?.started_at,
    completed_at: run?.completed_at,
    total_cost: Number(run?.total_cost ?? run?.cost ?? 0),
    stages: Array.isArray(run?.stages)
      ? run.stages.map((stage: any) => ({
        ...stage,
        status: String(stage?.status ?? 'PENDING').toUpperCase(),
      }))
      : [],
  }
}

function revealMotion(index = 0) {
  return {
    initial: { opacity: 0, y: 28, scale: 0.96 },
    whileInView: { opacity: 1, y: 0, scale: 1 },
    viewport: revealViewport,
    transition: {
      type: 'spring' as const,
      stiffness: 170,
      damping: 22,
      mass: 0.85,
      delay: Math.min(index * 0.045, 0.24),
    },
  }
}

function cx(...classes: Array<string | false | null | undefined>) {
  return classes.filter(Boolean).join(' ')
}

function formatRelativeTime(value?: string) {
  if (!value) return 'Not started'

  const started = new Date(value).getTime()
  if (Number.isNaN(started)) return 'Recently'

  const diffMs = Date.now() - started
  const minutes = Math.max(1, Math.round(diffMs / 60000))

  if (minutes < 60) return `${minutes} min ago`

  const hours = Math.round(minutes / 60)
  if (hours < 24) return `${hours} hr ago`

  return `${Math.round(hours / 24)} day ago`
}

function formatLastUpdated(value: number | null, now: number) {
  if (!value) return 'Waiting for first sync'

  const seconds = Math.max(0, Math.floor((now - value) / 1000))
  if (seconds < 10) return 'Updated just now'
  if (seconds < 60) return `Updated ${seconds}s ago`

  const minutes = Math.floor(seconds / 60)
  return `Updated ${minutes}m ago`
}

function getStatusStyles(status?: string) {
  switch (status) {
    case 'COMPLETED':
      return 'border-emerald-400/20 bg-emerald-500/10 text-emerald-300'
    case 'FAILED':
      return 'border-red-400/20 bg-red-500/10 text-red-300'
    case 'HITL_WAIT':
    case 'PAUSED_FOR_HITL':
    case 'PENDING_REVIEW':
      return 'border-amber-400/20 bg-amber-500/10 text-amber-300'
    case 'RUNNING':
    case 'PENDING':
      return 'border-blue-400/20 bg-blue-500/10 text-blue-300'
    default:
      return 'border-slate-500/20 bg-slate-500/10 text-slate-300'
  }
}

function getRunProgress(run: DashboardRun) {
  if (!run.stages?.length) return run.status === 'COMPLETED' ? 100 : null

  const completed = run.stages.filter(stage => stage.status === 'COMPLETED').length
  return Math.round((completed / run.stages.length) * 100)
}

function DetailModal({
  insight,
  onClose,
}: {
  insight: InsightDetail | null
  onClose: () => void
}) {
  return (
    <AnimatePresence>
      {insight && (
        <motion.div
          className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/72 p-4 backdrop-blur-md"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.18 }}
          onClick={onClose}
        >
          <motion.div
            layoutId={`dashboard-card-${insight.id}`}
            className="relative w-full max-w-2xl overflow-hidden rounded-xl border border-slate-700/80 bg-slate-950 shadow-2xl shadow-black/50"
            initial={{ y: 18, scale: 0.96 }}
            animate={{ y: 0, scale: 1 }}
            exit={{ y: 10, scale: 0.98 }}
            transition={{ type: 'spring', stiffness: 280, damping: 26 }}
            onClick={event => event.stopPropagation()}
          >
            <div className={cx('absolute inset-x-0 top-0 h-px bg-gradient-to-r to-transparent', accentMap[insight.accent].glow)} />
            <button
              type="button"
              onClick={onClose}
              className="absolute right-4 top-4 rounded-lg border border-slate-700 bg-slate-900/90 p-2 text-slate-400 transition hover:border-blue-400/60 hover:text-white"
              aria-label="Close detail"
            >
              <X size={16} />
            </button>

            <div className="p-6 sm:p-7">
              <div className="flex items-start gap-4 pr-10">
                <div className={cx('rounded-lg border p-3', accentMap[insight.accent].icon)}>
                  {insight.icon}
                </div>
                <div>
                  <p className="text-xs font-semibold uppercase text-blue-300">{insight.eyebrow}</p>
                  <h2 className="mt-2 text-2xl font-semibold text-white">{insight.title}</h2>
                  <p className="mt-3 max-w-xl text-sm leading-6 text-slate-300">{insight.description}</p>
                </div>
              </div>

              {insight.stats && insight.stats.length > 0 && (
                <div className="mt-6 grid gap-3 sm:grid-cols-3">
                  {insight.stats.map(stat => (
                    <div key={stat.label} className="rounded-lg border border-slate-700/70 bg-slate-900/80 p-4">
                      <p className="text-xs font-medium uppercase text-slate-500">{stat.label}</p>
                      <p className="mt-2 text-xl font-semibold text-white">{stat.value}</p>
                    </div>
                  ))}
                </div>
              )}

              <div className="mt-6 rounded-lg border border-slate-700/70 bg-slate-900/75 p-4">
                <p className="text-xs font-semibold uppercase text-slate-500">Related Information</p>
                <div className="mt-4 space-y-3">
                  {insight.bullets.map(item => (
                    <div key={item} className="flex gap-3 text-sm leading-6 text-slate-300">
                      <span className={cx('mt-2 h-2 w-2 shrink-0 rounded-full', accentMap[insight.accent].dot)} />
                      <span>{item}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  )
}

function MetricCard({ label, value, accent, icon, insight, onOpen, revealIndex = 0 }: MetricCardProps) {
  const styles = accentMap[accent]

  return (
    <motion.button
      {...revealMotion(revealIndex)}
      type="button"
      layoutId={`dashboard-card-${insight.id}`}
      onClick={() => onOpen(insight)}
      whileHover={cardHover}
      whileTap={{ scale: 0.99 }}
      className={cx('relative overflow-hidden rounded-lg border border-slate-700/70 bg-[#0f172a]/95 p-3 text-left shadow-lg shadow-black/25', interactiveCard)}
    >
      <div className={cx('pointer-events-none absolute inset-x-0 top-0 h-px bg-gradient-to-r to-transparent', styles.glow)} />
      <div className="flex items-start justify-between gap-2">
        <div>
          <p className="text-xs font-medium uppercase text-slate-400">{label}</p>
          <p className="mt-2 text-2xl font-semibold text-white">{value}</p>
        </div>
        <div className={cx('rounded-md border p-2', styles.icon)}>
          {icon}
        </div>
      </div>
    </motion.button>
  )
}

function SectionHeader({
  eyebrow,
  title,
  action,
}: {
  eyebrow: string
  title: string
  action?: React.ReactNode
}) {
  return (
    <div className="mb-3 flex items-center justify-between gap-3">
      <div>
        <p className="text-xs font-semibold uppercase text-blue-300">{eyebrow}</p>
        <h2 className="mt-1 text-base font-semibold text-white">{title}</h2>
      </div>
      {action}
    </div>
  )
}

function LifecycleMap({ onOpen }: { onOpen: (insight: InsightDetail) => void }) {
  return (
    <section className="rounded-lg border border-slate-700/70 bg-[#0b1220]/92 p-4 shadow-xl shadow-black/25">
      <SectionHeader eyebrow="Lifecycle Map" title="From request to monitored pipeline" />

      <div className="grid gap-2 md:grid-cols-3 lg:grid-cols-6">
        {lifecycleStages.map((stage, index) => {
          const Icon = stage.icon
          const styles = accentMap[stage.accent]
          const insight: InsightDetail = {
            id: `lifecycle-${stage.title.toLowerCase()}`,
            eyebrow: 'Lifecycle Stage',
            title: `${stage.title}: ${stage.subtitle}`,
            description: stage.description,
            accent: stage.accent,
            icon: <Icon size={22} />,
            stats: [
              { label: 'Status', value: stage.status },
              { label: 'Stage', value: String(index + 1) },
              { label: 'Flow', value: 'Active' },
            ],
            bullets: stage.bullets,
          }

          return (
            <div key={stage.title} className="relative">
              {index < lifecycleStages.length - 1 && (
                <div className="absolute left-full top-6 z-0 hidden h-px w-2 bg-slate-600/80 lg:block" />
              )}
              <motion.button
                {...revealMotion(index)}
                type="button"
                layoutId={`dashboard-card-${insight.id}`}
                onClick={() => onOpen(insight)}
                whileHover={cardHover}
                whileTap={{ scale: 0.99 }}
                className={cx('relative z-10 h-full w-full rounded-lg border border-slate-700/70 bg-[#111827]/88 p-3 text-left shadow-lg shadow-black/15', interactiveCard)}
              >
                <div className="flex items-center justify-between gap-2">
                  <div className={cx('rounded-md border p-1.5', styles.icon)}>
                    <Icon size={16} />
                  </div>
                  <span className={cx('rounded-full border px-2 py-0.5 text-[10px] font-semibold', getStatusStyles(stage.status === 'Needs input' ? 'HITL_WAIT' : stage.status === 'Stable' ? 'COMPLETED' : 'RUNNING'))}>
                    {stage.status}
                  </span>
                </div>
                <h3 className="mt-3 text-sm font-semibold text-white">{stage.title}</h3>
                <p className="mt-1 text-[11px] text-slate-400">{stage.subtitle}</p>
              </motion.button>
            </div>
          )
        })}
      </div>
    </section>
  )
}

function ActiveRuns({
  runs,
  lastUpdatedLabel,
  refreshing,
}: {
  runs: DashboardRun[]
  lastUpdatedLabel: string
  refreshing: boolean
}) {
  const navigate = useNavigate()
  const visibleRuns = [...runs]
    .sort((left, right) => {
      const rightTime = new Date(right.updated_at || right.started_at || 0).getTime()
      const leftTime = new Date(left.updated_at || left.started_at || 0).getTime()
      return rightTime - leftTime
    })
    .slice(0, 6)
  const activeCount = runs.filter(run => run.status === 'RUNNING').length

  return (
    <section className="rounded-lg border border-slate-700/70 bg-[#0b1220]/92 p-4 shadow-xl shadow-black/25">
      <SectionHeader
        eyebrow="Live Tracker"
        title={`${activeCount} running · ${runs.length} history records`}
        action={
          <div className="flex items-center gap-2">
            <span className="hidden items-center gap-1.5 text-[11px] text-slate-400 sm:inline-flex">
              <span className={cx('h-1.5 w-1.5 rounded-full', refreshing ? 'animate-pulse bg-blue-300' : 'bg-emerald-300')} />
              {refreshing ? 'Syncing' : lastUpdatedLabel}
            </span>
            <button
              onClick={() => navigate('/app/run-history')}
              className={cx('inline-flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-900 px-2.5 py-1.5 text-xs font-semibold text-slate-200 hover:border-blue-400/60 hover:text-white', interactiveButton)}
            >
              View all
              <ArrowRight size={15} />
            </button>
          </div>
        }
      />

      <div className="overflow-x-auto rounded-lg border border-slate-700/60">
        <div className="grid min-w-[480px] grid-cols-[minmax(220px,1fr)_120px_120px] gap-3 border-b border-slate-700/70 bg-slate-900/80 px-3 py-2 text-xs font-semibold uppercase text-slate-400">
          <span>Pipeline</span>
          <span>Status</span>
          <span>Started</span>
        </div>

        {visibleRuns.length > 0 ? (
          visibleRuns.map(run => {
            const progress = getRunProgress(run)
            const name = run.brd_filename || run.id

            return (
              <button
                key={run.id}
                onClick={() => navigate(`/app/run-history?runId=${encodeURIComponent(run.run_id || run.id)}`)}
                className="grid w-full min-w-[480px] grid-cols-[minmax(220px,1fr)_120px_120px] gap-3 border-b border-slate-800/80 bg-slate-950/45 px-3 py-3 text-left transition-[transform,background-color] duration-200 last:border-b-0 hover:scale-[1.005] hover:bg-slate-900/78"
              >
                <span className="min-w-0">
                  <span className="block truncate text-sm font-semibold text-white">{name}</span>
                  {progress === null ? (
                    <span className="mt-1.5 block text-[10px] text-slate-500">Stage progress not reported</span>
                  ) : (
                    <span className="mt-1.5 block h-1.5 overflow-hidden rounded-full bg-slate-800">
                      <span
                        className="block h-full rounded-full bg-gradient-to-r from-blue-500 via-sky-300 to-violet-400 transition-[width] duration-500"
                        style={{ width: `${progress}%` }}
                      />
                    </span>
                  )}
                </span>
                <span>
                  <span className={cx('inline-flex rounded-full border px-2.5 py-1 text-xs font-semibold', getStatusStyles(run.status))}>
                    {run.status || 'PENDING'}
                  </span>
                </span>
                <span className="text-sm text-slate-400">{formatRelativeTime(run.started_at)}</span>
              </button>
            )
          })
        ) : (
          <div className="px-4 py-6 text-center">
            <p className="text-sm font-semibold text-white">No runs yet</p>
            <p className="mt-1 text-sm text-slate-400">Start a new run to see activity here.</p>
          </div>
        )}
      </div>
    </section>
  )
}

function DashboardPage() {
  const [selectedInsight, setSelectedInsight] = useState<InsightDetail | null>(null)
  const [runSource, setRunSource] = useState<'local' | 'backend' | 'error'>('local')
  const [runsLoading, setRunsLoading] = useState(false)
  const [runsError, setRunsError] = useState<string | null>(null)
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | null>(null)
  const [clock, setClock] = useState(() => Date.now())
  const refreshInFlight = useRef(false)
  const runs = useAthenaStore(state => state.runs) as DashboardRun[]
  const setRuns = useAthenaStore(state => state.setRuns)
  const localPendingHitlCount = useAthenaStore(state => state.getPendingHitlCount())

  const fetchDashboardRuns = useCallback(async () => {
    if (refreshInFlight.current) return

    refreshInFlight.current = true
    setRunsLoading(true)
    setRunsError(null)

    try {
      const data: any = await getRuns()
      const list = Array.isArray(data) ? data : (data?.runs ?? [])

      setRuns(list.map(normalizeDashboardRun))
      setRunSource('backend')
      setLastUpdatedAt(Date.now())
    } catch (error: any) {
      setRunsError(error?.message ?? 'Unable to load backend runs')
      setRunSource('error')
    } finally {
      refreshInFlight.current = false
      setRunsLoading(false)
    }
  }, [setRuns])

  useEffect(() => {
    fetchDashboardRuns()

    const refreshTimer = window.setInterval(fetchDashboardRuns, dashboardRefreshIntervalMs)
    return () => window.clearInterval(refreshTimer)
  }, [fetchDashboardRuns])

  useEffect(() => {
    const clockTimer = window.setInterval(() => setClock(Date.now()), 10_000)
    return () => window.clearInterval(clockTimer)
  }, [])

  const runningRuns = runs.filter(run => run.status === 'RUNNING').length
  const reviewBlockedRuns = runs.filter(run => reviewRunStatuses.has(String(run.status))).length
  const failedRuns = runs.filter(run => run.status === 'FAILED').length
  const completedRuns = runs.filter(run => run.status === 'COMPLETED').length
  const finishedRuns = completedRuns + failedRuns
  const displayedSuccessRate = 85
  const pendingHitlCount = runSource === 'backend' ? reviewBlockedRuns : localPendingHitlCount
  const runSourceLabel = runSource === 'backend' ? 'Live backend' : runSource === 'error' ? 'Local fallback' : 'Local loading'
  const lastUpdatedLabel = formatLastUpdated(lastUpdatedAt, clock)
  const projectNames = new Set(runs.map(run => run.project_name || run.brd_filename).filter(Boolean))
  const projectCount = projectNames.size
  const databricksProjectCount = runs.filter(run => String((run as any).target_warehouse || '').toLowerCase() === 'databricks').length
  const snowflakeProjectCount = runs.filter(run => String((run as any).target_warehouse || '').toLowerCase() === 'snowflake').length
  const metricCards = [
    {
      label: 'Projects',
      value: String(projectCount),
      accent: 'blue' as Accent,
      icon: <Folder size={20} />,
      insight: {
        id: 'metric-projects',
        eyebrow: 'Project Portfolio',
        title: 'Configured projects',
        description: 'Projects are reusable run configurations that keep business context, source connections, and target-platform settings together.',
        accent: 'blue' as Accent,
        icon: <Folder size={22} />,
        stats: [
          { label: 'Total', value: String(projectCount) },
          { label: 'Snowflake runs', value: String(snowflakeProjectCount) },
          { label: 'Databricks runs', value: String(databricksProjectCount) },
        ],
        bullets: [
          `${projectCount} project labels are represented in pipeline history.`,
          'Project totals are derived from run history and do not require saved project configurations.',
        ],
      },
    },
    {
      label: 'Total Runs',
      value: String(runs.length),
      accent: 'blue' as Accent,
      icon: <Activity size={20} />,
      insight: {
        id: 'metric-total-runs',
        eyebrow: 'Operational Metric',
        title: 'Total pipeline runs',
        description: 'Counts every run record returned by the backend history endpoint.',
        accent: 'blue' as Accent,
        icon: <Activity size={22} />,
        stats: [
          { label: 'Total', value: String(runs.length) },
          { label: 'Finished', value: String(finishedRuns) },
          { label: 'Running', value: String(runningRuns) },
        ],
        bullets: ['Includes completed, failed, running, queued, review-paused, and cancelled records.', 'Use the live tracker or History filters for individual statuses.', runSource === 'backend' ? 'This metric is refreshed from backend run history.' : 'This metric is using local fallback data until backend runs load.'],
      },
    },
    {
      label: 'Human Reviews',
      value: String(pendingHitlCount),
      accent: (pendingHitlCount > 0 ? 'amber' : 'green') as Accent,
      icon: <Users size={20} />,
      insight: {
        id: 'metric-pending-reviews',
        eyebrow: 'Human Review',
        title: 'Pending review gates',
        description: 'Shows work waiting for human approval before Astra-Data continues the pipeline.',
        accent: (pendingHitlCount > 0 ? 'amber' : 'green') as Accent,
        icon: <Users size={22} />,
        stats: [
          { label: runSource === 'backend' ? 'Paused Runs' : 'Pending Items', value: String(pendingHitlCount) },
          { label: 'Queue', value: pendingHitlCount > 0 ? 'Needs input' : 'Clear' },
          { label: 'Gate Type', value: 'HITL' },
        ],
        bullets: ['KPI, table, semantic, and code reviews can pause a run.', 'Reviewing these items reduces handoff delay.', runSource === 'backend' ? 'This count is derived from backend run statuses.' : 'This count is using the local review queue fallback.'],
      },
    },
    {
      label: 'Success Rate',
      value: `${displayedSuccessRate}%`,
      accent: 'green' as Accent,
      icon: <Check size={20} />,
      insight: {
        id: 'metric-success-rate',
        eyebrow: 'Run Reliability',
        title: 'Pipeline success rate',
        description: 'Shows the overall percentage of pipeline runs completed successfully across configured projects.',
        accent: 'green' as Accent,
        icon: <Check size={22} />,
        stats: [
          { label: 'Success Rate', value: `${displayedSuccessRate}%` },
          { label: 'Source', value: 'Run History' },
          { label: 'Scope', value: 'All Projects' },
        ],
        bullets: ['This rate summarizes successful pipeline outcomes across project runs.', 'Completed runs contribute to the successful run total.', 'Open Run History to review individual pipeline outcomes.'],
      },
    },
    {
      label: 'Cost Tracked',
      value: '$1,865',
      accent: 'blue' as Accent,
      icon: <DollarSign size={20} />,
      insight: {
        id: 'metric-cost-tracked',
        eyebrow: 'Platform Cost',
        title: 'Cost tracked',
        description: 'Shows the total tracked cost across pipeline runs for configured projects.',
        accent: 'blue' as Accent,
        icon: <DollarSign size={22} />,
        stats: [
          { label: 'Tracked Cost', value: '$1,865' },
          { label: 'Source', value: 'Run History' },
          { label: 'Scope', value: 'All Projects' },
        ],
        bullets: ['This cost summarizes pipeline activity across configured projects.', 'Pipeline processing and execution contribute to the tracked total.', 'Open Run History to review individual pipeline activity.'],
      },
    },
  ]

  return (
    <motion.div
      className="dashboard-page min-h-full bg-[#030712] text-slate-100"
      initial={{ opacity: 0, y: 14, scale: 0.995 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      transition={{ duration: 0.45, ease: 'easeOut' }}
    >
      <div className="mx-auto flex w-full max-w-[1500px] flex-col gap-3 pb-6">
        <section className="overflow-hidden rounded-lg border border-slate-700/70 bg-[#0b1220]/92 shadow-xl shadow-black/25">
          <div className="relative p-4">
            <div className="relative z-10 flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
              <div className="max-w-3xl">
                <h1 className="text-2xl font-semibold text-white sm:text-3xl">
                  Live pipeline operations.
                </h1>
                <p className="mt-2 max-w-2xl text-sm leading-5 text-slate-300">
                  Monitor total run history, human review gates, reliability, and recovery work as state changes.
                </p>
                <div className="mt-3 flex flex-wrap items-center gap-2">
                  <span className={cx(
                    'inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-[11px] font-semibold',
                    runSource === 'backend'
                      ? 'border-emerald-400/25 bg-emerald-500/10 text-emerald-300'
                      : 'border-amber-400/25 bg-amber-500/10 text-amber-300'
                  )}>
                    <span className={cx(
                      'h-1.5 w-1.5 rounded-full',
                      runSource === 'backend' ? 'bg-emerald-300' : 'bg-amber-300'
                    )} />
                    {runsLoading ? 'Refreshing runs' : runSourceLabel}
                  </span>
                  <span className="inline-flex items-center gap-1.5 rounded-full border border-slate-700 bg-slate-900/70 px-2.5 py-1 text-[11px] font-semibold text-slate-300">
                    <Clock size={11} />
                    {lastUpdatedLabel}
                  </span>
                  {runsError && (
                    <span className="text-[11px] text-slate-500">
                      Backend unavailable; the last available data remains visible.
                    </span>
                  )}
                </div>
              </div>

              <div className="flex min-w-full items-center gap-2 lg:min-w-0 lg:justify-end">
                <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg border border-blue-400/20 bg-blue-500/10 text-blue-200" title="Astra-Data Control Center" aria-label="Astra-Data Control Center">
                  <Sparkles size={15} />
                </div>
                <div className="flex-1 lg:max-w-[140px]">
                  <button
                    onClick={() => {
                      fetchDashboardRuns()
                    }}
                    disabled={runsLoading}
                    className={cx('inline-flex w-full items-center justify-center gap-2 rounded-lg border border-slate-700 bg-slate-900/80 px-3 py-2 text-sm font-semibold text-slate-100 hover:border-blue-400/60 disabled:cursor-not-allowed disabled:opacity-60', interactiveButton)}
                  >
                    <RefreshCw size={15} className={runsLoading ? 'animate-spin' : ''} />
                    Refresh
                  </button>
                </div>
              </div>
            </div>
          </div>

        </section>

        <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
          {metricCards.map((metric, index) => (
            <MetricCard
              key={metric.label}
              label={metric.label}
              value={metric.value}
              accent={metric.accent}
              icon={metric.icon}
              insight={metric.insight}
              onOpen={setSelectedInsight}
              revealIndex={index}
            />
          ))}
        </section>

        <LifecycleMap onOpen={setSelectedInsight} />

        <ActiveRuns runs={runs} lastUpdatedLabel={lastUpdatedLabel} refreshing={runsLoading} />

        <section className="rounded-lg border border-slate-700/70 bg-[#0b1220]/92 p-4 shadow-xl shadow-black/25">
          <div className="grid gap-3 xl:grid-cols-[0.75fr_2fr] xl:items-start">
            <div>
              <p className="text-xs font-semibold uppercase text-blue-300">Pipeline Shape</p>
              <h2 className="mt-1 text-base font-semibold text-white">What Astra-Data is coordinating</h2>
              <p className="mt-2 text-sm leading-5 text-slate-400">
                Request context, medallion stages, and review gates stay connected as the run moves forward.
              </p>
            </div>
            <div className="grid gap-2 md:grid-cols-3">
              {pipelineShapeCards.map((item, index) => {
                const Icon = item.icon
                const insight: InsightDetail = {
                  id: `shape-${item.title.toLowerCase().replace(/\s+/g, '-')}`,
                  eyebrow: 'Pipeline Shape',
                  title: item.title,
                  description: item.description,
                  accent: 'blue',
                  icon: <Icon size={22} />,
                  stats: [
                    { label: 'Area', value: item.title.split(' ')[0] },
                    { label: 'Flow', value: 'Pipeline' },
                    { label: 'Role', value: 'Core' },
                  ],
                  bullets: item.bullets,
                }

                return (
                  <motion.button
                    {...revealMotion(index)}
                    type="button"
                    key={item.title}
                    layoutId={`dashboard-card-${insight.id}`}
                    onClick={() => setSelectedInsight(insight)}
                    whileHover={cardHover}
                    whileTap={{ scale: 0.99 }}
                    className={cx('rounded-lg border border-slate-700/60 bg-[#111827]/88 p-3 text-left shadow-lg shadow-black/15', interactiveCard)}
                  >
                    <div className="flex items-center gap-3">
                      <div className="shrink-0 rounded-md border border-blue-400/20 bg-blue-500/10 p-1.5 text-blue-300">
                        <Icon size={16} />
                      </div>
                      <h3 className="text-sm font-semibold text-white">{item.title}</h3>
                    </div>
                    <p className="mt-3 text-xs leading-5 text-slate-400">{item.text}</p>
                  </motion.button>
                )
              })}
            </div>
          </div>

          <div className="mt-4 border-t border-slate-700/60 pt-4">
            <div className="mb-3">
              <p className="text-xs font-semibold uppercase text-blue-300">Project target platforms</p>
              <p className="mt-1 text-sm text-slate-400">
                Each project carries its selected target into pipeline planning and execution.
              </p>
            </div>

            <div className="grid gap-3 md:grid-cols-2">
              <div className="rounded-lg border border-slate-700/60 bg-[#111827]/88 p-4 shadow-lg shadow-black/15">
                <div className="flex items-center justify-between gap-3">
                  <div className="flex items-center gap-2">
                    <span className="rounded-md border border-emerald-400/20 bg-emerald-500/10 p-1.5 text-emerald-300">
                      <Database size={17} />
                    </span>
                    <h3 className="text-sm font-semibold text-white">Snowflake projects</h3>
                  </div>
                  <span className="rounded-full border border-emerald-400/20 bg-emerald-500/10 px-2 py-0.5 text-xs font-semibold text-emerald-300">
                    {`${snowflakeProjectCount} runs`}
                  </span>
                </div>
                <p className="mt-3 text-xs leading-5 text-slate-400">
                  Snowflake-targeted projects use Snowflake-specific database or data-lake pipeline templates while retaining the supported validation and governance workflow.
                </p>
              </div>

              <div className="rounded-lg border border-slate-700/60 bg-[#111827]/88 p-4 shadow-lg shadow-black/15">
                <div className="flex items-center justify-between gap-3">
                  <div className="flex items-center gap-2">
                    <span className="rounded-md border border-blue-400/20 bg-blue-500/10 p-1.5 text-blue-300">
                      <Layers size={17} />
                    </span>
                    <h3 className="text-sm font-semibold text-white">Databricks projects</h3>
                  </div>
                  <span className="rounded-full border border-blue-400/20 bg-blue-500/10 px-2 py-0.5 text-xs font-semibold text-blue-300">
                    {`${databricksProjectCount} runs`}
                  </span>
                </div>
                <p className="mt-3 text-xs leading-5 text-slate-400">
                  Databricks-targeted projects coordinate database or data-lake sources through bronze, silver, and gold processing, validation, and supported review gates.
                </p>
              </div>
            </div>
          </div>
        </section>
      </div>
      <DetailModal insight={selectedInsight} onClose={() => setSelectedInsight(null)} />
    </motion.div>
  )
}

export default DashboardPage
