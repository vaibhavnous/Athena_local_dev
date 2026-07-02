import { MOCK_RUNS } from '../data/mockData'

export const ENABLE_DEMO_FALLBACKS =
  String(process.env.REACT_APP_ENABLE_DEMO_FALLBACKS || '').toLowerCase() === 'true'

const ACTIVE_DEMO_RUN_ID = 'run_a3f8c2'
const FALLBACK_STARTED_AT_KEY = 'athena.demoFallbackStartedAt'
const configuredFallbackStageMs = Number(process.env.REACT_APP_DEMO_FALLBACK_STAGE_MS || 20000)
const DEMO_FALLBACK_STAGE_MS = Number.isFinite(configuredFallbackStageMs)
  ? Math.max(20000, configuredFallbackStageMs)
  : 20000

const PIPELINE_STAGE_ORDER = [
  { key: 'ingestion', label: 'BRD Ingest' },
  { key: 'memory', label: 'Memory Check' },
  { key: 'requirements', label: 'Requirement Extraction' },
  { key: 'kpis', label: 'KPI Extraction' },
  { key: 'gate1', label: 'KPI Review' },
  { key: 'nomination', label: 'Table Extraction' },
  { key: 'gate2', label: 'Table Review' },
  { key: 'discovery', label: 'Column Extraction' },
  { key: 'profiling', label: 'Column Profiling' },
  { key: 'enrichment', label: 'Semantic Enrichment' },
  { key: 'gate3', label: 'Semantic Review' },
  { key: 'bronze', label: 'Bronze Code Generation' },
  { key: 'gate4', label: 'Bronze Review' },
  { key: 'silver', label: 'Silver Code Generation' },
  { key: 'gate5', label: 'Silver Review' },
  { key: 'gold', label: 'Gold Code Generation' },
]

const ACTIVE_FALLBACK_SEQUENCE = PIPELINE_STAGE_ORDER.slice(0, 4)

let fallbackStartedAt: number | null = null

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value))
}

function getFallbackStartedAt() {
  if (fallbackStartedAt) return fallbackStartedAt

  if (typeof window !== 'undefined') {
    try {
      const rawValue = window.sessionStorage.getItem(FALLBACK_STARTED_AT_KEY)
      const storedValue = rawValue ? Number(rawValue) : 0
      if (Number.isFinite(storedValue) && storedValue > 0) {
        fallbackStartedAt = storedValue
        return fallbackStartedAt
      }

      fallbackStartedAt = Date.now()
      window.sessionStorage.setItem(FALLBACK_STARTED_AT_KEY, String(fallbackStartedAt))
      return fallbackStartedAt
    } catch {
      fallbackStartedAt = Date.now()
      return fallbackStartedAt
    }
  }

  fallbackStartedAt = Date.now()
  return fallbackStartedAt
}

function getActiveFallbackProgress() {
  const elapsedMs = Math.max(0, Date.now() - getFallbackStartedAt())
  const runningIndex = Math.floor(elapsedMs / DEMO_FALLBACK_STAGE_MS)
  const readyForReview = runningIndex >= ACTIVE_FALLBACK_SEQUENCE.length
  const currentStep = readyForReview ? null : ACTIVE_FALLBACK_SEQUENCE[runningIndex]
  const completedKeys = new Set(
    ACTIVE_FALLBACK_SEQUENCE
      .slice(0, readyForReview ? ACTIVE_FALLBACK_SEQUENCE.length : runningIndex)
      .map((step) => step.key)
  )

  return {
    completedKeys,
    currentKey: currentStep?.key || '',
    currentLabel: currentStep?.label || '',
    readyForReview,
    nextGate: readyForReview ? 1 : 0,
    status: readyForReview ? 'HITL_WAIT' : 'PROCESSING',
    resumeMessage: readyForReview
      ? 'KPI review is ready. The requirement intelligence phase completed and progress was saved.'
      : `${currentStep?.label || 'Pipeline stage'} is running. Progress is saved automatically.`,
  }
}

function buildDemoPipelineSteps(run: any, activeProgress: any = null) {
  if (run.id === ACTIVE_DEMO_RUN_ID && activeProgress) {
    return PIPELINE_STAGE_ORDER.map((step) => {
      let state = 'PENDING'
      if (activeProgress.completedKeys.has(step.key)) state = 'COMPLETED'
      if (step.key === activeProgress.currentKey) state = 'RUNNING'
      if (step.key === 'gate1' && activeProgress.readyForReview) state = 'HITL_WAIT'
      return { ...step, state }
    })
  }

  if (run.id === 'run_b7e1d3') {
    return PIPELINE_STAGE_ORDER.map((step) => ({ ...step, state: 'COMPLETED' }))
  }

  return PIPELINE_STAGE_ORDER.map((step) => ({
    ...step,
    state:
      step.key === 'ingestion' ? 'COMPLETED' :
      ['memory', 'requirements'].includes(step.key) ? 'FAILED' :
      'PENDING',
  }))
}

function buildDemoScripts(layer: 'bronze' | 'silver' | 'gold', run: any) {
  const tableName = run.id === 'run_b7e1d3' ? 'sales_pipeline' : 'claims'
  const path = `${layer}/${tableName}.py`
  const title = `${layer}_${tableName}`
  const bodies = {
    bronze: [
      '# Demo fallback Bronze script',
      `target_table = "bronze.${tableName}"`,
      'source_table = "source.claims"',
      'print(f"Loading {source_table} into {target_table}")',
    ].join('\n'),
    silver: [
      '# Demo fallback Silver script',
      `target_table = "silver.${tableName}_curated"`,
      `source_table = "bronze.${tableName}"`,
      'print(f"Curating {source_table} into {target_table}")',
    ].join('\n'),
    gold: [
      '# Demo fallback Gold script',
      `target_table = "gold.${tableName}_kpis"`,
      `source_table = "silver.${tableName}_curated"`,
      'print(f"Publishing analytics from {source_table} into {target_table}")',
    ].join('\n'),
  }

  return {
    run_id: run.id,
    scripts: [
      {
        script_path: path,
        target_table: `${layer}.${tableName}`,
        source_table: layer === 'bronze' ? 'source.claims' : `${layer === 'silver' ? 'bronze' : 'silver'}.${tableName}`,
        script_body: bodies[layer],
        ui_key: `${layer}|${path}`,
        title,
      },
    ],
  }
}

function adaptDemoRun(source: any) {
  const run = clone(source)
  const activeProgress = run.id === ACTIVE_DEMO_RUN_ID ? getActiveFallbackProgress() : null
  const pipeline_steps = buildDemoPipelineSteps(run, activeProgress)
  const next_gate = activeProgress ? activeProgress.nextGate : 0

  return {
    ...run,
    kpis: [],
    status: activeProgress?.status || run.status,
    source: run.source || 'database',
    is_demo_fallback: true,
    demo_review_fallback: run.id === ACTIVE_DEMO_RUN_ID,
    review_fallback_reason: run.id === ACTIVE_DEMO_RUN_ID ? 'Backend run hydration timed out. Saved run state is being used.' : undefined,
    resume_message: activeProgress?.resumeMessage || run.resume_message,
    next_gate,
    pipeline_steps,
    bronze: buildDemoScripts('bronze', run),
    silver: buildDemoScripts('silver', run),
    gold: buildDemoScripts('gold', run),
  }
}

export function getDemoRuns() {
  return MOCK_RUNS.map(adaptDemoRun)
}

export function getPrimaryDemoRun() {
  return getDemoRuns()[0] || null
}

export function isDemoFallbackRun(run: any) {
  return Boolean(run?.is_demo_fallback)
}
