import { MOCK_RUNS } from '../data/mockData'

export const ENABLE_DEMO_FALLBACKS =
  String(process.env.REACT_APP_ENABLE_DEMO_FALLBACKS || '').toLowerCase() === 'true'

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value))
}

function buildDemoPipelineSteps(run: any) {
  if (run.id === 'run_a3f8c2') {
    return [
      { key: 'ingestion', label: 'BRD Ingestion', state: 'COMPLETED' },
      { key: 'memory', label: 'Memory Intelligence', state: 'COMPLETED' },
      { key: 'domain_knowledge', label: 'Domain Knowledge Check', state: 'COMPLETED' },
      { key: 'requirements', label: 'Requirement Extraction', state: 'COMPLETED' },
      { key: 'kpis', label: 'KPI Extraction', state: 'COMPLETED' },
      { key: 'gate1', label: 'KPI Review', state: 'HITL_WAIT' },
      { key: 'nomination', label: 'Table Extraction', state: 'PENDING' },
      { key: 'gate2', label: 'Table Review', state: 'PENDING' },
      { key: 'discovery', label: 'Column Extraction', state: 'PENDING' },
      { key: 'profiling', label: 'Column Profiling', state: 'PENDING' },
      { key: 'enrichment', label: 'Semantic Enrichment', state: 'PENDING' },
      { key: 'gate3', label: 'Semantic Review', state: 'PENDING' },
      { key: 'bronze', label: 'Bronze Code Generation', state: 'PENDING' },
      { key: 'gate4', label: 'Bronze Review', state: 'PENDING' },
      { key: 'silver', label: 'Silver Code Generation', state: 'PENDING' },
      { key: 'gate5', label: 'Silver Review', state: 'PENDING' },
      { key: 'gold', label: 'Gold Code Generation', state: 'PENDING' },
    ]
  }

  if (run.id === 'run_b7e1d3') {
    return [
      { key: 'ingestion', label: 'BRD Ingestion', state: 'COMPLETED' },
      { key: 'memory', label: 'Memory Intelligence', state: 'COMPLETED' },
      { key: 'domain_knowledge', label: 'Domain Knowledge Check', state: 'COMPLETED' },
      { key: 'requirements', label: 'Requirement Extraction', state: 'COMPLETED' },
      { key: 'kpis', label: 'KPI Extraction', state: 'COMPLETED' },
      { key: 'gate1', label: 'KPI Review', state: 'COMPLETED' },
      { key: 'nomination', label: 'Table Extraction', state: 'COMPLETED' },
      { key: 'gate2', label: 'Table Review', state: 'COMPLETED' },
      { key: 'discovery', label: 'Column Extraction', state: 'COMPLETED' },
      { key: 'profiling', label: 'Column Profiling', state: 'COMPLETED' },
      { key: 'enrichment', label: 'Semantic Enrichment', state: 'COMPLETED' },
      { key: 'gate3', label: 'Semantic Review', state: 'COMPLETED' },
      { key: 'bronze', label: 'Bronze Code Generation', state: 'COMPLETED' },
      { key: 'gate4', label: 'Bronze Review', state: 'COMPLETED' },
      { key: 'silver', label: 'Silver Code Generation', state: 'COMPLETED' },
      { key: 'gate5', label: 'Silver Review', state: 'COMPLETED' },
      { key: 'gold', label: 'Gold Code Generation', state: 'COMPLETED' },
    ]
  }

  return [
    { key: 'ingestion', label: 'BRD Ingestion', state: 'COMPLETED' },
    { key: 'memory', label: 'Memory Intelligence', state: 'FAILED' },
    { key: 'domain_knowledge', label: 'Domain Knowledge Check', state: 'PENDING' },
    { key: 'requirements', label: 'Requirement Extraction', state: 'FAILED' },
    { key: 'kpis', label: 'KPI Extraction', state: 'PENDING' },
    { key: 'gate1', label: 'KPI Review', state: 'PENDING' },
    { key: 'nomination', label: 'Table Extraction', state: 'PENDING' },
    { key: 'gate2', label: 'Table Review', state: 'PENDING' },
    { key: 'discovery', label: 'Column Extraction', state: 'PENDING' },
    { key: 'profiling', label: 'Column Profiling', state: 'PENDING' },
    { key: 'enrichment', label: 'Semantic Enrichment', state: 'PENDING' },
    { key: 'gate3', label: 'Semantic Review', state: 'PENDING' },
    { key: 'bronze', label: 'Bronze Code Generation', state: 'PENDING' },
    { key: 'gate4', label: 'Bronze Review', state: 'PENDING' },
    { key: 'silver', label: 'Silver Code Generation', state: 'PENDING' },
    { key: 'gate5', label: 'Silver Review', state: 'PENDING' },
    { key: 'gold', label: 'Gold Code Generation', state: 'PENDING' },
  ]
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
  const pipeline_steps = buildDemoPipelineSteps(run)
  const next_gate =
    run.id === 'run_a3f8c2' ? 1 : 0

  return {
    ...run,
    source: run.source || 'database',
    is_demo_fallback: true,
    demo_review_fallback: run.id === 'run_a3f8c2',
    review_fallback_reason: run.id === 'run_a3f8c2' ? 'Backend run hydration timed out. Demo data is being used.' : undefined,
    resume_message:
      run.id === 'run_a3f8c2'
        ? 'Demo fallback KPI review is ready. Backend hydration timed out, so the saved demo path is being shown.'
        : run.resume_message,
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
