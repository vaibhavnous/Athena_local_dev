type PipelineStep = {
  key: string
  label: string
  detail?: string
  state: string
  complete?: boolean
}

function normalizeState(value: string | undefined) {
  const state = String(value || '').toUpperCase()
  if (state === 'COMPLETE') return 'COMPLETED'
  if (state === 'IN_PROGRESS' || state === 'PROCESSING') return 'RUNNING'
  if (state === 'PAUSED_FOR_HITL') return 'HITL_WAIT'
  if (state === 'SUCCESS' || state === 'PIPELINE_COMPLETED') return 'COMPLETED'
  return state || 'PENDING'
}

export function formatPipelineStepLabel(label?: string, key?: string) {
  const normalizedKey = String(key || '').toLowerCase()
  const cleanLabel = String(label || '').replace(/Stage \d+ — /, '').trim()
  const normalizedLabel = cleanLabel.toLowerCase()

  if (normalizedKey === 'ingestion' || normalizedLabel === 'ingestion') return 'BRD Ingest'
  if (normalizedKey === 'requirements' || normalizedLabel === 'req extract') return 'Requirement Extraction'
  if (normalizedKey === 'kpis' || normalizedLabel === 'kpi extract') return 'KPI Extraction'
  if (normalizedKey === 'nomination' || normalizedLabel === 'nomination' || normalizedLabel === 'table nomination') return 'Table Extraction'
  if (normalizedKey === 'discovery' || normalizedKey === 'schema' || normalizedLabel === 'metadata discovery') return 'Column Extraction'
  if (normalizedKey === 'profiling' || normalizedLabel === 'column profiling') return 'Column Profiling'
  if (normalizedKey === 'enrichment' || normalizedLabel === 'semantic enrichment') return 'Semantic Enrichment'
  if (normalizedKey === 'gate3' || normalizedLabel === 'enrichment review' || normalizedLabel === 'column review') return 'Semantic Review'
  return cleanLabel || fallbackStepLabel(normalizedKey)
}

export function getGateDisplayName(gate: number, sourceType?: string) {
  if (gate === 1) return 'KPI Review'
  if (gate === 2) return ['sftp', 'adls_gen2'].includes(String(sourceType || '').toLowerCase()) ? 'Feed Review' : 'Table Review'
  if (gate === 3) return 'Semantic Review'
  if (gate === 4) return 'Bronze Review'
  if (gate === 5) return 'Silver Review'
  return `Gate ${gate}`
}

export const PIPELINE_PHASE_TEMPLATES = {
  database: [
    {
      id: 'phase-1',
      label: 'Discovery & Requirement Intelligence',
      keys: ['ingestion', 'memory', 'requirements', 'kpis', 'gate1'],
    },
    {
      id: 'phase-2',
      label: 'Source & Metadata Intelligence',
      keys: ['nomination', 'gate2', 'discovery', 'profiling', 'enrichment', 'gate3'],
    },
    {
      id: 'phase-3',
      label: 'Bronze Layer (Ingestion)',
      keys: ['bronze', 'gate4'],
    },
    {
      id: 'phase-4',
      label: 'Silver Layer (Transformation)',
      keys: ['silver_merge_key_resolution', 'silver_merge_key_review', 'silver', 'gate5'],
    },
    {
      id: 'phase-5',
      label: 'Gold Layer (Analytics)',
      keys: ['gold'],
    },
  ],
  file: [
    {
      id: 'phase-1',
      label: 'Discovery & Requirement Intelligence',
      keys: ['ingestion', 'requirements', 'kpis', 'gate1'],
    },
    {
      id: 'phase-2',
      label: 'Source & Metadata Intelligence',
      keys: ['discovery', 'gate2', 'schema', 'profiling', 'enrichment', 'gate3'],
    },
    {
      id: 'phase-3',
      label: 'Bronze Layer (Ingestion)',
      keys: ['bronze', 'gate4'],
    },
    {
      id: 'phase-4',
      label: 'Silver Layer (Transformation)',
      keys: ['silver_merge_key_resolution', 'silver_merge_key_review', 'silver', 'gate5'],
    },
    {
      id: 'phase-5',
      label: 'Gold Layer (Analytics)',
      keys: ['gold'],
    },
  ],
}

export function isFileSource(run) {
  return run?.source === 'sftp' || run?.source === 'adls_gen2'
}

export function getPipelineSteps(run) {
  if (Array.isArray(run?.pipeline_steps) && run.pipeline_steps.length) {
    return withPendingReviewGate(run, run.pipeline_steps.map((step) => ({
      ...step,
      label: formatPipelineStepLabel(step.label, step.key),
      detail: step.detail || buildStepDetail(run, step.key, normalizeState(step.state), step.detail),
      state: normalizeState(step.state),
    })) as PipelineStep[])
  }
  if (Array.isArray(run?.stages) && run.stages.length) {
    return withPendingReviewGate(run, run.stages.map((stage) => ({
      key: stage.key,
      label: formatPipelineStepLabel(stage.name, stage.key),
      detail: stage.error || buildStepDetail(run, stage.key, normalizeState(stage.status), ''),
      state: normalizeState(stage.status),
      complete: normalizeState(stage.status) === 'COMPLETED',
    })) as PipelineStep[])
  }
  return withPendingReviewGate(run, [] as PipelineStep[])
}

function withPendingReviewGate(run, steps: PipelineStep[]) {
  const gate = Number(run?.next_gate || 0)
  if (gate < 1 || gate > 5) return steps

  const status = normalizeState(run?.status)
  const reviewReady = ['HITL_WAIT', 'PENDING_REVIEW', 'PAUSED_FOR_HITL'].includes(status)
  if (!reviewReady) return steps

  const gateKey = `gate${gate}`
  if (steps.some((step) => step.key === gateKey)) return steps

  return [
    ...steps,
    {
      key: gateKey,
      label: fallbackStepLabel(gateKey),
      detail: buildStepDetail(run, gateKey, 'HITL_WAIT', ''),
      state: 'HITL_WAIT',
      complete: false,
    },
  ]
}

export function getPhaseGroups(run, stepsOverride?) {
  const sourceType = isFileSource(run) ? 'file' : 'database'
  const templates = PIPELINE_PHASE_TEMPLATES[sourceType]
  const steps = Array.isArray(stepsOverride) ? stepsOverride : getPipelineSteps(run)
  const byKey = new Map<string, PipelineStep>(steps.map((step) => [step.key, step]))

  return templates.map((phase) => {
    const phaseSteps: PipelineStep[] = phase.keys.map((key) => {
      const step = byKey.get(key)
      return (
        step || {
          key,
          label: fallbackStepLabel(key),
          detail: '',
          state: syntheticStepState(key, byKey),
          complete: syntheticStepState(key, byKey) === 'COMPLETED',
        }
      )
    })

    const completed = phaseSteps.filter((step) => step.state === 'COMPLETED').length
    const waiting = phaseSteps.find((step) => step.state === 'HITL_WAIT')
    const running = phaseSteps.find((step) => step.state === 'RUNNING')
    const failed = phaseSteps.find((step) => step.state === 'FAILED')

    let status = 'Pending'
    if (failed) status = 'Failed'
    else if (waiting) status = 'Review'
    else if (running) status = 'Running'
    else if (completed === phaseSteps.length && phaseSteps.length > 0) status = 'Done'

    return {
      ...phase,
      steps: phaseSteps,
      completed,
      total: phaseSteps.length,
      status,
    }
  })
}

export function summarizeRunSource(run) {
  if (!run) return 'No source selected'
  if (run.source === 'adls_gen2') {
    return run.brd_filename || 'ADLS auto-discovery'
  }
  if (run.source === 'sftp') {
    return run.brd_filename || 'SFTP file source'
  }
  return run.brd_filename || 'BRD pipeline run'
}

export function statusTone(status) {
  const value = String(status || '').toLowerCase()
  if (value === 'done' || value === 'completed' || value === 'success' || value === 'pipeline_completed') return 'emerald'
  if (value === 'running' || value === 'processing' || value === 'submitted' || value === 'in_progress') return 'blue'
  if (value === 'review' || value === 'waiting' || value === 'hitl_wait' || value === 'paused_for_hitl' || value === 'paused_for_stage_confirmation') return 'amber'
  if (value === 'failed') return 'red'
  return 'slate'
}

function fallbackStepLabel(key) {
  const labels = {
    ingestion: 'BRD Ingest',
    memory: 'Memory Check',
    requirements: 'Requirement Extraction',
    kpis: 'KPI Extraction',
    gate1: 'KPI Review',
    nomination: 'Table Extraction',
    gate2: 'Table Review',
    discovery: 'Column Extraction',
    schema: 'Column Extraction',
    profiling: 'Column Profiling',
    enrichment: 'Semantic Enrichment',
    gate3: 'Semantic Review',
    bronze: 'Bronze Code Generation',
    gate4: 'Bronze Review',
    bronze_code_execution: 'Bronze Code Execution',
    silver_merge_key_resolution: 'Silver Merge Key Resolution',
    silver_merge_key_review: 'Silver Merge Key Review',
    silver: 'Silver Code Generation',
    gate5: 'Silver Review',
    silver_code_execution: 'Silver Code Execution',
    gold: 'Gold Code Generation',
    gold_code_execution: 'Gold Code Execution',
  }
  return labels[key] || key
}

function syntheticStepState(key, byKey: Map<string, PipelineStep>) {
  const state = (stepKey: string) => normalizeState(byKey.get(stepKey)?.state)
  const bronze = state('bronze')
  const gate4 = state('gate4')
  const silver = state('silver')
  const gate5 = state('gate5')
  const gold = state('gold')

  if (key === 'bronze_code_execution') {
    if (gate4 === 'COMPLETED' || bronze === 'COMPLETED') return 'COMPLETED'
    if (bronze === 'RUNNING') return 'PENDING'
  }
  if (key === 'silver_merge_key_resolution') {
    if (gate4 === 'COMPLETED' || gate4 === 'HITL_WAIT' || silver === 'RUNNING' || silver === 'COMPLETED') return 'COMPLETED'
  }
  if (key === 'silver_merge_key_review') {
    if (gate4 === 'COMPLETED' || silver === 'RUNNING' || silver === 'COMPLETED') return 'COMPLETED'
    if (gate4 === 'HITL_WAIT') return 'HITL_WAIT'
  }
  if (key === 'silver_code_execution') {
    if (gate5 === 'COMPLETED' || gold === 'RUNNING' || gold === 'COMPLETED') return 'COMPLETED'
  }
  if (key === 'gold_code_execution') {
    if (gold === 'COMPLETED') return 'COMPLETED'
  }
  return 'PENDING'
}

function buildStepDetail(run, key, state, existingDetail) {
  if (existingDetail) return existingDetail

  const nextGate = Number(run?.next_gate || 0)
  const resumeMessage = String(run?.resume_message || '').trim()
  const isFileSource = ['sftp', 'adls_gen2'].includes(String(run?.source || '').toLowerCase())
  const gateKeyMap = {
    gate1: 1,
    gate2: 2,
    gate3: 3,
    gate4: 4,
    gate5: 5,
  }

  const readyGateMessage = (gateLabel, fallback) => {
    if (resumeMessage && nextGate > 0 && gateKeyMap[key] === nextGate) return resumeMessage
    if (state === 'HITL_WAIT' || nextGate > 0) return fallback
    if (state === 'COMPLETED') return `${gateLabel} completed.`
    if (state === 'RUNNING') return `${gateLabel} is being prepared.`
    return `${gateLabel} will open automatically when it is ready.`
  }

  switch (key) {
    case 'gate1':
      return readyGateMessage('KPI review', 'KPI review is ready. Validate the extracted KPIs before the pipeline continues.')
    case 'gate2':
      return readyGateMessage(
        isFileSource ? 'Feed review' : 'Table review',
        isFileSource
          ? 'Feed review is ready. Confirm the discovered feeds before metadata discovery continues.'
          : 'Table review is ready. Confirm the nominated tables before metadata discovery continues.'
      )
    case 'gate3':
      return readyGateMessage('Semantic review', 'Semantic review is ready. Validate enriched column metadata before Bronze generation starts.')
    case 'gate4':
      return readyGateMessage('Bronze review', 'Bronze review is ready. Validate generated Bronze artifacts before Silver generation starts.')
    case 'gate5':
      return readyGateMessage('Silver review', 'Silver review is ready. Validate generated Silver artifacts before downstream validation continues.')
    case 'discovery':
      if (isFileSource) {
        if (state === 'COMPLETED') return 'The selected ADLS or SFTP source was scanned and feed candidates were identified.'
        if (state === 'RUNNING') return 'Scanning the selected ADLS or SFTP source and preparing feed candidates.'
        return 'Feed discovery begins after KPI review is approved.'
      }
      if (state === 'COMPLETED') return 'Column metadata was discovered for the approved source set.'
      if (state === 'RUNNING') return 'Collecting source metadata and column definitions.'
      return 'Metadata discovery begins after Gate 2 approval.'
    case 'profiling':
      if (state === 'COMPLETED') return 'Profiling metrics were captured for discovered columns.'
      if (state === 'RUNNING') return 'Computing source-side aggregates and profiling statistics.'
      return 'Column profiling starts after metadata discovery completes.'
    case 'enrichment':
      if (state === 'COMPLETED') return 'Semantic enrichment completed and is ready for review.'
      if (state === 'RUNNING') return 'Linking metadata to business meaning and downstream design hints.'
      return 'Semantic enrichment starts after profiling completes.'
    case 'bronze':
      if (state === 'COMPLETED') return 'Bronze scripts were generated.'
      if (state === 'RUNNING') return 'Generating Bronze ingestion artifacts.'
      return 'Bronze generation starts after Gate 3 approval.'
    case 'silver':
      if (state === 'COMPLETED') return 'Silver scripts were generated.'
      if (state === 'RUNNING') return 'Generating Silver transformation artifacts.'
      return 'Silver generation starts after Gate 4 approval.'
    case 'gold':
      if (state === 'COMPLETED') return 'Gold analytics scripts were generated.'
      if (state === 'RUNNING') return 'Generating Gold KPI scripts.'
      return 'Gold generation starts after Silver processing completes.'
    case 'bronze_code_execution':
      return 'UI-only marker: Bronze scripts are exported for external execution, not run inside Athena.'
    case 'silver_code_execution':
      return 'UI-only marker: Silver scripts are exported for external execution, not run inside Athena.'
    case 'gold_code_execution':
      return 'UI-only marker: Gold scripts are exported for external execution, not run inside Athena.'
    default:
      return existingDetail || ''
  }
}
