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
      keys: ['bronze'],
    },
    {
      id: 'phase-4',
      label: 'Silver Layer (Transformation)',
      keys: ['silver'],
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
      keys: ['pre_bronze', 'bronze', 'gate4', 'pull', 'bronze_validation'],
    },
    {
      id: 'phase-4',
      label: 'Silver Layer (Transformation)',
      keys: ['silver', 'gate5', 'dq_validation'],
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
    return run.pipeline_steps.map((step) => ({
      ...step,
      state: normalizeState(step.state),
    })) as PipelineStep[]
  }
  if (Array.isArray(run?.stages) && run.stages.length) {
    return run.stages.map((stage) => ({
      key: stage.key,
      label: stage.name || fallbackStepLabel(stage.key),
      detail: stage.error || '',
      state: normalizeState(stage.status),
      complete: normalizeState(stage.status) === 'COMPLETED',
    })) as PipelineStep[]
  }
  return [] as PipelineStep[]
}

export function getPhaseGroups(run) {
  const sourceType = isFileSource(run) ? 'file' : 'database'
  const templates = PIPELINE_PHASE_TEMPLATES[sourceType]
  const steps = getPipelineSteps(run)
  const byKey = new Map<string, PipelineStep>(steps.map((step) => [step.key, step]))

  return templates.map((phase) => {
    const phaseSteps: PipelineStep[] = phase.keys.map((key) => {
      const step = byKey.get(key)
      return (
        step || {
          key,
          label: fallbackStepLabel(key),
          detail: '',
          state: 'PENDING',
          complete: false,
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
  if (value === 'review' || value === 'waiting' || value === 'hitl_wait' || value === 'paused_for_hitl') return 'amber'
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
    nomination: 'Table Nomination',
    gate2: 'Source Review',
    discovery: 'Metadata Discovery',
    schema: 'Schema Snapshot',
    profiling: 'Column Profiling',
    enrichment: 'Semantic Enrichment',
    gate3: 'Semantic Review',
    pre_bronze: 'Pre-Bronze Readiness',
    bronze: 'Bronze Scripts',
    gate4: 'Bronze Review',
    pull: 'SFTP Pull',
    bronze_validation: 'Bronze Validation',
    silver: 'Silver Scripts',
    gate5: 'Silver Review',
    dq_validation: 'DQ Validation',
    gold: 'Gold Scripts',
  }
  return labels[key] || key
}
