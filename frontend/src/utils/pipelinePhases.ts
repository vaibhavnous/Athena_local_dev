type PipelineStep = {
  key: string
  label: string
  detail?: string
  state: string
  complete?: boolean
}

export function normalizeState(value: string | undefined) {
  const state = String(value || '').toUpperCase()
  if (state === 'COMPLETE' || state === 'EXTERNAL_COMPLETED') return 'COMPLETED'
  if (state === 'IN_PROGRESS' || state === 'PROCESSING' || state === 'SUBMITTED' || state === 'EXTERNAL_RUNNING' || state === 'EXTERNAL_WAITING') return 'RUNNING'
  if (state === 'PAUSED_FOR_HITL' || state === 'PENDING_REVIEW') return 'HITL_WAIT'
  if (state === 'SUCCESS' || state === 'PIPELINE_COMPLETED') return 'COMPLETED'
  if (state === 'EXTERNAL_FAILED') return 'FAILED'
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
      keys: ['bronze', 'gate4', 'bronze_code_execution'],
    },
    {
      id: 'phase-4',
      label: 'Silver Layer (Transformation)',
      keys: ['silver_merge_key_resolution', 'silver_merge_key_review', 'silver', 'gate5', 'silver_code_execution'],
    },
    {
      id: 'phase-5',
      label: 'Gold Layer (Analytics)',
      keys: ['gold', 'gold_code_execution'],
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
      keys: ['bronze', 'gate4', 'bronze_code_execution'],
    },
    {
      id: 'phase-4',
      label: 'Silver Layer (Transformation)',
      keys: ['silver_merge_key_resolution', 'silver_merge_key_review', 'silver', 'gate5', 'silver_code_execution'],
    },
    {
      id: 'phase-5',
      label: 'Gold Layer (Analytics)',
      keys: ['gold', 'gold_code_execution'],
    },
  ],
}

export function isFileSource(run) {
  return run?.source === 'sftp' || run?.source === 'adls_gen2'
}

export function getPipelineSteps(run) {
  if (Array.isArray(run?.pipeline_steps) && run.pipeline_steps.length) {
    const steps = run.pipeline_steps.map((step) => ({
      ...step,
      label: formatPipelineStepLabel(step.label, step.key),
      detail: step.detail || buildStepDetail(run, step.key, normalizeState(step.state), step.detail),
      state: normalizeState(step.state),
    })) as PipelineStep[]
    return withPendingReviewGate(run, clearStaleWaitingSteps(run, applyExternalExecutionState(run, steps)))
  }
  if (Array.isArray(run?.stages) && run.stages.length) {
    const steps = run.stages.map((stage) => ({
      key: stage.key,
      label: formatPipelineStepLabel(stage.name, stage.key),
      detail: stage.error || buildStepDetail(run, stage.key, normalizeState(stage.status), ''),
      state: normalizeState(stage.status),
      complete: normalizeState(stage.status) === 'COMPLETED',
    })) as PipelineStep[]
    return withPendingReviewGate(run, clearStaleWaitingSteps(run, applyExternalExecutionState(run, steps)))
  }
  return withPendingReviewGate(run, applyExternalExecutionState(run, [] as PipelineStep[]))
}

function applyExternalExecutionState(run, steps: PipelineStep[]) {
  const runState = normalizeState(run?.status)
  const progress = run?.external_execution && typeof run.external_execution === 'object' ? run.external_execution : {}
  const rawProgressState = String(progress.status || '').trim()
  const progressState = rawProgressState ? normalizeState(rawProgressState) : ''
  const stageKey = String(progress.stage_key || run?.background_stage || '').trim()
  if (!stageKey || runState !== 'RUNNING' || (progressState && progressState !== 'RUNNING')) return steps

  const sourceType = isFileSource(run) ? 'file' : 'database'
  const orderedKeys = PIPELINE_PHASE_TEMPLATES[sourceType].flatMap((phase) => phase.keys)
  const targetIndex = orderedKeys.indexOf(stageKey)
  if (targetIndex < 0) return steps

  let found = false
  const detail = String(progress.message || run?.resume_message || '').trim()
  const next = steps.map((step) => {
    const stepIndex = orderedKeys.indexOf(step.key)
    const state = normalizeState(step.state)
    if (step.key === stageKey) {
      found = true
      return {
        ...step,
        state: 'RUNNING',
        detail: detail || buildStepDetail(run, step.key, 'RUNNING', step.detail),
        complete: false,
      }
    }
    if (stepIndex >= 0 && stepIndex < targetIndex && state !== 'FAILED') {
      return {
        ...step,
        state: 'COMPLETED',
        complete: true,
      }
    }
    return step
  })

  if (found) return next
  return [
    ...next,
    {
      key: stageKey,
      label: fallbackStepLabel(stageKey),
      detail,
      state: 'RUNNING',
      complete: false,
    },
  ]
}

function clearStaleWaitingSteps(run, steps: PipelineStep[]) {
  const sourceType = isFileSource(run) ? 'file' : 'database'
  const orderedKeys = PIPELINE_PHASE_TEMPLATES[sourceType].flatMap((phase) => phase.keys)
  const indexByKey = new Map(orderedKeys.map((key, index) => [key, index]))
  const progressedStates = new Set(['RUNNING', 'HITL_WAIT', 'FAILED', 'COMPLETED', 'SUCCESS', 'PIPELINE_COMPLETED'])
  const progressedIndexes = steps
    .map((step) => indexByKey.get(step.key) ?? -1)
    .filter((index, itemIndex) => index >= 0 && progressedStates.has(normalizeState(steps[itemIndex]?.state)))

  if (!progressedIndexes.length) return steps
  const furthestProgressIndex = Math.max(...progressedIndexes)

  return steps.map((step) => {
    const stepIndex = indexByKey.get(step.key) ?? -1
    const state = normalizeState(step.state)
    if (stepIndex >= 0 && stepIndex < furthestProgressIndex && ['HITL_WAIT', 'RUNNING'].includes(state)) {
      return {
        ...step,
        state: 'COMPLETED',
        complete: true,
      }
    }
    return step
  })
}

function withPendingReviewGate(run, steps: PipelineStep[]) {
  const gate = Number(run?.next_gate || 0)
  const status = normalizeState(run?.status)
  const reviewReady = status === 'HITL_WAIT'
  if (!reviewReady) return steps

  if (run?.next_review_key === 'silver_merge_key_review') {
    if (steps.some((step) => step.key === 'silver_merge_key_review')) {
      return steps.map((step) => step.key === 'silver_merge_key_review'
        ? { ...step, state: 'HITL_WAIT', complete: false }
        : step)
    }
    return [
      ...steps,
      {
        key: 'silver_merge_key_review',
        label: 'Silver Merge Key Review',
        detail: buildStepDetail(run, 'silver_merge_key_review', 'HITL_WAIT', ''),
        state: 'HITL_WAIT',
        complete: false,
      },
    ]
  }

  if (run?.next_review_key === 'gold_review') {
    return steps.map((step) => step.key === 'gold_code_execution'
      ? { ...step, label: 'Gold Review & Execution', state: 'HITL_WAIT', complete: false }
      : step)
  }

  if (gate < 1 || gate > 5) return steps

  const gateKey = `gate${gate}`
  if (steps.some((step) => step.key === gateKey)) {
    return steps.map((step) => step.key === gateKey
      ? { ...step, state: 'HITL_WAIT', complete: false }
      : step)
  }

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

    const completed = phaseSteps.filter((step) => normalizeState(step.state) === 'COMPLETED').length
    const waiting = phaseSteps.find((step) => normalizeState(step.state) === 'HITL_WAIT')
    const running = phaseSteps.find((step) => normalizeState(step.state) === 'RUNNING')
    const failed = phaseSteps.find((step) => normalizeState(step.state) === 'FAILED')

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
  const state = normalizeState(status)
  if (value === 'done' || state === 'COMPLETED') return 'emerald'
  if (value === 'running' || state === 'RUNNING' || value === 'submitted') return 'blue'
  if (value === 'review' || value === 'waiting' || state === 'HITL_WAIT' || value === 'paused_for_stage_confirmation') return 'amber'
  if (state === 'FAILED') return 'red'
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
  const bronzeExecution = state('bronze_code_execution')
  const mergeReview = state('silver_merge_key_review')
  const silver = state('silver')
  const gate5 = state('gate5')
  const silverExecution = state('silver_code_execution')
  const gold = state('gold')
  const goldExecution = state('gold_code_execution')
  const progressed = (...states: string[]) => states.some((item) => ['RUNNING', 'HITL_WAIT', 'FAILED', 'COMPLETED'].includes(item))
  const silverProgressed = progressed(silver, gate5, silverExecution, gold, goldExecution)

  if (key === 'bronze') {
    if (progressed(bronzeExecution, mergeReview, silver, gate5, silverExecution, gold, goldExecution)) return 'COMPLETED'
  }
  if (key === 'gate4') {
    if (progressed(bronzeExecution, mergeReview, silver, gate5, silverExecution, gold, goldExecution)) return 'COMPLETED'
  }
  if (key === 'bronze_code_execution') {
    if (bronzeExecution === 'COMPLETED') return 'COMPLETED'
    if (bronze === 'RUNNING') return 'PENDING'
  }
  if (key === 'silver_merge_key_resolution') {
    if (bronzeExecution === 'COMPLETED' || mergeReview === 'HITL_WAIT' || mergeReview === 'COMPLETED' || silverProgressed) return 'COMPLETED'
  }
  if (key === 'silver_merge_key_review') {
    if (mergeReview === 'HITL_WAIT') return 'HITL_WAIT'
    if (mergeReview === 'COMPLETED' || silverProgressed) return 'COMPLETED'
  }
  if (key === 'silver') {
    if (progressed(gate5, silverExecution, gold, goldExecution)) return 'COMPLETED'
  }
  if (key === 'gate5') {
    if (progressed(silverExecution, gold, goldExecution)) return 'COMPLETED'
  }
  if (key === 'silver_code_execution') {
    if (progressed(gold, goldExecution)) return 'COMPLETED'
  }
  if (key === 'gold') {
    if (progressed(goldExecution)) return 'COMPLETED'
  }
  if (key === 'gold_code_execution') {
    if (goldExecution === 'COMPLETED') return 'COMPLETED'
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
    case 'silver_merge_key_review':
      if (state === 'HITL_WAIT') return 'Silver Merge Key Review is ready. Approve merge keys before Silver generation starts.'
      if (state === 'COMPLETED') return 'Silver merge keys were approved.'
      return 'Silver Merge Key Review opens after Bronze execution completes.'
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
      return 'Silver generation starts after Silver Merge Key Review approval.'
    case 'gold':
      if (state === 'COMPLETED') return 'Gold analytics scripts were generated.'
      if (state === 'RUNNING') return 'Generating Gold KPI scripts.'
      return 'Gold generation starts after Silver processing completes.'
    case 'bronze_code_execution':
      if (String(run?.target_warehouse || '').toLowerCase() === 'snowflake') {
        if (state === 'COMPLETED') return 'Approved Bronze scripts were executed in Snowflake.'
        if (state === 'RUNNING') return 'Executing approved Bronze scripts in Snowflake.'
        return 'Bronze execution starts immediately after Gate 4 approval for Snowflake runs.'
      }
      return 'UI-only marker: Bronze scripts are exported for external execution, not run inside Astra Data.'
    case 'silver_code_execution':
      if (String(run?.target_warehouse || '').toLowerCase() === 'snowflake') {
        if (state === 'COMPLETED') return 'Approved Silver scripts were executed in Snowflake.'
        if (state === 'RUNNING') return 'Executing approved Silver scripts in Snowflake.'
        return 'Silver execution starts immediately after Gate 5 approval for Snowflake runs.'
      }
      return 'UI-only marker: Silver scripts are exported for external execution, not run inside Astra Data.'
    case 'gold_code_execution':
      if (String(run?.target_warehouse || '').toLowerCase() === 'snowflake') {
        if (state === 'COMPLETED') return 'Generated Gold scripts were executed in Snowflake.'
        if (state === 'RUNNING') return 'Executing generated Gold scripts in Snowflake.'
        return 'Gold execution starts after Gold generation for Snowflake runs.'
      }
      return 'UI-only marker: Gold scripts are exported for external execution, not run inside Astra Data.'
    default:
      return existingDetail || ''
  }
}
