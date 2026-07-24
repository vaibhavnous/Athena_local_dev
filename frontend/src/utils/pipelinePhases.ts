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

  if (['feed discovery', 'discover source objects', 'feed nomination', 'schema snapshot'].includes(normalizedLabel)) return cleanLabel
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
      keys: ['ingestion', 'memory', 'requirements', 'kpis', 'gate1'],
    },
    {
      id: 'phase-2',
      label: 'Feed & Metadata Intelligence',
      keys: [
        'feed_discovery',
        'feed_nomination',
        'gate2',
        'column_extraction',
        'freshness_check',
        'column_profiling',
        'semantic_enrichment',
        'gate3',
        'plan_seal',
      ],
    },
    {
      id: 'phase-3',
      label: 'Metadata Bootstrap & Source Validation',
      keys: [
        'metadata_bootstrap',
        'metadata_codegen',
        'gate4_metadata',
        'runtime_config',
        'validate_source',
        'discover_source_objects',
        'stage_to_landing',
      ],
    },
    {
      id: 'phase-4',
      label: 'Bronze Layer (Ingestion & DQ)',
      keys: ['bronze_autoloader', 'bronze_dq'],
    },
    {
      id: 'phase-5',
      label: 'Silver Layer (Transformation & DQ)',
      keys: ['bronze_to_silver', 'silver_dq'],
    },
    {
      id: 'phase-6',
      label: 'Gold Layer & Deployment',
      keys: ['silver_to_gold', 'gold_dq', 'gate5_publish', 'finalize'],
    },
  ],
}

export function isFileSource(run) {
  return run?.source === 'sftp' || run?.source === 'adls_gen2'
}

const FILE_VISIBLE_STEP_GROUPS = [
  { key: 'feed_discovery', label: 'Feed Discovery', components: ['feed_discovery', 'discovery'] },
  { key: 'feed_nomination', label: 'Feed Nomination', components: ['feed_nomination', 'nomination'] },
  { key: 'gate2', label: 'Feed Review', components: ['gate2'] },
  { key: 'column_extraction', label: 'Column Extraction', components: ['column_extraction', 'schema'] },
  { key: 'freshness_check', label: 'Freshness Check', components: ['freshness_check', 'plan_freshness'] },
  { key: 'column_profiling', label: 'Column Profiling', components: ['column_profiling', 'profiling'] },
  { key: 'semantic_enrichment', label: 'Semantic Enrichment', components: ['semantic_enrichment', 'enrichment'] },
  { key: 'gate3', label: 'Semantic Review', components: ['gate3'] },
  { key: 'plan_seal', label: 'Plan Seal Check', components: ['plan_seal'] },
  { key: 'metadata_bootstrap', label: 'Bootstrap Metadata', components: ['metadata_bootstrap', 'pre_bronze_bootstrap_metadata'] },
  { key: 'metadata_codegen', label: 'Metadata Codegen', components: ['metadata_codegen', 'pre_bronze_metadata_codegen', 'bronze'] },
  { key: 'gate4_metadata', label: 'Metadata Codegen Review', components: ['gate4_metadata', 'pre_bronze_metadata_codegen_review', 'gate4', 'runtime_bundle_handoff'] },
  { key: 'runtime_config', label: 'Load Runtime Config', components: ['runtime_config', 'pre_bronze_runtime_config'] },
  { key: 'validate_source', label: 'Validate Source', components: ['validate_source', 'pre_bronze_validate_source'] },
  { key: 'discover_source_objects', label: 'Discover Source Objects', components: ['discover_source_objects', 'pre_bronze_discover_source_objects'] },
  { key: 'stage_to_landing', label: 'Stage To Landing', components: ['stage_to_landing', 'pre_bronze_stage_to_landing'] },
  { key: 'bronze_autoloader', label: 'Bronze Ingestion', components: ['bronze_autoloader', 'bronze_code_execution'] },
  { key: 'bronze_dq', label: 'Bronze Data Quality', components: ['bronze_dq', 'bronze_runtime_validation'] },
  { key: 'bronze_to_silver', label: 'Silver Transformation', components: ['bronze_to_silver', 'silver_merge_key_resolution', 'silver_merge_key_review', 'silver', 'silver_code_execution'] },
  { key: 'silver_dq', label: 'Silver Data Quality', components: ['silver_dq', 'silver_runtime_validation'] },
  { key: 'silver_to_gold', label: 'Gold Model Build', components: ['silver_to_gold', 'gold', 'gold_code_execution'] },
  { key: 'gold_dq', label: 'Gold Data Quality', components: ['gold_dq', 'gold_runtime_validation'] },
  { key: 'gate5_publish', label: 'Final Publish Review', components: ['gate5_publish', 'gate5', 'gold_review'] },
  { key: 'finalize', label: 'Finalize Run', components: ['finalize', 'final_publish'] },
]

function groupedFileStep(group, steps: PipelineStep[]): PipelineStep {
  const members = group.components
    .map((key) => steps.find((step) => step.key === key))
    .filter(Boolean)
  const direct = members.find((step) => step.key === group.key)
  if (direct) return { ...direct, label: group.label }
  const states = members.map((step) => normalizeState(step.state))
  const state =
    states.includes('FAILED') ? 'FAILED' :
    states.includes('HITL_WAIT') ? 'HITL_WAIT' :
    states.includes('RUNNING') ? 'RUNNING' :
    members.length > 0 && states.every((item) => item === 'COMPLETED') ? 'COMPLETED' :
    'PENDING'
  const active = members.find((step) => ['FAILED', 'HITL_WAIT', 'RUNNING'].includes(normalizeState(step.state)))
    || members[members.length - 1]
  return {
    key: group.key,
    label: group.label,
    detail: active?.detail || '',
    state,
    complete: state === 'COMPLETED',
  }
}

function collapseFileSteps(steps: PipelineStep[]): PipelineStep[] {
  const groupedComponentKeys = new Set(FILE_VISIBLE_STEP_GROUPS.flatMap((group) => group.components))
  const phaseOne = steps.filter((step) => !groupedComponentKeys.has(step.key))
  return [
    ...phaseOne,
    ...FILE_VISIBLE_STEP_GROUPS.map((group) => groupedFileStep(group, steps)),
  ]
}

export function fileVisibleStepKey(key: string): string {
  return FILE_VISIBLE_STEP_GROUPS.find((group) => group.components.includes(key))?.key || key
}

function resolvePipelineSteps(run, steps: PipelineStep[]): PipelineStep[] {
  const visibleSteps = isFileSource(run) ? collapseFileSteps(steps) : steps
  return withPendingReviewGate(
    run,
    clearStaleWaitingSteps(run, applyExternalExecutionState(run, visibleSteps)),
  )
}

export function getPipelineSteps(run) {
  if (Array.isArray(run?.pipeline_steps) && run.pipeline_steps.length) {
    const steps = run.pipeline_steps.map((step) => ({
      ...step,
      label: formatPipelineStepLabel(step.label, step.key),
      detail: step.detail || buildStepDetail(run, step.key, normalizeState(step.state), step.detail),
      state: normalizeState(step.state),
    })) as PipelineStep[]
    return resolvePipelineSteps(run, steps)
  }
  if (Array.isArray(run?.stages) && run.stages.length) {
    const steps = run.stages.map((stage) => ({
      key: stage.key,
      label: formatPipelineStepLabel(stage.name, stage.key),
      detail: stage.error || buildStepDetail(run, stage.key, normalizeState(stage.status), ''),
      state: normalizeState(stage.status),
      complete: normalizeState(stage.status) === 'COMPLETED',
    })) as PipelineStep[]
    return resolvePipelineSteps(run, steps)
  }
  return resolvePipelineSteps(run, [] as PipelineStep[])
}

function applyExternalExecutionState(run, steps: PipelineStep[]) {
  const runState = normalizeState(run?.status)
  const progress = run?.external_execution && typeof run.external_execution === 'object' ? run.external_execution : {}
  const rawProgressState = String(progress.status || '').trim()
  const progressState = rawProgressState ? normalizeState(rawProgressState) : ''
  const rawStageKey = String(progress.stage_key || run?.background_stage || '').trim()
  const stageKey = isFileSource(run) ? fileVisibleStepKey(rawStageKey) : rawStageKey
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
      label: fallbackStepLabel(stageKey, sourceType),
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
  const executionKeys = new Set([
    'bronze_code_execution',
    'silver_code_execution',
    'gold_code_execution',
    'bronze_autoloader',
    'bronze_to_silver',
    'silver_to_gold',
  ])
  const progressedStates = new Set(['RUNNING', 'HITL_WAIT', 'FAILED', 'COMPLETED', 'SUCCESS', 'PIPELINE_COMPLETED'])
  const progressedIndexes = steps
    .map((step) => indexByKey.get(step.key) ?? -1)
    .filter((index, itemIndex) => index >= 0 && progressedStates.has(normalizeState(steps[itemIndex]?.state)))

  if (!progressedIndexes.length) return steps
  const furthestProgressIndex = Math.max(...progressedIndexes)

  return steps.map((step) => {
    const stepIndex = indexByKey.get(step.key) ?? -1
    const state = normalizeState(step.state)
    if (
      stepIndex >= 0 &&
      stepIndex < furthestProgressIndex &&
      !executionKeys.has(step.key) &&
      ['PENDING', 'HITL_WAIT', 'RUNNING'].includes(state)
    ) {
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

  if (isFileSource(run)) {
    const visibleReviewKey =
      run?.next_review_key === 'silver_merge_key_review' ? 'bronze_to_silver' :
      run?.next_review_key === 'gold_review' ? 'gate5_publish' :
      gate === 4 ? 'gate4_metadata' :
      gate === 5 ? 'gate5_publish' :
      gate >= 1 && gate <= 3 ? `gate${gate}` :
      ''
    if (!visibleReviewKey) return steps
    return steps.map((step) => step.key === visibleReviewKey
      ? { ...step, state: 'HITL_WAIT', complete: false }
      : step)
  }

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
    if (steps.some((step) => step.key === 'gold_review')) {
      return steps.map((step) => step.key === 'gold_review'
        ? { ...step, state: 'HITL_WAIT', complete: false }
        : step)
    }
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
      label: fallbackStepLabel(gateKey, isFileSource(run) ? 'file' : 'database'),
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
  const orderedKeys = templates.flatMap((phase) => phase.keys)
  const indexByKey = new Map(orderedKeys.map((key, index) => [key, index]))
  const furthestProgressIndex = steps.reduce((furthest, step) => {
    const index = indexByKey.get(step.key) ?? -1
    return normalizeState(step.state) === 'PENDING' ? furthest : Math.max(furthest, index)
  }, -1)

  return templates.map((phase) => {
    const phaseSteps: PipelineStep[] = phase.keys.map((key) => {
      const step = byKey.get(key)
      const syntheticState = syntheticStepState(key, byKey, indexByKey, furthestProgressIndex)
      return (
        step || {
          key,
          label: fallbackStepLabel(key, sourceType),
          detail: '',
          state: syntheticState,
          complete: syntheticState === 'COMPLETED',
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
  const runIds = new Set([run.id, run.run_id].filter(Boolean).map(String))
  const runName = [run.brd_filename, run.display_name, run.project_name, run.project?.name]
    .map((value) => String(value || '').trim())
    .find((value) => value && !runIds.has(value))
  if (run.source === 'adls_gen2') {
    return runName || 'ADLS auto-discovery'
  }
  if (run.source === 'sftp') {
    return runName || 'SFTP file source'
  }
  return runName || 'BRD pipeline run'
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

function fallbackStepLabel(key, sourceType = 'database') {
  const labels = {
    ingestion: 'BRD Ingest',
    memory: 'Memory Check',
    requirements: 'Requirement Extraction',
    kpis: 'KPI Extraction',
    gate1: 'KPI Review',
    nomination: sourceType === 'file' ? 'Feed Nomination' : 'Table Extraction',
    gate2: sourceType === 'file' ? 'Feed Review' : 'Table Review',
    discovery: sourceType === 'file' ? 'Feed Discovery' : 'Column Extraction',
    schema: sourceType === 'file' ? 'Schema Snapshot' : 'Column Extraction',
    profiling: 'Column Profiling',
    enrichment: 'Semantic Enrichment',
    gate3: 'Semantic Review',
    bronze: 'Bronze Code Generation',
    gate4: 'Bronze Review',
    pre_bronze_bootstrap_metadata: 'Bootstrap Metadata',
    plan_seal: 'Seal Approved Plan',
    plan_freshness: 'Validate Plan Freshness',
    pre_bronze_metadata_codegen: 'Metadata Code Generation',
    pre_bronze_metadata_codegen_review: 'Metadata Code Review',
    runtime_bundle_handoff: 'Runtime Bundle Handoff',
    pre_bronze_runtime_config: 'Prepare Runtime Configuration',
    pre_bronze_validate_source: 'Validate Source Access',
    pre_bronze_discover_source_objects: 'Discover Source Objects',
    pre_bronze_stage_to_landing: 'Stage Files to Landing',
    bronze_code_execution: 'Bronze Code Execution',
    bronze_runtime_validation: 'Bronze Runtime Validation',
    silver_merge_key_resolution: 'Silver Merge Key Resolution',
    silver_merge_key_review: 'Silver Merge Key Review',
    silver: 'Silver Code Generation',
    gate5: sourceType === 'file' ? 'Silver Code Review' : 'Silver Review',
    silver_code_execution: 'Silver Code Execution',
    silver_runtime_validation: 'Silver Runtime Validation',
    gold: 'Gold Code Generation',
    gold_review: 'Gold Code Review',
    gold_code_execution: 'Gold Code Execution',
    gold_runtime_validation: 'Gold Runtime Validation',
    final_publish: 'Final Publish (Target Gate 5)',
    finalize: 'Finalize Run',
  }
  return labels[key] || key
}

function syntheticStepState(
  key,
  byKey: Map<string, PipelineStep>,
  indexByKey: Map<string, number>,
  furthestProgressIndex: number,
) {
  const keyIndex = indexByKey.get(key) ?? -1
  const executionKeys = new Set(['bronze_code_execution', 'silver_code_execution', 'gold_code_execution'])
  if (keyIndex >= 0 && keyIndex < furthestProgressIndex && !executionKeys.has(key)) return 'COMPLETED'

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
