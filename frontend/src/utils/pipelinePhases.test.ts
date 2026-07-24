import { getPhaseGroups, getPipelineSteps, summarizeRunSource } from './pipelinePhases'

const phaseState = (run: any, phaseId: string, stepKey: string) => {
  const phase = getPhaseGroups(run, getPipelineSteps(run)).find((item) => item.id === phaseId)
  return phase?.steps.find((step) => step.key === stepKey)?.state
}

test('renders Snowflake bronze execution as active without advancing Silver', () => {
  const run = {
    status: 'RUNNING',
    target_warehouse: 'snowflake',
    background_stage: 'bronze_code_execution',
    pipeline_steps: [
      { key: 'bronze', state: 'COMPLETED' },
      { key: 'gate4', state: 'COMPLETED' },
      { key: 'bronze_code_execution', state: 'RUNNING' },
      { key: 'silver_merge_key_review', state: 'PENDING' },
      { key: 'silver', state: 'PENDING' },
    ],
  }

  expect(phaseState(run, 'phase-3', 'bronze_code_execution')).toBe('RUNNING')
  expect(phaseState(run, 'phase-4', 'silver_merge_key_review')).toBe('PENDING')
  expect(phaseState(run, 'phase-4', 'silver')).toBe('PENDING')
})

test('promotes an existing merge-key step when the backend pauses for review', () => {
  const run = {
    status: 'HITL_WAIT',
    next_review_key: 'silver_merge_key_review',
    pipeline_steps: [
      { key: 'bronze_code_execution', state: 'COMPLETED' },
      { key: 'silver_merge_key_resolution', state: 'COMPLETED' },
      { key: 'silver_merge_key_review', state: 'PENDING' },
      { key: 'silver', state: 'PENDING' },
    ],
  }

  expect(phaseState(run, 'phase-4', 'silver_merge_key_review')).toBe('HITL_WAIT')
  expect(phaseState(run, 'phase-4', 'silver')).toBe('PENDING')
})

test('renders Silver and Gold execution frontiers independently', () => {
  const silverRun = {
    status: 'RUNNING',
    background_stage: 'silver_code_execution',
    pipeline_steps: [
      { key: 'silver_merge_key_review', state: 'COMPLETED' },
      { key: 'silver', state: 'COMPLETED' },
      { key: 'gate5', state: 'COMPLETED' },
      { key: 'silver_code_execution', state: 'RUNNING' },
      { key: 'gold', state: 'PENDING' },
    ],
  }
  const goldRun = {
    status: 'RUNNING',
    background_stage: 'gold_code_execution',
    pipeline_steps: [
      { key: 'silver_code_execution', state: 'COMPLETED' },
      { key: 'gold', state: 'COMPLETED' },
      { key: 'gold_code_execution', state: 'RUNNING' },
    ],
  }

  expect(phaseState(silverRun, 'phase-4', 'silver_code_execution')).toBe('RUNNING')
  expect(phaseState(silverRun, 'phase-5', 'gold')).toBe('PENDING')
  expect(phaseState(goldRun, 'phase-4', 'silver_code_execution')).toBe('COMPLETED')
  expect(phaseState(goldRun, 'phase-5', 'gold_code_execution')).toBe('RUNNING')
})

test('does not infer Silver generation or execution from a completed merge-key review', () => {
  const run = {
    status: 'RUNNING',
    pipeline_steps: [
      { key: 'bronze_code_execution', state: 'COMPLETED' },
      { key: 'silver_merge_key_resolution', state: 'COMPLETED' },
      { key: 'silver_merge_key_review', state: 'COMPLETED' },
      { key: 'silver', state: 'PENDING' },
      { key: 'gate5', state: 'PENDING' },
      { key: 'silver_code_execution', state: 'PENDING' },
    ],
  }

  expect(phaseState(run, 'phase-4', 'silver')).toBe('PENDING')
  expect(phaseState(run, 'phase-4', 'gate5')).toBe('PENDING')
  expect(phaseState(run, 'phase-4', 'silver_code_execution')).toBe('PENDING')
})

test('shows Gold execution as waiting while generated Gold code is under review', () => {
  const run = {
    status: 'HITL_WAIT',
    next_review_key: 'gold_review',
    pipeline_steps: [
      { key: 'gold', label: 'Gold Code Generation', state: 'COMPLETED' },
      { key: 'gold_code_execution', label: 'Gold Code Execution', state: 'PENDING' },
    ],
  }

  expect(getPipelineSteps(run).find((step) => step.key === 'gold_code_execution')).toMatchObject({
    label: 'Gold Review & Execution',
    state: 'HITL_WAIT',
  })
})

test('does not invent Bronze or Silver execution success from later Gold progress', () => {
  const run = {
    status: 'FAILED',
    pipeline_steps: [
      { key: 'bronze', state: 'PENDING' },
      { key: 'gate4', state: 'PENDING' },
      { key: 'bronze_code_execution', state: 'PENDING' },
      { key: 'silver_merge_key_resolution', state: 'PENDING' },
      { key: 'silver_merge_key_review', state: 'PENDING' },
      { key: 'silver', state: 'PENDING' },
      { key: 'gate5', state: 'PENDING' },
      { key: 'silver_code_execution', state: 'PENDING' },
      { key: 'gold', state: 'COMPLETED' },
      { key: 'gold_code_execution', state: 'FAILED' },
    ],
  }

  const phases = getPhaseGroups(run, getPipelineSteps(run))
  expect(phases.find((phase) => phase.id === 'phase-3')?.status).toBe('Pending')
  expect(phases.find((phase) => phase.id === 'phase-4')?.status).toBe('Pending')
  expect(phases.find((phase) => phase.id === 'phase-5')?.status).toBe('Failed')
})

test('uses the project name instead of rendering a run ID as the pipeline name', () => {
  expect(summarizeRunSource({
    id: 'run-6',
    run_id: 'run-6',
    brd_filename: 'run-6',
    project_name: 'Vialto',
    source: 'database',
  })).toBe('Vialto')
})

test('uses the six-phase SFTP and ADLS workflow without changing database phases', () => {
  const run = {
    source: 'adls_gen2',
    status: 'RUNNING',
    background_stage: 'bronze_code_execution',
    pipeline_steps: [
      { key: 'pre_bronze_bootstrap_metadata', state: 'COMPLETED' },
      { key: 'plan_seal', state: 'COMPLETED' },
      { key: 'plan_freshness', state: 'COMPLETED' },
      { key: 'pre_bronze_metadata_codegen', state: 'COMPLETED' },
      { key: 'pre_bronze_metadata_codegen_review', state: 'COMPLETED' },
      { key: 'bronze', state: 'COMPLETED' },
      { key: 'gate4', state: 'COMPLETED' },
      { key: 'runtime_bundle_handoff', state: 'COMPLETED' },
      { key: 'pre_bronze_runtime_config', state: 'COMPLETED' },
      { key: 'pre_bronze_validate_source', state: 'COMPLETED' },
      { key: 'pre_bronze_discover_source_objects', state: 'COMPLETED' },
      { key: 'pre_bronze_stage_to_landing', state: 'COMPLETED' },
      { key: 'bronze_code_execution', state: 'RUNNING' },
    ],
  }

  const phases = getPhaseGroups(run, getPipelineSteps(run))

  expect(phases.map((phase) => phase.label)).toEqual([
    'Discovery & Requirement Intelligence',
    'Feed & Metadata Intelligence',
    'Metadata Bootstrap & Source Validation',
    'Bronze Layer (Ingestion & DQ)',
    'Silver Layer (Transformation & DQ)',
    'Gold Layer & Deployment',
  ])
  expect(phases.find((phase) => phase.id === 'phase-3')?.steps.map((step) => step.key)).toEqual([
    'pre_bronze_bootstrap_metadata',
    'plan_seal',
    'plan_freshness',
    'pre_bronze_metadata_codegen',
    'pre_bronze_metadata_codegen_review',
    'bronze',
    'gate4',
  ])
  expect(phaseState(run, 'phase-4', 'bronze_code_execution')).toBe('RUNNING')
  expect(phases.find((phase) => phase.id === 'phase-6')?.steps.map((step) => step.key)).toEqual([
    'gold',
    'gold_review',
    'gold_code_execution',
    'gold_runtime_validation',
    'final_publish',
    'finalize',
  ])
})

test('keeps the SFTP Gold review separate from Gold execution', () => {
  const run = {
    source: 'sftp',
    status: 'HITL_WAIT',
    next_review_key: 'gold_review',
    pipeline_steps: [
      { key: 'gold', state: 'COMPLETED' },
      { key: 'gold_review', state: 'PENDING' },
      { key: 'gold_code_execution', state: 'PENDING' },
    ],
  }

  expect(phaseState(run, 'phase-6', 'gold_review')).toBe('HITL_WAIT')
  expect(phaseState(run, 'phase-6', 'gold_code_execution')).toBe('PENDING')
})
