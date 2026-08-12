import {
  getPhaseGroups,
  getPipelineSteps,
  isGenerationFirstDatabaseRun,
  normalizeState,
  summarizeRunSource,
} from './pipelinePhases'

const phaseState = (run: any, phaseId: string, stepKey: string) => {
  const phase = getPhaseGroups(run, getPipelineSteps(run)).find((item) => item.id === phaseId)
  return phase?.steps.find((step) => step.key === stepKey)?.state
}

test('treats completed-with-warnings as completed in the UI', () => {
  expect(normalizeState('COMPLETED_WITH_WARNINGS')).toBe('COMPLETED')
})

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

test('groups marked database runs into generation and execution phases', () => {
  const run = {
    source: 'database',
    target_warehouse: 'databricks',
    execution_engine: 'native',
    database_flow_version: 'generation_first_v1',
    status: 'HITL_WAIT',
    next_review_key: 'gold_review',
    pipeline_steps: [
      { key: 'bronze', state: 'COMPLETED' },
      { key: 'gate4', state: 'COMPLETED' },
      { key: 'silver_merge_key_resolution', state: 'COMPLETED' },
      { key: 'silver_merge_key_review', state: 'COMPLETED' },
      { key: 'silver', state: 'COMPLETED' },
      { key: 'gate5', state: 'COMPLETED' },
      { key: 'gold', state: 'COMPLETED' },
      { key: 'gold_review', state: 'PENDING' },
      { key: 'bronze_code_execution', state: 'PENDING' },
      { key: 'silver_code_execution', state: 'PENDING' },
      { key: 'gold_code_execution', state: 'PENDING' },
    ],
  }

  const phases = getPhaseGroups(run, getPipelineSteps(run))

  expect(phases.map((phase) => phase.label)).toEqual([
    'Discovery & Requirement Intelligence',
    'Source & Metadata Intelligence',
    'Code Generation & Reviews',
    'Target Execution',
  ])
  expect(phases.find((phase) => phase.id === 'phase-3')?.steps.map((step) => step.key)).toEqual([
    'metadata_ddl',
    'metadata_ddl_review',
    'bronze',
    'gate4',
    'silver_merge_key_resolution',
    'silver_merge_key_review',
    'silver',
    'gate5',
    'gold',
    'gold_review',
  ])
  expect(phaseState(run, 'phase-3', 'gold_review')).toBe('HITL_WAIT')
  expect(phases.find((phase) => phase.id === 'phase-4')?.steps.map((step) => step.key)).toEqual([
    'metadata_setup_execution',
    'bronze_code_execution',
    'silver_code_execution',
    'gold_code_execution',
  ])
})

test('recognizes the revised generation-first database flow version', () => {
  expect(isGenerationFirstDatabaseRun({
    source: 'database',
    target_warehouse: 'databricks',
    database_flow_version: 'generation_first_v2',
  })).toBe(true)
})

test.each(['databricks', 'snowflake'])('adds report generation after native %s Gold execution', (target) => {
  const run = {
    source: 'database',
    target_warehouse: target,
    execution_engine: 'native',
    database_flow_version: 'generation_first_v2',
    report_generation_enabled: true,
    report_generation_status: 'RUNNING',
    status: 'RUNNING',
    background_stage: 'report_generation',
    pipeline_steps: [
      { key: 'metadata_setup_execution', state: 'COMPLETED' },
      { key: 'bronze_code_execution', state: 'COMPLETED' },
      { key: 'silver_code_execution', state: 'COMPLETED' },
      { key: 'gold_code_execution', state: 'COMPLETED' },
      { key: 'report_generation', state: 'RUNNING' },
    ],
  }

  const execution = getPhaseGroups(run, getPipelineSteps(run)).find((phase) => phase.id === 'phase-4')

  expect(execution?.label).toBe('Target Execution & Report Generation')
  expect(execution?.steps.map((step) => step.key)).toEqual([
    'metadata_setup_execution',
    'bronze_code_execution',
    'silver_code_execution',
    'gold_code_execution',
    'report_generation',
  ])
  expect(execution?.steps.at(-1)).toMatchObject({
    key: 'report_generation',
    state: 'RUNNING',
  })
})

test('does not infer missing generation stages from legacy Bronze execution progress', () => {
  const run = {
    source: 'database',
    target_warehouse: 'databricks',
    database_flow_version: 'generation_first_v1',
    status: 'RUNNING',
    background_stage: 'bronze_code_execution',
    pipeline_steps: [
      { key: 'bronze', state: 'COMPLETED' },
      { key: 'gate4', state: 'COMPLETED' },
      { key: 'bronze_code_execution', state: 'RUNNING' },
      { key: 'silver', state: 'PENDING' },
      { key: 'gold', state: 'PENDING' },
      { key: 'gold_review', state: 'PENDING' },
    ],
  }

  expect(phaseState(run, 'phase-3', 'silver')).toBe('PENDING')
  expect(phaseState(run, 'phase-3', 'gold')).toBe('PENDING')
  expect(phaseState(run, 'phase-3', 'gold_review')).toBe('PENDING')
  expect(phaseState(run, 'phase-4', 'bronze_code_execution')).toBe('RUNNING')
})

test('keeps all target executions pending after generation and reviews complete', () => {
  const run = {
    source: 'database',
    target_warehouse: 'snowflake',
    execution_engine: 'native',
    database_flow_version: 'generation_first_v1',
    status: 'PAUSED_FOR_STAGE_CONFIRMATION',
    stage_confirmation: {
      awaiting_confirmation: true,
      last_completed_stage_key: 'gold_review',
      next_stage_key: 'bronze_code_execution',
    },
    pipeline_steps: [
      { key: 'gold', state: 'COMPLETED' },
      { key: 'gold_review', state: 'COMPLETED' },
      { key: 'bronze_code_execution', state: 'PENDING' },
      { key: 'silver_code_execution', state: 'PENDING' },
      { key: 'gold_code_execution', state: 'PENDING' },
    ],
  }

  expect(phaseState(run, 'phase-4', 'bronze_code_execution')).toBe('PENDING')
  expect(phaseState(run, 'phase-4', 'silver_code_execution')).toBe('PENDING')
  expect(phaseState(run, 'phase-4', 'gold_code_execution')).toBe('PENDING')
})

test('keeps unmarked Snowflake dbt runs on the legacy interleaved phase layout', () => {
  const phases = getPhaseGroups({
    source: 'database',
    target_warehouse: 'snowflake',
    execution_engine: 'dbt',
    pipeline_steps: [],
  })

  expect(phases.map((phase) => phase.label)).toEqual([
    'Discovery & Requirement Intelligence',
    'Source & Metadata Intelligence',
    'Bronze Layer (Ingestion)',
    'Silver Layer (Transformation)',
    'Gold Layer (Analytics)',
  ])
})

test('recognizes Snowflake dbt deployment mode when a sparse response omits the engine', () => {
  const run = {
    source: 'database',
    target_warehouse: 'snowflake',
    dbt_deployment_mode: 'generate_and_deploy',
    database_flow_version: 'generation_first_v1',
    pipeline_steps: [
      { key: 'gold_code_execution', label: 'Code Execution', state: 'RUNNING' },
    ],
  }

  const phases = getPhaseGroups(run, getPipelineSteps(run))
  const target = phases.find((phase) => phase.label === 'Code Execution & Report Generation')

  expect(target?.steps).toHaveLength(2)
  expect(target?.steps.map((step) => step.label)).toEqual(['Metadata Setup Execution', 'Code Execution'])
})

test('groups marked Snowflake dbt runs into generation and one deployment phase', () => {
  const run = {
    source: 'database',
    target_warehouse: 'snowflake',
    execution_engine: 'dbt',
    dbt_deployment_mode: 'generate_and_deploy',
    database_flow_version: 'generation_first_v1',
    status: 'PAUSED_FOR_STAGE_CONFIRMATION',
    stage_confirmation: {
      awaiting_confirmation: true,
      last_completed_stage_key: 'gold_review',
      next_stage_key: 'gold_code_execution',
    },
    pipeline_steps: [
      { key: 'bronze', state: 'COMPLETED' },
      { key: 'gate4', state: 'COMPLETED' },
      { key: 'silver_merge_key_resolution', state: 'COMPLETED' },
      { key: 'silver_merge_key_review', state: 'COMPLETED' },
      { key: 'silver', state: 'COMPLETED' },
      { key: 'gate5', state: 'COMPLETED' },
      { key: 'gold', state: 'COMPLETED' },
      { key: 'gold_review', state: 'COMPLETED' },
      { key: 'gold_code_execution', label: 'Code Execution', state: 'PENDING' },
    ],
  }

  const phases = getPhaseGroups(run, getPipelineSteps(run))

  expect(phases.map((phase) => phase.label)).toEqual([
    'Discovery & Requirement Intelligence',
    'Source & Metadata Intelligence',
    'Code Generation & Reviews',
    'Code Execution & Report Generation',
  ])
  expect(phases.find((phase) => phase.label === 'Code Execution & Report Generation')?.steps).toHaveLength(2)
  expect(phases.find((phase) => phase.label === 'Code Execution & Report Generation')?.steps[0]).toMatchObject({
    key: 'metadata_setup_execution',
    label: 'Metadata Setup Execution',
    state: 'PENDING',
  })
  expect(phases.find((phase) => phase.label === 'Code Execution & Report Generation')?.steps[1]).toMatchObject({
    key: 'gold_code_execution',
    label: 'Code Execution',
    state: 'PENDING',
  })
})

test('adds report generation after deployment only for report-enabled dbt runs', () => {
  const run = {
    source: 'database',
    target_warehouse: 'snowflake',
    execution_engine: 'dbt',
    dbt_deployment_mode: 'generate_and_deploy',
    database_flow_version: 'generation_first_v1',
    report_generation_enabled: true,
    report_generation_status: 'RUNNING',
    status: 'RUNNING',
    background_stage: 'report_generation',
    pipeline_steps: [
      { key: 'gold_code_execution', label: 'Code Execution', state: 'COMPLETED' },
      { key: 'report_generation', label: 'Report Generation', state: 'RUNNING' },
    ],
  }

  const target = getPhaseGroups(run, getPipelineSteps(run))
    .find((phase) => phase.label === 'Code Execution & Report Generation')

  expect(target?.steps).toMatchObject([
    { key: 'metadata_setup_execution', state: 'COMPLETED' },
    { key: 'gold_code_execution', state: 'COMPLETED' },
    { key: 'report_generation', state: 'RUNNING' },
  ])
})

test('marked generate-only Snowflake dbt runs end after generation and review', () => {
  const phases = getPhaseGroups({
    source: 'database',
    target_warehouse: 'snowflake',
    execution_engine: 'dbt',
    dbt_deployment_mode: 'generate_only',
    database_flow_version: 'generation_first_v1',
    pipeline_steps: [],
  })

  expect(phases.map((phase) => phase.label)).toEqual([
    'Discovery & Requirement Intelligence',
    'Source & Metadata Intelligence',
    'Code Generation & Reviews',
  ])
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

test('keeps the ADLS merge-key review visible as its own Silver-stage gate', () => {
  const run = {
    source: 'adls_gen2',
    database_flow_version: 'generation_first_v2',
    status: 'HITL_WAIT',
    next_review_key: 'silver_merge_key_review',
    pipeline_steps: [
      { key: 'silver_merge_key_resolution', state: 'COMPLETED' },
      { key: 'silver_merge_key_review', state: 'PENDING' },
      { key: 'silver', state: 'PENDING' },
    ],
  }

  expect(phaseState(run, 'phase-3', 'silver_merge_key_resolution')).toBe('COMPLETED')
  expect(phaseState(run, 'phase-3', 'silver_merge_key_review')).toBe('HITL_WAIT')
  expect(phaseState(run, 'phase-3', 'silver')).toBe('PENDING')
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

test('uses the database generation-first stages for ADLS without exposing internal file stages', () => {
  const run = {
    source: 'adls_gen2',
    database_flow_version: 'generation_first_v2',
    target_warehouse: 'databricks',
    report_generation_enabled: true,
    status: 'HITL_WAIT',
    next_review_key: 'gold_review',
    pipeline_steps: [
      { key: 'metadata_ddl', state: 'COMPLETED' },
      { key: 'metadata_ddl_review', state: 'COMPLETED' },
      { key: 'bronze', state: 'COMPLETED' },
      { key: 'gate4', state: 'COMPLETED' },
      { key: 'silver_merge_key_resolution', state: 'COMPLETED' },
      { key: 'silver_merge_key_review', state: 'COMPLETED' },
      { key: 'silver', state: 'COMPLETED' },
      { key: 'gate5', state: 'COMPLETED' },
      { key: 'gold', state: 'COMPLETED' },
      { key: 'gold_review', state: 'PENDING' },
    ],
  }

  const phases = getPhaseGroups(run, getPipelineSteps(run))

  expect(phases.map((phase) => phase.label)).toEqual([
    'Discovery & Requirement Intelligence',
    'Source & Metadata Intelligence',
    'Code Generation & Reviews',
    'Target Execution & Report Generation',
  ])
  expect(phases.find((phase) => phase.id === 'phase-3')?.steps.map((step) => step.key)).toEqual([
    'metadata_ddl',
    'metadata_ddl_review',
    'bronze',
    'gate4',
    'silver_merge_key_resolution',
    'silver_merge_key_review',
    'silver',
    'gate5',
    'gold',
    'gold_review',
  ])
  expect(phases.flatMap((phase) => phase.steps).some((step) =>
    ['metadata_bootstrap', 'plan_seal', 'freshness_check', 'bronze_autoloader'].includes(step.key)
  )).toBe(false)
})

test('shows the shared Metadata DDL and Bronze reviews for ADLS', () => {
  const run = {
    source: 'adls_gen2',
    database_flow_version: 'generation_first_v2',
    status: 'HITL_WAIT',
    next_gate: 4,
    pipeline_steps: [
      { key: 'metadata_ddl', state: 'COMPLETED' },
      { key: 'metadata_ddl_review', state: 'COMPLETED' },
      { key: 'bronze', state: 'COMPLETED' },
      { key: 'gate4', state: 'PENDING' },
    ],
  }

  const phases = getPhaseGroups(run, getPipelineSteps(run))
  const phase = phases.find((item) => item.id === 'phase-3')
  expect(phase?.steps.find((step) => step.key === 'gate4')?.state).toBe('HITL_WAIT')
  expect(phase?.steps.find((step) => step.key === 'metadata_ddl_review')?.state).toBe('COMPLETED')
})

test('maps the legacy SFTP Gold review onto the visible final publish review', () => {
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

  expect(phaseState(run, 'phase-6', 'gate5_publish')).toBe('HITL_WAIT')
  expect(phaseState(run, 'phase-6', 'silver_to_gold')).toBe('PENDING')
})

test('shows only the furthest SFTP phase as running for legacy partial snapshots', () => {
  const run = {
    source: 'adls_gen2',
    status: 'RUNNING',
    pipeline_steps: [
      { key: 'ingestion', state: 'RUNNING' },
      { key: 'discovery', state: 'RUNNING' },
    ],
  }

  const phases = getPhaseGroups(run, getPipelineSteps(run))

  expect(phases.find((phase) => phase.id === 'phase-1')?.status).not.toBe('Running')
  expect(phases.find((phase) => phase.id === 'phase-2')?.status).toBe('Running')
  expect(phases.filter((phase) => phase.status === 'Running')).toHaveLength(1)
})
