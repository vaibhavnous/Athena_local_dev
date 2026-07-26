import { getPhaseGroups, getPipelineSteps, summarizeRunSource } from './pipelinePhases'

const phaseState = (run: any, phaseId: string, stepKey: string) => {
  const phase = getPhaseGroups(run, getPipelineSteps(run)).find((item) => item.id === phaseId)
  return phase?.steps.find((step) => step.key === stepKey)?.state
}

test('renders Snowflake bronze execution after Gold review without advancing later execution', () => {
  const run = {
    status: 'RUNNING',
    target_warehouse: 'snowflake',
    background_stage: 'bronze_code_execution',
    pipeline_steps: [
      { key: 'bronze', state: 'COMPLETED' },
      { key: 'gate4', state: 'COMPLETED' },
      { key: 'silver_merge_key_review', state: 'COMPLETED' },
      { key: 'silver', state: 'COMPLETED' },
      { key: 'gate5', state: 'COMPLETED' },
      { key: 'gold', state: 'COMPLETED' },
      { key: 'gold_review', state: 'COMPLETED' },
      { key: 'bronze_code_execution', state: 'RUNNING' },
      { key: 'silver_code_execution', state: 'PENDING' },
    ],
  }

  expect(phaseState(run, 'phase-4', 'bronze_code_execution')).toBe('RUNNING')
  expect(phaseState(run, 'phase-4', 'silver_code_execution')).toBe('PENDING')
})

test('promotes an existing merge-key step when the backend pauses for review', () => {
  const run = {
    status: 'HITL_WAIT',
    next_review_key: 'silver_merge_key_review',
    pipeline_steps: [
      { key: 'bronze', state: 'COMPLETED' },
      { key: 'gate4', state: 'COMPLETED' },
      { key: 'silver_merge_key_resolution', state: 'COMPLETED' },
      { key: 'silver_merge_key_review', state: 'PENDING' },
      { key: 'silver', state: 'PENDING' },
    ],
  }

  expect(phaseState(run, 'phase-3', 'silver_merge_key_review')).toBe('HITL_WAIT')
  expect(phaseState(run, 'phase-3', 'silver')).toBe('PENDING')
  expect(phaseState(run, 'phase-4', 'bronze_code_execution')).toBe('PENDING')
})

test('renders ordered execution frontiers after Gold review', () => {
  const bronzeRun = {
    status: 'RUNNING',
    background_stage: 'bronze_code_execution',
    pipeline_steps: [
      { key: 'gold', state: 'COMPLETED' },
      { key: 'gold_review', state: 'COMPLETED' },
      { key: 'bronze_code_execution', state: 'RUNNING' },
      { key: 'silver_code_execution', state: 'PENDING' },
      { key: 'gold_code_execution', state: 'PENDING' },
    ],
  }
  const silverRun = {
    status: 'RUNNING',
    background_stage: 'silver_code_execution',
    pipeline_steps: [
      { key: 'gold', state: 'COMPLETED' },
      { key: 'gold_review', state: 'COMPLETED' },
      { key: 'bronze_code_execution', state: 'COMPLETED' },
      { key: 'silver_code_execution', state: 'RUNNING' },
      { key: 'gold_code_execution', state: 'PENDING' },
    ],
  }
  const goldRun = {
    status: 'RUNNING',
    background_stage: 'gold_code_execution',
    pipeline_steps: [
      { key: 'gold', state: 'COMPLETED' },
      { key: 'gold_review', state: 'COMPLETED' },
      { key: 'bronze_code_execution', state: 'COMPLETED' },
      { key: 'silver_code_execution', state: 'COMPLETED' },
      { key: 'gold_code_execution', state: 'RUNNING' },
    ],
  }

  expect(phaseState(bronzeRun, 'phase-4', 'bronze_code_execution')).toBe('RUNNING')
  expect(phaseState(silverRun, 'phase-4', 'silver_code_execution')).toBe('RUNNING')
  expect(phaseState(goldRun, 'phase-4', 'gold_code_execution')).toBe('RUNNING')
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
    ],
  }

  expect(phaseState(run, 'phase-3', 'silver')).toBe('PENDING')
  expect(phaseState(run, 'phase-3', 'gate5')).toBe('PENDING')
  expect(phaseState(run, 'phase-4', 'silver_code_execution')).toBe('PENDING')
})

test('shows Gold review as waiting while generated Gold code is under review', () => {
  const run = {
    status: 'HITL_WAIT',
    next_review_key: 'gold_review',
    pipeline_steps: [
      { key: 'gold', label: 'Gold Code Generation', state: 'COMPLETED' },
      { key: 'gold_code_execution', label: 'Gold Code Execution', state: 'PENDING' },
    ],
  }

  expect(getPipelineSteps(run).find((step) => step.key === 'gold_review')).toMatchObject({
    label: 'Gold Review',
    state: 'HITL_WAIT',
  })
  expect(phaseState(run, 'phase-4', 'bronze_code_execution')).toBe('PENDING')
  expect(phaseState(run, 'phase-4', 'silver_code_execution')).toBe('PENDING')
  expect(phaseState(run, 'phase-4', 'gold_code_execution')).toBe('PENDING')
})

test('groups database code lifecycle phases by generation and execution', () => {
  const phases = getPhaseGroups({
    source: 'database',
    pipeline_steps: [
      { key: 'bronze', state: 'COMPLETED' },
      { key: 'gate4', state: 'COMPLETED' },
      { key: 'gold_review', state: 'HITL_WAIT' },
    ],
  }, getPipelineSteps({
    source: 'database',
    pipeline_steps: [
      { key: 'bronze', state: 'COMPLETED' },
      { key: 'gate4', state: 'COMPLETED' },
      { key: 'gold_review', state: 'HITL_WAIT' },
    ],
  }))

  expect(phases.map((item) => item.label)).toEqual([
    'Discovery & Requirement Intelligence',
    'Source & Metadata Intelligence',
    'Code Generation & Review',
    'Code Execution & Results',
  ])
  expect(phases.find((item) => item.id === 'phase-3')?.steps.map((step) => step.key)).toEqual([
    'bronze',
    'gate4',
    'silver_merge_key_resolution',
    'silver_merge_key_review',
    'silver',
    'gate5',
    'gold',
    'gold_review',
  ])
  expect(phases.find((item) => item.id === 'phase-4')?.steps.map((step) => step.key)).toEqual([
    'bronze_code_execution',
    'silver_code_execution',
    'gold_code_execution',
  ])
})

test('does not render Snowflake dbt as a separate Gold phase', () => {
  const nativeRun = {
    status: 'PIPELINE_COMPLETED',
    target_warehouse: 'snowflake',
    pipeline_steps: [
      { key: 'gold', label: 'Gold Code Generation', state: 'COMPLETED' },
      { key: 'gold_code_execution', label: 'Gold Code Execution', state: 'COMPLETED' },
    ],
  }
  const dbtRun = {
    ...nativeRun,
    execution_engine: 'dbt',
    snowflake_dbt_deploy_status: 'NOT_APPLICABLE_CODEGEN_ONLY',
    pipeline_steps: [
      ...nativeRun.pipeline_steps,
      { key: 'snowflake_dbt_deploy', label: 'Snowflake dbt', state: 'COMPLETED' },
    ],
  }

  expect(getPhaseGroups(nativeRun, getPipelineSteps(nativeRun)).find((item) => item.id === 'phase-4')?.steps)
    .not.toContainEqual(expect.objectContaining({ key: 'snowflake_dbt_deploy' }))
  expect(getPhaseGroups(dbtRun, getPipelineSteps(dbtRun)).find((item) => item.id === 'phase-4')?.steps)
    .not.toContainEqual(expect.objectContaining({ key: 'snowflake_dbt_deploy' }))
  expect(getPhaseGroups(dbtRun, getPipelineSteps(dbtRun)).find((item) => item.id === 'phase-4')?.label)
    .toBe('dbt Artifact Finalization')
  expect(getPhaseGroups(dbtRun, getPipelineSteps(dbtRun)).find((item) => item.id === 'phase-4')?.steps)
    .toContainEqual(expect.objectContaining({ key: 'gold_code_execution', label: 'Gold dbt Artifacts Finalized' }))
})

test('labels pending dbt Gold review without execution wording', () => {
  const run = {
    status: 'HITL_WAIT',
    target_warehouse: 'snowflake',
    execution_engine: 'dbt',
    next_review_key: 'gold_review',
    pipeline_steps: [
      { key: 'gold', label: 'Gold Code Generation', state: 'COMPLETED' },
      { key: 'gold_code_execution', label: 'Gold Code Execution', state: 'PENDING' },
    ],
  }

  expect(getPipelineSteps(run).find((step) => step.key === 'gold_review')).toMatchObject({
    label: 'Gold Review',
    state: 'HITL_WAIT',
  })
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
