import { getPhaseGroups, getPipelineSteps } from './pipelinePhases'

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
