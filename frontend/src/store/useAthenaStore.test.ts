import useAthenaStore from './useAthenaStore'

function resetStore() {
  useAthenaStore.setState({ runs: [], activeRunId: null })
}

test('keeps a later phase when a slower status response reports an earlier phase', () => {
  resetStore()
  useAthenaStore.getState().addRun({
    id: 'run-1',
    status: 'RUNNING',
    pipeline_steps: [{ key: 'silver_code_execution', state: 'RUNNING' }],
  })

  useAthenaStore.getState().updateRun('run-1', {
    id: 'run-1',
    status: 'RUNNING',
    pipeline_steps: [{ key: 'bronze_code_execution', state: 'RUNNING' }],
  })

  expect(useAthenaStore.getState().runs[0].pipeline_steps[0].key).toBe('silver_code_execution')
})

test('does not erase stage detail from a sparse hydration fallback', () => {
  resetStore()
  useAthenaStore.getState().addRun({
    id: 'run-2',
    status: 'RUNNING',
    stages: [{ key: 'gold', status: 'RUNNING' }],
  })

  useAthenaStore.getState().updateRun('run-2', {
    id: 'run-2',
    status: 'RUNNING',
    stages: [],
    background_stage: 'gold_code_execution',
  })

  expect(useAthenaStore.getState().runs[0].stages).toEqual([{ key: 'gold', status: 'RUNNING' }])
})

test('clears the completed-stage dialog when the next stage starts', () => {
  resetStore()
  useAthenaStore.getState().addRun({
    id: 'run-3',
    status: 'PAUSED_FOR_STAGE_CONFIRMATION',
    background_stage: null,
    stage_confirmation: { awaiting_confirmation: true, next_stage_key: 'silver' },
  })

  useAthenaStore.getState().updateRun('run-3', {
    id: 'run-3',
    status: 'RUNNING',
    background_stage: 'silver',
  })

  expect(useAthenaStore.getState().runs[0].stage_confirmation).toBeNull()
})

test('keeps detailed HITL status when history refresh returns an UNKNOWN summary', () => {
  resetStore()
  useAthenaStore.getState().addRun({
    id: 'run-hitl',
    status: 'HITL_WAIT',
    next_gate: 3,
    pipeline_steps: [{ key: 'gate3', state: 'HITL_WAIT' }],
  })

  useAthenaStore.getState().setRuns([{ id: 'run-hitl', status: 'UNKNOWN' }])

  expect(useAthenaStore.getState().runs[0]).toMatchObject({
    status: 'HITL_WAIT',
    next_gate: 3,
    pipeline_steps: [{ key: 'gate3', state: 'HITL_WAIT' }],
  })
})
