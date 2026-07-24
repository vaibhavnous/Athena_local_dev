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

test('accepts SFTP nomination after discovery in the six-phase order', () => {
  resetStore()
  useAthenaStore.getState().addRun({
    id: 'run-sftp',
    source: 'adls_gen2',
    status: 'RUNNING',
    pipeline_steps: [{ key: 'discovery', state: 'RUNNING' }],
  })

  useAthenaStore.getState().updateRun('run-sftp', {
    id: 'run-sftp',
    source: 'adls_gen2',
    status: 'RUNNING',
    pipeline_steps: [
      { key: 'discovery', state: 'COMPLETED' },
      { key: 'nomination', state: 'RUNNING' },
    ],
  })

  expect(useAthenaStore.getState().runs[0].pipeline_steps).toEqual([
    { key: 'discovery', state: 'COMPLETED' },
    { key: 'nomination', state: 'RUNNING' },
  ])
})

test('does not let a stale Gate 3 snapshot replace an active Phase 3 snapshot', () => {
  resetStore()
  useAthenaStore.getState().addRun({
    id: 'run-phase-3',
    source: 'adls_gen2',
    status: 'RUNNING',
    background_stage: 'bronze',
    next_gate: null,
    pipeline_steps: [
      { key: 'gate3', state: 'COMPLETED' },
      { key: 'bronze', state: 'RUNNING' },
    ],
  })

  useAthenaStore.getState().updateRun('run-phase-3', {
    id: 'run-phase-3',
    source: 'adls_gen2',
    status: 'HITL_WAIT',
    background_stage: null,
    next_gate: 3,
    pipeline_steps: [
      { key: 'gate3', state: 'HITL_WAIT' },
      // A persisted Bronze artifact made the old ranker treat both snapshots
      // as equally advanced, allowing the monitor to oscillate.
      { key: 'bronze', state: 'COMPLETED' },
    ],
  })

  expect(useAthenaStore.getState().runs[0]).toMatchObject({
    status: 'RUNNING',
    background_stage: 'bronze',
    next_gate: null,
    pipeline_steps: [
      { key: 'gate3', state: 'COMPLETED' },
      { key: 'bronze', state: 'RUNNING' },
    ],
  })
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

test('keeps known runs when polling returns a transient empty snapshot', () => {
  resetStore()
  useAthenaStore.getState().addRun({ id: 'run-4', status: 'RUNNING' })

  useAthenaStore.getState().setRuns([])

  expect(useAthenaStore.getState().runs).toEqual([{ id: 'run-4', status: 'RUNNING' }])
  expect(useAthenaStore.getState().activeRunId).toBe('run-4')
})

test('does not replace a run name with the run ID from sparse status polling', () => {
  resetStore()
  useAthenaStore.getState().addRun({
    id: 'run-5',
    run_id: 'run-5',
    brd_filename: 'Vialto',
    status: 'RUNNING',
  })

  useAthenaStore.getState().updateRun('run-5', {
    id: 'run-5',
    run_id: 'run-5',
    brd_filename: 'run-5',
    status: 'RUNNING',
  })

  expect(useAthenaStore.getState().runs[0].brd_filename).toBe('Vialto')
})
