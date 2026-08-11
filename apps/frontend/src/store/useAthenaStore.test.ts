import useAthenaStore from './useAthenaStore'

function resetStore() {
  useAthenaStore.setState({ runs: [], activeRunId: null, notifications: [] })
}

test('does not stack an identical notification while it is already visible', () => {
  resetStore()
  const notification = {
    type: 'error',
    title: 'Review failed',
    message: 'Please retry.',
    duration: 0,
  }

  const firstId = useAthenaStore.getState().addNotification(notification)
  const duplicateId = useAthenaStore.getState().addNotification(notification)

  expect(duplicateId).toBe(firstId)
  expect(useAthenaStore.getState().notifications).toHaveLength(1)
})

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

test('accepts sparse completion after the final background stage finishes', () => {
  resetStore()
  useAthenaStore.getState().addRun({
    id: 'run-dbt',
    status: 'RUNNING',
    background_stage: 'gold_code_execution',
    pipeline_steps: [
      { key: 'gold', state: 'COMPLETED' },
      { key: 'gold_code_execution', state: 'RUNNING' },
    ],
  })

  useAthenaStore.getState().updateRun('run-dbt', {
    id: 'run-dbt',
    status: 'PIPELINE_COMPLETED',
    background_stage: null,
    stages: [],
    snowflake_gold_execution_status: 'COMPLETED',
  })

  expect(useAthenaStore.getState().runs[0]).toMatchObject({
    status: 'PIPELINE_COMPLETED',
    background_stage: null,
    pipeline_steps: [
      { key: 'gold', state: 'COMPLETED' },
      { key: 'gold_code_execution', state: 'COMPLETED', complete: true },
    ],
  })
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

test.each(['REGENERATE_REQUIRED', 'FAILED'])(
  'honors explicit execution-gate clears from a sparse %s review payload',
  (status) => {
    resetStore()
    useAthenaStore.getState().addRun({
      id: 'run-generation-first',
      status: 'PAUSED_FOR_STAGE_CONFIRMATION',
      database_flow_version: 'generation_first_v1',
      execution_ready: true,
      awaiting_stage_confirmation: true,
      next_stage_key: 'bronze_code_execution',
      next_stage_label: 'Bronze Target Execution',
      stage_confirmation: {
        awaiting_confirmation: true,
        last_completed_stage_key: 'gold_review',
        next_stage_key: 'bronze_code_execution',
      },
      pipeline_steps: [{ key: 'gold_review', state: 'COMPLETED' }],
    })

    // The list/status endpoints intentionally omit heavyweight pipeline detail.
    useAthenaStore.getState().setRuns([{
      id: 'run-generation-first',
      source: 'database',
      target_warehouse: 'databricks',
      database_flow_version: 'generation_first_v1',
      generation_first_execution: true,
      status,
      stage_confirmation: null,
      execution_ready: false,
      awaiting_stage_confirmation: false,
      next_stage_key: null,
      next_stage_label: null,
    }])

    expect(useAthenaStore.getState().runs[0]).toMatchObject({
      status,
      stage_confirmation: null,
      execution_ready: false,
      awaiting_stage_confirmation: false,
      next_stage_key: null,
      next_stage_label: null,
      pipeline_steps: [{ key: 'gold_review', state: 'COMPLETED' }],
    })
  },
)

test('clears a stale execution gate from a degraded regeneration fallback', () => {
  resetStore()
  useAthenaStore.getState().addRun({
    id: 'run-fallback-regeneration',
    status: 'PAUSED_FOR_STAGE_CONFIRMATION',
    database_flow_version: 'generation_first_v1',
    execution_ready: true,
    awaiting_stage_confirmation: true,
    next_stage_key: 'bronze_code_execution',
    stage_confirmation: {
      awaiting_confirmation: true,
      last_completed_stage_key: 'gold_review',
      next_stage_key: 'bronze_code_execution',
    },
    pipeline_steps: [{ key: 'gold_review', state: 'COMPLETED' }],
  })

  useAthenaStore.getState().setRuns([{
    id: 'run-fallback-regeneration',
    status: 'REGENERATE_REQUIRED',
    stage_confirmation: null,
  }])

  expect(useAthenaStore.getState().runs[0]).toMatchObject({
    status: 'REGENERATE_REQUIRED',
    stage_confirmation: null,
    execution_ready: false,
    awaiting_stage_confirmation: false,
    next_stage_key: null,
    pipeline_steps: [{ key: 'gold_review', state: 'COMPLETED' }],
  })
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

test('keeps an authoritative sparse status when a fallback summary becomes UNKNOWN', () => {
  resetStore()
  useAthenaStore.getState().addRun({
    id: 'run-authoritative',
    status: 'HITL_WAIT',
    status_authoritative: true,
    hydration_fallback: false,
  })

  useAthenaStore.getState().setRuns([{
    id: 'run-authoritative',
    status: 'UNKNOWN',
    status_authoritative: false,
    hydration_fallback: true,
  }])

  expect(useAthenaStore.getState().runs[0]).toMatchObject({
    status: 'HITL_WAIT',
    status_authoritative: true,
    hydration_fallback: false,
  })
})

test('keeps known runs when polling returns a transient empty snapshot', () => {
  resetStore()
  useAthenaStore.getState().addRun({ id: 'run-4', status: 'RUNNING' })

  useAthenaStore.getState().setRuns([])

  expect(useAthenaStore.getState().runs).toEqual([{ id: 'run-4', status: 'RUNNING' }])
  expect(useAthenaStore.getState().activeRunId).toBe('run-4')
})

test('does not show a failed run from a hydration fallback', () => {
  resetStore()
  useAthenaStore.getState().addRun({
    id: 'run-fallback-failure',
    status: 'RUNNING',
    background_stage: 'silver',
  })

  useAthenaStore.getState().setRuns([{
    id: 'run-fallback-failure',
    status: 'FAILED',
    error: 'temporary fallback failure',
    hydration_fallback: true,
  }])

  expect(useAthenaStore.getState().runs[0]).toMatchObject({
    status: 'RUNNING',
    background_stage: 'silver',
    pending_failure_confirmation: true,
  })
})

test('accepts a failed status confirmed by the persisted checkpoint', () => {
  resetStore()
  useAthenaStore.getState().addRun({
    id: 'run-confirmed-failure',
    status: 'UNKNOWN',
  })

  useAthenaStore.getState().updateRun('run-confirmed-failure', {
    id: 'run-confirmed-failure',
    status: 'FAILED',
    error: 'dbt build failed',
    hydration_fallback: true,
    status_authoritative: true,
  })

  expect(useAthenaStore.getState().runs[0]).toMatchObject({
    status: 'FAILED',
    error: 'dbt build failed',
    pending_failure_confirmation: false,
  })
})

test('keeps authoritative Snowflake dbt settings during sparse run-list hydration', () => {
  resetStore()
  useAthenaStore.getState().addRun({
    id: 'run-snowflake-dbt',
    status: 'RUNNING',
    source: 'database',
    target_warehouse: 'snowflake',
    execution_engine: 'dbt',
    dbt_deployment_mode: 'generate_and_deploy',
    database_flow_version: 'generation_first_v1',
    pipeline_steps: [{ key: 'gold_code_execution', state: 'RUNNING' }],
  })

  useAthenaStore.getState().setRuns([{
    id: 'run-snowflake-dbt',
    status: 'UNKNOWN',
    source: 'database',
    target_warehouse: null,
    execution_engine: 'native',
    dbt_deployment_mode: 'generate_only',
    database_flow_version: null,
    stages: [],
    hydration_fallback: false,
    status_authoritative: true,
  }])

  expect(useAthenaStore.getState().runs[0]).toMatchObject({
    target_warehouse: 'snowflake',
    execution_engine: 'dbt',
    dbt_deployment_mode: 'generate_and_deploy',
    database_flow_version: 'generation_first_v1',
  })
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
