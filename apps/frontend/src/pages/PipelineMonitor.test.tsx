import React from 'react'
import { render, waitFor } from '@testing-library/react'

const mockUpdateRun = jest.fn()

jest.mock('../api/athenaApi', () => ({
  abortRun: jest.fn(),
  continueStage: jest.fn(),
  retryFailedStage: jest.fn(),
  getRun: jest.fn(),
  getRunStatus: jest.fn(),
  getRuns: jest.fn().mockResolvedValue([]),
  getRunScripts: jest.fn(() => new Promise(() => {})),
  restartRun: jest.fn(),
  resumeFromFailure: jest.fn(),
}))
jest.mock('react-router-dom', () => ({
  __esModule: true,
  useLocation: () => ({ pathname: '/app/data-discovery', state: null }),
  useNavigate: () => jest.fn(),
}), { virtual: true })
jest.mock('../store/useAthenaStore', () => ({
  __esModule: true,
  default: () => ({
    runs: [{
      id: 'run-1',
      run_id: 'run-1',
      status: 'RUNNING',
      stages: [{ key: 'discovery', name: 'Metadata Discovery', status: 'RUNNING' }],
    }],
    activeRunId: 'run-1',
    setActiveRun: jest.fn(),
    setRuns: jest.fn(),
    updateRun: mockUpdateRun,
    setServerOnline: jest.fn(),
    addNotification: jest.fn(),
    addRun: jest.fn(),
  }),
}))
jest.mock('../components/pipeline/PhasedPipelineDag', () => () => <div>Pipeline phases</div>)
jest.mock('../components/pipeline/StageNode', () => () => <div>Stage node</div>)
jest.mock('../components/pipeline/PipelineLogsPanel', () => () => <div>Pipeline logs</div>)
jest.mock('../components/shared/PythonCodeDialog', () => () => null)
jest.mock('../components/shared/DashboardLayout', () => ({ PageHeader: () => <div>Header</div> }))

import PipelineMonitor, { buildPipelineDisplayPhase, isInterruptedRunFailure, markNextPendingStage, markPreparingReview, normalizePipelineTimeline, pipelineActivityFromLogs, reviewWaitPatchFromLogs } from './PipelineMonitor'
import { getRunStatus } from '../api/athenaApi'

test('hydrates detailed stages for the active run', async () => {
  ;(getRunStatus as jest.Mock).mockResolvedValue({
    run: {
      id: 'run-1',
      status: 'HITL_WAIT',
      stages: [{ id: 'stage_01', name: 'Ingestion', status: 'COMPLETED' }],
    },
  })

  const view = render(<PipelineMonitor />)

  await waitFor(() => expect(mockUpdateRun).toHaveBeenCalledWith(
    'run-1',
    expect.objectContaining({ status: 'HITL_WAIT', stages: expect.any(Array) }),
  ))
  view.unmount()
})

test('recognizes only backend-restart failures as transient interrupted failures', () => {
  expect(isInterruptedRunFailure({
    status: 'FAILED',
    error_type: 'InterruptedRun',
    error: 'Backend process restarted while this run was active.',
  })).toBe(true)
  expect(isInterruptedRunFailure({ status: 'FAILED', error: 'Generated SQL is invalid' })).toBe(false)
  expect(isInterruptedRunFailure({ status: 'RUNNING', error_type: 'InterruptedRun' })).toBe(false)
})

test('shows rotating markers for the active phase and stage', () => {
  ;(getRunStatus as jest.Mock).mockImplementation(() => new Promise(() => {}))

  const view = render(<PipelineMonitor />)

  expect(view.container.querySelectorAll('[data-running-indicator="rotation"]')).toHaveLength(1)
  view.unmount()
})

test('recovers the pending review gate from HITL checkpoint logs', () => {
  expect(reviewWaitPatchFromLogs([
    {
      stage: 'gate2',
      message: 'END Table Review stage=gate2 status=HITL_WAIT duration_s=2.1',
    },
  ])).toMatchObject({
    status: 'HITL_WAIT',
    next_gate: 2,
    background_stage: 'gate2',
  })
})

test('shows a review gate as loading while its content is prepared', () => {
  const phase = markPreparingReview({
    id: 'phase-2',
    status: 'Review',
    completed: 1,
    total: 2,
    steps: [
      { key: 'nomination', state: 'COMPLETED' },
      { key: 'gate2', state: 'HITL_WAIT' },
    ],
  }, 2)

  expect(phase.status).toBe('Running')
  expect(phase.steps[1]).toMatchObject({
    state: 'RUNNING',
    preparingReview: true,
  })
})

test('keeps only the preparing review active when an older stage snapshot is still running', () => {
  const phase = markPreparingReview({
    id: 'phase-2',
    status: 'Running',
    completed: 0,
    total: 3,
    steps: [
      { key: 'nomination', state: 'RUNNING', complete: false },
      { key: 'gate2', state: 'PENDING', complete: false },
      { key: 'discovery', state: 'PENDING', complete: false },
    ],
  }, 2)

  expect(phase.steps).toEqual([
    expect.objectContaining({ key: 'nomination', state: 'COMPLETED', complete: true }),
    expect.objectContaining({ key: 'gate2', state: 'RUNNING', preparingReview: true }),
    expect.objectContaining({ key: 'discovery', state: 'PENDING', complete: false }),
  ])
  expect(phase.steps.filter((step) => step.state === 'RUNNING')).toHaveLength(1)
  expect(phase.completed).toBe(1)
})

test('normalizes mixed polling snapshots to one active substage and correct counts', () => {
  const phases = normalizePipelineTimeline([
    {
      id: 'phase-1',
      status: 'Running',
      completed: 1,
      total: 2,
      steps: [
        { key: 'kpis', state: 'RUNNING' },
        { key: 'gate1', state: 'HITL_WAIT', preparingReview: true },
      ],
    },
    {
      id: 'phase-2',
      status: 'Pending',
      completed: 0,
      total: 1,
      steps: [{ key: 'nomination', state: 'PENDING' }],
    },
  ], { status: 'HITL_WAIT' })

  expect(phases[0]).toMatchObject({ status: 'Review', completed: 1, total: 2 })
  expect(phases[0].steps).toMatchObject([
    { key: 'kpis', state: 'COMPLETED', complete: true },
    { key: 'gate1', state: 'HITL_WAIT', preparingReview: true },
  ])
  expect(phases.flatMap((phase) => phase.steps).filter((step) =>
    ['RUNNING', 'HITL_WAIT', 'FAILED'].includes(step.state)
  )).toHaveLength(1)
})

test('shows the next pending stage as starting while an active run awaits backend progress', () => {
  const phases = markNextPendingStage([
    {
      id: 'phase-1',
      status: 'Pending',
      completed: 2,
      total: 5,
      steps: [
        { key: 'ingestion', state: 'COMPLETED' },
        { key: 'memory', state: 'COMPLETED' },
        { key: 'requirements', state: 'PENDING' },
        { key: 'kpis', state: 'PENDING' },
        { key: 'gate1', state: 'PENDING' },
      ],
    },
  ], { status: 'RUNNING' })

  expect(phases[0].status).toBe('Running')
  expect(phases[0].steps[2]).toMatchObject({
    key: 'requirements',
    state: 'RUNNING',
    inferredProgress: true,
  })
  expect(phases[0].steps[3].state).toBe('PENDING')
})

test('does not infer a next stage when the backend already reports an active stage', () => {
  const phases = markNextPendingStage([
    {
      id: 'phase-1',
      status: 'Running',
      steps: [
        { key: 'requirements', state: 'RUNNING' },
        { key: 'kpis', state: 'PENDING' },
      ],
    },
  ], { status: 'RUNNING' })

  expect(phases[0].steps[1]).not.toHaveProperty('inferredProgress')
})

test('uses execution logs to infer progress when detail hydration reports UNKNOWN', () => {
  const phases = markNextPendingStage([
    {
      id: 'phase-1',
      status: 'Pending',
      steps: [
        { key: 'memory', state: 'COMPLETED' },
        { key: 'requirements', state: 'PENDING' },
      ],
    },
  ], { status: 'UNKNOWN' }, true)

  expect(phases[0].steps[1]).toMatchObject({
    state: 'RUNNING',
    inferredProgress: true,
  })
})

test('recognizes stage logs but ignores run-detail timeout warnings as pipeline activity', () => {
  expect(pipelineActivityFromLogs([
    { stage: 'memory_lookup', message: 'END memory lookup' },
  ])).toBe(true)
  expect(pipelineActivityFromLogs([
    { stage: 'runs_router', message: 'GET run detail timed out' },
  ])).toBeNull()
})

test('renders the SFTP metadata-bootstrap phase in the latest monitor UI', () => {
  const phase = {
    id: 'phase-3',
    status: 'Running',
    steps: [
      { key: 'pre_bronze_bootstrap_metadata', state: 'COMPLETED' },
      { key: 'plan_seal', state: 'RUNNING' },
      { key: 'plan_freshness', state: 'PENDING' },
      { key: 'pre_bronze_metadata_codegen', state: 'PENDING' },
      { key: 'bronze', state: 'PENDING' },
      { key: 'gate4', state: 'PENDING' },
    ],
  }

  const display = buildPipelineDisplayPhase(phase, phase.steps, { source: 'sftp', status: 'RUNNING' })

  expect(display.steps.map((step) => step.label)).toEqual([
    'Bootstrap Metadata',
    'Seal Approved Plan',
    'Validate Plan Freshness',
    'Metadata Code Generation',
    'Bronze Code Generation',
  ])
  expect(display.status).toBe('Running')
})

test('keeps code review gates out of the monitor substage list', () => {
  const phase = {
    id: 'phase-3',
    label: 'Code Generation & Reviews',
    status: 'Review',
    steps: [
      { key: 'bronze', state: 'COMPLETED' },
      { key: 'gate4', state: 'COMPLETED' },
      { key: 'silver_merge_key_resolution', state: 'COMPLETED' },
      { key: 'silver_merge_key_review', state: 'COMPLETED' },
      { key: 'silver', state: 'COMPLETED' },
      { key: 'gate5', state: 'COMPLETED' },
      { key: 'gold', state: 'COMPLETED' },
      { key: 'gold_review', state: 'HITL_WAIT' },
    ],
  }

  const display = buildPipelineDisplayPhase(phase, phase.steps, { source: 'database' })

  expect(display.steps.map((step) => step.key)).toEqual([
    'bronze',
    'silver_merge_key_resolution',
    'silver_merge_key_review',
    'silver',
    'gold',
  ])
})

test('renders deployment followed by report generation for enabled Snowflake dbt runs', () => {
  const phase = {
    id: 'phase-4',
    label: 'Code Execution & Report Generation',
    status: 'Pending',
    steps: [
      { key: 'gold_code_execution', label: 'Code Execution', state: 'PENDING' },
      { key: 'report_generation', label: 'Report Generation', state: 'PENDING' },
    ],
  }
  const run = {
    source: 'database',
    target_warehouse: 'snowflake',
    execution_engine: 'dbt',
    dbt_deployment_mode: 'generate_and_deploy',
    database_flow_version: 'generation_first_v1',
    report_generation_enabled: true,
  }

  const display = buildPipelineDisplayPhase(phase, phase.steps, run)

  expect(display.steps).toMatchObject([
    { key: 'gold_code_execution', label: 'Code Execution', state: 'PENDING' },
    { key: 'report_generation', label: 'Report Generation', state: 'PENDING' },
  ])
})
