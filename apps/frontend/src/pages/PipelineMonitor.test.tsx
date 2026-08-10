import React from 'react'
import { act, fireEvent, render, waitFor } from '@testing-library/react'

const mockUpdateRun = jest.fn()
const mockSetActiveRun = jest.fn()
const mockNavigate = jest.fn()
const mockLocation = { pathname: '/app/data-discovery', state: null }
let mockActiveRun: any = {
  id: 'run-1',
  run_id: 'run-1',
  status: 'RUNNING',
  stages: [{ key: 'discovery', name: 'Metadata Discovery', status: 'RUNNING' }],
}

jest.mock('../api/athenaApi', () => ({
  abortRun: jest.fn(),
  continueStage: jest.fn(),
  retryFailedStage: jest.fn(),
  fetchKpiReviews: jest.fn(),
  getRun: jest.fn(),
  getRunStatus: jest.fn(),
  getRuns: jest.fn().mockResolvedValue([]),
  getRunScripts: jest.fn(() => new Promise(() => {})),
  restartRun: jest.fn(),
  resumeFromFailure: jest.fn(),
}))
jest.mock('react-router-dom', () => ({
  __esModule: true,
  useLocation: () => mockLocation,
  useNavigate: () => mockNavigate,
}), { virtual: true })
jest.mock('../store/useAthenaStore', () => ({
  __esModule: true,
  default: () => ({
    runs: [mockActiveRun],
    activeRunId: 'run-1',
    setActiveRun: mockSetActiveRun,
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

import PipelineMonitor, { buildPipelineDisplayPhase, markNextPendingStage, markPreparingReview, pipelineActivityFromLogs, reviewWaitPatchFromLogs } from './PipelineMonitor'
import { fetchKpiReviews, getRunStatus } from '../api/athenaApi'

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

test('does not cancel Gate 1 hydration when an unrelated run snapshot refreshes', async () => {
  let finishKpiLoad
  mockActiveRun = {
    id: 'run-1',
    run_id: 'run-1',
    status: 'HITL_WAIT',
    next_gate: 1,
    source: 'database',
    stages: [],
  }
  ;(getRunStatus as jest.Mock).mockImplementation(() => new Promise(() => {}))
  ;(fetchKpiReviews as jest.Mock).mockImplementation(() => new Promise((resolve) => { finishKpiLoad = resolve }))
  mockNavigate.mockClear()

  const view = render(<PipelineMonitor />)
  await waitFor(() => expect(fetchKpiReviews).toHaveBeenCalledTimes(1))

  mockActiveRun = { ...mockActiveRun, updated_at: '2026-08-03T10:27:15Z' }
  view.rerender(<PipelineMonitor />)
  expect(fetchKpiReviews).toHaveBeenCalledTimes(1)

  await act(async () => finishKpiLoad({
    run_id: 'run-1',
    source: 'database',
    kpis: [{ queue_id: 'run-1:1:0', name: 'Claim Count' }],
  }))
  await waitFor(() => expect(mockNavigate).toHaveBeenCalledWith(
    '/app/hitl?runId=run-1&gate=1',
    expect.any(Object),
  ))
  view.unmount()

  mockActiveRun = {
    id: 'run-1',
    run_id: 'run-1',
    status: 'RUNNING',
    stages: [{ key: 'discovery', name: 'Metadata Discovery', status: 'RUNNING' }],
  }
})

test('shows rotating markers for the active phase and stage', () => {
  ;(getRunStatus as jest.Mock).mockImplementation(() => new Promise(() => {}))

  const view = render(<PipelineMonitor />)

  expect(view.container.querySelectorAll('[data-running-indicator="rotation"]')).toHaveLength(1)
  view.unmount()
})

test('shows a completed Gold execution warning summary', () => {
  ;(getRunStatus as jest.Mock).mockImplementation(() => new Promise(() => {}))
  mockActiveRun = {
    id: 'run-1',
    run_id: 'run-1',
    status: 'PIPELINE_COMPLETED',
    source: 'database',
    target_warehouse: 'databricks',
    database_flow_version: 'generation_first_v1',
    pipeline_steps: [
      {
        key: 'gold_code_execution',
        label: 'Gold Target Execution',
        state: 'COMPLETED_WITH_WARNINGS',
        detail: 'Gold completed with warnings: 9/10 tables succeeded.',
      },
    ],
  }

  const view = render(<PipelineMonitor />)

  fireEvent.click(view.getByText('Target Execution'))
  expect(view.getByText('Gold completed with warnings: 9/10 tables succeeded.')).toBeInTheDocument()
  view.unmount()
  mockActiveRun = {
    id: 'run-1',
    run_id: 'run-1',
    status: 'RUNNING',
    stages: [{ key: 'discovery', name: 'Metadata Discovery', status: 'RUNNING' }],
  }
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

test('shows metadata DDL generation while keeping review gates out of the monitor substage list', () => {
  const phase = {
    id: 'phase-3',
    label: 'Code Generation & Reviews',
    status: 'Review',
    steps: [
      { key: 'metadata_ddl', state: 'COMPLETED' },
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
    'metadata_ddl',
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
      { key: 'metadata_setup_execution', label: 'Metadata Setup Execution', state: 'PENDING' },
      { key: 'gold_code_execution', label: 'Code Execution', state: 'PENDING' },
      { key: 'report_generation', label: 'Report Generation', state: 'PENDING' },
    ],
  }
  const run = {
    source: 'database',
    target_warehouse: 'snowflake',
    execution_engine: 'dbt',
    dbt_deployment_mode: 'generate_and_deploy',
    database_flow_version: 'generation_first_v2',
    report_generation_enabled: true,
  }

  const display = buildPipelineDisplayPhase(phase, phase.steps, run)

  expect(display.steps).toMatchObject([
    { key: 'metadata_setup_execution', label: 'Metadata Setup Execution', state: 'PENDING' },
    { key: 'gold_code_execution', label: 'Code Execution', state: 'PENDING' },
    { key: 'report_generation', label: 'Report Generation', state: 'PENDING' },
  ])
})
