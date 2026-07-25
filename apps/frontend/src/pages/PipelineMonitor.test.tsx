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
  getRunScripts: jest.fn(),
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
    runs: [{ id: 'run-1', run_id: 'run-1', status: 'RUNNING', stages: [] }],
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

import PipelineMonitor from './PipelineMonitor'
import { getRun } from '../api/athenaApi'

test('hydrates detailed stages for the active run', async () => {
  ;(getRun as jest.Mock).mockResolvedValue({
    id: 'run-1',
    status: 'HITL_WAIT',
    stages: [{ id: 'stage_01', name: 'Ingestion', status: 'COMPLETED' }],
  })

  const view = render(<PipelineMonitor />)

  await waitFor(() => expect(mockUpdateRun).toHaveBeenCalledWith(
    'run-1',
    expect.objectContaining({ status: 'HITL_WAIT', stages: expect.any(Array) }),
  ))
  view.unmount()
})
