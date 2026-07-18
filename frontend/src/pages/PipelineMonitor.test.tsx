import React from 'react'
import { render, waitFor } from '@testing-library/react'
import PipelineMonitor from './PipelineMonitor'
import { getRun } from '../api/athenaApi'

const mockUpdateRun = jest.fn()

jest.mock('../api/athenaApi', () => ({
  abortRun: jest.fn(),
  retryFailedStage: jest.fn(),
  getRun: jest.fn(),
  getRunScripts: jest.fn(),
}))
jest.mock('../store/useAthenaStore', () => ({
  __esModule: true,
  default: () => ({
    runs: [{ id: 'run-1', run_id: 'run-1', status: 'RUNNING', stages: [] }],
    activeRunId: 'run-1',
    updateRun: mockUpdateRun,
    addNotification: jest.fn(),
  }),
}))
jest.mock('../components/pipeline/PhasedPipelineDag', () => () => <div>Pipeline phases</div>)
jest.mock('../components/pipeline/StageNode', () => () => <div>Stage node</div>)
jest.mock('../components/pipeline/PipelineLogsPanel', () => () => <div>Pipeline logs</div>)
jest.mock('../components/shared/PythonCodeDialog', () => () => null)
jest.mock('../components/shared/DashboardLayout', () => ({ PageHeader: () => <div>Header</div> }))

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
