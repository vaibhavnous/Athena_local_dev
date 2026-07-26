import React from 'react'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import useAthenaStore from '../store/useAthenaStore'
import { getRun, getRuns } from '../api/athenaApi'

const mockSetSearchParams = jest.fn()
let mockSearchParams = new URLSearchParams()

jest.mock('../api/athenaApi', () => ({
  getRun: jest.fn(),
  getRuns: jest.fn(),
}))

jest.mock('react-router-dom', () => ({
  __esModule: true,
  useSearchParams: () => [mockSearchParams, mockSetSearchParams],
}), { virtual: true })

jest.mock('../components/shared/PythonCodeDialog', () => (props: any) =>
  props.isOpen ? <div role="dialog">Code dialog for {props.runId}:{props.stageName}</div> : null
)

import RunHistoryPage from './RunHistoryPage'

const runDetail = {
  id: 'run-history-1',
  run_id: 'run-history-1',
  brd_filename: 'history-run.docx',
  source: 'database',
  status: 'SUCCESS',
  pipeline_steps: [
    { key: 'bronze', label: 'Bronze Code Generation', state: 'COMPLETED' },
    { key: 'gate4', label: 'Bronze Review', state: 'COMPLETED' },
    { key: 'silver_merge_key_resolution', label: 'Silver Merge Key Resolution', state: 'COMPLETED' },
    { key: 'silver_merge_key_review', label: 'Silver Merge Key Review', state: 'COMPLETED' },
    { key: 'silver', label: 'Silver Code Generation', state: 'COMPLETED' },
    { key: 'gate5', label: 'Silver Review', state: 'COMPLETED' },
    { key: 'gold', label: 'Gold Code Generation', state: 'COMPLETED' },
    { key: 'gold_review', label: 'Gold Review', state: 'COMPLETED' },
  ],
}

beforeEach(() => {
  jest.clearAllMocks()
  mockSearchParams = new URLSearchParams()
  useAthenaStore.setState({ runs: [], activeRunId: null })
  ;(getRuns as jest.Mock).mockResolvedValue([runDetail])
  ;(getRun as jest.Mock).mockResolvedValue(runDetail)
})

test('shows View Code actions for completed historical generation stages', async () => {
  render(<RunHistoryPage />)

  expect((await screen.findAllByText('history-run.docx'))[0]).toBeInTheDocument()
  await waitFor(() => expect(getRuns).toHaveBeenCalledWith(500))

  fireEvent.click(await screen.findByText('Code Generation & Review'))

  await waitFor(() => expect(screen.getAllByText('View Code')).toHaveLength(3))
  fireEvent.click(screen.getAllByText('View Code')[0])

  expect(screen.getByRole('dialog')).toHaveTextContent('Code dialog for run-history-1:Bronze Code Generation')
})
