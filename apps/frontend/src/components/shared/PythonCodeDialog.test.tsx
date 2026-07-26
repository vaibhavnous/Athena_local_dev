import React from 'react'
import { render, screen, waitFor } from '@testing-library/react'
import PythonCodeDialog from './PythonCodeDialog'
import { getRunScripts } from '../../api/athenaApi'

jest.mock('../../api/athenaApi', () => ({
  getRunScripts: jest.fn(),
}))

jest.mock('framer-motion', () => {
  const React = require('react')
  const passthrough = (Tag: string) => ({ children, initial, animate, exit, transition, ...props }: any) =>
    React.createElement(Tag, props, children)

  return {
    __esModule: true,
    AnimatePresence: ({ children }: any) => React.createElement(React.Fragment, null, children),
    motion: {
      button: passthrough('button'),
      section: passthrough('section'),
    },
  }
})

beforeEach(() => {
  jest.clearAllMocks()
})

test('renders backend script bodies from run script bundles', async () => {
  ;(getRunScripts as jest.Mock).mockResolvedValue({
    run_id: 'run-code-1',
    gold: {
      scripts: [
        {
          script_path: 'generated_code/snowflake/run-code-1/gold/fact_claims.sql',
          script_body: 'select 1 as fact_claims',
          dimension_script_body: 'select 2 as dim_claims',
          language: 'sql',
        },
      ],
    },
  })

  render(
    <PythonCodeDialog
      isOpen
      onClose={jest.fn()}
      stageName="Gold Code Generation"
      runId="run-code-1"
    />
  )

  await waitFor(() => expect(getRunScripts).toHaveBeenCalledWith('run-code-1'))
  expect(await screen.findByText('fact_claims.sql')).toBeInTheDocument()
  expect(screen.getByText(/select 1 as fact_claims/)).toBeInTheDocument()
  expect(screen.getByText(/select 2 as dim_claims/)).toBeInTheDocument()
})
