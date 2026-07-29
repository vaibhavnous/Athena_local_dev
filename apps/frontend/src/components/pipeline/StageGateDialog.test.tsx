import React from 'react'
import { fireEvent, render, screen } from '@testing-library/react'
import StageGateDialog from './StageGateDialog'

test('renders the execution boundary with the existing gate dialog', () => {
  const onContinue = jest.fn()

  render(
    <StageGateDialog
      isOpen
      completedStage={{ name: 'Gold Code Review' }}
      nextStage={{ name: 'Bronze → Silver → Gold Target Execution' }}
      onContinue={onContinue}
      onCancel={jest.fn()}
      title="Code Generation Complete"
      prompt="All generated code has been reviewed. Start ordered target execution now?"
      continueLabel="Start Execution"
      showAutoAdvance={false}
    />,
  )

  expect(screen.getByText('Code Generation Complete')).toBeInTheDocument()
  expect(screen.getByText('Bronze → Silver → Gold Target Execution')).toBeInTheDocument()
  expect(screen.queryByText(/auto-advance/i)).not.toBeInTheDocument()

  fireEvent.click(screen.getByRole('button', { name: 'Start Execution' }))
  expect(onContinue).toHaveBeenCalledWith(false)
})
