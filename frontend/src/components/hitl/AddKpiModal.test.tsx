import React from 'react'
import { fireEvent, render, screen } from '@testing-library/react'
import AddKpiModal from './AddKpiModal'

test('submits a valid reviewer-authored KPI', () => {
  const onAdd = jest.fn()
  render(<AddKpiModal isOpen submitting={false} onClose={jest.fn()} onAdd={onAdd} />)

  fireEvent.change(screen.getByPlaceholderText('e.g. Customer Acquisition Cost'), { target: { value: 'Claim Closure Rate' } })
  fireEvent.change(screen.getByPlaceholderText(/Clear, measurable description/), { target: { value: 'Percentage of claims closed during the period.' } })
  fireEvent.click(screen.getByRole('button', { name: 'Add KPI' }))

  expect(onAdd).toHaveBeenCalledWith({
    name: 'Claim Closure Rate',
    definition: 'Percentage of claims closed during the period.',
  })
})
