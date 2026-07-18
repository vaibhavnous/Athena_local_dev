import React from 'react'
import { fireEvent, render, screen } from '@testing-library/react'
import CodeReviewEditorModal from './CodeReviewEditorModal'

test('saves an edited generated-code draft', () => {
  const onSave = jest.fn()
  render(
    <CodeReviewEditorModal
      item={{ type: 'BRONZE', fileName: 'bronze_claims.sql', code: 'SELECT 1;' }}
      onClose={jest.fn()}
      onSave={onSave}
    />
  )

  fireEvent.click(screen.getByRole('button', { name: 'Edit' }))
  fireEvent.change(screen.getByRole('textbox', { name: 'Edit bronze_claims.sql' }), {
    target: { value: 'SELECT 2;' },
  })
  fireEvent.click(screen.getByRole('button', { name: 'Save' }))

  expect(onSave).toHaveBeenCalledWith('SELECT 2;')
  expect(screen.getByText('Draft saved')).toBeInTheDocument()
})
