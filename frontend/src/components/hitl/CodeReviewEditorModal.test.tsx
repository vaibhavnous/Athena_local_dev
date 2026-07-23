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

test.each(['BRONZE', 'SILVER', 'GOLD'])('uses the shared %s code review layout', (type) => {
  render(
    <CodeReviewEditorModal
      item={{ type, fileName: `${type.toLowerCase()}_transform.sql`, code: 'CREATE TABLE demo;' }}
      onClose={jest.fn()}
      onSave={jest.fn()}
      onSubmit={jest.fn()}
    />
  )

  expect(screen.getByRole('heading', { name: new RegExp(`Code Review.*${type.toLowerCase()}`) })).toBeInTheDocument()
  expect(screen.getAllByText(`${type.toLowerCase()}_transform.sql`).length).toBeGreaterThan(0)
  expect(screen.getByRole('button', { name: 'Submit & Run Stage' })).toBeInTheDocument()
})

test('keeps the Athena desktop modal width instead of expanding across the viewport', () => {
  render(
    <CodeReviewEditorModal
      item={{ type: 'SILVER', fileName: 'silver_transform.py', code: 'print("ready")' }}
      onClose={jest.fn()}
      onSave={jest.fn()}
    />
  )

  expect(screen.getByRole('dialog').firstElementChild).toHaveClass('max-w-4xl')
  expect(screen.getByRole('dialog').firstElementChild).not.toHaveClass('max-w-[1344px]')
})
