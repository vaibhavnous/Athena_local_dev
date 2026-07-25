import React from 'react'
import { fireEvent, render, screen } from '@testing-library/react'
import SemanticReviewCard from './SemanticReviewCard'

test('edits only the selected table and saves its semantic draft', () => {
  const onDraftChange = jest.fn()
  render(
    <SemanticReviewCard
      item={{
        queue_id: 'table-1',
        item_detail: {
          table_name: 'claim_information',
          columns: [{ column_name: 'ClaimID', suggested_display_name: 'claim_id', semantic_type: 'ID' }],
        },
      }}
      localDecision={null}
      rejectionReason=""
      onApprove={jest.fn()}
      onReject={jest.fn()}
      onDraftChange={onDraftChange}
    />
  )

  fireEvent.click(screen.getByRole('button', { name: 'Edit' }))
  expect(screen.getByRole('dialog', { name: 'Edit claim_information semantic enrichment' })).toBeInTheDocument()

  fireEvent.change(screen.getByDisplayValue('claim_id'), { target: { value: 'claim identifier' } })
  fireEvent.click(screen.getByRole('button', { name: /Save Changes/i }))

  const savedDraft = onDraftChange.mock.calls.at(-1)?.[1]
  expect(savedDraft.table_name).toBe('claim_information')
  expect(savedDraft.columns[0].suggested_display_name).toBe('claim identifier')
  expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
})
