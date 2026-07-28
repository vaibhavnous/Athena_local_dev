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
          columns: [{ column_name: 'ClaimID', suggested_display_name: 'claim_id', semantic_type: 'ID', business_description: 'Unique claim identifier' }],
        },
      }}
      localDecision={null}
      rejectionReason=""
      onApprove={jest.fn()}
      onReject={jest.fn()}
      onClearDecision={jest.fn()}
      onDraftChange={onDraftChange}
    />
  )

  fireEvent.click(screen.getByRole('button', { name: 'Edit' }))
  expect(screen.getByRole('dialog', { name: 'Edit claim_information semantic enrichment' })).toBeInTheDocument()

  fireEvent.change(screen.getByDisplayValue('claim_id'), { target: { value: 'claim identifier' } })
  fireEvent.change(screen.getByDisplayValue('ID'), { target: { value: 'MEASURE' } })
  fireEvent.click(screen.getByRole('button', { name: /Save Changes/i }))

  const savedDraft = onDraftChange.mock.calls.at(-1)?.[1]
  expect(savedDraft.table_name).toBe('claim_information')
  expect(savedDraft.columns[0].suggested_display_name).toBe('claim identifier')
  expect(savedDraft.columns[0]).toMatchObject({ semantic_type: 'MEASURE', is_measure: true, is_dimension: false })
  expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
})

test('keeps enriched columns collapsed until requested and shows the full review table', () => {
  render(
    <SemanticReviewCard
      item={{
        queue_id: 'table-2',
        item_detail: {
          table_name: 'policy_transactions',
          table_summary: 'Policy transaction enrichment',
          columns: [{
            column_name: 'POLICY_ID',
            suggested_display_name: 'policy_id',
            semantic_type: 'ID',
            business_description: 'Unique policy identifier',
            enrichment_source: 'llm',
            is_dimension: true,
          }],
        },
      }}
      localDecision={null}
      rejectionReason=""
      onApprove={jest.fn()}
      onReject={jest.fn()}
      onClearDecision={jest.fn()}
      onDraftChange={jest.fn()}
    />
  )

  expect(screen.queryByText('Display Name')).not.toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: 'Show columns (1)' }))
  expect(screen.getByText('Display Name')).toBeInTheDocument()
  expect(screen.getByText('Policy transaction enrichment')).toBeInTheDocument()
})

test('shows the reviewed state and lets the reviewer change the decision', () => {
  const onClearDecision = jest.fn()
  render(
    <SemanticReviewCard
      item={{ queue_id: 'table-3', item_detail: { table_name: 'measures', columns: [] } }}
      localDecision="APPROVED"
      rejectionReason=""
      onApprove={jest.fn()}
      onReject={jest.fn()}
      onClearDecision={onClearDecision}
      onDraftChange={jest.fn()}
    />
  )

  expect(screen.getByText('Approved')).toBeInTheDocument()
  expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: /Change decision/i }))
  expect(onClearDecision).toHaveBeenCalledWith('table-3')
})

test('requires a reason before rejecting an enrichment item', () => {
  const onReject = jest.fn()
  render(
    <SemanticReviewCard
      item={{ queue_id: 'table-4', item_detail: { table_name: 'claims', columns: [] } }}
      localDecision={null}
      rejectionReason=""
      onApprove={jest.fn()}
      onReject={onReject}
      onClearDecision={jest.fn()}
      onDraftChange={jest.fn()}
    />
  )

  fireEvent.click(screen.getByRole('button', { name: 'Reject' }))
  expect(onReject).not.toHaveBeenCalled()
  const confirm = screen.getByRole('button', { name: /Confirm Reject/i })
  expect(confirm).toBeDisabled()
  fireEvent.change(screen.getByPlaceholderText(/Describe why this enrichment/i), { target: { value: 'Wrong semantic mapping' } })
  fireEvent.click(confirm)
  expect(onReject).toHaveBeenCalledWith('table-4', 'Wrong semantic mapping')
})
