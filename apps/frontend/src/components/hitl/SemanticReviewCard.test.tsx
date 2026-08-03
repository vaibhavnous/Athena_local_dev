import React from 'react'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
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

test('derives dimension flags from semantic type without clearing independent PII review', () => {
  const onDraftChange = jest.fn()
  render(
    <SemanticReviewCard
      item={{
        queue_id: 'table-dimension-contract',
        item_detail: {
          table_name: 'claim_information',
          columns: [{
            column_name: 'LossDate',
            suggested_display_name: 'loss_date',
            semantic_type: 'DIMENSION',
            business_description: 'Business date when the insured loss occurred',
            is_pii_candidate: true,
            pii_type: 'PERSONAL_DATE',
          }],
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
  fireEvent.change(screen.getByDisplayValue('DIMENSION'), { target: { value: 'DATE' } })
  fireEvent.click(screen.getByRole('button', { name: /Save Changes/i }))

  expect(onDraftChange.mock.calls.at(-1)?.[1].columns[0]).toMatchObject({
    semantic_type: 'DATE',
    is_measure: false,
    is_dimension: true,
    is_pii_candidate: true,
  })
})

test('keeps the editor open until the semantic draft is durably saved', async () => {
  let finishSave
  const onSaveDraft = jest.fn(() => new Promise((resolve) => { finishSave = resolve }))
  render(
    <SemanticReviewCard
      item={{
        queue_id: 'run-1:3:item-1',
        revision: 'a'.repeat(64),
        item_detail: {
          table_name: 'sales.claims',
          columns: [{ column_name: 'ClaimID', suggested_display_name: 'claim_id', semantic_type: 'ID', business_description: 'Unique claim identifier' }],
        },
      }}
      localDecision={null}
      rejectionReason=""
      onApprove={jest.fn()}
      onReject={jest.fn()}
      onClearDecision={jest.fn()}
      onDraftChange={jest.fn()}
      onSaveDraft={onSaveDraft}
    />
  )

  fireEvent.click(screen.getByRole('button', { name: 'Edit' }))
  fireEvent.click(screen.getByRole('button', { name: /Save Changes/i }))

  expect(onSaveDraft).toHaveBeenCalledWith(
    'run-1:3:item-1',
    expect.objectContaining({ table_name: 'sales.claims' }),
    'a'.repeat(64)
  )
  expect(screen.getByRole('button', { name: 'Saving...' })).toBeDisabled()
  expect(screen.getByRole('dialog')).toBeInTheDocument()

  await act(async () => finishSave({ revision: 'b'.repeat(64) }))
  await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
})

test('keeps the saved display name when a stale run refresh rerenders the card', async () => {
  const staleItem = {
    queue_id: 'run-1:3:item-1',
    revision: 'a'.repeat(64),
    item_detail: {
      table_name: 'sales.claims',
      columns: [{ column_name: 'ClaimID', suggested_display_name: 'Claim_ID', semantic_type: 'ID', business_description: 'Unique claim identifier' }],
    },
  }
  const onSaveDraft = jest.fn(async (_id, draft) => ({
    edited_content: draft,
    revision: 'b'.repeat(64),
  }))
  const props = {
    localDecision: null,
    rejectionReason: '',
    onApprove: jest.fn(),
    onReject: jest.fn(),
    onClearDecision: jest.fn(),
    onDraftChange: jest.fn(),
    onSaveDraft,
  }
  const view = render(<SemanticReviewCard item={staleItem} {...props} />)

  fireEvent.click(screen.getByRole('button', { name: 'Edit' }))
  fireEvent.change(screen.getByDisplayValue('Claim_ID'), { target: { value: 'Claim ID' } })
  fireEvent.click(screen.getByRole('button', { name: /Save Changes/i }))
  await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())

  view.rerender(<SemanticReviewCard item={{ ...staleItem, item_detail: { ...staleItem.item_detail, columns: [...staleItem.item_detail.columns] } }} {...props} />)
  fireEvent.click(screen.getByRole('button', { name: 'Edit' }))
  expect(screen.getByDisplayValue('Claim ID')).toBeInTheDocument()
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

test('requires PII type before saving or approving an Aadhaar candidate', async () => {
  const onApprove = jest.fn()
  render(
    <SemanticReviewCard
      item={{
        queue_id: 'table-aadhaar',
        item_detail: {
          table_name: 'claims',
          columns: [{
            column_name: 'S_AADHAAR_ATTACHED',
            suggested_display_name: 'Aadhaar Attached',
            semantic_type: 'FLAG',
            business_description: 'Indicates whether Aadhaar documentation is attached to the claim.',
            is_pii_candidate: true,
            pii_type: null,
          }],
        },
      }}
      localDecision={null}
      rejectionReason=""
      onApprove={onApprove}
      onReject={jest.fn()}
      onClearDecision={jest.fn()}
      onDraftChange={jest.fn()}
    />
  )

  expect(screen.getByText(/S_AADHAAR_ATTACHED: PII type is required/)).toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
  expect(onApprove).not.toHaveBeenCalled()
  expect(screen.getByRole('dialog')).toBeInTheDocument()
  expect(screen.getByRole('button', { name: /Save Changes/i })).toBeDisabled()

  fireEvent.change(screen.getByRole('textbox', { name: 'S_AADHAAR_ATTACHED PII type' }), { target: { value: 'AADHAAR_NUMBER' } })
  fireEvent.click(screen.getByRole('button', { name: /Save Changes/i }))
  await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
  fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
  expect(onApprove).toHaveBeenCalledWith('table-aadhaar', expect.objectContaining({
    columns: [expect.objectContaining({ pii_type: 'AADHAAR_NUMBER' })],
  }))
})
