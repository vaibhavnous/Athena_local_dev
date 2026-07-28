import { decisionKey, normalizeDecision } from './ComplianceGovernance'

test('normalizes column decisions used by compliance review controls', () => {
  expect(decisionKey({ table_name: 'claims', column_name: 'ssn' })).toBe('claims.ssn')
  expect(normalizeDecision('Approved')).toBe('Approved')
  expect(normalizeDecision('REJECTED')).toBe('Rejected')
  expect(normalizeDecision('Needs Info')).toBe('Pending')
})
