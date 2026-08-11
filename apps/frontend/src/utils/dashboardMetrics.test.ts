import { getConnectedSourceSummary, getDashboardTrackerRuns, getLiveTrackerRuns } from './dashboardMetrics'

test('live tracker includes only active and review-blocked pipelines', () => {
  const runs = [
    { id: 'unknown', status: 'UNKNOWN', updated_at: '2026-08-11T10:00:00Z' },
    { id: 'complete', status: 'COMPLETED', updated_at: '2026-08-11T11:00:00Z' },
    { id: 'review', status: 'HITL_WAIT', updated_at: '2026-08-11T12:00:00Z' },
    { id: 'running', status: 'RUNNING', updated_at: '2026-08-11T13:00:00Z' },
  ]

  expect(getLiveTrackerRuns(runs).map(run => run.id)).toEqual(['running', 'review'])
  expect(getDashboardTrackerRuns(runs).map(run => run.id)).toEqual(['running', 'review', 'complete'])
})

test('connected sources are distinct project connections, not tables or projects', () => {
  const projects = [
    { id: 'one', connectionType: 'database', connectionName: 'insurance' },
    { id: 'two', connectionType: 'database', connectionName: 'insurance' },
    { id: 'three', connectionType: 'data_lake', dataLakeName: 'claims-adls' },
  ]

  expect(getConnectedSourceSummary(projects)).toEqual({ databases: 1, dataLakes: 1, total: 2 })
})
