const liveTrackerStatuses = new Set(['RUNNING', 'HITL_WAIT', 'PAUSED_FOR_HITL', 'PENDING_REVIEW'])
const trackerHistoryStatuses = new Set(['COMPLETED', 'FAILED'])

function runTimestamp(run: any) {
  return new Date(run?.updated_at || run?.completed_at || run?.started_at || 0).getTime()
}

export function getLiveTrackerRuns(runs: any[], limit = 6) {
  return [...runs]
    .filter(run => liveTrackerStatuses.has(String(run?.status || '').toUpperCase()))
    .sort((left, right) => {
      return runTimestamp(right) - runTimestamp(left)
    })
    .slice(0, limit)
}

export function getDashboardTrackerRuns(runs: any[], limit = 8) {
  const activeRuns = getLiveTrackerRuns(runs, runs.length)
  const historicalRuns = [...runs]
    .filter(run => trackerHistoryStatuses.has(String(run?.status || '').toUpperCase()))
    .sort((left, right) => runTimestamp(right) - runTimestamp(left))
  return [...activeRuns, ...historicalRuns].slice(0, limit)
}

export function getConnectedSourceSummary(projects: any[]) {
  const databases = new Set(
    projects
      .filter(project => project?.connectionType === 'database')
      .map(project => project.connectionName || project.databaseName || project.id)
      .filter(Boolean),
  )
  const dataLakes = new Set(
    projects
      .filter(project => project?.connectionType === 'data_lake')
      .map(project => project.connectionName || project.dataLakeName || project.id)
      .filter(Boolean),
  )
  return {
    databases: databases.size,
    dataLakes: dataLakes.size,
    total: databases.size + dataLakes.size,
  }
}
