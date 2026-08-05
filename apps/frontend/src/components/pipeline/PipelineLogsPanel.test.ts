import { isInternalPipelineLog } from './PipelineLogsPanel'

const log = (stage: string, message: string) => ({
  log_id: `${stage}:${message}`,
  run_id: 'run-1',
  notebook_name: null,
  stage,
  step_name: null,
  log_level: 'INFO',
  message,
  duration_seconds: null,
  logged_at: '2026-08-04T10:00:00Z',
})

test('hides checkpoint and polling implementation logs from the execution panel', () => {
  expect(isInternalPipelineLog(log('checkpoint', 'Ignored stale checkpoint write'))).toBe(true)
  expect(isInternalPipelineLog(log('bronze', 'Saving checkpoint context=bronze:complete'))).toBe(true)
  expect(isInternalPipelineLog(log('pipeline_router', 'Failed to fetch pipeline status'))).toBe(true)
})

test('keeps actual pipeline execution logs visible', () => {
  expect(isInternalPipelineLog(log('profiling', 'START Column Profiling stage=profiling'))).toBe(false)
  expect(isInternalPipelineLog(log('bronze_code_execution', 'Loaded source table 7/7'))).toBe(false)
})
