import { displayStageForLog } from './PipelineLogsPanel'

test('uses checkpoint context to correct a stale layer label', () => {
  expect(displayStageForLog({
    stage: 'bronze',
    message: 'Checkpoint save finished context=silver_merge_key_review:background_complete elapsed_seconds=9.974',
  })).toBe('silver')
})

test('keeps Bronze and Silver review gates with their owning layer', () => {
  expect(displayStageForLog({ stage: 'bronze', message: 'Saving checkpoint context=gate4:complete' })).toBe('bronze')
  expect(displayStageForLog({ stage: 'bronze', message: 'Saving checkpoint context=gate5:complete' })).toBe('silver')
})
