import { expandMergedCodeReviewItems, mergeCodeReviewItems, normalizeRunScripts } from './codeReviewArtifacts'

test.each([
  ['bronze', '.sql'],
  ['silver', '.py'],
  ['gold', '.sql'],
] as const)('merges and safely expands every %s code review', (layer, extension) => {
  const source = [
    { key: 'one', fileName: `one${extension}`, code: 'first code', reviewPayload: { table: 'one' } },
    { key: 'two', fileName: `two${extension}`, code: 'second code', reviewPayload: { table: 'two' } },
  ]

  const merged = mergeCodeReviewItems(source, layer)
  merged[0].code = merged[0].code?.replace('second code', 'edited second code')
  const expanded = expandMergedCodeReviewItems(merged)

  expect(merged).toHaveLength(1)
  expect(merged[0].fileName).toBe(`${layer === 'bronze' ? 'bronze_ingest' : `${layer}_transform`}${extension}`)
  expect(expanded.map((item) => item.code)).toEqual(['first code', 'edited second code'])
  expect(expanded.map((item) => item.reviewPayload.table)).toEqual(['one', 'two'])
})

test('normalizes backend run scripts into one history viewer file', () => {
  const files = normalizeRunScripts({
    gold: {
      scripts: [{
        script_path: 'generated/gold/fact_claims.sql',
        script_body: 'create table fact_claims;',
        dimension_script_path: 'generated/gold/dim_policy.sql',
        dimension_script_body: 'create table dim_policy;',
      }],
    },
  }, 'gold')

  expect(files).toHaveLength(1)
  expect(files[0].fileName).toBe('gold_transform.sql')
  expect(files[0].code).toContain('create table fact_claims;')
  expect(files[0].code).toContain('create table dim_policy;')
})
