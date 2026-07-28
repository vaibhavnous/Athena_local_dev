type CodeReviewItem = {
  key: string
  code?: string
  fileName?: string
  [key: string]: any
}

const fileNames = {
  bronze: 'bronze_ingest',
  silver: 'silver_transform',
  gold: 'gold_transform',
}

function baseName(value: unknown): string {
  return String(value || '').split(/[\\/]/).pop() || ''
}

export function mergeCodeReviewItems(items: CodeReviewItem[], layer: keyof typeof fileNames): CodeReviewItem[] {
  if (!items.length) return []

  const extensions = items.map((item) => item.fileName?.match(/\.[^.]+$/)?.[0]).filter(Boolean)
  const extension = extensions.length === items.length && new Set(extensions).size === 1 ? extensions[0] : '.txt'
  const comment = extension === '.py' ? '#' : '--'
  const markers = items.map((item, index) => `${comment} ===== ATHENA_FILE_${index}: ${item.fileName || item.key} =====`)

  return [{
    ...items[0],
    key: `${layer}-merged-code`,
    title: `${fileNames[layer]}${extension}`,
    fileName: `${fileNames[layer]}${extension}`,
    code: items.map((item, index) => `${markers[index]}\n${String(item.code || '').trimEnd()}`).join('\n\n'),
    sourceItems: items,
    mergeMarkers: markers,
  }]
}

export function normalizeRunScripts(payload: any, layer: keyof typeof fileNames): CodeReviewItem[] {
  const scripts = Array.isArray(payload?.[layer]?.scripts) ? payload[layer].scripts : []
  const items = scripts.flatMap((script: any, index: number) => {
    if (typeof script === 'string') {
      return [{ key: `${layer}-${index}`, fileName: `${layer}_${index + 1}.py`, code: script }]
    }

    const fallbackExtension = String(script?.language || '').toLowerCase() === 'sql' ? 'sql' : 'py'
    const mainFileName = baseName(script?.script_path || script?.filename || script?.file_name || script?.name)
      || `${layer}_${index + 1}.${fallbackExtension}`
    const mainCode = script?.script_body
      || script?.generated_bronze_script
      || script?.generated_silver_script
      || script?.generated_gold_script
      || script?.code
      || script?.content
      || script?.script
      || script?.sql
      || script?.python_code
      || ''
    const dimensionCode = script?.dimension_script_body || script?.dimension_body || ''
    const dimensionFileName = baseName(script?.dimension_script_path) || `dimension_${index + 1}.${fallbackExtension}`

    return [
      ...(mainCode ? [{ key: `${layer}-${index}`, fileName: mainFileName, code: mainCode }] : []),
      ...(dimensionCode ? [{ key: `${layer}-${index}-dimension`, fileName: dimensionFileName, code: dimensionCode }] : []),
    ]
  })

  return mergeCodeReviewItems(items, layer)
}

export function expandMergedCodeReviewItems(items: CodeReviewItem[]): CodeReviewItem[] {
  return items.flatMap((item) => {
    if (!Array.isArray(item.sourceItems) || !Array.isArray(item.mergeMarkers)) return item

    const starts = item.mergeMarkers.map((marker: string) => String(item.code || '').indexOf(marker))
    if (starts.some((start: number) => start < 0) || starts.some((start: number, index: number) => index > 0 && start <= starts[index - 1])) {
      return item.sourceItems
    }

    return item.sourceItems.map((sourceItem: CodeReviewItem, index: number) => {
      const contentStart = starts[index] + item.mergeMarkers[index].length
      const contentEnd = starts[index + 1] ?? String(item.code || '').length
      return {
        ...sourceItem,
        code: String(item.code || '').slice(contentStart, contentEnd).replace(/^\r?\n/, '').replace(/\r?\n\r?\n$/, ''),
        decisionKey: item.key,
      }
    })
  })
}
