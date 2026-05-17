// @ts-nocheck
import React, { useState, useMemo } from 'react'
import { ChevronDown, ChevronRight, Copy, Check } from 'lucide-react'

/**
 * JsonViewer — dark syntax-highlighted JSON viewer with collapse/expand.
 * @param {{ data: object|string, maxHeight?: number, defaultExpanded?: boolean }} props
 */
function JsonViewer({ data, maxHeight = 300, defaultExpanded = true }) {
  const [expanded, setExpanded] = useState(defaultExpanded)
  const [copied, setCopied] = useState(false)

  const jsonString = useMemo(() => {
    try {
      if (typeof data === 'string') {
        return JSON.stringify(JSON.parse(data), null, 2)
      }
      return JSON.stringify(data, null, 2)
    } catch {
      return String(data)
    }
  }, [data])

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(jsonString)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {
      /* ignore */
    }
  }

  const highlighted = useMemo(() => highlightJson(jsonString), [jsonString])

  return (
    <div className="rounded-lg border border-bg-border bg-bg-base overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 bg-bg-card border-b border-bg-border">
        <button
          onClick={() => setExpanded(!expanded)}
          className="flex items-center gap-1.5 text-xs text-text-tertiary hover:text-text-secondary transition-colors font-mono"
        >
          {expanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
          <span>JSON</span>
          <span className="text-text-secondary">
            ({Math.round(jsonString.length / 1024 * 10) / 10} KB)
          </span>
        </button>
        <button
          onClick={handleCopy}
          className={`flex items-center gap-1 text-xs transition-colors ${copied ? 'text-accent-green' : 'text-text-tertiary hover:text-text-secondary'}`}
        >
          {copied ? <Check size={11} /> : <Copy size={11} />}
          {copied ? 'Copied' : 'Copy'}
        </button>
      </div>

      {/* Content */}
      {expanded && (
        <div
          className="overflow-auto p-4"
          style={{ maxHeight }}
        >
          <pre
            className="text-xs font-mono leading-relaxed"
            dangerouslySetInnerHTML={{ __html: highlighted }}
          />
        </div>
      )}
    </div>
  )
}

/**
 * Basic JSON syntax highlighter using regex.
 * Returns HTML string with span classes.
 */
function highlightJson(json) {
  return json
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(
      /("(?:\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(?:\s*:)?|\b(?:true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?)/g,
      (match) => {
        let cls = ''
        if (/^"/.test(match)) {
          if (/:$/.test(match)) {
            // Key
            cls = 'color: #7dd3fc' // light blue
          } else {
            // String value
            cls = 'color: #86efac' // light green
          }
        } else if (/true|false/.test(match)) {
          cls = 'color: #fbbf24' // amber
        } else if (/null/.test(match)) {
          cls = 'color: #f87171' // red
        } else {
          cls = 'color: #c4b5fd' // purple — numbers
        }
        return `<span style="${cls}">${match}</span>`
      }
    )
}

export default JsonViewer

