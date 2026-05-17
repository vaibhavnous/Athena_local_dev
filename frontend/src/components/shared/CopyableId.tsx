// @ts-nocheck
import React, { useState, useCallback } from 'react'
import { Copy, Check } from 'lucide-react'

/**
 * CopyableId — shows a truncated ID in monospace with a copy button.
 * @param {{ id: string, chars?: number, prefix?: string }} props
 */
function CopyableId({ id = '', chars = 8, prefix = '' }) {
  const [copied, setCopied] = useState(false)

  const handleCopy = useCallback(async (e) => {
    e.stopPropagation()
    try {
      await navigator.clipboard.writeText(id)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch (err) {
      // Fallback for non-HTTPS environments
      const textarea = document.createElement('textarea')
      textarea.value = id
      textarea.style.position = 'fixed'
      textarea.style.opacity = '0'
      document.body.appendChild(textarea)
      textarea.select()
      document.execCommand('copy')
      document.body.removeChild(textarea)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    }
  }, [id])

  const displayId = prefix
    ? `${prefix}${id.slice(0, chars)}`
    : id.length > chars
    ? `${id.slice(0, chars)}…`
    : id

  return (
    <span className="inline-flex items-center gap-1 group">
      <span
        className="font-mono text-xs text-text-tertiary bg-bg-border/50 rounded px-1.5 py-0.5 border border-bg-border"
        title={id}
      >
        {displayId}
      </span>
      <button
        onClick={handleCopy}
        className={`
          p-0.5 rounded transition-all duration-150
          ${copied
            ? 'text-accent-green'
            : 'text-text-tertiary hover:text-text-secondary opacity-0 group-hover:opacity-100'
          }
        `}
        title={copied ? 'Copied!' : 'Copy full ID'}
      >
        {copied ? <Check size={11} /> : <Copy size={11} />}
      </button>
    </span>
  )
}

export default CopyableId

