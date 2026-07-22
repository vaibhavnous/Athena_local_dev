// @ts-nocheck
import React, { useEffect, useMemo, useState } from 'react'
import { createPortal } from 'react-dom'
import { Code2, Copy, Download, FileCode2, Pencil, Play, RotateCcw, Save, X } from 'lucide-react'

function codeLanguage(fileName = '') {
  const extension = String(fileName).split('.').pop()?.toLowerCase()
  if (extension === 'sql') return 'sql'
  if (extension === 'py') return 'python'
  if (extension === 'json') return 'json'
  return extension || 'code'
}

function downloadDraft(fileName, code) {
  const blob = new Blob([code || ''], { type: 'text/plain;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = fileName || 'generated_code.txt'
  anchor.click()
  URL.revokeObjectURL(url)
}

const CODE_TOKEN_PATTERN = /(\b(?:ALTER|AND|AS|BEGIN|BY|CASE|CREATE|DATABASE|ELSE|END|EXISTS|FILE|FORMAT|FROM|IF|INSERT|INTO|NOT|NULL|OR|SCHEMA|SELECT|TABLE|TYPE|UPDATE|VALUES|WHEN|WHERE)\b|'[^']*'|"[^"]*"|\b\d+(?:\.\d+)?\b)/gi

function highlightedLine(line, language) {
  if (/^\s*(--|#)/.test(line)) return <span className="italic text-[#697486]">{line || ' '}</span>
  if (!['sql', 'python'].includes(language)) return line || ' '
  return line.split(CODE_TOKEN_PATTERN).map((token, index) => {
    if (!token) return null
    if (/^(?:ALTER|AND|AS|BEGIN|BY|CASE|CREATE|DATABASE|ELSE|END|EXISTS|FILE|FORMAT|FROM|IF|INSERT|INTO|NOT|NULL|OR|SCHEMA|SELECT|TABLE|TYPE|UPDATE|VALUES|WHEN|WHERE)$/i.test(token)) {
      return <span key={index} className="text-[#d678e8]">{token}</span>
    }
    if (/^['"]/.test(token)) return <span key={index} className="text-[#9dcc75]">{token}</span>
    if (/^\d/.test(token)) return <span key={index} className="text-[#d99b62]">{token}</span>
    return <React.Fragment key={index}>{token}</React.Fragment>
  })
}

function CodeReviewEditorModal({ item, onClose, onSave, onSubmit = null, submitting = false, submitLabel = 'Submit & Run Stage', submitDisabled = false }) {
  const originalCode = String(item?.code || '# Generated code is not available yet.')
  const [draftCode, setDraftCode] = useState(originalCode)
  const [savedCode, setSavedCode] = useState(originalCode)
  const [editing, setEditing] = useState(false)
  const [saved, setSaved] = useState(false)
  const fileName = item?.fileName || `${String(item?.title || 'generated_code').replace(/[^a-zA-Z0-9_.-]+/g, '_')}.txt`
  const language = codeLanguage(fileName)
  const lines = useMemo(() => draftCode.split('\n'), [draftCode])
  const dirty = draftCode !== savedCode

  useEffect(() => {
    const handleKeyDown = (event) => {
      if (event.key === 'Escape') onClose()
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 's') {
        event.preventDefault()
        onSave(draftCode)
        setSavedCode(draftCode)
        setSaved(true)
        setEditing(false)
      }
    }
    document.addEventListener('keydown', handleKeyDown)
    const previousOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    return () => {
      document.removeEventListener('keydown', handleKeyDown)
      document.body.style.overflow = previousOverflow
    }
  }, [draftCode, onClose, onSave])

  const saveDraft = () => {
    onSave(draftCode)
    setSavedCode(draftCode)
    setSaved(true)
    setEditing(false)
  }

  const copyDraft = async () => {
    try {
      await navigator.clipboard.writeText(draftCode)
    } catch {
      const textarea = document.createElement('textarea')
      textarea.value = draftCode
      document.body.appendChild(textarea)
      textarea.select()
      document.execCommand('copy')
      textarea.remove()
    }
  }

  return createPortal(
    <div className="fixed inset-0 z-[100] flex items-center justify-center bg-black/80 p-3 backdrop-blur-[2px] sm:p-6" role="dialog" aria-modal="true" aria-label={`Code review ${fileName}`}>
      <div className="flex h-[90vh] w-full max-w-[1344px] flex-col overflow-hidden rounded-[22px] border border-[#26334a] bg-[#091221] shadow-[0_35px_120px_rgba(0,0,0,0.75)]">
        <div className="flex shrink-0 flex-wrap items-center justify-between gap-3 border-b border-[#26334a] px-5 py-4 sm:px-7">
          <div className="flex items-center gap-3">
            <Code2 size={20} className="text-[#4388ff]" />
            <h2 className="text-lg font-extrabold text-white">Code Review — {String(item?.type || 'code').toLowerCase()}</h2>
            <span className="rounded-md bg-[#202b3d] px-2.5 py-1 font-mono text-xs text-[#9ea9bc]">1 file</span>
          </div>
          <div className="flex items-center gap-2">
            {editing || dirty ? (
              <>
                <button type="button" onClick={() => { setDraftCode(savedCode); setSaved(false) }} className="inline-flex h-10 items-center gap-2 rounded-lg border border-[#26334a] bg-[#111b2d] px-4 text-sm font-semibold text-[#b5bfd0] hover:bg-[#182438] hover:text-white">
                  <RotateCcw size={15} /> Revert
                </button>
                <button type="button" onClick={saveDraft} className="inline-flex h-10 items-center gap-2 rounded-lg border border-[#3567b9] bg-[#132a51] px-4 text-sm font-semibold text-white hover:bg-[#193665]">
                  <Save size={15} /> Save
                </button>
              </>
            ) : (
              <button type="button" onClick={() => setEditing(true)} className="inline-flex h-10 items-center gap-2 rounded-lg border border-[#26334a] bg-[#111b2d] px-4 text-sm font-semibold text-[#b5bfd0] hover:bg-[#182438] hover:text-white">
                <Pencil size={15} /> Edit
              </button>
            )}
            <button type="button" onClick={copyDraft} className="hidden h-10 items-center gap-2 rounded-lg border border-[#26334a] bg-[#111b2d] px-4 text-sm font-semibold text-[#b5bfd0] hover:bg-[#182438] hover:text-white sm:inline-flex"><Copy size={15} /> Copy</button>
            <button type="button" onClick={() => downloadDraft(fileName, draftCode)} className="hidden h-10 items-center gap-2 rounded-lg border border-[#26334a] bg-[#111b2d] px-4 text-sm font-semibold text-[#b5bfd0] hover:bg-[#182438] hover:text-white md:inline-flex"><Download size={15} /> Download</button>
            <button type="button" onClick={onClose} aria-label="Close code review" className="flex h-10 w-10 items-center justify-center rounded-lg text-[#9ea9bc] hover:bg-white/5 hover:text-white"><X size={20} /></button>
          </div>
        </div>

        <div className="flex h-12 shrink-0 items-end border-b border-[#26334a] bg-[#08101e] px-5">
          <div className="flex h-12 items-center gap-2 border-b-2 border-[#4388ff] px-3 font-mono text-sm font-semibold text-white">
            <FileCode2 size={16} className="text-[#4388ff]" /> {fileName}
          </div>
        </div>

        <div className="relative min-h-0 flex-1 overflow-hidden bg-[#282c34]">
          {editing ? (
            <textarea
              autoFocus
              value={draftCode}
              onChange={(event) => { setDraftCode(event.target.value); setSaved(false) }}
              spellCheck={false}
              aria-label={`Edit ${fileName}`}
              className="h-full w-full resize-none overflow-auto bg-[#282c34] px-7 py-6 font-mono text-[14px] leading-7 text-[#eef1f6] outline-none sm:px-10"
            />
          ) : (
            <div className="h-full overflow-auto py-5 font-mono text-[14px] leading-7">
              {lines.map((line, index) => (
                <div key={index} className="grid min-w-max grid-cols-[64px_1fr] px-4 hover:bg-white/[0.025]">
                  <span className="select-none pr-5 text-right text-[#667287]">{index + 1}</span>
                  <span className="whitespace-pre pr-8 text-[#abb2bf]">{highlightedLine(line, language)}</span>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="flex shrink-0 items-center justify-between gap-4 border-t border-[#26334a] bg-[#08101e] px-5 py-3 text-xs sm:px-7">
          <div className="flex items-center gap-3 font-mono text-[#7f8ba0]"><span>{fileName}</span><span className="rounded border border-[#274c86] bg-[#102348] px-2 py-1 text-[#5797ff]">{language}</span></div>
          <div className="flex items-center gap-4">
            <span className={dirty ? 'text-amber-400' : saved ? 'text-emerald-400' : 'text-[#7f8ba0]'}>{dirty ? 'Unsaved changes' : saved ? 'Draft saved' : 'No changes'}</span>
            <span className="text-[#667287]">{lines.length} lines</span>
            {onSubmit && (
              <button
                type="button"
                disabled={submitting || submitDisabled || dirty}
                onClick={onSubmit}
                className="inline-flex h-10 items-center gap-2 rounded-lg bg-emerald-600 px-5 font-sans text-sm font-bold text-white transition-colors hover:bg-emerald-500 disabled:cursor-not-allowed disabled:opacity-50"
              >
                <Play size={15} /> {submitting ? 'Submitting...' : submitLabel}
              </button>
            )}
          </div>
        </div>
      </div>
    </div>,
    document.body
  )
}

export default CodeReviewEditorModal
