// @ts-nocheck
import React, { useEffect, useMemo, useState } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { AlertCircle, Check, Code2, Copy, Download, FileCode2, Loader2, X } from 'lucide-react'
import { getRunScripts } from '../../api/athenaApi'

const layerFromStage = (stageName = '') => {
  const value = String(stageName).toLowerCase()
  if (value.includes('silver')) return 'silver'
  if (value.includes('gold')) return 'gold'
  return 'bronze'
}

const normalizeScripts = (payload, layer) => {
  const scripts = payload?.[layer]?.scripts || []
  return scripts.map((script, index) => {
    if (typeof script === 'string') return { filename: `${layer}_${index + 1}.py`, code: script }
    return {
      filename: script.filename || script.file_name || script.name || `${layer}_${index + 1}.${script.language === 'sql' ? 'sql' : 'py'}`,
      code: script.code || script.content || script.script || script.sql || script.python_code || '',
    }
  })
}

export default function PythonCodeDialog({ isOpen, onClose, stageName, runId, title }) {
  const layer = useMemo(() => layerFromStage(stageName), [stageName])
  const [files, setFiles] = useState([])
  const [activeTab, setActiveTab] = useState(0)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [copied, setCopied] = useState(false)

  useEffect(() => {
    if (!isOpen || !runId) return
    let cancelled = false
    setLoading(true); setError(''); setFiles([]); setActiveTab(0)
    getRunScripts(runId)
      .then(payload => { if (!cancelled) setFiles(normalizeScripts(payload, layer)) })
      .catch(fetchError => { if (!cancelled) setError(fetchError?.message || 'Generated code could not be loaded.') })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [isOpen, layer, runId])

  useEffect(() => {
    if (!isOpen) return
    const close = event => { if (event.key === 'Escape') onClose() }
    window.addEventListener('keydown', close)
    return () => window.removeEventListener('keydown', close)
  }, [isOpen, onClose])

  const activeFile = files[activeTab]
  const copy = async () => {
    if (!activeFile?.code) return
    await navigator.clipboard.writeText(activeFile.code)
    setCopied(true); window.setTimeout(() => setCopied(false), 1500)
  }
  const download = () => {
    if (!activeFile) return
    const url = URL.createObjectURL(new Blob([activeFile.code], { type: 'text/plain' }))
    const anchor = document.createElement('a')
    anchor.href = url; anchor.download = activeFile.filename; anchor.click(); URL.revokeObjectURL(url)
  }

  return <AnimatePresence>{isOpen && <>
    <motion.button aria-label="Close generated code" initial={{opacity:0}} animate={{opacity:1}} exit={{opacity:0}} onClick={onClose} className="fixed inset-0 z-50 bg-black/70 backdrop-blur-sm"/>
    <motion.section initial={{opacity:0,scale:.97,y:12}} animate={{opacity:1,scale:1,y:0}} exit={{opacity:0,scale:.97,y:12}} className="fixed inset-x-3 bottom-[4%] top-[4%] z-50 mx-auto flex max-w-5xl flex-col overflow-hidden rounded-xl border border-bg-border bg-bg-card shadow-2xl sm:inset-x-6">
      <header className="flex items-center justify-between border-b border-bg-border bg-bg-base px-4 py-3">
        <div className="flex items-center gap-2"><Code2 size={15} className="text-accent-blue"/><h2 className="text-sm font-semibold">{title || `${layer[0].toUpperCase()+layer.slice(1)} generated code`}</h2>{files.length>0&&<span className="rounded bg-bg-border px-1.5 py-0.5 text-[10px] text-text-muted">{files.length} file{files.length===1?'':'s'}</span>}</div>
        <div className="flex items-center gap-2">{activeFile&&<><button onClick={copy} className="btn-secondary flex items-center gap-1.5"><>{copied?<Check size={12}/>:<Copy size={12}/>}</>{copied?'Copied':'Copy'}</button><button onClick={download} className="btn-secondary flex items-center gap-1.5"><Download size={12}/>Download</button></>}<button onClick={onClose} className="rounded-md p-2 text-text-tertiary hover:bg-bg-hover hover:text-white"><X size={15}/></button></div>
      </header>
      {files.length>1&&<nav className="flex overflow-x-auto border-b border-bg-border bg-bg-base">{files.map((file,index)=><button key={`${file.filename}:${index}`} onClick={()=>setActiveTab(index)} className={`flex items-center gap-1.5 border-r border-bg-border px-4 py-2.5 font-mono text-xs ${index===activeTab?'border-t-2 border-t-accent-blue bg-bg-card text-white':'border-t-2 border-t-transparent text-text-muted'}`}><FileCode2 size={12}/>{file.filename}</button>)}</nav>}
      <div className="min-h-0 flex-1 overflow-auto bg-[#080d18]">
        {loading&&<div className="flex h-full items-center justify-center gap-2 text-sm text-text-tertiary"><Loader2 size={18} className="animate-spin text-accent-blue"/>Loading generated code...</div>}
        {!loading&&error&&<div className="flex h-full items-center justify-center gap-2 text-sm text-red-400"><AlertCircle size={18}/>{error}</div>}
        {!loading&&!error&&!activeFile&&<div className="flex h-full flex-col items-center justify-center gap-2 text-text-tertiary"><Code2 size={24}/><p className="text-sm">{layer[0].toUpperCase()+layer.slice(1)} code is not available yet.</p><p className="text-xs">It will appear after generation completes.</p></div>}
        {!loading&&activeFile&&<pre className="min-h-full whitespace-pre p-5 font-mono text-xs leading-6 text-slate-200"><code>{activeFile.code}</code></pre>}
      </div>
      {activeFile&&<footer className="flex items-center justify-between border-t border-bg-border bg-bg-base px-4 py-2 text-[10px] text-text-muted"><span className="font-mono">{activeFile.filename}</span><span>{String(activeFile.code||'').split('\n').length} lines</span></footer>}
    </motion.section>
  </>}</AnimatePresence>
}
