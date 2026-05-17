// @ts-nocheck
import React, { useCallback, useRef, useState } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { FileText, Loader2, Play, Upload, X } from 'lucide-react'
import * as mammoth from 'mammoth'
import { startRun, uploadBrd } from '../../api/athenaApi'
import useAthenaStore from '../../store/useAthenaStore'

const MAX_UPLOAD_SIZE_BYTES = 5 * 1024 * 1024

const DEFAULT_FORM = {
  brdText: '',
  fileName: '',
  provider: 'azure_openai',
  deployment: '',
  databaseType: 'azure_sql',
  databaseName: 'insurance',
}

const DATABASE_OPTIONS = {
  azure_sql: [
    { id: 'insurance', name: 'insurance' },
    { id: 'AdventureWorksDW2019', name: 'AdventureWorksDW2019' },
    { id: 'AdventureWorks2019', name: 'AdventureWorks2019' },
  ],
  postgresql: [
    { id: 'postgres', name: 'postgres' },
    { id: 'insurance', name: 'insurance' },
  ],
}

function NewRunModal({ isOpen, onClose }) {
  const fileInputRef = useRef(null)
  const addRun = useAthenaStore((s) => s.addRun)
  const addNotification = useAthenaStore((s) => s.addNotification)
  const settings = useAthenaStore((s) => s.settings)

  const [form, setForm] = useState(() => ({
    ...DEFAULT_FORM,
    provider: settings.provider || DEFAULT_FORM.provider,
    deployment: settings.azure_deployment || DEFAULT_FORM.deployment,
    databaseName: DEFAULT_FORM.databaseName,
  }))
  const [uploadedFile, setUploadedFile] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [isDragging, setIsDragging] = useState(false)

  const resetState = () => {
    setForm({
      ...DEFAULT_FORM,
      provider: settings.provider || DEFAULT_FORM.provider,
      deployment: settings.azure_deployment || DEFAULT_FORM.deployment,
      databaseName: DEFAULT_FORM.databaseName,
    })
    setUploadedFile(null)
    setError(null)
    setIsDragging(false)
  }

  const handleClose = () => {
    if (loading) return
    resetState()
    onClose()
  }

  const readFile = useCallback((file) => {
    if (!file) return

    const fileExtension = file.name.split('.').pop()?.toLowerCase()
    setError(null)

    if (file.size > MAX_UPLOAD_SIZE_BYTES) {
      setUploadedFile(null)
      setForm((current) => ({ ...current, fileName: '' }))
      setError('File is too large. Please upload a BRD smaller than 5 MB.')
      return
    }

    if (fileExtension === 'docx') {
      setUploadedFile(file)
      const reader = new FileReader()
      reader.onload = async (event) => {
        try {
          const arrayBuffer = event.target?.result
          const result = await mammoth.extractRawText({ arrayBuffer })
          setForm((current) => ({
            ...current,
            brdText: result.value,
            fileName: file.name,
          }))
        } catch (readError) {
          console.error('[NewRunModal] DOCX parse failed', readError)
          setError('Failed to parse the DOCX file.')
        }
      }
      reader.readAsArrayBuffer(file)
      return
    }

    if (fileExtension === 'txt') {
      setUploadedFile(file)
      const reader = new FileReader()
      reader.onload = (event) => {
        setForm((current) => ({
          ...current,
          brdText: event.target?.result || '',
          fileName: file.name,
        }))
      }
      reader.readAsText(file)
      return
    }

    setUploadedFile(null)
    setForm((current) => ({ ...current, fileName: '' }))
    setError('Unsupported file type. Upload a .txt or .docx BRD.')
  }, [])

  const handleDrop = (event) => {
    event.preventDefault()
    setIsDragging(false)
    const file = event.dataTransfer.files[0]
    if (file) readFile(file)
  }

  const handleFileInput = (event) => {
    const file = event.target.files[0]
    if (file) readFile(file)
    event.target.value = ''
  }

  const handleSubmit = async (event) => {
    event.preventDefault()
    if (!form.brdText.trim()) {
      setError('Please provide BRD text or upload a file.')
      return
    }

    setLoading(true)
    setError(null)

    try {
      if (uploadedFile) {
        await uploadBrd(uploadedFile)
      }

      const run = await startRun({
        brd_text: form.brdText,
        brd_filename: form.fileName || 'pasted_brd.txt',
        provider: form.provider,
        deployment: form.deployment || undefined,
        database_type: form.databaseType,
        database_name: form.databaseName,
        budget: settings.budget,
        maxKpis: settings.maxKpis,
        devMode: settings.devMode,
      })

      addRun({
        id: run.run_id,
        run_id: run.run_id,
        brd_filename: form.fileName || 'pasted_brd.txt',
        status: run.status || 'RUNNING',
        provider: form.provider,
        deployment: form.deployment || null,
        started_at: new Date().toISOString(),
        stages: [],
        kpis: [],
      })

      addNotification({
        type: 'success',
        title: 'Run Started',
        message: `Pipeline submitted for ${form.fileName || 'pasted_brd.txt'}.`,
        duration: 4000,
      })

      handleClose()
    } catch (submitError) {
      console.error('[NewRunModal] Failed to start run', submitError)
      setError(submitError.message || 'Failed to start the pipeline run.')
    } finally {
      setLoading(false)
    }
  }

  return (
    <AnimatePresence>
      {isOpen && (
        <>
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 z-40 bg-black/60 backdrop-blur-sm"
            onClick={handleClose}
          />

          <motion.div
            initial={{ x: '100%' }}
            animate={{ x: 0 }}
            exit={{ x: '100%' }}
            transition={{ type: 'spring', stiffness: 300, damping: 30 }}
            className="fixed right-0 top-0 z-50 flex h-full w-full max-w-lg flex-col border-l border-bg-border bg-bg-card shadow-2xl"
          >
            <div className="flex items-center justify-between border-b border-bg-border px-6 py-4">
              <div>
                <h2 className="text-lg font-bold text-text-primary">New Pipeline Run</h2>
                <p className="mt-0.5 text-xs text-text-tertiary">
                  Submit a BRD to the FastAPI pipeline.
                </p>
              </div>
              <button
                onClick={handleClose}
                disabled={loading}
                className="flex h-8 w-8 items-center justify-center rounded-lg text-text-tertiary transition-colors hover:bg-bg-hover hover:text-text-secondary"
              >
                <X size={16} />
              </button>
            </div>

            <form onSubmit={handleSubmit} className="flex-1 overflow-y-auto">
              <div className="space-y-5 p-6">
                <div>
                  <label className="label">BRD Document</label>
                  <div
                    onDragOver={(event) => {
                      event.preventDefault()
                      setIsDragging(true)
                    }}
                    onDragLeave={() => setIsDragging(false)}
                    onDrop={handleDrop}
                    onClick={() => fileInputRef.current?.click()}
                    className={`relative cursor-pointer rounded-xl border-2 border-dashed p-6 text-center transition-all duration-200 ${
                      isDragging
                        ? 'scale-[1.01] border-accent-blue bg-accent-blue/5'
                        : form.fileName
                        ? 'border-accent-green/40 bg-accent-green/5'
                        : 'border-bg-border hover:border-accent-blue hover:bg-bg-hover'
                    }`}
                  >
                    <input
                      ref={fileInputRef}
                      type="file"
                      accept=".txt,.docx"
                      className="hidden"
                      onChange={handleFileInput}
                    />
                    {form.fileName ? (
                      <div className="flex items-center justify-center gap-2 text-accent-green">
                        <FileText size={20} />
                        <span className="text-sm font-medium">{form.fileName}</span>
                      </div>
                    ) : (
                      <div className="space-y-2">
                        <Upload size={24} className="mx-auto text-text-tertiary" />
                        <p className="text-sm text-text-secondary">
                          Drop `.txt` or `.docx` here, or <span className="text-accent-blue">browse</span>
                        </p>
                        <p className="text-xs text-text-tertiary">Max 5 MB</p>
                      </div>
                    )}
                  </div>
                </div>

                <div className="flex items-center gap-3">
                  <div className="h-px flex-1 bg-bg-border" />
                  <span className="text-xs text-text-tertiary">or paste text</span>
                  <div className="h-px flex-1 bg-bg-border" />
                </div>

                <div>
                  <label className="label">BRD Text</label>
                  <textarea
                    className="input-field resize-none"
                    rows={8}
                    placeholder="Paste the business requirements document here..."
                    value={form.brdText}
                    onChange={(event) =>
                      setForm((current) => ({ ...current, brdText: event.target.value }))
                    }
                  />
                </div>

                <div className="space-y-3 rounded-lg border border-bg-border bg-bg-base p-4">
                  <div>
                    <label className="label">Database Type</label>
                    <select
                      className="input-field appearance-none"
                      value={form.databaseType}
                      onChange={(event) => {
                        const databaseType = event.target.value
                        setForm((current) => ({
                          ...current,
                          databaseType,
                          databaseName: DATABASE_OPTIONS[databaseType]?.[0]?.id || '',
                        }))
                      }}
                    >
                      <option value="azure_sql">Azure SQL</option>
                      <option value="postgresql">PostgreSQL</option>
                    </select>
                  </div>
                  <div>
                    <label className="label">Database Name</label>
                    <select
                      className="input-field appearance-none"
                      value={form.databaseName}
                      onChange={(event) =>
                        setForm((current) => ({ ...current, databaseName: event.target.value }))
                      }
                    >
                      {DATABASE_OPTIONS[form.databaseType]?.map((database) => (
                        <option key={database.id} value={database.id}>
                          {database.name}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>

                {error && (
                  <div className="rounded-lg border border-accent-red/30 bg-red-500/10 p-3 text-sm text-accent-red">
                    {error}
                  </div>
                )}
              </div>
            </form>

            <div className="flex gap-3 border-t border-bg-border px-6 py-4">
              <button
                type="button"
                onClick={handleClose}
                disabled={loading}
                className="btn-secondary flex-1"
              >
                Cancel
              </button>
              <button
                onClick={handleSubmit}
                disabled={loading || !form.brdText.trim()}
                className="btn-primary flex flex-1 items-center justify-center gap-2 disabled:cursor-not-allowed disabled:opacity-50"
              >
                {loading ? (
                  <>
                    <Loader2 size={15} className="animate-spin" />
                    Starting...
                  </>
                ) : (
                  <>
                    <Play size={14} />
                    Start Run
                  </>
                )}
              </button>
            </div>
          </motion.div>
        </>
      )}
    </AnimatePresence>
  )
}

export default NewRunModal
