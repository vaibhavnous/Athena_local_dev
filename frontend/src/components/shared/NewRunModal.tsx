// @ts-nocheck
import React, { useCallback, useEffect, useRef, useState } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import {
  ArrowLeft,
  BookOpen,
  ChevronDown,
  FileText,
  Loader2,
  Play,
  Upload,
  X,
} from 'lucide-react'
import * as mammoth from 'mammoth'
import { startRun, uploadBrd } from '../../api/athenaApi'
import useAthenaStore from '../../store/useAthenaStore'

const MAX_UPLOAD_SIZE_BYTES = 5 * 1024 * 1024

const DEFAULT_FORM = {
  projectName: '',
  projectDescription: '',
  source: 'database',
  sftpEntity: 'transactions',
  brdText: '',
  fileName: '',
  provider: 'azure_openai',
  deployment: '',
  databaseType: 'azure_sql',
  databaseName: 'insurance',
  useDomainKb: false,
  stageConfirmationEnabled: true,
}

function buildInitialForm(settings, seedRun) {
  const seedSource = seedRun?.source || DEFAULT_FORM.source
  return {
    ...DEFAULT_FORM,
    provider: settings.provider || DEFAULT_FORM.provider,
    deployment: settings.azure_deployment || DEFAULT_FORM.deployment,
    databaseName: DEFAULT_FORM.databaseName,
    ...(seedRun
      ? {
          projectName: seedRun.brd_filename || '',
          source: seedSource,
          sftpEntity: normalizeFileEntity(seedSource, seedRun.sftp_entity || DEFAULT_FORM.sftpEntity),
          fileName: seedRun.brd_filename || '',
          provider: seedRun.provider || settings.provider || DEFAULT_FORM.provider,
          deployment: seedRun.deployment || settings.azure_deployment || DEFAULT_FORM.deployment,
          databaseType: seedRun.database_type || DEFAULT_FORM.databaseType,
          databaseName: seedRun.database_name || DEFAULT_FORM.databaseName,
          useDomainKb: !!seedRun.use_domain_kb,
        }
      : {}),
  }
}

const SOURCE_OPTIONS = [
  { id: 'database', label: 'Database' },
  { id: 'data_lake', label: 'Data Lake' },
]

const DATA_LAKE_OPTIONS = [
  { id: 'adls_gen2', label: 'ADLS' },
]

const PROVIDER_OPTIONS = [
  { id: 'azure_openai', label: 'Azure OpenAI' },
  { id: 'openai', label: 'OpenAI' },
  { id: 'anthropic', label: 'Anthropic' },
]

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

function buildSftpRunLabel(entity) {
  if (entity === 'both') return 'sftp:Vendor1:employee+transactions'
  return `sftp:Vendor1:${entity || 'transactions'}`
}

function buildAdlsRunLabel(entity) {
  if (entity === 'auto') return 'adls:auto-discovered'
  if (entity === 'both') return 'adls:Vendor1:employee+transactions'
  return `adls:Vendor1:${entity || 'auto'}`
}

function isFileSource(source) {
  return source === 'sftp' || source === 'adls_gen2'
}

function normalizeFileEntity(source, entity) {
  if (source === 'adls_gen2') return 'auto'
  if (source === 'sftp') {
    return ['transactions', 'employee', 'both'].includes(entity) ? entity : 'transactions'
  }
  return entity || DEFAULT_FORM.sftpEntity
}

function buildFileRunLabel(source, entity) {
  const normalizedEntity = normalizeFileEntity(source, entity)
  if (source === 'adls_gen2') return buildAdlsRunLabel(normalizedEntity)
  return buildSftpRunLabel(normalizedEntity)
}

function connectionTypeFromSource(source) {
  return source === 'adls_gen2' || source === 'sftp' ? 'data_lake' : 'database'
}

function NewRunModal({ isOpen, onClose, initialSeedRun = null }) {
  const fileInputRef = useRef(null)
  const sourceSectionRef = useRef(null)
  const addRun = useAthenaStore((s) => s.addRun)
  const addNotification = useAthenaStore((s) => s.addNotification)
  const settings = useAthenaStore((s) => s.settings)

  const [form, setForm] = useState(() => buildInitialForm(settings, initialSeedRun))
  const [uploadedFile, setUploadedFile] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [isDragging, setIsDragging] = useState(false)
  const [openSourceSelect, setOpenSourceSelect] = useState(null)

  const resetState = () => {
    setForm(buildInitialForm(settings, initialSeedRun))
    setUploadedFile(null)
    setError(null)
    setIsDragging(false)
  }

  const handleConnectionTypeChange = (connectionType) => {
    setOpenSourceSelect(null)
    setForm((current) => {
      const source = connectionType === 'data_lake' ? 'adls_gen2' : 'database'
      return {
        ...current,
        source,
        sftpEntity: normalizeFileEntity(source, current.sftpEntity),
        useDomainKb: source === 'database' ? current.useDomainKb : false,
      }
    })
  }

  const handleDataLakeTypeChange = (source) => {
    setOpenSourceSelect(null)
    setForm((current) => ({
      ...current,
      source,
      sftpEntity: normalizeFileEntity(source, current.sftpEntity),
      useDomainKb: false,
    }))
  }

  useEffect(() => {
    if (!isOpen) return
    setForm(buildInitialForm(settings, initialSeedRun))
    setUploadedFile(null)
    setError(null)
    setIsDragging(false)
    setOpenSourceSelect(null)
  }, [initialSeedRun, isOpen, settings])

  useEffect(() => {
    if (!openSourceSelect) return

    const handlePointerDown = (event) => {
      if (!sourceSectionRef.current?.contains(event.target)) {
        setOpenSourceSelect(null)
      }
    }

    const handleEscape = (event) => {
      if (event.key === 'Escape') {
        setOpenSourceSelect(null)
      }
    }

    document.addEventListener('mousedown', handlePointerDown)
    document.addEventListener('keydown', handleEscape)
    return () => {
      document.removeEventListener('mousedown', handlePointerDown)
      document.removeEventListener('keydown', handleEscape)
    }
  }, [openSourceSelect])

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
        try {
          await uploadBrd(uploadedFile)
        } catch (uploadError) {
          console.warn('[NewRunModal] BRD upload failed; continuing with parsed text', uploadError)
        }
      }

      const normalizedSftpEntity = normalizeFileEntity(form.source, form.sftpEntity)
      const displayName =
        form.projectName.trim() ||
        form.fileName ||
        (isFileSource(form.source)
          ? buildFileRunLabel(form.source, normalizedSftpEntity)
          : 'pasted_brd.txt')

      const run = await startRun({
        source: form.source,
        sftp_entity: normalizedSftpEntity,
        brd_text: form.brdText,
        brd_filename: displayName,
        provider: form.provider,
        deployment: form.deployment || undefined,
        database_type: form.databaseType,
        database_name: form.databaseName,
        budget: settings.budget,
        maxKpis: settings.maxKpis,
        devMode: settings.devMode,
        use_domain_kb: !!form.useDomainKb,
        stage_confirmation_enabled: form.stageConfirmationEnabled,
      })

      addRun({
        id: run.run_id,
        run_id: run.run_id,
        brd_filename: displayName,
        status: run.status || 'RUNNING',
        source: form.source,
        sftp_entity: normalizedSftpEntity,
        provider: form.provider,
        deployment: form.deployment || null,
        started_at: new Date().toISOString(),
        stages: [],
        kpis: [],
      })

      addNotification({
        type: 'success',
        title: 'Run Started',
        message: isFileSource(form.source)
          ? `Pipeline submitted for the ${form.source === 'adls_gen2' ? 'ADLS Gen2' : 'SFTP'} source.`
          : `Pipeline submitted for ${displayName}.`,
        duration: 4000,
      })

      handleClose()
    } catch (submitError) {
      console.error('[NewRunModal] Failed to start run', submitError)
      const message =
        submitError?.code === 'ECONNABORTED' || /timeout/i.test(submitError?.message || '')
          ? 'Backend did not start the run within 90 seconds. Check the API service and database connection.'
          : submitError?.data?.message || submitError?.message || 'Failed to start the pipeline run.'
      setError(message)
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
            className="fixed inset-0 z-40 bg-[#020617]/75 backdrop-blur-sm"
            onClick={handleClose}
          />

          <motion.div
            initial={{ opacity: 0, y: 20, scale: 0.98 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: 16, scale: 0.98 }}
            transition={{ type: 'spring', stiffness: 280, damping: 28 }}
            className="fixed inset-0 z-50 overflow-y-auto"
          >
            <div className="mx-auto flex min-h-full w-full items-start justify-center px-6 py-12">
              <div className="w-full max-w-[468px]">
                <div className="mb-5 flex items-start justify-between">
                  <div className="flex items-start gap-3">
                    <button
                      type="button"
                      onClick={handleClose}
                      className="mt-0.5 flex h-7 w-7 items-center justify-center rounded-md border border-[#2a374e] bg-[#172131] text-slate-300 transition-colors hover:bg-[#1a2537] hover:text-white"
                    >
                      <ArrowLeft size={12} />
                    </button>
                    <div>
                      <h2 className="text-[16px] font-semibold leading-none text-white">New Pipeline Run</h2>
                      <p className="mt-1 text-[10px] text-[#9fb1ca]">
                        {isFileSource(form.source)
                          ? 'Upload or paste a BRD, then configure the file source.'
                          : 'Upload or paste a BRD to extract KPIs.'}
                      </p>
                    </div>
                  </div>

                  <button
                    type="button"
                    onClick={handleClose}
                    disabled={loading}
                    className="flex h-7 w-7 items-center justify-center rounded-md text-slate-500 transition-colors hover:bg-[#172131] hover:text-white"
                  >
                    <X size={12} />
                  </button>
                </div>

              <div className="rounded-lg border border-[#243149] bg-[#141c2a] shadow-[0_18px_50px_rgba(0,0,0,0.28)]">
                <form onSubmit={handleSubmit}>
                  <div className="px-[18px] py-5">
                      <div className="space-y-5">
                        <Field label="Project Name" required>
                          <input
                            value={form.projectName}
                            onChange={(event) => setForm((current) => ({ ...current, projectName: event.target.value }))}
                            placeholder="Enter project name..."
                            className="modal-input h-7 rounded-md border-[#26344b] bg-[#0d1422] px-3 text-[11px] text-white placeholder:text-[#6f84a4]"
                          />
                        </Field>

                        <Field label="Project Description" required>
                          <textarea
                            value={form.projectDescription}
                            onChange={(event) => setForm((current) => ({ ...current, projectDescription: event.target.value }))}
                            placeholder="Briefly describe the project..."
                            className="modal-input min-h-[58px] resize-none rounded-md border-[#26344b] bg-[#0d1422] px-3 py-2 text-[11px] text-white placeholder:text-[#6f84a4]"
                          />
                        </Field>

                        <>
                          <div>
                            <label className="modal-label">BRD Document *</label>
                            <div
                              onDragOver={(event) => {
                                event.preventDefault()
                                setIsDragging(true)
                              }}
                              onDragLeave={() => setIsDragging(false)}
                              onDrop={handleDrop}
                              onClick={() => fileInputRef.current?.click()}
                              className={`mt-2 cursor-pointer rounded-lg border border-dashed px-4 py-9 text-center transition-all ${
                                isDragging
                                  ? 'border-[#4f89f2] bg-[#12203a]'
                                  : form.fileName
                                  ? 'border-emerald-400/30 bg-emerald-500/8'
                                  : 'border-[#2b3950] bg-[#141c2a] hover:border-[#4f89f2]/40 hover:bg-[#172131]'
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
                                <div className="flex items-center justify-center gap-2 text-emerald-300">
                                  <FileText size={18} />
                                  <span className="text-sm font-medium">{form.fileName}</span>
                                </div>
                              ) : (
                                <div>
                                  <Upload size={18} className="mx-auto text-slate-400" />
                                  <p className="mt-3 text-[11px] text-slate-200">
                                    Drop `.txt` or `.docx` here, or <span className="text-accent-blue">browse</span>
                                  </p>
                                  <p className="mt-1 text-[10px] text-[#8ba0bf]">Max 5 MB</p>
                                </div>
                              )}
                            </div>
                          </div>

                          <div className="flex items-center gap-3">
                            <div className="h-px flex-1 bg-[#26344b]" />
                            <span className="text-[10px] text-[#8ba0bf]">or paste text</span>
                            <div className="h-px flex-1 bg-[#26344b]" />
                          </div>

                          <Field label="BRD Text" required>
                            <textarea
                              className="modal-input min-h-[104px] resize-none rounded-md border-[#26344b] bg-[#0d1422] px-3 py-2 text-[11px] text-white placeholder:text-[#6f84a4]"
                              placeholder="Paste your Business Requirements Document here..."
                              value={form.brdText}
                              onChange={(event) =>
                                setForm((current) => ({ ...current, brdText: event.target.value }))
                              }
                            />
                          </Field>
                        </>

                        <div ref={sourceSectionRef} className="rounded-md border border-[#243149] bg-[#151f2d] p-3">
                          <div className="space-y-3">
                            <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-white">Source Connection</div>

                            <Field label="Connection Type" required compact>
                              <ModalSelect
                                id="connectionType"
                                value={connectionTypeFromSource(form.source)}
                                options={SOURCE_OPTIONS}
                                openSelect={openSourceSelect}
                                setOpenSelect={setOpenSourceSelect}
                                onChange={handleConnectionTypeChange}
                                activeBorder
                              />
                            </Field>

                            {form.source === 'database' && (
                              <>
                                <Field label="Database Type" required compact>
                                  <div className="relative">
                                    <select
                                      className="modal-input h-8 w-full appearance-none rounded-md border-[#26344b] bg-[#0d1422] px-3 pr-8 text-[11px] font-semibold text-white"
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
                                      <option value="" disabled>Select type...</option>
                                      <option value="azure_sql">Azure SQL</option>
                                      <option value="postgresql">PostgreSQL</option>
                                    </select>
                                    <ChevronDown size={15} className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-white" />
                                  </div>
                                </Field>
                                <Field label="Database Name" compact>
                                  <div className="relative">
                                    <select
                                      className="modal-input h-8 w-full appearance-none rounded-md border-[#26344b] bg-[#0d1422] px-3 pr-8 text-[11px] font-semibold text-white"
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
                                    <ChevronDown size={15} className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-white" />
                                  </div>
                                </Field>
                              </>
                            )}

                            {connectionTypeFromSource(form.source) === 'data_lake' && (
                              <div className="space-y-3">
                                <Field label="Data Lake Type" required compact>
                                  <ModalSelect
                                    id="dataLakeType"
                                    value={form.source === 'adls_gen2' ? 'adls_gen2' : ''}
                                    options={DATA_LAKE_OPTIONS}
                                    placeholder="Select type..."
                                    openSelect={openSourceSelect}
                                    setOpenSelect={setOpenSourceSelect}
                                    onChange={handleDataLakeTypeChange}
                                  />
                                </Field>

                                <div className="rounded-md border border-[#26344b] bg-[#0d1422] px-3 py-3">
                                  <div className="text-[11px] font-semibold text-white">ADLS Source</div>
                                  <div className="mt-2 grid gap-2 text-[10px] text-[#9fb1ca]">
                                    <div className="rounded border border-[#1e2a3d] bg-[#101827] px-2.5 py-2">
                                      <div className="text-[#6f84a4]">Account</div>
                                      <div className="mt-1 break-all font-mono text-white">https://atheastorage.dfs.core.windows.net</div>
                                    </div>
                                    <div className="grid grid-cols-2 gap-2">
                                      <div className="rounded border border-[#1e2a3d] bg-[#101827] px-2.5 py-2">
                                        <div className="text-[#6f84a4]">File system</div>
                                        <div className="mt-1 font-mono text-white">ADLS_FILE_SYSTEM</div>
                                      </div>
                                      <div className="rounded border border-[#1e2a3d] bg-[#101827] px-2.5 py-2">
                                        <div className="text-[#6f84a4]">Root</div>
                                        <div className="mt-1 font-mono text-white">ADLS_SOURCE_ROOT</div>
                                      </div>
                                    </div>
                                    <div className="rounded border border-[#1e2a3d] bg-[#101827] px-2.5 py-2">
                                      <div className="text-[#6f84a4]">Mode</div>
                                      <div className="mt-1 text-white">Auto-discover folders and files</div>
                                    </div>
                                  </div>
                                </div>
                              </div>
                            )}
                          </div>
                        </div>

                        {form.source === 'database' && (
                          <label className="flex h-10 items-center gap-3 rounded-md border border-[#243149] bg-[#151f2d] px-3 text-[11px] font-semibold text-white">
                            <input
                              type="checkbox"
                              checked={!!form.useDomainKb}
                              onChange={(event) =>
                                setForm((current) => ({ ...current, useDomainKb: event.target.checked }))
                              }
                              className="h-4 w-4 accent-[#3f82ff]"
                            />
                            <BookOpen size={13} className="text-slate-300" />
                            <span>Use Domain Knowledge Base</span>
                          </label>
                        )}

                        {form.source === 'database' && (
                          <label className="flex items-center gap-3 rounded-md border border-[#243149] bg-[#151f2d] px-3 py-3 text-[11px] font-semibold text-white">
                            <input
                              type="checkbox"
                              checked={!!form.stageConfirmationEnabled}
                              onChange={(event) =>
                                setForm((current) => ({ ...current, stageConfirmationEnabled: event.target.checked }))
                              }
                              className="h-4 w-4 accent-[#3f82ff]"
                            />
                            <span>Ask before moving to every next stage</span>
                          </label>
                        )}

                        <div>
                          <div className="space-y-3">
                            <div>
                              <label className="modal-label">LLM Provider</label>
                              <div className="mt-2 grid grid-cols-3 gap-2">
                                {PROVIDER_OPTIONS.map((option) => {
                                  const active = form.provider === option.id
                                  return (
                                    <button
                                      key={option.id}
                                      type="button"
                                      onClick={() => setForm((current) => ({ ...current, provider: option.id }))}
                                      className={`h-8 rounded-md border px-2 text-[10px] font-medium transition-colors ${
                                        active
                                          ? 'border-[#4585f5] bg-[#2453a6] text-white'
                                          : 'border-[#26344b] bg-[#0d1422] text-[#a6b6cf] hover:text-white'
                                      }`}
                                    >
                                      {option.label}
                                    </button>
                                  )
                                })}
                              </div>
                            </div>

                            <Field label="Azure Endpoint" compact>
                              <input
                                className="modal-input h-8 rounded-md border-[#26344b] bg-[#0d1422] px-3 text-[11px] text-white"
                                placeholder="https://athena-openai.openai.azure.com/"
                                value={settings.azure_endpoint || ''}
                                readOnly
                              />
                            </Field>

                            <Field label="Deployment Name" compact>
                              <input
                                className="modal-input h-8 rounded-md border-[#26344b] bg-[#0d1422] px-3 text-[11px] text-white"
                                value={form.deployment}
                                onChange={(event) => setForm((current) => ({ ...current, deployment: event.target.value }))}
                                placeholder="gpt-4o-athena"
                              />
                            </Field>

                            <div className="grid grid-cols-2 gap-3">
                              <Field label="Budget (USD)" compact>
                                <input className="modal-input h-8 rounded-md border-[#26344b] bg-[#0d1422] px-3 text-[11px] text-white" value={settings.budget} readOnly />
                              </Field>
                              <Field label="Max KPIs" compact>
                                <input className="modal-input h-8 rounded-md border-[#26344b] bg-[#0d1422] px-3 text-[11px] text-white" value={settings.maxKpis} readOnly />
                              </Field>
                            </div>
                          </div>
                        </div>

                        {error && (
                          <div className="rounded-md border border-red-400/20 bg-red-500/10 px-3 py-2 text-xs text-red-300">
                            {error}
                          </div>
                        )}
                      </div>
                  </div>

                  <div className="flex items-center gap-2 border-t border-[#243149] px-[18px] py-3">
                    <button
                      type="button"
                      onClick={handleClose}
                      disabled={loading}
                      className="inline-flex h-8 flex-1 items-center justify-center rounded-md border border-[#26344b] bg-[#2a3443] px-4 text-[11px] font-semibold text-slate-200 transition-colors hover:bg-[#263142] hover:text-white disabled:opacity-50"
                    >
                      Cancel
                    </button>
                    <button
                      type="submit"
                      disabled={loading || !form.brdText.trim()}
                      className="inline-flex h-8 flex-1 items-center justify-center gap-1.5 rounded-md bg-[#3f7df0] px-4 text-[11px] font-semibold text-white transition-colors hover:bg-[#4f89f2] disabled:cursor-not-allowed disabled:opacity-50"
                    >
                      {loading ? (
                        <>
                          <Loader2 size={13} className="animate-spin" />
                          Starting...
                        </>
                      ) : (
                        <>
                          <Play size={13} />
                          Start Run
                        </>
                      )}
                    </button>
                  </div>
                </form>
              </div>
              </div>
            </div>
          </motion.div>
        </>
      )}
    </AnimatePresence>
  )
}

function Field({ label, required = false, compact = false, children }) {
  return (
    <div>
      <label className={`modal-label ${compact ? '' : 'mb-2'}`}>
        {label} {required ? '*' : ''}
      </label>
      {children}
    </div>
  )
}

function ModalSelect({
  id,
  value,
  options,
  placeholder = 'Select type...',
  openSelect,
  setOpenSelect,
  onChange,
  activeBorder = false,
}) {
  const open = openSelect === id
  const selected = options.find((option) => option.id === value)

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setOpenSelect(open ? null : id)}
        className={`flex h-8 w-full items-center justify-between rounded-md border bg-[#0d1422] px-3 text-left text-[11px] font-semibold text-white transition-colors ${
          open ? 'border-[#4585f5] ring-1 ring-[#4585f5]' : activeBorder ? 'border-[#4585f5]' : 'border-[#26344b] hover:border-[#4585f5]/70'
        }`}
      >
        <span className={selected ? 'text-white' : 'text-[#6f84a4]'}>
          {selected?.label || placeholder}
        </span>
        <ChevronDown size={15} className={`text-white transition-transform ${open ? 'rotate-180' : ''}`} />
      </button>

      {open && (
        <div className="absolute left-0 right-0 top-[calc(100%+4px)] z-[80] overflow-hidden rounded-md border border-[#4585f5] bg-[#070d1a] shadow-[0_16px_36px_rgba(0,0,0,0.45)]">
          <button
            type="button"
            disabled
            className="block h-8 w-full cursor-default px-3 text-left text-[10px] font-medium text-[#6f84a4]"
          >
            {placeholder}
          </button>
          {options.map((option) => (
            <button
              key={option.id}
              type="button"
              onClick={() => onChange(option.id)}
              className={`block h-9 w-full px-3 text-left text-[11px] font-semibold transition-colors ${
                option.id === value ? 'bg-[#1b2a45] text-white' : 'bg-[#070d1a] text-white hover:bg-[#172131]'
              }`}
            >
              {option.label}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

export default NewRunModal
