// @ts-nocheck
import React, { useCallback, useRef, useState } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import {
  ArrowLeft,
  BookOpen,
  Database,
  FileText,
  FolderOpen,
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
  stageConfirmationEnabled: true,
}

const SOURCE_OPTIONS = [
  { id: 'database', label: 'Database', icon: Database },
  { id: 'sftp', label: 'SFTP', icon: FolderOpen },
  { id: 'adls_gen2', label: 'ADLS', icon: FolderOpen },
]

const PROVIDER_OPTIONS = [
  { id: 'azure_openai', label: 'Azure OpenAI' },
  { id: 'openai', label: 'OpenAI' },
  { id: 'anthropic', label: 'Anthropic' },
]

const SFTP_OPTIONS = [
  {
    id: 'transactions',
    name: 'Vendor1 transactions',
    host: 'localhost:2222',
    path: '/cash-project/Vendor1/transactions/',
  },
  {
    id: 'employee',
    name: 'Vendor1 employee',
    host: 'localhost:2222',
    path: '/cash-project/Vendor1/employee/',
  },
  {
    id: 'both',
    name: 'Vendor1 both feeds',
    host: 'localhost:2222',
    path: '/cash-project/Vendor1/{transactions,employee}/',
  },
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

function buildFileRunLabel(source, entity) {
  if (source === 'adls_gen2') return buildAdlsRunLabel(entity)
  return buildSftpRunLabel(entity)
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
    if (!isFileSource(form.source) && !form.brdText.trim()) {
      setError('Please provide BRD text or upload a file.')
      return
    }

    setLoading(true)
    setError(null)

    try {
      if (uploadedFile) {
        await uploadBrd(uploadedFile)
      }

      const displayName =
        form.projectName.trim() ||
        form.fileName ||
        (isFileSource(form.source)
          ? buildFileRunLabel(form.source, form.sftpEntity)
          : 'pasted_brd.txt')

      const run = await startRun({
        source: form.source,
        sftp_entity: form.sftpEntity,
        brd_text: form.brdText,
        brd_filename: displayName,
        provider: form.provider,
        deployment: form.deployment || undefined,
        database_type: form.databaseType,
        database_name: form.databaseName,
        budget: settings.budget,
        maxKpis: settings.maxKpis,
        devMode: settings.devMode,
        stage_confirmation_enabled: form.stageConfirmationEnabled,
      })

      addRun({
        id: run.run_id,
        run_id: run.run_id,
        brd_filename: displayName,
        status: run.status || 'RUNNING',
        source: form.source,
        sftp_entity: form.sftpEntity,
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
        submitError?.data?.message || submitError?.message || 'Failed to start the pipeline run.'
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
                          ? 'Configure a file-source ingestion run.'
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

                        {!isFileSource(form.source) && (
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
                        )}

                        {isFileSource(form.source) && (
                          <div className="space-y-4">
                            <Field label="Pipeline Context">
                              <textarea
                                className="modal-input min-h-[104px] resize-none rounded-md border-[#26344b] bg-[#0d1422] px-3 py-2 text-[11px] text-white placeholder:text-[#6f84a4]"
                                placeholder="Optional business context for this file pipeline..."
                                value={form.brdText}
                                onChange={(event) =>
                                  setForm((current) => ({ ...current, brdText: event.target.value }))
                                }
                              />
                            </Field>

                            <div className="rounded-[8px] border border-[#243149] bg-[#0d1422] p-3">
                              {form.source === 'adls_gen2' ? (
                                <div className="space-y-2 text-xs text-slate-300">
                                  <div className="font-medium text-white">ADLS Source</div>
                                  <div>Account: `https://atheastorage.dfs.core.windows.net`</div>
                                  <div className="text-slate-400">File system: backend `ADLS_FILE_SYSTEM`</div>
                                  <div className="text-slate-400">Root: backend `ADLS_SOURCE_ROOT`</div>
                                  <div className="text-slate-400">Mode: auto-discover folders and files</div>
                                </div>
                              ) : (
                                <div className="space-y-3">
                                  <Field label="Configured SFTP Source">
                                    <select
                                      className="modal-input h-10 rounded-md border-[#26344b] bg-[#101827] px-3 text-xs text-white"
                                      value={form.sftpEntity}
                                      onChange={(event) =>
                                        setForm((current) => ({ ...current, sftpEntity: event.target.value }))
                                      }
                                    >
                                      {SFTP_OPTIONS.map((option) => (
                                        <option key={option.id} value={option.id}>
                                          {option.name}
                                        </option>
                                      ))}
                                    </select>
                                  </Field>
                                  <div className="space-y-1 text-xs text-slate-400">
                                    <div>{SFTP_OPTIONS.find((option) => option.id === form.sftpEntity)?.host || SFTP_OPTIONS[0].host}</div>
                                    <div>{SFTP_OPTIONS.find((option) => option.id === form.sftpEntity)?.path || SFTP_OPTIONS[0].path}</div>
                                  </div>
                                </div>
                              )}
                            </div>
                          </div>
                        )}

                        <div className="rounded-md border border-[#243149] bg-[#151f2d] p-3">
                          <div className="space-y-3">
                            <div>
                              <label className="modal-label">Database Connection</label>
                              <div className="mt-2 grid grid-cols-3 gap-2">
                                {SOURCE_OPTIONS.map((option) => {
                                  const Icon = option.icon
                                  const active = form.source === option.id
                                  return (
                                    <button
                                      key={option.id}
                                      type="button"
                                      onClick={() =>
                                        setForm((current) => ({
                                          ...current,
                                          source: option.id,
                                          sftpEntity: option.id === 'adls_gen2' ? 'auto' : current.sftpEntity,
                                        }))
                                      }
                                      className={`flex h-9 items-center justify-center gap-1.5 rounded-md border text-xs font-medium transition-colors ${
                                        active
                                          ? 'border-[#4585f5] bg-[#2453a6] text-white'
                                          : 'border-[#26344b] bg-[#0d1422] text-[#a6b6cf] hover:text-white'
                                      }`}
                                    >
                                      <Icon size={13} />
                                      {option.label}
                                    </button>
                                  )
                                })}
                              </div>
                            </div>

                            {form.source === 'database' && (
                              <>
                                <Field label="Database Type" required compact>
                                  <select
                                    className="modal-input h-8 rounded-md border-[#26344b] bg-[#0d1422] px-3 text-[11px] font-semibold text-white"
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
                                </Field>
                                <Field label="Database Name" compact>
                                  <select
                                    className="modal-input h-8 rounded-md border-[#26344b] bg-[#0d1422] px-3 text-[11px] font-semibold text-white"
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
                                </Field>
                              </>
                            )}

                            {form.source === 'sftp' && (
                              <>
                                <Field label="Configured SFTP Source">
                                  <select
                                    className="modal-input h-10 rounded-md border-[#26344b] bg-[#0d1422] px-3 text-xs text-white"
                                    value={form.sftpEntity}
                                    onChange={(event) =>
                                      setForm((current) => ({ ...current, sftpEntity: event.target.value }))
                                    }
                                  >
                                    {SFTP_OPTIONS.map((option) => (
                                      <option key={option.id} value={option.id}>
                                        {option.name}
                                      </option>
                                    ))}
                                  </select>
                                </Field>
                                <div className="space-y-1 text-xs text-slate-400">
                                  <div>{SFTP_OPTIONS.find((option) => option.id === form.sftpEntity)?.host || SFTP_OPTIONS[0].host}</div>
                                  <div>{SFTP_OPTIONS.find((option) => option.id === form.sftpEntity)?.path || SFTP_OPTIONS[0].path}</div>
                                </div>
                              </>
                            )}

                            {form.source === 'adls_gen2' && (
                              <div className="space-y-2 text-xs text-slate-300">
                                <div className="font-medium text-white">ADLS Source</div>
                                <div>Account: `https://atheastorage.dfs.core.windows.net`</div>
                                <div className="text-slate-400">File system: backend `ADLS_FILE_SYSTEM`</div>
                                <div className="text-slate-400">Root: backend `ADLS_SOURCE_ROOT`</div>
                                <div className="text-slate-400">Mode: auto-discover folders and files</div>
                              </div>
                            )}
                          </div>
                        </div>

                        <label className="flex h-10 items-center gap-3 rounded-md border border-[#243149] bg-[#151f2d] px-3 text-[11px] font-semibold text-white">
                          <span className="relative inline-flex h-4 w-7 items-center rounded-full bg-[#26344b]">
                            <span className="ml-0.5 h-3 w-3 rounded-full bg-white" />
                          </span>
                          <BookOpen size={13} className="text-slate-300" />
                          <span>Use Domain Knowledge Base</span>
                        </label>

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
                      disabled={loading || (!isFileSource(form.source) && !form.brdText.trim())}
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

export default NewRunModal
