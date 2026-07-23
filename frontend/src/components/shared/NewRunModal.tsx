// @ts-nocheck
import React, { useCallback, useEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import {
  ArrowLeft,
  ChevronDown,
  FileText,
  Folder,
  Loader2,
  Play,
  Upload,
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
  targetWarehouse: 'databricks',
  useDomainKb: false,
  domainProfile: '',
  knowledgeBaseId: '',
  complianceEnabled: false,
  stageConfirmationEnabled: false,
}

function buildInitialForm(settings, seedRun, project) {
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
          targetWarehouse: seedRun.target_warehouse || DEFAULT_FORM.targetWarehouse,
          useDomainKb: !!seedRun.use_domain_kb,
          domainProfile: seedRun.domain_profile || '',
          knowledgeBaseId: seedRun.knowledge_base_id || '',
          complianceEnabled: !!seedRun.compliance_enabled,
        }
      : {}),
    ...(project ? {
      projectName: project.name,
      projectDescription: project.description,
      source: project.connectionType === 'data_lake' ? 'adls_gen2' : 'database',
      databaseType: project.dbType || DEFAULT_FORM.databaseType,
      databaseName: project.databaseName || DEFAULT_FORM.databaseName,
      targetWarehouse: String(project.target || 'Databricks').toLowerCase(),
      useDomainKb: !!project.useDomainKB,
      domainProfile: project.domainProfile || '',
      knowledgeBaseId: project.knowledgeBaseId || '',
    } : {}),
  }
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

function NewRunModal({ isOpen, onClose, initialSeedRun = null, pageMode = false, project = null }) {
  const navigate = useNavigate()
  const fileInputRef = useRef(null)
  const addRun = useAthenaStore((s) => s.addRun)
  const setActiveRun = useAthenaStore((s) => s.setActiveRun)
  const addNotification = useAthenaStore((s) => s.addNotification)
  const settings = useAthenaStore((s) => s.settings)

  const [form, setForm] = useState(() => buildInitialForm(settings, initialSeedRun, project))
  const [uploadedFile, setUploadedFile] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [isDragging, setIsDragging] = useState(false)

  const resetState = () => {
    setForm(buildInitialForm(settings, initialSeedRun, project))
    setUploadedFile(null)
    setError(null)
    setIsDragging(false)
  }

  useEffect(() => {
    if (!isOpen) return
    setForm(buildInitialForm(settings, initialSeedRun, project))
    setUploadedFile(null)
    setError(null)
    setIsDragging(false)
  }, [initialSeedRun, isOpen, project, settings])

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

    const normalizedSftpEntity = normalizeFileEntity(form.source, form.sftpEntity)
    const displayName =
      form.projectName.trim() ||
      form.fileName ||
      (isFileSource(form.source)
        ? buildFileRunLabel(form.source, normalizedSftpEntity)
        : 'pasted_brd.txt')
    const startedAt = new Date().toISOString()
    addNotification({
      type: 'info',
      title: 'Starting Run',
      message: isFileSource(form.source)
        ? `Submitting the ${form.source === 'adls_gen2' ? 'ADLS Gen2' : 'SFTP'} pipeline run.`
        : `Submitting ${displayName}.`,
      duration: 3000,
    })

    try {
      if (uploadedFile) {
        try {
          await uploadBrd(uploadedFile)
        } catch (uploadError) {
          console.warn('[NewRunModal] BRD upload failed; continuing with parsed text', uploadError)
        }
      }

      const run = await startRun({
        project_id: project?.id,
        source: form.source,
        sftp_entity: normalizedSftpEntity,
        brd_text: form.brdText,
        brd_filename: displayName,
        provider: form.provider,
        deployment: form.deployment || undefined,
        database_type: form.databaseType,
        database_name: form.databaseName,
        target_warehouse: form.targetWarehouse,
        budget: settings.budget,
        maxKpis: settings.maxKpis,
        devMode: settings.devMode,
        use_domain_kb: !!form.useDomainKb,
        compliance_enabled: !!form.complianceEnabled,
        stage_confirmation_enabled: form.stageConfirmationEnabled,
      })

      const newRun = {
        id: run.run_id,
        run_id: run.run_id,
        project_id: project?.id || null,
        project_name: project?.name || null,
        brd_filename: displayName,
        status: run.status || 'RUNNING',
        background_stage: 'ingestion',
        resume_message: 'BRD Ingest is running.',
        source: form.source,
        sftp_entity: normalizedSftpEntity,
        provider: form.provider,
        deployment: form.deployment || null,
        target_warehouse: form.targetWarehouse,
        use_domain_kb: !!form.useDomainKb,
        started_at: startedAt,
        stages: [],
        kpis: [],
      }

      addRun(newRun)
      setActiveRun(run.run_id)
      resetState()
      onClose()
      navigate('/app/data-discovery', {
        replace: true,
        state: {
          pendingRun: {
            id: run.run_id,
            label: displayName,
            startedAt,
          },
        },
      })

      addNotification({
        type: 'success',
        title: 'Run Started',
        message: isFileSource(form.source)
          ? `Pipeline submitted for the ${form.source === 'adls_gen2' ? 'ADLS Gen2' : 'SFTP'} source.`
          : `Pipeline submitted for ${displayName}.`,
        duration: 4000,
      })
    } catch (submitError) {
      console.error('[NewRunModal] Failed to start run', submitError)
      const message =
        submitError?.code === 'ECONNABORTED' || /timeout/i.test(submitError?.message || '')
          ? 'Backend did not start the run within 90 seconds. Check the API service and database connection.'
          : submitError?.data?.message || submitError?.message || 'Failed to start the pipeline run.'
      setError(message)
      addNotification({
        type: 'error',
        title: 'Run Start Failed',
        message,
        duration: 5000,
      })
    } finally {
      setLoading(false)
    }
  }

  return (
    <AnimatePresence>
      {isOpen && (
        <>
          {!pageMode && (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="fixed inset-0 z-40 bg-[#020617]/75 backdrop-blur-sm"
              onClick={handleClose}
            />
          )}

          <motion.div
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: 12 }}
            transition={{ type: 'spring', stiffness: 280, damping: 28 }}
            className={pageMode ? 'relative h-full overflow-y-auto' : 'fixed inset-0 z-50 overflow-y-auto'}
          >
            <div className={pageMode
              ? 'mx-auto flex min-h-full w-full items-start justify-center px-4 pb-4 pt-2 sm:px-6'
              : 'mx-auto flex min-h-full w-full items-start justify-center px-6 py-10'}>
              <div className="w-full max-w-5xl">
                <div className="mb-3 flex items-start justify-between">
                  <div className="flex items-center gap-3">
                    <button
                      type="button"
                      onClick={handleClose}
                      className="flex h-9 w-9 items-center justify-center rounded-lg border border-bg-border bg-bg-card text-text-tertiary transition-colors hover:bg-bg-hover hover:text-text-secondary"
                    >
                      <ArrowLeft size={16} />
                    </button>
                    <div>
                      <h2 className="text-xl font-bold text-text-primary">New Pipeline Run</h2>
                      <p className="mt-0.5 text-xs text-text-tertiary">Select a project and upload a BRD to extract KPIs</p>
                    </div>
                  </div>
                </div>

              <div className="overflow-hidden rounded-xl border border-bg-border bg-bg-card shadow-sm">
                <form onSubmit={handleSubmit}>
                  <div className="grid border-b border-bg-border lg:grid-cols-2">
                    <div className="space-y-3 border-b border-bg-border p-4 lg:border-b-0 lg:border-r">
                        <Field label={project ? 'Project' : 'Project Name'} required>
                          <div className="relative">
                            <input
                              value={form.projectName}
                              onChange={(event) => setForm((current) => ({ ...current, projectName: event.target.value }))}
                              placeholder="Enter project name..."
                              className="input-field h-11 pr-10"
                              readOnly={!!project}
                            />
                            {project && <ChevronDown size={15} className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-text-tertiary" />}
                          </div>
                        </Field>

                        {project && (
                          <div className="rounded-lg border border-bg-border bg-bg-base p-3">
                            <div className="mb-1 flex items-center gap-2">
                              <Folder size={13} className="text-accent-blue" />
                              <span className="text-sm font-semibold text-text-primary">{project.name}</span>
                            </div>
                            <p className="text-xs leading-relaxed text-text-secondary">{project.description || 'No description'}</p>
                            <div className="mt-3 inline-flex items-center gap-1.5 rounded-md border border-bg-border bg-bg-card px-2.5 py-1.5">
                              <span className="text-[10px] font-semibold uppercase text-text-tertiary">Target:</span>
                              <span className="text-xs font-medium text-text-secondary">{project.target || form.targetWarehouse}</span>
                            </div>
                          </div>
                        )}

                        <>
                          <div>
                            <label className="mb-1 block text-xs font-medium text-text-secondary">
                              BRD Document <span className="text-accent-red">*</span>
                            </label>
                            <div
                              onDragOver={(event) => {
                                event.preventDefault()
                                setIsDragging(true)
                              }}
                              onDragLeave={() => setIsDragging(false)}
                              onDrop={handleDrop}
                              onClick={() => fileInputRef.current?.click()}
                              className={`flex h-20 cursor-pointer items-center rounded-xl border-2 border-dashed p-3 transition-all ${
                                isDragging
                                  ? 'border-[#4f89f2] bg-[#12203a]'
                                  : form.fileName
                                  ? 'border-emerald-400/30 bg-emerald-500/8'
                                  : 'border-[#566174] bg-[#111827] hover:border-[#4f89f2]/50 hover:bg-[#172131]'
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
                                <div className="flex min-w-0 items-center gap-3 text-accent-green">
                                  <span className="flex h-9 w-9 flex-shrink-0 items-center justify-center rounded-lg border border-bg-border bg-bg-base">
                                    <FileText size={18} />
                                  </span>
                                  <span className="min-w-0">
                                    <span className="block truncate text-sm font-medium">{form.fileName}</span>
                                    <span className="block text-xs text-text-tertiary">Click to replace - .txt or .docx - Max 5 MB</span>
                                  </span>
                                </div>
                              ) : (
                                <div className="flex items-center gap-3">
                                  <span className="flex h-9 w-9 items-center justify-center rounded-lg border border-bg-border bg-bg-base text-text-tertiary">
                                    <Upload size={18} />
                                  </span>
                                  <div>
                                  <p className="text-sm font-medium text-text-primary">
                                    Drop .txt or .docx here, or <span className="text-accent-blue">browse</span>
                                  </p>
                                  <p className="text-xs text-text-tertiary">Max 5 MB</p>
                                  </div>
                                </div>
                              )}
                            </div>
                          </div>

                          <Field label="BRD Content" required>
                            <textarea
                              className="input-field min-h-[180px] resize-none overflow-y-auto"
                              placeholder="Uploaded BRD content will appear here..."
                              value={form.brdText}
                              readOnly={!!project}
                              onChange={(event) =>
                                setForm((current) => ({ ...current, brdText: event.target.value }))
                              }
                            />
                          </Field>
                        </>
                    </div>

                    <div className="p-4">
                      <div className="space-y-4">
                        <div>
                          <h3 className="text-base font-semibold text-text-primary">Project Source Configuration</h3>
                          <p className="mt-0.5 text-xs text-text-tertiary">Read-only configuration loaded from the selected project.</p>
                        </div>

                        <div>
                          <div className="mb-2 text-xs font-medium text-text-secondary">Source Configuration</div>
                          <div className="space-y-3">
                            <Field label="Source Type" compact>
                              <div className="relative">
                                <input
                                  className="input-field h-11 cursor-not-allowed pr-10 opacity-80"
                                  value={connectionTypeFromSource(form.source) === 'database' ? 'Database' : 'Data Lake'}
                                  readOnly
                                />
                                <ChevronDown size={15} className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-text-tertiary" />
                              </div>
                            </Field>

                            {form.source === 'database' ? (
                              <>
                                <Field label="Database Type" compact>
                                  <input
                                    className="input-field h-11 cursor-not-allowed opacity-80"
                                    value={form.databaseType === 'azure_sql' ? 'Azure SQL DB' : form.databaseType === 'postgresql' ? 'PostgreSQL' : form.databaseType || '-'}
                                    readOnly
                                  />
                                </Field>
                                <Field label="Database Name" compact>
                                  <input className="input-field h-11 cursor-not-allowed opacity-80" value={form.databaseName || '-'} readOnly />
                                </Field>
                                <div className="rounded-lg border border-bg-border bg-bg-base p-3">
                                  <div className="flex items-center justify-between gap-3">
                                    <span className="text-xs font-medium text-text-secondary">Domain Knowledge Base</span>
                                    <span className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold ${
                                      form.useDomainKb
                                        ? 'border-accent-blue/20 bg-accent-blue/10 text-accent-blue'
                                        : 'border-bg-border bg-bg-card text-text-tertiary'
                                    }`}>
                                      {form.useDomainKb ? 'Enabled' : 'Disabled'}
                                    </span>
                                  </div>
                                  {form.useDomainKb && (
                                    <div className="mt-3 grid gap-3 sm:grid-cols-2">
                                      <Field label="Domain Profile" compact>
                                        <input className="input-field h-11 cursor-not-allowed opacity-80" value={form.domainProfile || '-'} readOnly />
                                      </Field>
                                      <Field label="Knowledge Base ID" compact>
                                        <input className="input-field h-11 cursor-not-allowed opacity-80" value={form.knowledgeBaseId || '-'} readOnly />
                                      </Field>
                                    </div>
                                  )}
                                </div>
                              </>
                            ) : (
                              <>
                                <Field label="Data Lake Type" compact>
                                  <input className="input-field h-11 cursor-not-allowed opacity-80" value="ADLS" readOnly />
                                </Field>
                                <Field label="Data Entity" compact>
                                  <input className="input-field h-11 cursor-not-allowed opacity-80" value={form.sftpEntity || '-'} readOnly />
                                </Field>
                              </>
                            )}
                          </div>
                        </div>

                        {error && (
                          <div className="rounded-md border border-accent-red/30 bg-red-500/10 px-3 py-2 text-xs text-accent-red">
                            {error}
                          </div>
                        )}
                      </div>
                    </div>
                  </div>

                  <div className="flex items-center gap-3 border-t border-bg-border px-4 py-3">
                    <button
                      type="button"
                      onClick={handleClose}
                      disabled={loading}
                      className="btn-secondary inline-flex h-11 flex-1 items-center justify-center disabled:opacity-50"
                    >
                      Cancel
                    </button>
                    <button
                      type="submit"
                      disabled={loading || !form.brdText.trim()}
                      className="btn-primary inline-flex h-11 flex-1 items-center justify-center gap-2 disabled:cursor-not-allowed disabled:opacity-50"
                    >
                      {loading ? (
                        <>
                          <Loader2 size={18} className="animate-spin" />
                          Starting...
                        </>
                      ) : (
                        <>
                          <Play size={18} />
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
      <label className={`mb-1 block text-xs font-medium text-text-secondary ${compact ? '' : ''}`}>
        {label} {required ? <span className="text-accent-red">*</span> : null}
      </label>
      {children}
    </div>
  )
}

export default NewRunModal
