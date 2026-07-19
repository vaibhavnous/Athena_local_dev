// @ts-nocheck
import React, { useCallback, useEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import {
  ArrowLeft,
  BookOpenCheck,
  ChevronDown,
  FileText,
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
    } : {}),
  }
}

const SOURCE_OPTIONS = [
  { id: 'database', label: 'Database' },
  { id: 'data_lake', label: 'Data Lake' },
]

const DATA_LAKE_OPTIONS = [
  { id: 'adls_gen2', label: 'ADLS' },
]

const TARGET_WAREHOUSE_OPTIONS = [
  { id: 'databricks', label: 'Databricks' },
  { id: 'snowflake', label: 'Snowflake' },
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

function NewRunModal({ isOpen, onClose, initialSeedRun = null, pageMode = false, project = null }) {
  const navigate = useNavigate()
  const fileInputRef = useRef(null)
  const sourceSectionRef = useRef(null)
  const addRun = useAthenaStore((s) => s.addRun)
  const setActiveRun = useAthenaStore((s) => s.setActiveRun)
  const addNotification = useAthenaStore((s) => s.addNotification)
  const settings = useAthenaStore((s) => s.settings)

  const [form, setForm] = useState(() => buildInitialForm(settings, initialSeedRun, project))
  const [uploadedFile, setUploadedFile] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [isDragging, setIsDragging] = useState(false)
  const [openSourceSelect, setOpenSourceSelect] = useState(null)

  const resetState = () => {
    setForm(buildInitialForm(settings, initialSeedRun, project))
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
    setForm(buildInitialForm(settings, initialSeedRun, project))
    setUploadedFile(null)
    setError(null)
    setIsDragging(false)
    setOpenSourceSelect(null)
  }, [initialSeedRun, isOpen, project, settings])

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
            initial={{ opacity: 0, y: 20, scale: 0.98 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: 16, scale: 0.98 }}
            transition={{ type: 'spring', stiffness: 280, damping: 28 }}
            className={pageMode ? 'relative h-full overflow-y-auto' : 'fixed inset-0 z-50 overflow-y-auto'}
          >
            <div className={pageMode
              ? 'mx-auto flex min-h-full w-full items-start justify-center px-3 py-3 [zoom:0.82]'
              : 'mx-auto flex min-h-full w-full items-start justify-center px-6 py-12'}>
              <div className="w-full max-w-[1317px]">
                <div className="mb-5 flex items-start justify-between">
                  <div className="flex items-start gap-4">
                    <button
                      type="button"
                      onClick={handleClose}
                      className="mt-0.5 flex h-11 w-11 items-center justify-center rounded-[10px] border border-[#233047] bg-[#111a2b] text-slate-300 transition-colors hover:bg-[#172131] hover:text-white"
                    >
                      <ArrowLeft size={20} />
                    </button>
                    <div>
                      <h2 className="text-[28px] font-semibold leading-tight text-white">New Pipeline Run</h2>
                      <p className="mt-1 text-[15px] text-[#b5c3da]">
                        {isFileSource(form.source)
                          ? 'Upload or paste a BRD, then configure the file source.'
                          : 'Upload or paste a BRD to extract KPIs.'}
                      </p>
                    </div>
                  </div>
                </div>

              <div className="overflow-hidden rounded-xl border border-[#223047] bg-[#111827] shadow-[0_22px_60px_rgba(0,0,0,0.32)]">
                <form onSubmit={handleSubmit}>
                  <div className="grid min-h-[684px] lg:grid-cols-2">
                    <div className="space-y-5 px-[21px] py-6">
                        <Field label="Project Name" required>
                          <input
                            value={form.projectName}
                            onChange={(event) => setForm((current) => ({ ...current, projectName: event.target.value }))}
                            placeholder="Enter project name..."
                            className="modal-input h-[60px] rounded-[10px] border-[#26344b] bg-[#070d1a] px-4 text-[18px] text-white placeholder:text-[#b8c5db]"
                            readOnly={!!project}
                          />
                        </Field>

                        <Field label="Project Description" required>
                          <textarea
                            value={form.projectDescription}
                            onChange={(event) => setForm((current) => ({ ...current, projectDescription: event.target.value }))}
                            placeholder="Briefly describe the project..."
                            className="modal-input min-h-[107px] resize-none rounded-[10px] border-[#26344b] bg-[#070d1a] px-4 py-4 text-[18px] text-white placeholder:text-[#b8c5db]"
                            readOnly={!!project}
                          />
                        </Field>

                        <>
                          <div>
                            <label className="mb-2 block text-[16px] font-semibold leading-tight text-slate-100">
                              BRD Document <span className="text-[#ff5c57]">*</span>
                            </label>
                            <div
                              onDragOver={(event) => {
                                event.preventDefault()
                                setIsDragging(true)
                              }}
                              onDragLeave={() => setIsDragging(false)}
                              onDrop={handleDrop}
                              onClick={() => fileInputRef.current?.click()}
                              className={`mt-2 flex min-h-[108px] cursor-pointer items-center rounded-[14px] border border-dashed px-[17px] py-4 transition-all ${
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
                                <div className="flex items-center gap-4 text-emerald-300">
                                  <span className="flex h-12 w-12 items-center justify-center rounded-[10px] border border-emerald-400/20 bg-emerald-400/10">
                                    <FileText size={22} />
                                  </span>
                                  <span className="text-[18px] font-semibold">{form.fileName}</span>
                                </div>
                              ) : (
                                <div className="flex items-center gap-4">
                                  <span className="flex h-12 w-12 items-center justify-center rounded-[10px] border border-[#26344b] bg-[#0b1220] text-slate-300">
                                    <Upload size={24} />
                                  </span>
                                  <div>
                                  <p className="text-[20px] font-semibold leading-tight text-white">
                                    Drop .txt or .docx here, or <span className="text-[#3f82ff]">browse</span>
                                  </p>
                                  <p className="mt-1 text-[16px] text-[#b5c3da]">Max 5 MB</p>
                                  </div>
                                </div>
                              )}
                            </div>
                          </div>

                          <div className="flex items-center gap-3">
                            <div className="h-px flex-1 bg-[#26344b]" />
                            <span className="text-[15px] font-semibold text-[#9fb1ca]">or paste text</span>
                            <div className="h-px flex-1 bg-[#26344b]" />
                          </div>

                          <Field label="BRD Text" required>
                            <textarea
                              className="modal-input min-h-[162px] resize-none rounded-[10px] border-[#26344b] bg-[#070d1a] px-4 py-4 text-[18px] text-white placeholder:text-[#b8c5db]"
                              placeholder="Paste your Business Requirements Document here..."
                              value={form.brdText}
                              onChange={(event) =>
                                setForm((current) => ({ ...current, brdText: event.target.value }))
                              }
                            />
                          </Field>
                        </>
                    </div>

                    <div ref={sourceSectionRef} className="border-t border-[#223047] px-[22px] py-6 lg:border-l lg:border-t-0">
                      <div className="space-y-6">
                        <div>
                          <h3 className="text-[24px] font-semibold leading-tight text-white">Run Configuration</h3>
                          <p className="mt-1 text-[15px] text-[#b5c3da]">Select the source and AI settings for this run.</p>
                        </div>

                        <div className="space-y-4">
                          <Field label="Target Warehouse" required compact>
                            <ModalSelect
                              id="targetWarehouse"
                              value={form.targetWarehouse}
                              options={TARGET_WAREHOUSE_OPTIONS}
                              openSelect={openSourceSelect}
                              setOpenSelect={setOpenSourceSelect}
                              onChange={(targetWarehouse) =>
                                setForm((current) => ({ ...current, targetWarehouse }))
                              }
                              activeBorder
                              disabled={!!project}
                            />
                          </Field>

                          <div className="text-[16px] font-semibold text-white">Source Connection</div>

                            <Field label="Connection Type" required compact>
                              <ModalSelect
                                id="connectionType"
                                value={connectionTypeFromSource(form.source)}
                                options={SOURCE_OPTIONS}
                                openSelect={openSourceSelect}
                                setOpenSelect={setOpenSourceSelect}
                                onChange={handleConnectionTypeChange}
                                activeBorder
                                disabled={!!project}
                              />
                            </Field>

                            {form.source === 'database' && (
                              <>
                                <Field label="Database Type" required compact>
                                  <div className="relative">
                                    <select
                                      className="modal-input h-[60px] w-full appearance-none rounded-[10px] border-[#26344b] bg-[#070d1a] px-5 pr-12 text-[18px] text-white"
                                      value={form.databaseType}
                                      disabled={!!project}
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
                                      <option value="azure_sql">Azure SQL DB</option>
                                      <option value="postgresql">PostgreSQL</option>
                                    </select>
                                    <ChevronDown size={21} className="pointer-events-none absolute right-4 top-1/2 -translate-y-1/2 text-white" />
                                  </div>
                                </Field>
                                <Field label="Database Name" compact>
                                  <div className="relative">
                                    <select
                                      className="modal-input h-[60px] w-full appearance-none rounded-[10px] border-[#26344b] bg-[#070d1a] px-5 pr-12 text-[18px] text-white"
                                      value={form.databaseName}
                                      disabled={!!project}
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
                                    <ChevronDown size={21} className="pointer-events-none absolute right-4 top-1/2 -translate-y-1/2 text-white" />
                                  </div>
                                </Field>
                                <label className={`flex cursor-pointer items-start gap-3 rounded-[10px] border px-4 py-4 transition-colors ${
                                  form.useDomainKb
                                    ? 'border-[#3f82ff] bg-[#102144]'
                                    : 'border-[#26344b] bg-[#070d1a] hover:border-[#3f82ff]/60'
                                }`}>
                                  <input
                                    type="checkbox"
                                    checked={form.useDomainKb}
                                    onChange={(event) =>
                                      setForm((current) => ({ ...current, useDomainKb: event.target.checked }))
                                    }
                                    className="mt-1 h-5 w-5 rounded border-[#4b5d78] bg-[#0b1220] text-[#3f82ff] accent-[#3f82ff]"
                                  />
                                  <span className="flex min-w-0 flex-1 gap-3">
                                    <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-[9px] border border-[#31415f] bg-[#0b1220] text-[#9fc0ff]">
                                      <BookOpenCheck size={18} />
                                    </span>
                                    <span className="min-w-0">
                                      <span className="block text-[16px] font-semibold text-white">Domain Knowledge Check</span>
                                    </span>
                                  </span>
                                </label>
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

                                <div className="rounded-[10px] border border-[#26344b] bg-[#070d1a] px-4 py-4">
                                  <div className="text-[16px] font-semibold text-white">ADLS Source</div>
                                  <div className="mt-3 grid gap-2 text-[13px] text-[#9fb1ca]">
                                    <div className="rounded border border-[#1e2a3d] bg-[#101827] px-3 py-2.5">
                                      <div className="text-[#6f84a4]">Account</div>
                                      <div className="mt-1 break-all font-mono text-white">https://atheastorage.dfs.core.windows.net</div>
                                    </div>
                                    <div className="grid grid-cols-2 gap-2">
                                      <div className="rounded border border-[#1e2a3d] bg-[#101827] px-3 py-2.5">
                                        <div className="text-[#6f84a4]">File system</div>
                                        <div className="mt-1 font-mono text-white">ADLS_FILE_SYSTEM</div>
                                      </div>
                                      <div className="rounded border border-[#1e2a3d] bg-[#101827] px-3 py-2.5">
                                        <div className="text-[#6f84a4]">Root</div>
                                        <div className="mt-1 font-mono text-white">ADLS_SOURCE_ROOT</div>
                                      </div>
                                    </div>
                                    <div className="rounded border border-[#1e2a3d] bg-[#101827] px-3 py-2.5">
                                      <div className="text-[#6f84a4]">Mode</div>
                                      <div className="mt-1 text-white">Auto-discover folders and files</div>
                                    </div>
                                  </div>
                                </div>
                              </div>
                            )}
                          </div>
                        </div>

                        {error && (
                          <div className="rounded-md border border-red-400/20 bg-red-500/10 px-3 py-2 text-xs text-red-300">
                            {error}
                          </div>
                        )}
                      </div>
                  </div>

                  <div className="flex items-center gap-4 border-t border-[#223047] px-[21px] py-4">
                    <button
                      type="button"
                      onClick={handleClose}
                      disabled={loading}
                      className="inline-flex h-[59px] flex-1 items-center justify-center rounded-[10px] bg-[#202b3a] px-4 text-[17px] font-semibold text-slate-100 transition-colors hover:bg-[#263142] hover:text-white disabled:opacity-50"
                    >
                      Cancel
                    </button>
                    <button
                      type="submit"
                      disabled={loading || !form.brdText.trim()}
                      className="inline-flex h-[59px] flex-1 items-center justify-center gap-3 rounded-[10px] bg-[#315da8] px-4 text-[17px] font-semibold text-white transition-colors hover:bg-[#3f72cc] disabled:cursor-not-allowed disabled:opacity-50"
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
      <label className={`mb-2 block text-[16px] font-semibold leading-tight text-slate-100 ${compact ? '' : ''}`}>
        {label} {required ? <span className="text-[#ff5c57]">*</span> : null}
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
  disabled = false,
}) {
  const open = openSelect === id
  const selected = options.find((option) => option.id === value)

  return (
    <div className="relative">
      <button
        type="button"
        disabled={disabled}
        onClick={() => !disabled && setOpenSelect(open ? null : id)}
        className={`flex h-[60px] w-full items-center justify-between rounded-[12px] border bg-[#070d1a] px-5 text-left text-[18px] text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.03)] transition-[border-color,box-shadow,background-color] duration-150 ${
          open ? 'border-[#4585f5] shadow-[0_0_0_1px_rgba(69,133,245,0.55),0_10px_26px_rgba(9,17,31,0.22)]' : activeBorder ? 'border-[#26344b] hover:border-[#4585f5]/70' : 'border-[#26344b] hover:border-[#4585f5]/70'
        }`}
      >
        <span className={selected ? 'text-white' : 'text-[#b8c5db]'}>
          {selected?.label || placeholder}
        </span>
        <ChevronDown size={21} className={`text-white transition-transform duration-150 ${open ? 'rotate-180' : ''}`} />
      </button>

      <AnimatePresence>
        {open && (
          <motion.div
            initial={{ opacity: 0, y: -4, scale: 0.985 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: -4, scale: 0.985 }}
            transition={{ duration: 0.14, ease: [0.2, 0.8, 0.2, 1] }}
            className="absolute left-0 right-0 top-[calc(100%+5px)] z-[80] origin-top overflow-hidden rounded-[12px] border border-[#335fba] bg-[#081020] p-1 shadow-[0_18px_44px_rgba(0,0,0,0.48)]"
          >
            <div className="px-4 py-2 text-left text-[13px] font-semibold text-[#7185a6]">
              {placeholder}
            </div>
            <div className="space-y-1">
              {options.map((option) => (
                <button
                  key={option.id}
                  type="button"
                  onClick={() => {
                    onChange(option.id)
                    setOpenSelect(null)
                  }}
                  className={`flex h-10 w-full items-center rounded-[8px] px-4 text-left text-[15px] font-semibold transition-[background-color,color] duration-120 ${
                    option.id === value ? 'bg-[#1b2a45] text-white' : 'text-[#dbe5f5] hover:bg-[#121d31] hover:text-white'
                  }`}
                >
                  {option.label}
                </button>
              ))}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

export default NewRunModal
