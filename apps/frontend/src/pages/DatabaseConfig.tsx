// @ts-nocheck
import React, { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Plus,
  Database,
  Trash2,
  Edit2,
  X,
  Save,
  Eye,
  EyeOff,
  ChevronDown,
  Loader2,
  AlertTriangle,
  Zap,
  CheckCircle2,
  XCircle
} from 'lucide-react'
import {
  useDbConfigurations,
  useCreateDbConfig,
  useUpdateDbConfig,
  useDeleteDbConfig,
  useTestDbConnection,
} from '../hooks/useDbConfig'
import { PageFrame, PageHeader } from '../components/shared/DashboardLayout'

const SOURCE_TYPES = [
  { value: 'database', label: 'Database' },
  { value: 'data_lake', label: 'Data Lake' }
]

const DATA_LAKE_SOURCE_TYPES = [
  // { value: 'SFTP', label: 'SFTP' },
  { value: 'ADLS', label: 'ADLS' }
]

const DATA_LAKE_INTEGRATION_TYPES = [
  { value: 'SFTP', label: 'SFTP' },
  { value: 'API', label: 'API' }
]

const DB_PRESETS = {
  azure_sql: {
    label: 'Azure SQL DB',
    driverClass: 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
    port: '1433'
  },
  postgresql: {
    label: 'PostgreSQL',
    driverClass: 'org.postgresql.Driver',
    port: '5432'
  },
  snowflake: {
    label: 'Snowflake',
    driverClass: 'net.snowflake.client.jdbc.SnowflakeDriver',
    port: '443'
  },
  custom: {
    label: 'Custom',
    driverClass: '',
    port: ''
  }
}

const EMPTY_CONNECTION = {
  id: null,
  name: '',
  sourceType: 'database',
  dbType: 'azure_sql',
  jdbcUrl: '',
  driverClass: 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
  username: '',
  password: '',
  host: '',
  port: '1433',
  databaseName: '',
  schema: '',
  integrationType: 'SFTP',
  dataLakeSourceType: 'ADLS',
  basePath: '',
  directoryName: '',
  secret: '',
  baseUrl: '',
  apiKey: ''
}

// ─── Main Page ────────────────────────────────────────────────────────────────

function DatabaseConfig () {
  const [showForm, setShowForm] = useState(false)
  const [editingConn, setEditingConn] = useState(null)

  const { data: connections = [], isLoading: loading, error: fetchError } = useDbConfigurations()
  const createMutation = useCreateDbConfig()
  const updateMutation = useUpdateDbConfig()
  const deleteMutation = useDeleteDbConfig()

  const pageError = fetchError?.message || deleteMutation.error?.message || null

  const handleAdd = () => {
    setEditingConn(null)
    setShowForm(true)
  }

  const handleEdit = (conn) => {
    setEditingConn(conn)
    setShowForm(true)
  }

  const handleDelete = (id) => deleteMutation.mutate(id)

  const handleSave = async (conn) => {
    if (conn.id && connections.find((c) => c.id === conn.id)) {
      await updateMutation.mutateAsync({ id: conn.id, data: conn })
    } else {
      await createMutation.mutateAsync(conn)
    }
    setShowForm(false)
    setEditingConn(null)
  }

  const handleClose = () => {
    setShowForm(false)
    setEditingConn(null)
  }

  return (
    <PageFrame>
      <PageHeader
        eyebrow="Configuration"
        title="Database source connections."
        description="Manage JDBC source connections used by Astra-Data pipeline discovery and generation."
        icon={Database}
        actions={
          <button onClick={handleAdd} className="btn-primary flex items-center justify-center gap-2">
            <Plus size={14} />
            Add Connection
          </button>
        }
      />

      {/* Page-level error */}
      {pageError && (
        <div className="flex items-start gap-2 p-3 bg-red-950/20 border border-accent-red/30 rounded-lg">
          <AlertTriangle size={14} className="text-accent-red mt-0.5 flex-shrink-0" />
          <p className="text-xs text-accent-red">{pageError}</p>
        </div>
      )}

      {/* Connection list */}
      {loading ? (
        <div className="card p-12 flex flex-col items-center gap-3">
          <Loader2 size={24} className="text-accent-blue animate-spin" />
          <p className="text-gray-500 text-sm">Loading connections…</p>
        </div>
      ) : connections.length === 0 ? (
        <div className="card p-12 flex flex-col items-center gap-3 text-center">
          <div className="w-14 h-14 rounded-xl bg-bg-base border border-bg-border flex items-center justify-center">
            <Database size={24} className="text-gray-600" />
          </div>
          <p className="text-gray-400 text-sm font-medium">No connections configured</p>
          <p className="text-gray-600 text-xs max-w-xs">
            Click <span className="text-accent-blue">Add Connection</span> to define a JDBC source for the Astra-Data pipeline.
          </p>
        </div>
      ) : (
        <div className="flex flex-col gap-3">
          {connections.map((conn) => (
            <ConnectionCard
              key={conn.id}
              conn={conn}
              onEdit={() => handleEdit(conn)}
              onDelete={() => handleDelete(conn.id)}
            />
          ))}
        </div>
      )}

      {/* Add / Edit slide-over */}
      <AnimatePresence>
        {showForm && (
          <ConnectionForm
            initial={editingConn}
            onSave={handleSave}
            onClose={handleClose}
          />
        )}
      </AnimatePresence>
    </PageFrame>
  )
}

// ─── Connection Card ───────────────────────────────────────────────────────────

function ConnectionCard ({ conn, onEdit, onDelete }) {
  const [confirmDelete, setConfirmDelete] = useState(false)
  const preset = DB_PRESETS[conn.dbType] || DB_PRESETS.custom
  const isDataLake = conn.sourceType === 'data_lake'
  const integrationType = conn.integrationType || 'SFTP'
  const isApiDataLake = isDataLake && integrationType === 'API'

  return (
    <div className="card p-5 flex items-start gap-4">
      <div className="w-10 h-10 rounded-lg bg-accent-blue/10 border border-accent-blue/20 flex items-center justify-center flex-shrink-0 mt-0.5">
        <Database size={18} className="text-accent-blue" />
      </div>

      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-1">
          <span className="text-sm font-semibold text-white truncate">{conn.name || 'Unnamed Connection'}</span>
          <span className="text-xs px-2 py-0.5 rounded-full bg-accent-blue/10 text-accent-blue border border-accent-blue/20">
            {isDataLake ? `Data Lake - ${integrationType}` : preset.label}
          </span>
        </div>

        <div className="grid grid-cols-2 gap-x-6 gap-y-1 mt-2">
          {isApiDataLake ? (
            <>
              {conn.baseUrl && <Detail label="Base URL" value={conn.baseUrl} mono />}
              {conn.apiKey && <Detail label="API Key" value="Configured" />}
            </>
          ) : isDataLake ? (
            <>
              {conn.dataLakeSourceType && <Detail label="Source Type" value={conn.dataLakeSourceType} />}
              {conn.basePath && <Detail label="Base Path" value={conn.basePath} mono />}
              {conn.directoryName && <Detail label="Directory" value={conn.directoryName} />}
            </>
          ) : (
            <>
              {conn.host && (
                <Detail label="Host" value={`${conn.host}${conn.port ? ':' + conn.port : ''}`} />
              )}
              {conn.databaseName && (
                <Detail label="Database" value={conn.databaseName} />
              )}
              {conn.schema && (
                <Detail label="Schema" value={conn.schema} />
              )}
              {conn.username && (
                <Detail label="Username" value={conn.username} />
              )}
              {conn.driverClass && (
                <Detail label="Driver" value={conn.driverClass} mono />
              )}
              {conn.jdbcUrl && (
                <Detail label="JDBC URL" value={conn.jdbcUrl} mono />
              )}
            </>
          )}
        </div>
      </div>

      <div className="flex items-center gap-2 flex-shrink-0">
        <button
          onClick={onEdit}
          className="w-8 h-8 rounded-lg flex items-center justify-center text-gray-400 hover:text-white hover:bg-bg-border transition-colors"
          title="Edit"
        >
          <Edit2 size={14} />
        </button>

        {confirmDelete ? (
          <div className="flex items-center gap-1">
            <button
              onClick={onDelete}
              className="text-xs text-accent-red hover:text-red-300 font-semibold px-2"
            >
              Delete
            </button>
            <button
              onClick={() => setConfirmDelete(false)}
              className="text-xs text-gray-500 hover:text-gray-300 px-1"
            >
              Cancel
            </button>
          </div>
        ) : (
          <button
            onClick={() => setConfirmDelete(true)}
            className="w-8 h-8 rounded-lg flex items-center justify-center text-gray-500 hover:text-accent-red hover:bg-red-950/20 transition-colors"
            title="Delete"
          >
            <Trash2 size={14} />
          </button>
        )}
      </div>
    </div>
  )
}

function Detail ({ label, value, mono = false }) {
  return (
    <div className="flex items-baseline gap-1.5 overflow-hidden">
      <span className="text-xs text-gray-500 flex-shrink-0">{label}:</span>
      <span className={`text-xs text-gray-300 truncate ${mono ? 'font-mono' : ''}`}>{value}</span>
    </div>
  )
}

// ─── Connection Form (slide-over) ─────────────────────────────────────────────

function ConnectionForm ({ initial, onSave, onClose }) {
  const isEdit = Boolean(initial?.id)

  const [form, setForm] = useState(() => ({
    ...EMPTY_CONNECTION,
    ...(initial || {})
  }))
  const [showPassword, setShowPassword] = useState(false)
  const [showSecret, setShowSecret] = useState(false)
  const [showApiKey, setShowApiKey] = useState(false)
  const [errors, setErrors] = useState({})
  const saveMutation = useCreateDbConfig()
  const updateMutation = useUpdateDbConfig()
  const testMutation = useTestDbConnection()

  const saving = saveMutation.isPending || updateMutation.isPending
  const saveError = saveMutation.error?.message || updateMutation.error?.message || null
  const testing = testMutation.isPending
  const testResult = testMutation.isSuccess
    ? { ok: true, message: 'Connection successful!' }
    : testMutation.isError
      ? { ok: false, message: testMutation.error?.message || 'Connection failed' }
      : null

  const set = (key, value) => {
    setForm((f) => ({ ...f, [key]: value }))
    testMutation.reset()  // clear test result when form changes
  }

  // When dbType changes, auto-populate driver class & port defaults
  const handleDbTypeChange = (dbType) => {
    const preset = DB_PRESETS[dbType] || DB_PRESETS.custom
    setForm((f) => ({
      ...f,
      dbType,
      driverClass: preset.driverClass,
      port: preset.port || f.port
    }))
  }

  const handleSourceTypeChange = (sourceType) => {
    setForm((f) => ({ ...f, sourceType }))
    setErrors({})
    testMutation.reset()
  }

  const handleIntegrationTypeChange = (integrationType) => {
    setForm((f) => ({ ...f, integrationType }))
    setErrors({})
    testMutation.reset()
  }

  const validate = () => {
    const e = {}
    if (!form.name.trim()) {
      e.name = form.sourceType === 'data_lake'
        ? 'Data lake name is required'
        : 'Connection name is required'
    }

    if (form.sourceType === 'data_lake') {
      if (!form.integrationType) e.integrationType = 'Integration type is required'
      if (form.integrationType === 'API') {
        if (!form.baseUrl.trim()) e.baseUrl = 'Base URL is required'
      } else {
        if (!form.dataLakeSourceType) e.dataLakeSourceType = 'Source type is required'
        if (!form.basePath.trim()) e.basePath = 'Base path is required'
        if (!form.directoryName.trim()) e.directoryName = 'Directory name is required'
        if (!isEdit && !form.secret.trim()) e.secret = 'Secret is required'
      }
    } else {
      if (!form.host.trim()) e.host = 'Host is required'
      if (!form.driverClass.trim()) e.driverClass = 'Driver class is required'
      if (!form.username.trim()) e.username = 'Username is required'
      if (!isEdit && !form.password.trim()) e.password = 'Password is required'
    }
    return e
  }

  const handleTest = () => testMutation.mutate({ ...form })

  const handleSave = async () => {
    const e = validate()
    if (Object.keys(e).length > 0) {
      setErrors(e)
      return
    }
    try {
      await onSave({ ...form })
    } catch (err) {
      // error surfaced via mutation state (saveError above)
    }
  }

  return (
    <>
      {/* Backdrop */}
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        className="fixed inset-0 bg-black/60 backdrop-blur-sm z-40"
        onClick={onClose}
      />

      {/* Panel */}
      <motion.div
        initial={{ x: '100%' }}
        animate={{ x: 0 }}
        exit={{ x: '100%' }}
        transition={{ type: 'spring', stiffness: 300, damping: 30 }}
        className="fixed right-0 top-0 h-full w-full max-w-lg bg-bg-card border-l border-bg-border z-50 flex flex-col shadow-2xl"
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-bg-border flex-shrink-0">
          <div>
            <h2 className="text-lg font-bold text-white">
              {isEdit ? 'Edit Connection' : 'Add Connection'}
            </h2>
            <p className="text-xs text-gray-500 mt-0.5">
              {form.sourceType === 'data_lake'
                ? 'Configure a Data Lake integration'
                : 'Configure a JDBC source database'}
            </p>
          </div>
          <button
            onClick={onClose}
            className="w-8 h-8 rounded-lg flex items-center justify-center text-gray-400 hover:text-white hover:bg-bg-border transition-colors"
          >
            <X size={16} />
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto p-6 space-y-5">

          {/* Connection / Data Lake Name */}
          <FormField
            label={form.sourceType === 'data_lake' ? 'Data Lake Name' : 'Connection Name'}
            error={errors.name}
            required
          >
            <input
              type="text"
              className={`input-field ${errors.name ? 'border-accent-red focus:ring-accent-red' : ''}`}
              placeholder={form.sourceType === 'data_lake' ? 'e.g. Claims Landing Lake' : 'e.g. Production Azure SQL'}
              value={form.name}
              onChange={(e) => { set('name', e.target.value); setErrors((err) => ({ ...err, name: null })) }}
            />
          </FormField>

          {/* Source Type */}
          <FormField label="Source Type">
            <div className="grid grid-cols-2 gap-2">
              {SOURCE_TYPES.map((opt) => (
                <button
                  key={opt.value}
                  type="button"
                  onClick={() => handleSourceTypeChange(opt.value)}
                  className={`
                    px-3 py-2.5 rounded-lg border text-xs font-medium transition-all duration-150
                    ${form.sourceType === opt.value
                      ? 'bg-accent-blue/15 border-accent-blue text-accent-blue'
                      : 'bg-bg-base border-bg-border text-gray-400 hover:border-gray-500 hover:text-gray-200'
                    }
                  `}
                >
                  {opt.label}
                </button>
              ))}
            </div>
          </FormField>

          {/* Database when sourceType = database */}
          {form.sourceType === 'database' && (
            <>
              {/* DB Type */}
              <FormField label="Database Type">
                <div className="relative">
                  <select
                    className="input-field appearance-none pr-8"
                    value={form.dbType}
                    onChange={(e) => handleDbTypeChange(e.target.value)}
                  >
                    {Object.entries(DB_PRESETS).map(([key, preset]) => (
                      <option key={key} value={key}>{preset.label}</option>
                    ))}
                  </select>
                  <ChevronDown size={14} className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-500 pointer-events-none" />
                </div>
              </FormField>

              <div className="h-px bg-bg-border" />

              {/* Host + Port */}
              <div className="grid grid-cols-3 gap-3">
                <div className="col-span-2">
                  <FormField label="Host" error={errors.host} required>
                    <input
                      type="text"
                      className={`input-field ${errors.host ? 'border-accent-red focus:ring-accent-red' : ''}`}
                      placeholder="e.g. myserver.database.windows.net"
                      value={form.host}
                      onChange={(e) => { set('host', e.target.value); setErrors((err) => ({ ...err, host: null })) }}
                    />
                  </FormField>
                </div>
                <div>
                  <FormField label="Port">
                    <input
                      type="text"
                      className="input-field"
                      placeholder="1433"
                      value={form.port}
                      onChange={(e) => set('port', e.target.value)}
                    />
                  </FormField>
                </div>
              </div>

              {/* Database Name + Schema */}
              <div className="grid grid-cols-2 gap-3">
                <FormField label="Database Name">
                  <input
                    type="text"
                    className="input-field"
                    placeholder="e.g. astra_data_db"
                    value={form.databaseName}
                    onChange={(e) => set('databaseName', e.target.value)}
                  />
                </FormField>
                <FormField label="Schema">
                  <input
                    type="text"
                    className="input-field"
                    placeholder="e.g. dbo"
                    value={form.schema}
                    onChange={(e) => set('schema', e.target.value)}
                  />
                </FormField>
              </div>

              {/* Username + Password */}
              <div className="grid grid-cols-2 gap-3">
                <FormField label="Username" error={errors.username} required>
                  <input
                    type="text"
                    className={`input-field ${errors.username ? 'border-accent-red focus:ring-accent-red' : ''}`}
                    placeholder="db_user"
                    value={form.username}
                    onChange={(e) => { set('username', e.target.value); setErrors((err) => ({ ...err, username: null })) }}
                  />
                </FormField>
                <FormField label={isEdit ? 'Password (leave blank to keep)' : 'Password'} error={errors.password} required={!isEdit}>
                  <div className="relative">
                    <input
                      type={showPassword ? 'text' : 'password'}
                      className={`input-field pr-9 ${errors.password ? 'border-accent-red focus:ring-accent-red' : ''}`}
                      placeholder="••••••••"
                      value={form.password}
                      onChange={(e) => { set('password', e.target.value); setErrors((err) => ({ ...err, password: null })) }}
                    />
                    <button
                      type="button"
                      onClick={() => setShowPassword((v) => !v)}
                      className="absolute right-2.5 top-1/2 -translate-y-1/2 text-gray-500 hover:text-gray-300 transition-colors"
                    >
                      {showPassword ? <EyeOff size={14} /> : <Eye size={14} />}
                    </button>
                  </div>
                </FormField>
              </div>

              <div className="h-px bg-bg-border" />

              {/* JDBC URL */}
              <FormField label="JDBC URL" hint="Auto-filled or override manually">
                <input
                  type="text"
                  className="input-field font-mono text-xs"
                  placeholder="jdbc:sqlserver://host:1433;databaseName=..."
                  value={form.jdbcUrl}
                  onChange={(e) => set('jdbcUrl', e.target.value)}
                />
              </FormField>

              {/* Driver Class */}
              <FormField label="Driver Class" error={errors.driverClass} required>
                <input
                  type="text"
                  className={`input-field font-mono text-xs ${errors.driverClass ? 'border-accent-red focus:ring-accent-red' : ''}`}
                  placeholder="com.example.Driver"
                  value={form.driverClass}
                  onChange={(e) => { set('driverClass', e.target.value); setErrors((err) => ({ ...err, driverClass: null })) }}
                />
              </FormField>
            </>
          )}

          {/* Data Lake when sourceType = data_lake */}
          {form.sourceType === 'data_lake' && (
            <>
              <FormField label="Integration Type" error={errors.integrationType} required>
                <div className="relative">
                  <select
                    className={`input-field appearance-none pr-8 ${errors.integrationType ? 'border-accent-red focus:ring-accent-red' : ''}`}
                    value={form.integrationType}
                    onChange={(e) => handleIntegrationTypeChange(e.target.value)}
                  >
                    {DATA_LAKE_INTEGRATION_TYPES.map((opt) => (
                      <option key={opt.value} value={opt.value}>{opt.label}</option>
                    ))}
                  </select>
                  <ChevronDown size={14} className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-500 pointer-events-none" />
                </div>
              </FormField>

              {form.integrationType === 'API' ? (
                <>
                  <div className="h-px bg-bg-border" />

                  <FormField label="Base URL" error={errors.baseUrl} required>
                    <input
                      type="url"
                      className={`input-field font-mono text-xs ${errors.baseUrl ? 'border-accent-red focus:ring-accent-red' : ''}`}
                      placeholder="https://api.example.com"
                      value={form.baseUrl}
                      onChange={(e) => { set('baseUrl', e.target.value); setErrors((err) => ({ ...err, baseUrl: null })) }}
                    />
                  </FormField>

                  <FormField label="API Key">
                    <div className="relative">
                      <input
                        type={showApiKey ? 'text' : 'password'}
                        className="input-field pr-9"
                        placeholder="Optional API key"
                        value={form.apiKey}
                        onChange={(e) => set('apiKey', e.target.value)}
                      />
                      <button
                        type="button"
                        onClick={() => setShowApiKey((v) => !v)}
                        className="absolute right-2.5 top-1/2 -translate-y-1/2 text-gray-500 hover:text-gray-300 transition-colors"
                      >
                        {showApiKey ? <EyeOff size={14} /> : <Eye size={14} />}
                      </button>
                    </div>
                  </FormField>
                </>
              ) : (
                <>
                  <FormField label="Source Type" error={errors.dataLakeSourceType} required>
                    <div className="relative">
                      <select
                        className={`input-field appearance-none pr-8 ${errors.dataLakeSourceType ? 'border-accent-red focus:ring-accent-red' : ''}`}
                        value={form.dataLakeSourceType}
                        onChange={(e) => {
                          set('dataLakeSourceType', e.target.value)
                          setErrors((err) => ({ ...err, dataLakeSourceType: null }))
                        }}
                      >
                        {DATA_LAKE_SOURCE_TYPES.map((opt) => (
                          <option key={opt.value} value={opt.value}>{opt.label}</option>
                        ))}
                      </select>
                      <ChevronDown size={14} className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-500 pointer-events-none" />
                    </div>
                  </FormField>

                  <div className="h-px bg-bg-border" />

                  <FormField label="Base Path" error={errors.basePath} required>
                    <input
                      type="text"
                      className={`input-field font-mono text-xs ${errors.basePath ? 'border-accent-red focus:ring-accent-red' : ''}`}
                      placeholder={form.dataLakeSourceType === 'ADLS' ? 'abfss://container@account.dfs.core.windows.net/path' : '/remote/base/path'}
                      value={form.basePath}
                      onChange={(e) => { set('basePath', e.target.value); setErrors((err) => ({ ...err, basePath: null })) }}
                    />
                  </FormField>

                  <FormField label="Directory Name" error={errors.directoryName} required>
                    <input
                      type="text"
                      className={`input-field ${errors.directoryName ? 'border-accent-red focus:ring-accent-red' : ''}`}
                      placeholder="e.g. inbound"
                      value={form.directoryName}
                      onChange={(e) => { set('directoryName', e.target.value); setErrors((err) => ({ ...err, directoryName: null })) }}
                    />
                  </FormField>

                  <FormField label={isEdit ? 'Secret (leave blank to keep)' : 'Secret'} error={errors.secret} required={!isEdit}>
                    <div className="relative">
                      <input
                        type={showSecret ? 'text' : 'password'}
                        className={`input-field pr-9 ${errors.secret ? 'border-accent-red focus:ring-accent-red' : ''}`}
                        placeholder="Enter secret"
                        value={form.secret}
                        onChange={(e) => { set('secret', e.target.value); setErrors((err) => ({ ...err, secret: null })) }}
                      />
                      <button
                        type="button"
                        onClick={() => setShowSecret((v) => !v)}
                        className="absolute right-2.5 top-1/2 -translate-y-1/2 text-gray-500 hover:text-gray-300 transition-colors"
                      >
                        {showSecret ? <EyeOff size={14} /> : <Eye size={14} />}
                      </button>
                    </div>
                  </FormField>
                </>
              )}
            </>
          )}

          {/* Test connection result */}
          {testResult && (
            <div className={`flex items-start gap-2 p-3 rounded-lg border ${
              testResult.ok
                ? 'bg-green-950/20 border-green-500/30'
                : 'bg-red-950/20 border-accent-red/30'
            }`}>
              {testResult.ok
                ? <CheckCircle2 size={14} className="text-green-400 mt-0.5 flex-shrink-0" />
                : <XCircle size={14} className="text-accent-red mt-0.5 flex-shrink-0" />}
              <p className={`text-xs ${testResult.ok ? 'text-green-400' : 'text-accent-red'}`}>
                {testResult.message}
              </p>
            </div>
          )}

          {/* Validation / API error summary */}
          {(Object.values(errors).some(Boolean) || saveError) && (
            <div className="flex items-start gap-2 p-3 bg-red-950/20 border border-accent-red/30 rounded-lg">
              <AlertTriangle size={14} className="text-accent-red mt-0.5 flex-shrink-0" />
              <p className="text-xs text-accent-red">
                {saveError || 'Please fill in all required fields.'}
              </p>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-4 border-t border-bg-border flex gap-3 flex-shrink-0">
          <button type="button" onClick={onClose} disabled={saving || testing} className="flex-1 btn-secondary">
            Cancel
          </button>
          <button
            type="button"
            onClick={handleTest}
            disabled={testing || saving}
            className="flex-1 btn-secondary flex items-center justify-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {testing ? (
              <>
                <Loader2 size={14} className="animate-spin" />
                Testing…
              </>
            ) : (
              <>
                <Zap size={14} />
                Test
              </>
            )}
          </button>
          <button
            onClick={handleSave}
            disabled={saving || testing}
            className="flex-1 btn-primary flex items-center justify-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {saving ? (
              <>
                <Loader2 size={14} className="animate-spin" />
                Saving…
              </>
            ) : (
              <>
                <Save size={14} />
                {isEdit ? 'Update' : 'Save'}
              </>
            )}
          </button>
        </div>
      </motion.div>
    </>
  )
}

function FormField ({ label, children, error, hint, required }) {
  return (
    <div>
      <label className="label">
        {label}
        {required && <span className="text-accent-red ml-0.5">*</span>}
      </label>
      {children}
      {hint && !error && <p className="text-xs text-gray-600 mt-1">{hint}</p>}
      {error && <p className="text-xs text-accent-red mt-1">{error}</p>}
    </div>
  )
}

export default DatabaseConfig
