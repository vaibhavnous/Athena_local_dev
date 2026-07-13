// @ts-nocheck
import React, { useState, useEffect } from 'react'
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
  AlertTriangle
} from 'lucide-react'
import {
  getConfigurations,
  createConfiguration,
  updateConfiguration,
  deleteConfiguration
} from '../api/athenaApi'

const SOURCE_TYPES = [
  { value: 'database', label: 'Database' },
  { value: 'data_lake', label: 'Data Lake' }
]

const DATA_LAKE_TYPES = [
  { value: 'adls_gen2', label: 'ADLS' }
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
  dataLakeType: 'adls_gen2'
}

// ─── Main Page ────────────────────────────────────────────────────────────────

function DatabaseConfig () {
  const [connections, setConnections] = useState([])
  const [loading, setLoading] = useState(true)
  const [pageError, setPageError] = useState(null)
  const [showForm, setShowForm] = useState(false)
  const [editingConn, setEditingConn] = useState(null)

  const fetchConnections = async () => {
    setLoading(true)
    setPageError(null)
    try {
      const data = await getConfigurations()
      setConnections(data)
    } catch (err) {
      setPageError(err.message || 'Failed to load connections')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { fetchConnections() }, [])

  const handleAdd = () => {
    setEditingConn(null)
    setShowForm(true)
  }

  const handleEdit = (conn) => {
    setEditingConn(conn)
    setShowForm(true)
  }

  const handleDelete = async (id) => {
    try {
      await deleteConfiguration(id)
      setConnections((prev) => prev.filter((c) => c.id !== id))
    } catch (err) {
      setPageError(err.message || 'Failed to delete connection')
    }
  }

  const handleSave = async (conn) => {
    try {
      if (conn.id && connections.find((c) => c.id === conn.id)) {
        await updateConfiguration(conn.id, conn)
        setConnections((prev) => prev.map((c) => (c.id === conn.id ? { ...c, ...conn } : c)))
      } else {
        const created = await createConfiguration(conn)
        setConnections((prev) => [{ ...conn, id: created.id }, ...prev])
      }
      setShowForm(false)
      setEditingConn(null)
    } catch (err) {
      // bubble to the form via re-throw so the form can display the error
      throw err
    }
  }

  const handleClose = () => {
    setShowForm(false)
    setEditingConn(null)
  }

  return (
    <div className="flex flex-col gap-6 max-w-6xl w-full pt-1">
      {/* Page header */}
      <div className="flex items-center justify-between mb-1 text-text-primary">
        <div>
          <h1 className="text-[16px] font-semibold text-text-primary m-0">Database Config</h1>
          <p className="text-[11px] text-text-tertiary mt-1 mb-0">Manage JDBC source connections for the pipeline</p>
        </div>
        <button
          onClick={handleAdd}
          className="bg-accent-blue border-transparent text-white rounded-lg px-4 py-2.5 text-[11px] font-medium flex items-center justify-center gap-1.5 hover:bg-blue-600 transition-all shadow-sm hover:shadow-md hover:-translate-y-0.5 focus:ring-2 focus:ring-accent-blue focus:ring-offset-2 focus:ring-offset-bg-base"
        >
          <Plus size={14} />
          Add Connection
        </button>
      </div>

      {/* Page-level error */}
      {pageError && (
        <div className="flex items-start gap-2 p-3 bg-red-900/10 border border-accent-red/30 rounded-lg">
          <AlertTriangle size={14} className="text-accent-red mt-0.5 flex-shrink-0" />
          <p className="text-[11px] text-accent-red m-0 leading-relaxed">{pageError}</p>
        </div>
      )}

      {/* Connection list */}
      {loading ? (
        <div className="bg-bg-card border border-bg-border rounded-xl p-12 flex flex-col items-center gap-3">
          <Loader2 size={18} className="text-accent-blue animate-spin" />
          <p className="text-text-tertiary text-[11px]">Loading connections…</p>
        </div>
      ) : connections.length === 0 ? (
        <div className="bg-bg-card border border-bg-border rounded-xl p-10 flex flex-col items-center gap-2 text-center shadow-sm">
          <div className="w-12 h-12 rounded-full bg-bg-base border border-bg-border flex items-center justify-center mb-1 transition-transform hover:scale-110 duration-300">
            <Database size={20} className="text-text-tertiary" />
          </div>
          <p className="text-text-secondary text-[13px] font-semibold m-0">No connections configured</p>
          <p className="text-text-tertiary text-[11px] max-w-xs leading-relaxed m-0 mt-1">
            Click <span className="text-accent-blue font-medium">Add Connection</span> to define a JDBC source for the Astra Data pipeline.
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
    </div>
  )
}

// ─── Connection Card ───────────────────────────────────────────────────────────

function ConnectionCard ({ conn, onEdit, onDelete }) {
  const [confirmDelete, setConfirmDelete] = useState(false)
  const preset = DB_PRESETS[conn.dbType] || DB_PRESETS.custom

  return (
    <div className="bg-bg-card border border-bg-border rounded-xl p-4 flex items-start gap-4 transition-all duration-300 hover:shadow-card hover:border-accent-blue/30 group">
      <div className="w-9 h-9 rounded-full bg-accent-blue/10 border border-accent-blue/20 flex items-center justify-center flex-shrink-0 mt-0.5 transition-transform group-hover:scale-110">
        <Database size={14} className="text-accent-blue" />
      </div>

      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-3 mb-2.5">
          <span className="text-[13px] font-semibold text-text-primary transition-colors group-hover:text-accent-blue truncate">{conn.name || 'Unnamed Connection'}</span>
          <span className="text-[10px] px-2 py-0.5 rounded-full bg-accent-blue/10 text-accent-blue border border-accent-blue/20">
            {preset.label}
          </span>
        </div>

        <div className="grid grid-cols-2 gap-x-6 gap-y-2">
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
        </div>
      </div>

      <div className="flex items-center gap-1.5 flex-shrink-0 mt-1">
        <button
          onClick={onEdit}
          className="w-7 h-7 rounded flex items-center justify-center text-text-tertiary hover:text-text-primary hover:bg-bg-hover transition-colors"
          title="Edit"
        >
          <Edit2 size={12} />
        </button>

        {confirmDelete ? (
          <div className="flex items-center gap-1 bg-red-500/5 px-2 rounded-lg border border-red-500/20">
            <button
              onClick={onDelete}
              className="text-[11px] text-accent-red hover:text-red-400 font-semibold px-1 py-1"
            >
              Confirm
            </button>
            <div className="w-px h-3 bg-bg-border mx-1"></div>
            <button
              onClick={() => setConfirmDelete(false)}
              className="text-[11px] text-text-tertiary hover:text-text-primary px-1 py-1"
            >
              Cancel
            </button>
          </div>
        ) : (
          <button
            onClick={() => setConfirmDelete(true)}
            className="w-7 h-7 rounded flex items-center justify-center text-text-tertiary hover:text-accent-red hover:bg-red-500/10 transition-colors"
            title="Delete"
          >
            <Trash2 size={12} />
          </button>
        )}
      </div>
    </div>
  )
}

function Detail ({ label, value, mono = false }) {
  return (
    <div className="flex items-baseline gap-2 overflow-hidden">
      <span className="text-[11px] text-text-tertiary flex-shrink-0">{label}:</span>
      <span className={`text-[11px] text-text-secondary truncate ${mono ? 'font-mono' : ''}`}>{value}</span>
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
  const [errors, setErrors] = useState({})
  const [saving, setSaving] = useState(false)
  const [saveError, setSaveError] = useState(null)

  const set = (key, value) => setForm((f) => ({ ...f, [key]: value }))

  const handleSourceTypeChange = (sourceType) => {
    setForm((f) => ({
      ...f,
      sourceType,
      ...(sourceType === 'data_lake'
        ? {
            dataLakeType: f.dataLakeType || 'adls_gen2',
            dbType: '',
            host: '',
            port: '',
            databaseName: '',
            schema: '',
            username: '',
            password: '',
            jdbcUrl: '',
            driverClass: '',
          }
        : {
            dataLakeType: f.dataLakeType || 'adls_gen2',
            dbType: f.dbType || 'azure_sql',
            driverClass: f.driverClass || DB_PRESETS.azure_sql.driverClass,
            port: f.port || DB_PRESETS.azure_sql.port,
          }),
    }))
    setErrors({})
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

  const validate = () => {
    const e = {}
    if (!form.name.trim()) e.name = 'Connection name is required'
    if (form.sourceType === 'database') {
      if (!form.host.trim()) e.host = 'Host is required'
      if (!form.driverClass.trim()) e.driverClass = 'Driver class is required'
      if (!form.username.trim()) e.username = 'Username is required'
      if (!isEdit && !form.password.trim()) e.password = 'Password is required'
    }
    return e
  }

  const handleSave = async () => {
    const e = validate()
    if (Object.keys(e).length > 0) {
      setErrors(e)
      return
    }
    setSaving(true)
    setSaveError(null)
    try {
      await onSave({ ...form })
    } catch (err) {
      setSaveError(err.message || 'Failed to save connection')
    } finally {
      setSaving(false)
    }
  }

  const inputClass = (err) => `w-full bg-bg-base border rounded-lg px-3 py-2.5 text-[11px] text-text-secondary focus:outline-none focus:ring-1 focus:ring-accent-blue focus:border-accent-blue transition-colors ${err ? 'border-accent-red focus:ring-accent-red' : 'border-bg-border hover:border-bg-border/80'}`

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
        className="fixed right-0 top-0 h-full w-full max-w-[440px] bg-bg-card border-l border-bg-border z-50 flex flex-col shadow-2xl"
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-5 border-b border-bg-border flex-shrink-0 bg-bg-hover/30">
          <div>
            <h2 className="text-[14px] font-semibold text-text-primary m-0">
              {isEdit ? 'Edit Connection' : 'Add Connection'}
            </h2>
            <p className="text-[11px] text-text-tertiary mt-1 m-0">Configure a JDBC source database</p>
          </div>
          <button
            onClick={onClose}
            className="w-7 h-7 rounded flex items-center justify-center text-text-tertiary hover:text-text-primary hover:bg-bg-base border border-transparent hover:border-bg-border transition-all"
          >
            <X size={14} />
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6">

          {/* Connection Name */}
          <FormField label="Connection Name" error={errors.name} required>
            <input
              type="text"
              className={inputClass(errors.name)}
              placeholder="e.g. Production Azure SQL"
              value={form.name}
              onChange={(e) => { set('name', e.target.value); setErrors((err) => ({ ...err, name: null })) }}
            />
          </FormField>

          {/* Source Type */}
          <FormField label="Source Type">
            <div className="flex gap-3">
              {SOURCE_TYPES.map((opt) => (
                <button
                  key={opt.value}
                  type="button"
                  disabled={opt.disabled}
                  onClick={() => !opt.disabled && handleSourceTypeChange(opt.value)}
                  className={`
                    flex-1 py-2 rounded-lg border text-[11px] font-medium transition-all duration-150 inline-flex items-center justify-center
                    ${opt.disabled
                      ? 'opacity-40 cursor-not-allowed bg-bg-base border-bg-border text-text-tertiary'
                      : form.sourceType === opt.value
                        ? 'bg-accent-blue/15 border-accent-blue text-accent-blue'
                        : 'bg-bg-base border-bg-border text-text-secondary hover:border-text-tertiary hover:text-text-primary hover:bg-bg-hover/50'
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
                <div className="relative group">
                  <select
                    className={`${inputClass()} appearance-none pr-8 cursor-pointer`}
                    value={form.dbType}
                    onChange={(e) => handleDbTypeChange(e.target.value)}
                  >
                    {Object.entries(DB_PRESETS).map(([key, preset]) => (
                      <option key={key} value={key}>{preset.label}</option>
                    ))}
                  </select>
                  <ChevronDown size={14} className="absolute right-3 top-1/2 -translate-y-1/2 text-text-tertiary pointer-events-none group-hover:text-text-secondary transition-colors" />
                </div>
              </FormField>

              <div className="h-px bg-bg-border/60" />

              {/* Host + Port */}
              <div className="grid grid-cols-4 gap-4">
                <div className="col-span-3">
                  <FormField label="Host" error={errors.host} required>
                    <input
                      type="text"
                      className={inputClass(errors.host)}
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
                      className={inputClass()}
                      placeholder="1433"
                      value={form.port}
                      onChange={(e) => set('port', e.target.value)}
                    />
                  </FormField>
                </div>
              </div>

              {/* Database Name + Schema */}
              <div className="grid grid-cols-2 gap-4">
                <FormField label="Database Name">
                  <input
                    type="text"
                    className={inputClass()}
                    placeholder="e.g. astra_data_db"
                    value={form.databaseName}
                    onChange={(e) => set('databaseName', e.target.value)}
                  />
                </FormField>
                <FormField label="Schema">
                  <input
                    type="text"
                    className={inputClass()}
                    placeholder="e.g. dbo"
                    value={form.schema}
                    onChange={(e) => set('schema', e.target.value)}
                  />
                </FormField>
              </div>

              {/* Username + Password */}
              <div className="grid grid-cols-2 gap-4">
                <FormField label="Username" error={errors.username} required>
                  <input
                    type="text"
                    className={inputClass(errors.username)}
                    placeholder="db_user"
                    value={form.username}
                    onChange={(e) => { set('username', e.target.value); setErrors((err) => ({ ...err, username: null })) }}
                  />
                </FormField>
                <FormField label={isEdit ? 'Password (blank to keep)' : 'Password'} error={errors.password} required={!isEdit}>
                  <div className="relative group">
                    <input
                      type={showPassword ? 'text' : 'password'}
                      className={`${inputClass(errors.password)} pr-9`}
                      placeholder="••••••••"
                      value={form.password}
                      onChange={(e) => { set('password', e.target.value); setErrors((err) => ({ ...err, password: null })) }}
                    />
                    <button
                      type="button"
                      onClick={() => setShowPassword((v) => !v)}
                      className="absolute right-2.5 top-1/2 -translate-y-1/2 text-text-tertiary hover:text-text-primary transition-colors"
                    >
                      {showPassword ? <EyeOff size={14} /> : <Eye size={14} />}
                    </button>
                  </div>
                </FormField>
              </div>

              <div className="h-px bg-bg-border/60" />

              {/* JDBC URL */}
              <FormField label="JDBC URL" hint="Auto-filled or override manually">
                <input
                  type="text"
                  className={`${inputClass()} font-mono`}
                  placeholder="jdbc:sqlserver://host:1433;databaseName=..."
                  value={form.jdbcUrl}
                  onChange={(e) => set('jdbcUrl', e.target.value)}
                />
              </FormField>

              {/* Driver Class */}
              <FormField label="Driver Class" error={errors.driverClass} required>
                <input
                  type="text"
                  className={`${inputClass(errors.driverClass)} font-mono`}
                  placeholder="com.example.Driver"
                  value={form.driverClass}
                  onChange={(e) => { set('driverClass', e.target.value); setErrors((err) => ({ ...err, driverClass: null })) }}
                />
              </FormField>
            </>
          )}

          {form.sourceType === 'data_lake' && (
            <div className="space-y-4">
              <FormField label="Data Lake Type" required>
                <div className="relative group">
                  <select
                    className={`${inputClass()} appearance-none pr-8 cursor-pointer`}
                    value={form.dataLakeType || 'adls_gen2'}
                    onChange={(e) => set('dataLakeType', e.target.value)}
                  >
                    {DATA_LAKE_TYPES.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                  <ChevronDown size={14} className="absolute right-3 top-1/2 -translate-y-1/2 text-text-tertiary pointer-events-none group-hover:text-text-secondary transition-colors" />
                </div>
              </FormField>

              <div className="rounded-lg border border-bg-border bg-bg-base p-4 text-[11px] text-text-secondary">
                <div className="font-semibold text-text-primary">ADLS Source</div>
                <div className="mt-2 space-y-1">
                  <div>Account: <span className="font-mono">https://atheastorage.dfs.core.windows.net</span></div>
                  <div>File system: backend <span className="font-mono">ADLS_FILE_SYSTEM</span></div>
                  <div>Root: backend <span className="font-mono">ADLS_SOURCE_ROOT</span></div>
                  <div>Mode: auto-discover folders and files</div>
                </div>
              </div>
            </div>
          )}

          {/* Validation / API error summary */}
          {(Object.values(errors).some(Boolean) || saveError) && (
            <div className="flex items-start gap-2.5 p-3.5 bg-red-900/10 border border-accent-red/30 rounded-lg">
              <AlertTriangle size={14} className="text-accent-red flex-shrink-0" />
              <p className="text-[11px] text-accent-red m-0 leading-relaxed">
                {saveError || 'Please fill in all required fields.'}
              </p>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-4 border-t border-bg-border flex gap-3 flex-shrink-0 bg-bg-base/50">
          <button type="button" onClick={onClose} disabled={saving} className="flex-1 bg-transparent border border-bg-border text-text-secondary hover:text-text-primary rounded-lg px-3 py-2.5 text-[11px] font-medium hover:bg-bg-hover transition-colors text-center disabled:opacity-50">
            Cancel
          </button>
          <button
            onClick={handleSave}
            disabled={saving}
            className="flex-1 bg-accent-blue border-transparent text-white rounded-lg px-3 py-2.5 text-[11px] font-medium flex items-center justify-center gap-1.5 hover:bg-blue-600 transition-all shadow-sm hover:shadow-md focus:ring-2 focus:ring-accent-blue focus:ring-offset-2 focus:ring-offset-bg-card disabled:opacity-50 disabled:hover:translate-y-0"
          >
            {saving ? (
              <>
                <Loader2 size={14} className="animate-spin" />
                Saving…
              </>
            ) : (
              <>
                <Save size={14} />
                {isEdit ? 'Update Connection' : 'Save Connection'}
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
      <label className="text-[10px] font-semibold text-text-primary uppercase tracking-widest mb-2 block ml-0.5">
        {label}
        {required && <span className="text-accent-red ml-1">*</span>}
      </label>
      {children}
      {hint && !error && <p className="text-[10px] text-text-tertiary mt-1.5 ml-0.5 m-0 leading-none">{hint}</p>}
      {error && <p className="text-[10px] text-accent-red mt-1.5 ml-0.5 m-0 leading-none">{error}</p>}
    </div>
  )
}

export default DatabaseConfig
