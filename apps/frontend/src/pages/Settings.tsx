// @ts-nocheck
import React, { useEffect, useState } from 'react'
import { Loader2, Save } from 'lucide-react'
import { getSettings, saveSettings } from '../api/athenaApi'
import useAthenaStore from '../store/useAthenaStore'

function Settings() {
  const settings = useAthenaStore((s) => s.settings)
  const updateSettings = useAthenaStore((s) => s.updateSettings)
  const addNotification = useAthenaStore((s) => s.addNotification)

  const [form, setForm] = useState({ ...settings })
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)

  useEffect(() => {
    let cancelled = false

    const load = async () => {
      try {
        const data = await getSettings()
        if (cancelled) return
        setForm(data)
        updateSettings(data)
      } catch (error) {
        if (cancelled) return
        console.warn('[Settings] Failed to load backend settings', error)
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    load()
    return () => {
      cancelled = true
    }
  }, [updateSettings])

  const setField = (key, value) => setForm((current) => ({ ...current, [key]: value }))

  const handleSave = async () => {
    setSaving(true)
    try {
      const saved = await saveSettings(form)
      updateSettings(saved)
      addNotification({
        type: 'success',
        title: 'Settings Saved',
        message: 'FastAPI settings payload updated.',
        duration: 3000,
      })
    } catch (error) {
      addNotification({
        type: 'error',
        title: 'Save Failed',
        message: error.message || 'Unable to save settings.',
        duration: 5000,
      })
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="max-w-4xl">
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h1 className="text-[16px] font-semibold text-text-primary m-0">Settings</h1>
          <p className="mt-1 text-[11px] text-text-tertiary">
            Current FastAPI pipeline defaults.
          </p>
        </div>
        <button
          onClick={handleSave}
          disabled={loading || saving}
          className="flex items-center justify-center gap-1.5 rounded-lg bg-accent-blue px-4 py-2.5 text-[11px] font-medium text-white transition-colors hover:bg-blue-600 disabled:opacity-50"
        >
          {saving ? <Loader2 size={12} className="animate-spin" /> : <Save size={12} />}
          Save Settings
        </button>
      </div>

      <div className="rounded-xl border border-bg-border bg-bg-card p-6">
        {loading ? (
          <div className="flex items-center gap-2 text-sm text-text-secondary">
            <Loader2 size={14} className="animate-spin" />
            Loading settings...
          </div>
        ) : (
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            <Field label="Provider">
              <input
                className="input-field"
                value={form.provider || ''}
                onChange={(event) => setField('provider', event.target.value)}
              />
            </Field>
            <Field label="Azure Deployment">
              <input
                className="input-field"
                value={form.azure_deployment || ''}
                onChange={(event) => setField('azure_deployment', event.target.value)}
              />
            </Field>
            <Field label="Budget">
              <input
                type="number"
                className="input-field"
                value={form.budget ?? ''}
                onChange={(event) => setField('budget', Number(event.target.value))}
              />
            </Field>
            <Field label="Max KPIs">
              <input
                type="number"
                className="input-field"
                value={form.maxKpis ?? ''}
                onChange={(event) => setField('maxKpis', Number(event.target.value))}
              />
            </Field>
            <Field label="Dev Mode">
              <select
                className="input-field"
                value={String(Boolean(form.devMode))}
                onChange={(event) => setField('devMode', event.target.value === 'true')}
              >
                <option value="false">false</option>
                <option value="true">true</option>
              </select>
            </Field>
          </div>
        )}
      </div>
    </div>
  )
}

function Field({ label, children }) {
  return (
    <div>
      <label className="mb-2 block text-[10px] font-semibold uppercase tracking-widest text-text-primary">
        {label}
      </label>
      {children}
    </div>
  )
}

export default Settings
