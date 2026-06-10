import axios from 'axios'

// Use an explicit IPv4 loopback by default. Some Windows setups resolve
// `localhost` to IPv6 (::1) first, which will fail if the backend only binds IPv4.
const API_BASE_URL = (process.env.REACT_APP_API_BASE_URL || 'http://127.0.0.1:8000').replace(/\/$/, '')

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 60000,
  headers: {
    'Content-Type': 'application/json'
  }
})

// Request interceptor
api.interceptors.request.use(
  (config) => config,
  (error) => Promise.reject(error)
)

// Response interceptor — normalize errors
api.interceptors.response.use(
  (response) => response.data,
  (error) => {
    const message = error.response?.data?.message || error.message || 'Network error'
    const normalized = new Error(message) as any
    normalized.status = error.response?.status
    normalized.data = error.response?.data
    return Promise.reject(normalized)
  }
)

export const startRun = (payload: {
  brd_text?: string
  source?: string
  sftp_entity?: string
  provider: string
  brd_filename?: string
  database_type?: string
  database_name?: string
  deployment?: string
  budget?: number
  maxKpis?: number
  devMode?: boolean
}) => api.post('/pipeline/run', payload)

export const uploadBrd = (file: File) => {
  const formData = new FormData()
  formData.append('file', file)
  return api.post('/pipeline/upload-brd', formData, {
    headers: { 'Content-Type': 'multipart/form-data' }
  })
}

export const getRunStatus = (runId: string) => api.get(`/pipeline/${runId}/status`)

export const getPipelineKpis = (runId: string) => api.get(`/kpi-reviews/${runId}`)

export const getRuns = () => api.get('/runs')

export const getRun = (runId: string) => api.get(`/runs/${runId}`)
export const getTableReviews = (runId: string) => api.get(`/table-reviews/${runId}`)
export const submitTableReviews = (runId: string, approvedTables: string[]) =>
  api.post(`/table-reviews/${runId}`, { approved_tables: approvedTables })
export const getEnrichmentReviews = (runId: string) => api.get(`/enrichment-reviews/${runId}`)
export const submitEnrichmentReview = (runId: string, approve: boolean) =>
  api.post(`/enrichment-reviews/${runId}`, { approve })

export const getBronzeReview = (runId: string) => api.get(`/bronze-reviews/${runId}`)

export const submitBronzeReview = (runId: string, action: 'APPROVED' | 'REJECTED' | 'REGENERATE') =>
  api.post(`/bronze-reviews/${runId}`, { action })

export const getSilverReview = (runId: string) => api.get(`/silver-reviews/${runId}`)

export const submitSilverReview = (runId: string, action: 'APPROVED' | 'REJECTED' | 'REGENERATE') =>
  api.post(`/silver-reviews/${runId}`, { action })

export const abortRun = (runId: string) => api.post(`/pipeline/${runId}/abort`)

export const getHitlQueue = (runId: string) => api.get(`/hitl/${runId}`)

export const submitDecisions = (
  runId: string,
  decisions: Array<{
    kpi_id: string
    decision: string
    reviewer?: string
    notes?: string
    edited_definition?: string
  }>
) => api.post(`/hitl/${runId}/decisions`, { decisions })

export const getKpis = (params: {
  domain?: string
  status?: string
  run_id?: string
  date_from?: string
  date_to?: string
} = {}) => api.get('/kpis', { params })

export const getCostAnalytics = () => api.get('/analytics/cost')

export const getSettings = () => api.get('/settings')

export const saveSettings = (data: object) => api.put('/settings', data)

// ── Database Configurations ───────────────────────────────────────────────────
export const getConfigurations = () => api.get('/configurations')
export const createConfiguration = (data: object) => api.post('/configurations', data)
export const updateConfiguration = (id: string | number, data: object) => api.put(`/configurations/${id}`, data)
export const deleteConfiguration = (id: string | number) => api.delete(`/configurations/${id}`)

// ── HITL Gate 1 — KPI Reviews ─────────────────────────────────────────────────
export const fetchKpiReviews = (runId: string, status: string | null = null) =>
  api.get(`/kpi-reviews/${runId}`, { params: status ? { status } : {} })

export const approveKpi = (queueId: string, reviewerId: string) =>
  api.post(`/kpi-reviews/${queueId}/approve`, { reviewer_id: reviewerId })

export const rejectKpi = (queueId: string, reviewerId: string, rejectionReason: string) =>
  api.post(`/kpi-reviews/${queueId}/reject`, {
    reviewer_id: reviewerId,
    rejection_reason: rejectionReason
  })

export const modifyKpi = (queueId: string, reviewerId: string, editedContent: object) =>
  api.post(`/kpi-reviews/${queueId}/modify`, {
    reviewer_id: reviewerId,
    edited_content: editedContent
  })

export const bulkKpiAction = (
  runId: string,
  reviewerId: string,
  action: 'APPROVED' | 'REJECTED',
  rejectionReason: string | null = null
) =>
  api.post(`/kpi-reviews/${runId}/bulk`, {
    reviewer_id: reviewerId,
    action,
    rejection_reason: rejectionReason
  })

// ── Pipeline Logs ─────────────────────────────────────────────────────────────

/**
 * Start background discovery of the internal run_id UUID from the
 * pipeline_execution_logs Databricks table.
 */
export const initiateLogsDiscovery = (databricksRunId: string) =>
  api.post(`/logs/discover/${databricksRunId}`)

/**
 * Poll for discovery status.
 * Returns { status: 'discovering'|'completed'|'failed', runId?: string, error?: string }
 */
export const getLogsDiscoveryStatus = (databricksRunId: string) =>
  api.get(`/logs/discover/${databricksRunId}/status`)

/**
 * Fetch all logs for a discovered run_id UUID.
 */
export const getPipelineLogs = (runId: string, limit = 1000) =>
  api.get(`/logs/${runId}`, { params: { limit } })

/**
 * Fetch logs written after sinceTimestamp (for incremental polling).
 */
export const getPipelineLogsSinceWithLimit = (runId: string, sinceTimestamp: string, limit = 300) =>
  api.get(`/logs/${runId}/since/${encodeURIComponent(sinceTimestamp)}`, { params: { limit } })

export default api
