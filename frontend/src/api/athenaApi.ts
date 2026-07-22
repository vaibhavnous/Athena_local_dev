import axios from 'axios'
import { getApiBaseUrl } from './baseUrl'
import { clearSession, getAccessToken, type AuthUser, type UserType } from '../auth/session'

const API_BASE_URL = getApiBaseUrl()

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 15000,
  headers: {
    'Content-Type': 'application/json'
  }
})

const READ_TIMEOUT = 15000
const RUNS_LIST_TIMEOUT = 10000
const RUN_DETAIL_TIMEOUT = 30000
const REVIEW_TIMEOUT = 90000
const WRITE_TIMEOUT = 90000
const UPLOAD_TIMEOUT = 45000
const LOG_TIMEOUT = 10000

// Request interceptor
api.interceptors.request.use(
  (config) => {
    const token = getAccessToken()
    if (token) config.headers.Authorization = `Bearer ${token}`
    return config
  },
  (error) => Promise.reject(error)
)

// Response interceptor — normalize errors
api.interceptors.response.use(
  (response) => response.data,
  (error) => {
    if (error.response?.status === 401) {
      clearSession()
      window.dispatchEvent(new Event('astra:unauthorized'))
      if (window.location.pathname !== '/login') {
        const next = `${window.location.pathname}${window.location.search}`
        window.location.assign(`/login?next=${encodeURIComponent(next)}`)
      }
    }
    const message = error.response?.data?.message || error.response?.data?.detail || error.message || 'Network error'
    const normalized = new Error(message) as any
    normalized.code = error.code
    normalized.status = error.response?.status
    normalized.data = error.response?.data
    return Promise.reject(normalized)
  }
)

export type { AuthUser, UserType }

export interface LoginResponse {
  access_token: string
  token_type: string
  expires_in: number
  user: AuthUser
}

export interface UserPayload {
  username: string
  email: string
  password?: string
  userType: UserType
}

export const login = (payload: { email: string; password: string }) =>
  api.post('/auth/login', payload) as unknown as Promise<LoginResponse>

export const getCurrentUser = () =>
  api.get('/auth/me') as unknown as Promise<AuthUser>

export const getAuthUsers = () =>
  api.get('/auth/users') as unknown as Promise<{ users: AuthUser[] }>

export const createAuthUser = (payload: UserPayload) =>
  api.post('/auth/users', payload) as unknown as Promise<{ user: AuthUser }>

export const updateAuthUser = (uid: string, payload: Partial<UserPayload>) =>
  api.patch(`/auth/users/${uid}`, payload) as unknown as Promise<{ user: AuthUser }>

export const setAuthUserStatus = (uid: string, isActive: boolean) =>
  api.patch(`/auth/users/${uid}/status`, { isActive }) as unknown as Promise<{ user: AuthUser }>

export const deleteAuthUser = (uid: string) =>
  api.delete(`/auth/users/${uid}`) as unknown as Promise<void>

export const getProjects = () => api.get('/projects', { timeout: READ_TIMEOUT })
export const getProject = (id: string) => api.get(`/projects/${id}`, { timeout: READ_TIMEOUT })
export const createProject = (data: object) => api.post('/projects', data, { timeout: WRITE_TIMEOUT })
export const updateProject = (id: string, data: object) => api.put(`/projects/${id}`, data, { timeout: WRITE_TIMEOUT })
export const deleteProject = (id: string) => api.delete(`/projects/${id}`, { timeout: WRITE_TIMEOUT })
export const getProjectRuns = (id: string) => api.get(`/projects/${id}/runs`, { timeout: RUNS_LIST_TIMEOUT })

export const startRun = (payload: {
  project_id?: string
  brd_text?: string
  source?: string
  sftp_entity?: string
  provider: string
  brd_filename?: string
  database_type?: string
  database_name?: string
  target_warehouse?: string
  deployment?: string
  budget?: number
  maxKpis?: number
  devMode?: boolean
  use_domain_kb?: boolean
  stage_confirmation_enabled?: boolean
  compliance_enabled?: boolean
  compliance_domain?: string
  compliance_countries?: string[]
}) => api.post('/pipeline/run', payload, { timeout: WRITE_TIMEOUT })

export const uploadBrd = (file: File) => {
  const formData = new FormData()
  formData.append('file', file)
  return api.post('/pipeline/upload-brd', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
    timeout: UPLOAD_TIMEOUT,
  })
}

export const getRunStatus = (runId: string) => api.get(`/pipeline/${runId}/status`, { timeout: READ_TIMEOUT })

export const getPipelineKpis = (runId: string) => api.get(`/kpi-reviews/${runId}`, { timeout: REVIEW_TIMEOUT })
export const createKpiReview = (runId: string, payload: { name: string; definition: string }) =>
  api.post(`/kpi-reviews/${runId}`, payload, { timeout: WRITE_TIMEOUT })

export const getRuns = () => api.get('/runs', { timeout: RUNS_LIST_TIMEOUT })

export const getRun = (runId: string) => api.get(`/runs/${runId}`, { timeout: RUN_DETAIL_TIMEOUT })
export const getRunScripts = (runId: string) => api.get(`/run-scripts/${runId}`, { timeout: REVIEW_TIMEOUT })
export const getRunLineage = (runId: string) => api.get(`/run-lineage/${runId}`, { timeout: REVIEW_TIMEOUT })
export const getTableReviews = (runId: string) => api.get(`/table-reviews/${runId}`, { timeout: REVIEW_TIMEOUT })
export const submitTableReviews = (runId: string, approvedTables: string[]) =>
  api.post(`/table-reviews/${runId}`, { approved_tables: approvedTables }, { timeout: WRITE_TIMEOUT })
export const getEnrichmentReviews = (runId: string) => api.get(`/enrichment-reviews/${runId}`, { timeout: REVIEW_TIMEOUT })
export const submitEnrichmentReview = (runId: string, approve: boolean, enrichedMetadata?: Record<string, any>) =>
  api.post(`/enrichment-reviews/${runId}`, { approve, enriched_metadata: enrichedMetadata }, { timeout: WRITE_TIMEOUT })
export const getComplianceReview = (runId: string) => api.get(`/compliance-reviews/${runId}`, { timeout: REVIEW_TIMEOUT })
export const submitComplianceReview = (
  runId: string,
  findings: Array<{ table_name: string; column_name: string; status: string; security_control?: string | null; reviewer_comments?: string | null }> = [],
  overall_comments?: string | null
) => api.post(`/compliance-reviews/${runId}`, { findings, overall_comments }, { timeout: WRITE_TIMEOUT })

export const getBronzeReview = (runId: string) => api.get(`/bronze-reviews/${runId}`, { timeout: REVIEW_TIMEOUT })

export const submitBronzeReview = (runId: string, action: 'APPROVED' | 'REJECTED' | 'REGENERATE', reviewArtifact?: Record<string, any>) =>
  api.post(`/bronze-reviews/${runId}`, { action, review_artifact: reviewArtifact }, { timeout: WRITE_TIMEOUT })

export const getSilverMergeKeyReview = (runId: string) => api.get(`/silver-merge-key-reviews/${runId}`, { timeout: REVIEW_TIMEOUT })

export const submitSilverMergeKeyReview = (runId: string, action: 'APPROVED' | 'REJECTED' | 'REGENERATE', reviewArtifact?: Record<string, any>) =>
  api.post(`/silver-merge-key-reviews/${runId}`, { action, review_artifact: reviewArtifact }, { timeout: WRITE_TIMEOUT })

export const getSilverReview = (runId: string) => api.get(`/silver-reviews/${runId}`, { timeout: REVIEW_TIMEOUT })

export const submitSilverReview = (runId: string, action: 'APPROVED' | 'REJECTED' | 'REGENERATE', reviewArtifact?: Record<string, any>) =>
  api.post(`/silver-reviews/${runId}`, { action, review_artifact: reviewArtifact }, { timeout: WRITE_TIMEOUT })

export const getGoldReview = (runId: string) => api.get(`/gold-reviews/${runId}`, { timeout: REVIEW_TIMEOUT })

export const submitGoldReview = (runId: string, action: 'APPROVED' | 'REJECTED' | 'REGENERATE', reviewArtifact?: Record<string, any>) =>
  api.post(`/gold-reviews/${runId}`, { action, review_artifact: reviewArtifact }, { timeout: WRITE_TIMEOUT })

export const abortRun = (runId: string) => api.post(`/pipeline/${runId}/abort`, undefined, { timeout: WRITE_TIMEOUT })
export const continueStage = (runId: string, autoAdvance = false) =>
  api.post(`/pipeline/${runId}/continue-stage`, { auto_advance: autoAdvance }, { timeout: WRITE_TIMEOUT })
export const retryFailedStage = (runId: string) => api.post(`/pipeline/${runId}/retry-failed-stage`, undefined, { timeout: WRITE_TIMEOUT })
export const resumeFromFailure = (runId: string) => api.post(`/pipeline/${runId}/resume-from-failure`, undefined, { timeout: WRITE_TIMEOUT })
export const restartRun = (runId: string) => api.post(`/pipeline/${runId}/restart`, undefined, { timeout: WRITE_TIMEOUT })

export const getHitlQueue = (runId: string) => api.get(`/hitl/${runId}`, { timeout: READ_TIMEOUT })

export const submitDecisions = (
  runId: string,
  decisions: Array<{
    kpi_id: string
    decision: string
    reviewer?: string
    notes?: string
    edited_definition?: string
    edited_content?: Record<string, any>
  }>
) => api.post(`/hitl/${runId}/decisions`, { decisions }, { timeout: WRITE_TIMEOUT })

export const getKpis = (params: {
  domain?: string
  status?: string
  run_id?: string
  date_from?: string
  date_to?: string
} = {}) => api.get('/kpis', { params, timeout: READ_TIMEOUT })

export const getCostAnalytics = () => api.get('/analytics/cost', { timeout: READ_TIMEOUT })

export const getSettings = () => api.get('/settings', { timeout: READ_TIMEOUT })

export const saveSettings = (data: object) => api.put('/settings', data, { timeout: WRITE_TIMEOUT })

// ── Database Configurations ───────────────────────────────────────────────────
export const getConfigurations = () => api.get('/configurations', { timeout: READ_TIMEOUT })
export const createConfiguration = (data: object) => api.post('/configurations', data, { timeout: WRITE_TIMEOUT })
export const updateConfiguration = (id: string | number, data: object) => api.put(`/configurations/${id}`, data, { timeout: WRITE_TIMEOUT })
export const deleteConfiguration = (id: string | number) => api.delete(`/configurations/${id}`, { timeout: WRITE_TIMEOUT })
export const testConnection = (data: object) => api.post('/configurations/test', data, { timeout: WRITE_TIMEOUT })

// ── HITL KPI Review — KPI Reviews ─────────────────────────────────────────────
export const fetchKpiReviews = (runId: string, status: string | null = null) =>
  api.get(`/kpi-reviews/${runId}`, { params: status ? { status } : {}, timeout: REVIEW_TIMEOUT })

export const approveKpi = (queueId: string, reviewerId: string) =>
  api.post(`/kpi-reviews/${queueId}/approve`, { reviewer_id: reviewerId }, { timeout: WRITE_TIMEOUT })

export const rejectKpi = (queueId: string, reviewerId: string, rejectionReason: string) =>
  api.post(`/kpi-reviews/${queueId}/reject`, {
    reviewer_id: reviewerId,
    rejection_reason: rejectionReason
  }, { timeout: WRITE_TIMEOUT })

export const modifyKpi = (queueId: string, reviewerId: string, editedContent: object) =>
  api.post(`/kpi-reviews/${queueId}/modify`, {
    reviewer_id: reviewerId,
    edited_content: editedContent
  }, { timeout: WRITE_TIMEOUT })

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
  }, { timeout: WRITE_TIMEOUT })

// ── Pipeline Logs ─────────────────────────────────────────────────────────────

/**
 * Start background discovery of the internal run_id UUID from the
 * pipeline_execution_logs Databricks table.
 */
export const initiateLogsDiscovery = (databricksRunId: string) =>
  api.post(`/logs/discover/${databricksRunId}`, undefined, { timeout: WRITE_TIMEOUT })

/**
 * Poll for discovery status.
 * Returns { status: 'discovering'|'completed'|'failed', runId?: string, error?: string }
 */
export const getLogsDiscoveryStatus = (databricksRunId: string) =>
  api.get(`/logs/discover/${databricksRunId}/status`, { timeout: READ_TIMEOUT })

/**
 * Fetch all logs for a discovered run_id UUID.
 */
export const getPipelineLogs = (runId: string, limit = 1000) =>
  api.get(`/logs/${runId}`, { params: { limit }, timeout: LOG_TIMEOUT })

/**
 * Fetch logs written after sinceTimestamp (for incremental polling).
 */
export const getPipelineLogsSinceWithLimit = (runId: string, sinceTimestamp: string, limit = 300) =>
  api.get(`/logs/${runId}/since/${encodeURIComponent(sinceTimestamp)}`, { params: { limit }, timeout: LOG_TIMEOUT })

export default api
