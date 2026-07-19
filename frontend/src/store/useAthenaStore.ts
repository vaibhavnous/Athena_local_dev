import { create } from 'zustand'

let notificationIdCounter = 0
const HITL_SOURCE_RUN_IDS_KEY = 'athena.hitlSourceRunIds'

function loadPersistedHitlSourceRunIds(): Record<string, string> {
  if (typeof window === 'undefined') return {}
  try {
    const raw = window.localStorage.getItem(HITL_SOURCE_RUN_IDS_KEY)
    const parsed = raw ? JSON.parse(raw) : {}
    return parsed && typeof parsed === 'object' ? parsed : {}
  } catch (error) {
    console.warn('[AthenaStore] Failed to load persisted HITL source run IDs:', error)
    return {}
  }
}

function persistHitlSourceRunIds(mapping: Record<string, string>) {
  if (typeof window === 'undefined') return
  try {
    window.localStorage.setItem(HITL_SOURCE_RUN_IDS_KEY, JSON.stringify(mapping))
  } catch (error) {
    console.warn('[AthenaStore] Failed to persist HITL source run IDs:', error)
  }
}

function stableStringify(value: unknown): string {
  try {
    return JSON.stringify(value) ?? ''
  } catch {
    return ''
  }
}

function hasUsefulRunDetail(run: any): boolean {
  return Boolean(
    (Array.isArray(run?.stages) && run.stages.length > 0) ||
      (Array.isArray(run?.pipeline_steps) && run.pipeline_steps.length > 0) ||
      run?.background_stage ||
      run?.external_execution ||
      run?.stage_confirmation ||
      Number(run?.next_gate || 0) > 0 ||
      run?.bronze ||
      run?.silver ||
      run?.gold
  )
}

function normalizeRunStatus(value: any): string {
  const status = String(value || '').toUpperCase()
  if (status === 'PROCESSING' || status === 'SUBMITTED' || status === 'IN_PROGRESS') return 'RUNNING'
  if (status === 'PENDING_REVIEW' || status === 'PAUSED_FOR_HITL') return 'HITL_WAIT'
  if (status === 'SUCCESS' || status === 'PIPELINE_COMPLETED') return 'COMPLETED'
  return status
}

const PIPELINE_PROGRESS_ORDER = [
  'ingestion', 'memory', 'requirements', 'kpis', 'gate1',
  'nomination', 'gate2', 'discovery', 'profiling', 'enrichment', 'gate3',
  'bronze', 'gate4', 'bronze_code_execution',
  'silver_merge_key_resolution', 'silver_merge_key_review', 'silver', 'gate5', 'silver_code_execution',
  'gold', 'gold_code_execution',
]

function runProgressIndex(run: any): number {
  const order = new Map(PIPELINE_PROGRESS_ORDER.map((key, index) => [key, index]))
  const steps = Array.isArray(run?.pipeline_steps) && run.pipeline_steps.length
    ? run.pipeline_steps
    : Array.isArray(run?.stages) ? run.stages : []
  let furthest = -1

  for (const step of steps) {
    const state = normalizeRunStatus(step?.state ?? step?.status)
    if (state !== 'PENDING') furthest = Math.max(furthest, order.get(String(step?.key || '')) ?? -1)
  }

  const backgroundStage = String(run?.external_execution?.stage_key || run?.background_stage || '').trim()
  return Math.max(furthest, order.get(backgroundStage) ?? -1)
}

function preserveProgressFields(existing: any, merged: any) {
  for (const key of [
    'status', 'stages', 'pipeline_steps', 'background_stage', 'external_execution',
    'snowflake_bronze_execution_status', 'snowflake_bronze_execution_progress',
    'snowflake_silver_execution_status', 'snowflake_silver_execution_progress',
    'snowflake_gold_execution_status', 'snowflake_gold_execution_progress',
    'stage_confirmation', 'next_gate', 'next_review_key', 'resume_message',
  ]) {
    if (existing[key] !== undefined) merged[key] = existing[key]
  }
  return merged
}

function mergeRunPreservingDetail(existing: any, incoming: any): any {
  if (!existing) return incoming
  if (!incoming) return existing

  const incomingHasDetail = hasUsefulRunDetail(incoming)
  const existingHasDetail = hasUsefulRunDetail(existing)
  const merged = { ...existing, ...incoming }
  const incomingStatus = normalizeRunStatus(incoming?.status)
  const hasIncomingStatus = incoming?.status !== undefined && incoming?.status !== null
  const incomingActive = incomingStatus === 'RUNNING'
  const incomingTerminal =
    incomingStatus === 'FAILED' ||
    incomingStatus === 'COMPLETED' ||
    incomingStatus === 'ABORTED' ||
    incomingStatus === 'CANCELLED' ||
    incomingStatus === 'CANCELED'
  const incomingPausedOrTerminal =
    incomingStatus === 'HITL_WAIT' ||
    incomingTerminal

  // The history endpoint intentionally returns lightweight UNKNOWN summaries.
  // Keep the authoritative status from the selected run's detail response.
  if (existingHasDetail && !incomingHasDetail && (!incomingStatus || incomingStatus === 'UNKNOWN')) {
    merged.status = existing.status
  }

  // Status hydration can return a sparse checkpoint snapshot after its detail query times out.
  // Keep the furthest known stage; a slower response must not move the UI back to an earlier phase.
  if (runProgressIndex(incoming) < runProgressIndex(existing)) {
    return preserveProgressFields(existing, merged)
  }

  for (const key of ['stages', 'pipeline_steps']) {
    if (Array.isArray(existing[key]) && existing[key].length > 0 && Array.isArray(incoming[key]) && incoming[key].length === 0) {
      merged[key] = existing[key]
    }
  }

  if (existingHasDetail && !incomingHasDetail) {
    for (const key of [
      'stages',
      'pipeline_steps',
      'background_stage',
      'external_execution',
      'snowflake_bronze_execution_status',
      'snowflake_bronze_execution_progress',
      'snowflake_silver_execution_status',
      'snowflake_silver_execution_progress',
      'snowflake_gold_execution_status',
      'snowflake_gold_execution_progress',
      'stage_confirmation',
      'next_gate',
      'next_review_key',
      'resume_message',
      'bronze',
      'silver',
      'gold',
      'script_counts',
      'kpis',
      'nominated_tables',
      'certified_tables',
      'enriched_metadata',
      'enriched_columns',
      'enriched_joins',
      'gold_review_artifact',
    ]) {
      if (existing[key] !== undefined && (incoming[key] === undefined || incoming[key] === null)) merged[key] = existing[key]
    }
  }

  if (incomingActive) {
    for (const key of [
      'background_stage',
      'external_execution',
      'snowflake_bronze_execution_progress',
      'snowflake_silver_execution_progress',
      'snowflake_gold_execution_progress',
    ]) {
      if (existing[key] !== undefined && (incoming[key] === undefined || incoming[key] === null)) merged[key] = existing[key]
    }
  }

  if (incomingActive) {
    if (incoming.next_gate === undefined || incoming.next_gate === null) merged.next_gate = null
    if (incoming.next_review_key === undefined || incoming.next_review_key === null) merged.next_review_key = null
    if (incoming.background_stage && (incoming.stage_confirmation === undefined || incoming.stage_confirmation === null)) {
      merged.stage_confirmation = null
    }
  }

  if (hasIncomingStatus && incomingPausedOrTerminal) {
    if (incoming.background_stage === undefined || incoming.background_stage === null) merged.background_stage = null
    if (incoming.external_execution === undefined || incoming.external_execution === null) merged.external_execution = null
  }

  if (hasIncomingStatus && incomingTerminal) {
    if (incoming.next_gate === undefined || incoming.next_gate === null) merged.next_gate = null
    if (incoming.next_review_key === undefined || incoming.next_review_key === null) merged.next_review_key = null
  }

  return merged
}

interface Notification {
  id: number
  type: string
  title: string
  message: string
  duration: number
  action?: { label: string; href: string }
}

interface AthenaState {
  runs: any[]
  activeRunId: string | null
  hitlQueues: Record<string, any[]>
  hitlSourceRunIds: Record<string, string>
  kpiLibrary: any[]
  costData: any[]
  settings: any
  sidebarCollapsed: boolean
  notifications: Notification[]
  serverOnline: boolean
  demoModeBannerDismissed: boolean
  userRole: string | null

  setRuns: (runs: any[]) => void
  addRun: (run: any) => void
  updateRun: (runId: string, updates: any) => void
  setActiveRun: (runId: string | null) => void
  updateStageStatus: (runId: string, stageId: string, updates: any) => void

  setHitlQueue: (runId: string, kpis: any[]) => void
  setHitlSourceRunId: (runId: string, sourceRunId: string) => void
  updateKpiDecision: (runId: string, kpiId: string, decision: any) => void
  submitDecisions: (runId: string, decisions: any[]) => void

  setKpiLibrary: (kpis: any[]) => void
  addKpisToLibrary: (kpis: any[]) => void

  setCostData: (costData: any[]) => void
  updateSettings: (updates: any) => void

  toggleSidebar: () => void
  setSidebarCollapsed: (collapsed: boolean) => void
  setServerOnline: (online: boolean) => void
  dismissDemoBanner: () => void
  setUserRole: (role: string | null) => void

  addNotification: (notification: Partial<Notification>) => number
  removeNotification: (id: number) => void

  getActiveRun: () => any
  getPendingHitlCount: () => number
  getRunById: (runId: string) => any
  getHitlSourceRunId: (runId: string) => string | null
}

const DEFAULT_SETTINGS = {
  provider: 'azure_openai',
  azure_deployment: '',
  budget: 5,
  maxKpis: 25,
  devMode: false,
}

const useAthenaStore = create<AthenaState>((set, get) => ({
  runs: [],
  activeRunId: null,
  hitlQueues: {},
  hitlSourceRunIds: loadPersistedHitlSourceRunIds(),
  kpiLibrary: [],
  costData: [],
  settings: DEFAULT_SETTINGS,
  sidebarCollapsed: false,
  notifications: [],
  serverOnline: false,
  demoModeBannerDismissed: false,
  userRole: 'Data Engineer',

  setRuns: (runs) =>
    set((state) => {
      const backendRuns = Array.isArray(runs) ? runs : []
      // Keep the last valid snapshot when polling temporarily returns no history.
      if (backendRuns.length === 0 && state.runs.length > 0) return state

      const existingById = new Map(state.runs.map((run) => [run.id, run]))
      const activeRunId = state.activeRunId
      const activeExisting =
        activeRunId ? state.runs.find((run) => run.id === activeRunId) || null : null
      const activePresentInBackend =
        !!activeRunId && backendRuns.some((run) => run.id === activeRunId)
      const shouldPreserveMissingActive =
        Boolean(activeExisting) &&
        !activePresentInBackend &&
        !activeExisting?.is_demo_fallback
      const mergedBackendRuns = backendRuns.map((run) =>
        mergeRunPreservingDetail(existingById.get(run.id), run)
      )

      const mergedRuns =
        shouldPreserveMissingActive
          ? [activeExisting, ...mergedBackendRuns.filter((run) => run.id !== activeExisting.id)]
          : mergedBackendRuns

      const nextActiveRunId =
        activeRunId && mergedRuns.some((run) => run.id === activeRunId)
          ? activeRunId
          : null

      const runsUnchanged = stableStringify(state.runs) === stableStringify(mergedRuns)
      if (runsUnchanged && state.activeRunId === nextActiveRunId) {
        return state
      }

      return {
        runs: mergedRuns,
        activeRunId: nextActiveRunId,
      }
    }),

  addRun: (run) =>
    set((state) => ({
      runs: [run, ...state.runs.filter((item) => item.id !== run.id)],
      activeRunId: run.id,
    })),

  updateRun: (runId, updates) =>
    set((state) => {
      const currentRun = state.runs.find((run) => run.id === runId)
      if (!currentRun) return state

      const nextRun = mergeRunPreservingDetail(currentRun, updates)
      if (stableStringify(currentRun) === stableStringify(nextRun)) {
        return state
      }

      return {
        runs: state.runs.map((run) =>
          run.id === runId ? nextRun : run
        ),
      }
    }),

  setActiveRun: (runId) =>
    set((state) => (state.activeRunId === runId ? state : { activeRunId: runId })),

  updateStageStatus: (runId, stageId, updates) =>
    set((state) => ({
      runs: state.runs.map((run) => {
        if (run.id !== runId) return run
        const stages = Array.isArray(run.stages) ? run.stages : []
        return {
          ...run,
          stages: stages.map((stage: any) =>
            stage.id === stageId ? { ...stage, ...updates } : stage
          ),
        }
      }),
    })),

  setHitlQueue: (runId, kpis) =>
    set((state) => ({
      hitlQueues: { ...state.hitlQueues, [runId]: kpis },
    })),

  setHitlSourceRunId: (runId, sourceRunId) =>
    set((state) => {
      const next = { ...state.hitlSourceRunIds, [runId]: sourceRunId }
      persistHitlSourceRunIds(next)
      return { hitlSourceRunIds: next }
    }),

  updateKpiDecision: (runId, kpiId, decision) =>
    set((state) => {
      const queue = state.hitlQueues[runId] || []
      return {
        hitlQueues: {
          ...state.hitlQueues,
          [runId]: queue.map((kpi) =>
            (kpi.queue_id || kpi.id) === kpiId ? { ...kpi, ...decision } : kpi
          ),
        },
      }
    }),

  submitDecisions: (runId, decisions) => {
    set((state) => {
      const queue = state.hitlQueues[runId] || []
      const updatedQueue = queue.map((kpi: any) => {
        const kpiKey = kpi.queue_id || kpi.id
        const decision = decisions.find((item) => item.kpi_id === kpiKey)
        if (!decision) return kpi
        return {
          ...kpi,
          decision: decision.decision,
          reviewer: decision.reviewer || 'reviewer',
          reviewed_at: new Date().toISOString(),
          status: decision.decision,
          ...(decision.edited_definition
            ? { definition: decision.edited_definition }
            : {}),
        }
      })
      return {
        hitlQueues: { ...state.hitlQueues, [runId]: updatedQueue },
        runs: state.runs.map((run) =>
          run.id === runId ? { ...run, kpis: updatedQueue } : run
        ),
      }
    })
  },

  setKpiLibrary: (kpis) => set({ kpiLibrary: kpis }),

  addKpisToLibrary: (kpis) =>
    set((state) => ({
      kpiLibrary: [...state.kpiLibrary, ...kpis],
    })),

  setCostData: (costData) => set({ costData }),

  updateSettings: (updates) =>
    set((state) => ({
      settings: { ...state.settings, ...updates },
    })),

  toggleSidebar: () =>
    set((state) => ({ sidebarCollapsed: !state.sidebarCollapsed })),

  setSidebarCollapsed: (collapsed) => set({ sidebarCollapsed: collapsed }),

  setServerOnline: (online) =>
    set((state) => (state.serverOnline === online ? state : { serverOnline: online })),

  dismissDemoBanner: () => set({ demoModeBannerDismissed: true }),

  setUserRole: (role) => set({ userRole: role }),

  addNotification: (notification) => {
    const id = ++notificationIdCounter
    const newNotification: Notification = {
      id,
      type: 'info',
      title: '',
      message: '',
      duration: 4000,
      ...notification,
    }
    set((state) => ({
      notifications: [...state.notifications, newNotification],
    }))
    if (newNotification.duration > 0) {
      window.setTimeout(() => {
        get().removeNotification(id)
      }, newNotification.duration)
    }
    return id
  },

  removeNotification: (id) =>
    set((state) => ({
      notifications: state.notifications.filter((n) => n.id !== id),
    })),

  getActiveRun: () => {
    const { runs, activeRunId } = get()
    return runs.find((run) => run.id === activeRunId) || null
  },

  getPendingHitlCount: () => {
    const { hitlQueues } = get()
    return Object.values(hitlQueues).reduce((total, queue) => {
      return (
        total +
        queue.filter((item) => !item.decision || item.status === 'PENDING_REVIEW')
          .length
      )
    }, 0)
  },

  getRunById: (runId) => {
    return get().runs.find((run) => run.id === runId) || null
  },

  getHitlSourceRunId: (runId) => {
    return get().hitlSourceRunIds[runId] || null
  },
}))

export default useAthenaStore
