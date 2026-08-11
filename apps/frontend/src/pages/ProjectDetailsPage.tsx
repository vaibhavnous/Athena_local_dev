// @ts-nocheck
import React, { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { AnimatePresence } from 'framer-motion'
import { AlertTriangle, ArrowLeft, CalendarDays, Edit2, FileText, Folder, Info, Loader2, Play, RefreshCw } from 'lucide-react'
import { useNavigate, useParams } from 'react-router-dom'
import { getRun } from '../api/athenaApi'
import { PageHeader } from '../components/shared/DashboardLayout'
import PythonCodeDialog from '../components/shared/PythonCodeDialog'
import RunReportDialog from '../components/shared/RunReportDialog'
import { useDbConfigurations } from '../hooks/useDbConfig'
import { useProject, useProjectRuns, useUpdateProject } from '../hooks/useProjects'
import { getPhaseGroups, normalizeState, statusTone } from '../utils/pipelinePhases'
import {
  formatCompactDate,
  formatFullDate,
  formatRelativeTime,
  RunHistoryPhaseRow,
  RunStatusPill,
} from './RunHistoryPage'
import { ProjectForm } from './ProjectInitiation'

export default function ProjectDetailsPage() {
  const { projectId } = useParams()
  const navigate = useNavigate()
  const { data: project, isLoading, error } = useProject(projectId)
  const { data: connections = [], isLoading: connectionsLoading } = useDbConfigurations()
  const updateProject = useUpdateProject()
  const [editing, setEditing] = useState(false)
  const [selectedRunId, setSelectedRunId] = useState('')
  const [runInfoOpen, setRunInfoOpen] = useState(true)
  const [codeDialogStage, setCodeDialogStage] = useState('')
  const [reportOpen, setReportOpen] = useState(false)
  const {
    data: runs = [],
    isLoading: runsLoading,
    isFetching: runsFetching,
    error: runsRequestError,
    refetch: refetchRuns,
  } = useProjectRuns(projectId)

  useEffect(() => {
    if (!runs.length) {
      setSelectedRunId('')
      return
    }
    if (!runs.some((run) => String(run.run_id) === selectedRunId)) {
      setSelectedRunId(String(runs[0].run_id))
    }
  }, [runs, selectedRunId])

  useEffect(() => {
    setRunInfoOpen(true)
    setCodeDialogStage('')
    setReportOpen(false)
  }, [selectedRunId])

  const selectedSummary = runs.find((run) => String(run.run_id) === selectedRunId) || null
  const {
    data: detailRun,
    isLoading: detailLoading,
    isFetching: detailFetching,
    error: detailError,
    refetch: refetchDetail,
  } = useQuery({
    queryKey: ['project-run-detail', selectedRunId],
    queryFn: () => getRun(selectedRunId),
    enabled: Boolean(selectedRunId),
    retry: false,
    staleTime: 15 * 1000,
  })
  const selectedRun = detailRun
    ? { ...selectedSummary, ...detailRun, id: detailRun.id || selectedRunId, run_id: selectedRunId }
    : selectedSummary
      ? { ...selectedSummary, id: selectedRunId }
      : null
  const phases = selectedRun ? getPhaseGroups(selectedRun) : []
  const runsError = requestErrorMessage(runsRequestError, 'Failed to load project runs.')
  const selectedFailed = normalizeState(selectedRun?.status) === 'FAILED'

  if (isLoading) return <LoadingState label="Loading project..." />
  if (error || !project) return <div className="card flex min-h-[320px] flex-col items-center justify-center gap-3"><AlertTriangle className="text-red-400"/><p>{error?.message || 'Project not found'}</p><button className="btn-secondary" onClick={()=>navigate('/app/project')}>Back to projects</button></div>

  return <div className="flex min-h-full flex-col gap-4">
    <PageHeader
      eyebrow="Project Details"
      title={project.name}
      description={<><p>{project.description}</p><div className="mt-3 flex flex-wrap gap-2"><Meta label="Target" value={project.target}/><Meta label="Source Type" value={project.connectionType === 'database' ? 'Database' : 'Data Lake'}/>{project.dbType && <Meta label="Database Type" value={formatDatabaseType(project.dbType)}/>}<Meta label={project.connectionType === 'database' ? 'Database Name' : 'Data Lake Name'} value={project.databaseName || project.dataLakeName || project.connectionName || '-'}/><Meta label="Domain Knowledge Base" value={project.useDomainKB ? 'Enabled' : 'Disabled'}/></div></>}
      icon={Folder}
      leadingAction={<button onClick={()=>navigate('/app/project')} className="mt-1 text-text-tertiary hover:text-white" aria-label="Back to projects"><ArrowLeft size={17}/></button>}
      actions={
        <div className="flex w-full flex-wrap items-center gap-2 sm:w-auto sm:flex-nowrap sm:justify-end">
          <span className="shrink-0 rounded-full border border-emerald-500/20 bg-emerald-500/10 px-2.5 py-1 text-[10px] font-semibold text-emerald-400">
            {project.status === 'ACTIVE' ? 'Active' : 'Archived'}
          </span>
          <button className="btn-secondary flex shrink-0 items-center gap-2 whitespace-nowrap" onClick={()=>setEditing(true)}>
            <Edit2 size={13}/>Edit
          </button>
          <button className="btn-primary flex shrink-0 items-center gap-2 whitespace-nowrap" onClick={()=>navigate(`/app/project/${project.id}/new-run`)}>
            <Play size={13}/>Start Run
          </button>
        </div>
      }
    />

    <section className="card overflow-hidden">
      <header className="flex items-center justify-between border-b border-bg-border p-4">
        <div className="flex items-center gap-2"><FileText size={15} className="text-accent-blue"/><h2 className="text-sm font-semibold">Run History</h2><span className="rounded-full border border-bg-border px-2 py-0.5 text-[10px] text-text-muted">{runs.length} run{runs.length === 1 ? '' : 's'}</span></div>
        <button className="btn-secondary flex items-center gap-2" onClick={()=>refetchRuns()} disabled={runsFetching}><RefreshCw size={12} className={runsFetching?'animate-spin':''}/>Refresh</button>
      </header>
      {runsError && <ErrorBanner message={runsError} onRetry={refetchRuns} />}
      {runsLoading ? <LoadingState label="Loading runs..." compact/> : runsError && !runs.length ? null : !runs.length ? <EmptyRuns/> : (
        <div className="grid min-h-[520px] lg:grid-cols-[300px_minmax(0,1fr)]">
          <div className="border-b border-bg-border lg:border-b-0 lg:border-r">
            {runs.map((run) => {
              const runId = String(run.run_id)
              const active = runId === selectedRunId
              return <button key={runId} type="button" onClick={()=>setSelectedRunId(runId)} className={`w-full border-b border-bg-border border-l-2 px-4 py-3 text-left transition-colors ${active ? 'border-l-accent-blue bg-accent-blue/10' : 'border-l-transparent hover:bg-bg-hover'}`}>
                <div className="flex items-start justify-between gap-2"><div className="min-w-0"><div className="flex items-center gap-2"><FileText size={12} className="shrink-0 text-text-muted"/><span className="truncate text-xs font-semibold text-white">{run.brd_filename || 'Untitled run'}</span></div><div className="mt-2 flex items-center gap-2 text-[10px] text-text-muted"><span className="max-w-[82px] truncate font-mono">{runId.slice(0, 8)}...</span><CalendarDays size={9}/><span>{formatCompactDate(run.started_at || run.last_activity)}</span></div><div className="mt-1 text-[10px] text-text-muted">{formatRelativeTime(run.completed_at || run.updated_at || run.last_activity)}</div></div><RunStatusPill status={run.status} tone={statusTone(run.status)}/></div>
              </button>
            })}
          </div>

          <div className="min-w-0 p-4 sm:p-5">
            {selectedRun && <>
              <div className="mb-4 flex items-start justify-between gap-3"><div className="min-w-0"><h3 className="truncate text-base font-bold text-white">{selectedRun.brd_filename || 'Untitled run'}</h3><p className="mt-1 break-all font-mono text-xs text-text-muted">{selectedRunId}</p></div><div className="flex items-center gap-2"><RunStatusPill status={selectedRun.status} tone={statusTone(selectedRun.status)} large/><button type="button" onClick={()=>refetchDetail()} disabled={detailFetching} aria-label="Refresh selected run" className="rounded-md p-2 text-text-tertiary hover:bg-bg-hover hover:text-white"><RefreshCw size={15} className={detailFetching ? 'animate-spin' : ''}/></button></div></div>
              {detailLoading && <LoadingState label="Loading run details..." compact/>}
              {detailError && <ErrorBanner message={requestErrorMessage(detailError, 'Failed to load run details.')} onRetry={refetchDetail}/>}
              {selectedFailed && (selectedRun.error || selectedRun.error_message) && <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 p-3 text-xs text-red-300"><div className="mb-1 font-semibold">Failed at {selectedRun.failed_background_stage || selectedRun.last_failed_stage_key || 'pipeline stage'}</div><div className="break-words">{selectedRun.error || selectedRun.error_message}</div></div>}

              <div className="overflow-hidden rounded-lg border border-bg-border bg-bg-base/40">
                <button type="button" onClick={()=>setRunInfoOpen((open)=>!open)} aria-expanded={runInfoOpen} className="flex w-full items-center justify-between px-4 py-3 text-left text-xs font-semibold hover:bg-bg-hover"><span>Run Info</span><span className="text-text-muted">{runInfoOpen ? 'Hide' : 'Show'}</span></button>
                {runInfoOpen && <div className="grid gap-2.5 border-t border-bg-border px-4 py-3 text-xs"><InfoRow label="Project Name" value={project.name}/><InfoRow label="Project Description" value={project.description}/><InfoRow label="Source" value={selectedRun.source || project.connectionType}/><InfoRow label="Database Type" value={selectedRun.database_type || selectedRun.db_type || project.dbType || '-'}/><InfoRow label="Database Name" value={selectedRun.database_name || project.databaseName || '-'}/><InfoRow label="Started" value={formatFullDate(selectedRun.started_at || selectedRun.last_activity)}/><InfoRow label="Last Updated" value={formatFullDate(selectedRun.completed_at || selectedRun.updated_at || selectedRun.last_activity)}/><InfoRow label="Knowledge Base" value={selectedRun.knowledge_base_id || selectedRun.domain_profile || (selectedRun.use_domain_knowledge_base || project.useDomainKB ? 'Enabled' : 'Not used')}/></div>}
              </div>

              <div className="mt-5"><h4 className="mb-3 text-xs font-semibold text-text-secondary">Stages by Phase</h4><div className="overflow-hidden rounded-lg border border-bg-border bg-bg-base/40">{phases.length ? phases.map((phase,index)=><RunHistoryPhaseRow key={phase.id} phase={phase} index={index+1} onViewCode={setCodeDialogStage} onViewReport={()=>setReportOpen(true)}/>) : <div className="p-6 text-center text-xs text-text-muted">Detailed stage information is not available for this run.</div>}</div></div>
            </>}
          </div>
        </div>
      )}
    </section>

    <PythonCodeDialog isOpen={Boolean(codeDialogStage)} onClose={()=>setCodeDialogStage('')} stageName={codeDialogStage} runId={selectedRunId} title={`Generated Code — ${codeDialogStage}`}/>
    <RunReportDialog isOpen={reportOpen} onClose={()=>setReportOpen(false)} report={selectedRun?.run_report}/>
    <AnimatePresence>{editing && <ProjectForm initial={project} connections={connections} connectionsLoading={connectionsLoading} busy={updateProject.isPending} onClose={()=>setEditing(false)} onSave={async data=>{await updateProject.mutateAsync({id:project.id,data});setEditing(false)}}/>}</AnimatePresence>
  </div>
}

function requestErrorMessage(error, fallback) {
  if (!error) return ''
  return error?.code === 'ECONNABORTED' || /timeout/i.test(error?.message || '')
    ? 'This request is taking longer than expected. Please try again.'
    : error?.message || fallback
}

function formatDatabaseType(value) {
  const type = String(value || '').toLowerCase()
  if (type === 'azure_sql') return 'Azure SQL DB'
  return value || '-'
}

function Meta({label,value}) { return <span className="rounded-md border border-bg-border bg-bg-base px-2.5 py-1.5 text-xs"><b className="mr-1 text-[10px] uppercase text-text-muted">{label}:</b>{value}</span> }
function InfoRow({label,value}) { return <div className="grid gap-1.5 sm:grid-cols-[190px_minmax(0,1fr)]"><div className="flex items-center gap-2 text-text-secondary"><Info size={13}/><span>{label}</span></div><div className="break-words font-mono text-white">{value || '-'}</div></div> }
function LoadingState({label,compact=false}) { return <div className={`flex items-center justify-center gap-2 text-xs text-text-tertiary ${compact?'p-8':'min-h-[60vh]'}`}><Loader2 size={16} className="animate-spin"/>{label}</div> }
function ErrorBanner({message,onRetry}) { return <div className="flex items-center justify-center gap-3 border-b border-red-500/20 bg-red-500/5 p-4 text-xs text-red-300"><AlertTriangle size={14}/><span>{message}</span><button type="button" onClick={()=>onRetry()} className="rounded-md border border-red-500/30 px-2.5 py-1 font-semibold hover:bg-red-500/10">Try again</button></div> }
function EmptyRuns() { return <div className="flex flex-col items-center gap-2 p-12 text-center text-text-tertiary"><FileText size={22}/><p className="text-sm">No pipeline runs found.</p><p className="text-xs">Start the first run from this project.</p></div> }
