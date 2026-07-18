// @ts-nocheck
import React, { useEffect, useState } from 'react'
import { AnimatePresence } from 'framer-motion'
import { AlertTriangle, ArrowLeft, CalendarDays, Edit2, FileText, Folder, Loader2, Play, RefreshCw } from 'lucide-react'
import { useNavigate, useParams } from 'react-router-dom'
import { getProjectRuns } from '../api/athenaApi'
import { PageHeader } from '../components/shared/DashboardLayout'
import { useDbConfigurations } from '../hooks/useDbConfig'
import { useProject, useUpdateProject } from '../hooks/useProjects'
import { ProjectForm } from './ProjectInitiation'

export default function ProjectDetailsPage() {
  const { projectId } = useParams()
  const navigate = useNavigate()
  const { data: project, isLoading, error } = useProject(projectId)
  const { data: connections = [], isLoading: connectionsLoading } = useDbConfigurations()
  const updateProject = useUpdateProject()
  const [editing, setEditing] = useState(false)
  const [runs, setRuns] = useState([])
  const [runsLoading, setRunsLoading] = useState(false)
  const [runsError, setRunsError] = useState(null)

  const loadRuns = async () => {
    if (!projectId) return
    setRunsLoading(true); setRunsError(null)
    try { setRuns(await getProjectRuns(projectId) as any[]) }
    catch (loadError) { setRunsError(loadError?.message || 'Failed to load project runs') }
    finally { setRunsLoading(false) }
  }
  useEffect(() => { loadRuns() }, [projectId])

  if (isLoading) return <div className="flex min-h-[60vh] items-center justify-center gap-2 text-sm text-text-tertiary"><Loader2 size={18} className="animate-spin"/>Loading project...</div>
  if (error || !project) return <div className="card flex min-h-[320px] flex-col items-center justify-center gap-3"><AlertTriangle className="text-red-400"/><p>{error?.message || 'Project not found'}</p><button className="btn-secondary" onClick={()=>navigate('/app/project')}>Back to projects</button></div>

  return <div className="flex min-h-full flex-col gap-4">
    <PageHeader eyebrow="Project Details" title={project.name} description={<><p>{project.description}</p><div className="mt-3 flex flex-wrap gap-2"><Meta label="Target" value={project.target}/><Meta label="Source" value={project.connectionType === 'database' ? 'Database' : 'Data Lake'}/><Meta label="Connection" value={project.databaseName || project.dataLakeName || project.connectionName || '-'}/></div></>} icon={Folder}
      leadingAction={<button onClick={()=>navigate('/app/project')} className="mt-1 text-text-tertiary hover:text-white" aria-label="Back to projects"><ArrowLeft size={17}/></button>}
      actions={<div className="flex items-center gap-2"><span className="rounded-full border border-emerald-500/20 bg-emerald-500/10 px-2.5 py-1 text-[10px] font-semibold text-emerald-400">{project.status === 'ACTIVE' ? 'Active' : 'Archived'}</span><button className="btn-secondary flex items-center gap-2" onClick={()=>setEditing(true)}><Edit2 size={13}/>Edit</button><button className="btn-primary flex items-center gap-2" onClick={()=>navigate(`/app/project/${project.id}/new-run`)}><Play size={13}/>Start Run</button></div>}/>
    <section className="card overflow-hidden">
      <header className="flex items-center justify-between border-b border-bg-border p-4"><div className="flex items-center gap-2"><FileText size={15} className="text-accent-blue"/><h2 className="text-sm font-semibold">Run History</h2><span className="rounded-full border border-bg-border px-2 py-0.5 text-[10px] text-text-muted">{runs.length} runs</span></div><button className="btn-secondary flex items-center gap-2" onClick={loadRuns} disabled={runsLoading}><RefreshCw size={12} className={runsLoading?'animate-spin':''}/>Refresh</button></header>
      {runsError ? <div className="p-8 text-center text-xs text-red-400">{runsError}</div> : runsLoading ? <div className="flex items-center justify-center gap-2 p-12 text-xs text-text-tertiary"><Loader2 className="animate-spin" size={16}/>Loading runs...</div> : !runs.length ? <div className="flex flex-col items-center gap-2 p-12 text-center text-text-tertiary"><FileText size={22}/><p className="text-sm">No pipeline runs found.</p><p className="text-xs">Start the first run from this project.</p></div> : <div>{runs.map(run=><button key={run.run_id} onClick={()=>navigate(`/app/run-history?runId=${run.run_id}`)} className="flex w-full items-center justify-between border-b border-bg-border px-4 py-3 text-left last:border-0 hover:bg-bg-hover"><div><p className="text-xs font-semibold">{run.brd_filename || 'Untitled run'}</p><p className="mt-1 font-mono text-[10px] text-text-muted">{run.run_id}</p></div><div className="flex items-center gap-3"><span className="flex items-center gap-1 text-[10px] text-text-muted"><CalendarDays size={10}/>{run.last_activity ? new Date(run.last_activity).toLocaleString() : '-'}</span><span className="rounded-full border border-bg-border px-2 py-1 text-[10px]">{run.status || 'Unknown'}</span></div></button>)}</div>}
    </section>
    <AnimatePresence>{editing && <ProjectForm initial={project} connections={connections} connectionsLoading={connectionsLoading} busy={updateProject.isPending} onClose={()=>setEditing(false)} onSave={async data => {await updateProject.mutateAsync({id:project.id,data}); setEditing(false)}}/>}</AnimatePresence>
  </div>
}

function Meta({label,value}) { return <span className="rounded-md border border-bg-border bg-bg-base px-2.5 py-1.5 text-xs"><b className="mr-1 text-[10px] uppercase text-text-muted">{label}:</b>{value}</span> }
