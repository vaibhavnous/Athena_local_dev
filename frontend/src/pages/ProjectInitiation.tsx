// @ts-nocheck
import React, { useMemo, useState } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { AlertTriangle, BookOpen, Edit2, Folder, Loader2, Play, Plus, Save, Search, Trash2, X } from 'lucide-react'
import { useNavigate } from 'react-router-dom'
import { PageHeader } from '../components/shared/DashboardLayout'
import { useDbConfigurations } from '../hooks/useDbConfig'
import { useCreateProject, useDeleteProject, useProjects, useUpdateProject } from '../hooks/useProjects'

const EMPTY = {
  name: '', description: '', target: 'Databricks', status: 'ACTIVE', connectionType: '',
  connectionName: '', dbType: '', databaseName: '', integrationType: '', dataLakeType: '',
  dataLakeName: '', useDomainKB: false, domainProfile: '', knowledgeBaseId: '',
}

const KB = { Insurance: 'PC_Insurance_V1', Basel: 'BASEL_DW_V1' }

export default function ProjectInitiation() {
  const navigate = useNavigate()
  const [search, setSearch] = useState('')
  const [editing, setEditing] = useState(null)
  const [formOpen, setFormOpen] = useState(false)
  const { data: projects = [], isLoading, error } = useProjects()
  const create = useCreateProject()
  const update = useUpdateProject()
  const remove = useDeleteProject()
  const { data: connections = [], isLoading: connectionsLoading } = useDbConfigurations()
  const filtered = useMemo(() => projects.filter(project =>
    `${project.name} ${project.description} ${project.ownerEmail || ''}`.toLowerCase().includes(search.toLowerCase())
  ), [projects, search])

  const openForm = (project = null) => { setEditing(project); setFormOpen(true) }
  const save = async (data, startAfterSave = false) => {
    const project = editing
      ? await update.mutateAsync({ id: editing.id, data })
      : await create.mutateAsync(data)
    setFormOpen(false)
    setEditing(null)
    if (startAfterSave) navigate(`/app/project/${project.id}/new-run`)
  }

  return (
    <div className="flex h-full min-h-0 flex-col gap-5 bg-bg-base">
      <PageHeader eyebrow="Projects" title="Projects." description={`${projects.length} project${projects.length === 1 ? '' : 's'} configured for governed pipeline execution.`} icon={Folder}
        actions={<button type="button" className="btn-primary flex h-10 items-center justify-center gap-2 whitespace-nowrap" onClick={() => openForm()}><Plus size={15}/>New Project</button>} />
      {(error || create.error || update.error || remove.error) && <div className="flex items-center gap-2 rounded-lg border border-red-500/30 bg-red-950/20 p-3 text-xs text-red-400"><AlertTriangle size={14}/>{String((error || create.error || update.error || remove.error)?.message)}</div>}
      <div className="relative w-full max-w-md"><Search size={13} className="absolute left-3 top-1/2 -translate-y-1/2 text-text-muted"/><input aria-label="Search projects" className="input-field h-10 pl-9 text-sm" value={search} onChange={e => setSearch(e.target.value)} placeholder="Search projects..."/></div>
      {isLoading ? <div className="card flex items-center justify-center gap-2 p-12 text-sm text-text-tertiary"><Loader2 className="animate-spin" size={18}/>Loading projects...</div>
      : filtered.length === 0 && search ? <div className="card flex flex-col items-center gap-3 p-12"><Search size={28} className="text-text-tertiary"/><p className="text-sm text-text-secondary">No matching projects</p></div>
      : <div className="grid grid-cols-1 items-stretch gap-4 sm:grid-cols-2 lg:grid-cols-3">{filtered.map(project => <ProjectCard key={project.id} project={project} onOpen={() => navigate(`/app/project/${project.id}`)} onStart={() => navigate(`/app/project/${project.id}/new-run`)} onEdit={() => openForm(project)} onDelete={() => remove.mutate(project.id)} deleting={remove.isPending}/>)}<NewProjectCard onClick={() => openForm()}/></div>}
      <AnimatePresence>{formOpen && <ProjectForm initial={editing} connections={connections} connectionsLoading={connectionsLoading} busy={create.isPending || update.isPending} onClose={() => setFormOpen(false)} onSave={save}/>}</AnimatePresence>
    </div>
  )
}

function ProjectCard({ project, onOpen, onStart, onEdit, onDelete, deleting }) {
  const [confirmDelete, setConfirmDelete] = useState(false)
  const archived = project.status === 'ARCHIVED'
  const dbType = project.dbType === 'azure_sql' ? 'Azure SQL DB' : project.dbType
  const source = project.connectionType === 'database' ? `Database${dbType ? ` / ${dbType}` : ''}` : `Data Lake${project.integrationType ? ` / ${project.integrationType}` : ''}`
  const updated = project.updatedAt && !Number.isNaN(new Date(project.updatedAt).getTime()) ? new Intl.DateTimeFormat('en-GB').format(new Date(project.updatedAt)) : '--'
  const owner = project.owner || project.ownerEmail
  return <div role="button" tabIndex={0} onClick={onOpen} onKeyDown={event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); onOpen() } }} className="group relative h-full cursor-pointer rounded-xl pt-3 outline-none focus-visible:ring-2 focus-visible:ring-accent-blue/60">
    <span className="pointer-events-none absolute left-0 top-0 h-5 w-24 rounded-t-lg border border-b-0 border-bg-border bg-bg-card transition-colors group-hover:border-accent-blue/40"/>
    <article className="card relative flex h-full min-h-[250px] flex-col rounded-tl-none p-4 transition-[border-color,background-color,transform,box-shadow] duration-200 group-hover:-translate-y-0.5 group-hover:border-accent-blue/40 group-hover:bg-bg-hover/30 group-hover:shadow-card">
      <div className="mb-3 flex items-start justify-between gap-3"><span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg border border-accent-blue/20 bg-accent-blue/10"><Folder size={19} className="fill-accent-blue/10 text-accent-blue"/></span><span className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold ${archived ? 'border-bg-border bg-bg-base text-text-tertiary' : 'border-accent-green/20 bg-accent-green/10 text-accent-green'}`}>{archived ? 'Archived' : 'Active'}</span></div>
      <div className="min-w-0"><h2 className="truncate text-base font-semibold text-text-primary" title={project.name}>{project.name}</h2><p className="mt-1 line-clamp-2 min-h-[2.5rem] text-xs leading-5 text-text-secondary">{project.description || 'No project description'}</p></div>
      <dl className="mt-3 space-y-1.5 rounded-lg border border-bg-border bg-bg-base p-2.5 text-[11px] text-text-tertiary">
        <div className="flex items-center justify-between gap-3"><dt className="text-text-muted">Target</dt><dd className="truncate font-medium text-text-secondary">{project.target || '--'}</dd></div>
        <div className="flex items-center justify-between gap-3"><dt className="text-text-muted">Source</dt><dd className="truncate font-medium text-text-secondary">{source}</dd></div>
        {owner && <div className="flex items-center justify-between gap-3"><dt className="text-text-muted">Owner</dt><dd className="truncate font-medium text-text-secondary" title={owner}>{owner}</dd></div>}
      </dl>
      <div className="mt-auto pt-3"><p className="mb-2 text-[10px] text-text-muted">Updated {updated}</p><div className="flex items-center gap-2 border-t border-bg-border pt-3">
        <button type="button" disabled={archived} onClick={event => { event.stopPropagation(); onStart() }} className="btn-primary flex h-9 flex-1 items-center justify-center gap-2 px-3 py-2 text-xs disabled:cursor-not-allowed disabled:opacity-50"><Play size={12}/>Start Run</button>
        {confirmDelete ? <><button type="button" disabled={deleting} onClick={event => { event.stopPropagation(); onDelete() }} className="inline-flex h-9 items-center justify-center rounded-lg border border-accent-red/30 bg-accent-red/10 px-2.5 text-xs font-semibold text-accent-red disabled:opacity-50">Yes</button><button type="button" onClick={event => { event.stopPropagation(); setConfirmDelete(false) }} className="inline-flex h-9 items-center justify-center rounded-lg border border-bg-border bg-bg-base px-2.5 text-xs font-semibold text-text-secondary">No</button></> : <><button type="button" onClick={event => { event.stopPropagation(); onEdit() }} className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-bg-border bg-bg-base text-text-secondary hover:border-accent-blue/40 hover:text-accent-blue" aria-label={`Edit ${project.name}`}><Edit2 size={13}/></button><button type="button" onClick={event => { event.stopPropagation(); setConfirmDelete(true) }} className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-bg-border bg-bg-base text-text-secondary hover:border-accent-red/40 hover:text-accent-red" aria-label={`Delete ${project.name}`}><Trash2 size={13}/></button></>}
      </div></div>
    </article>
  </div>
}

function NewProjectCard({ onClick }) {
  return <button type="button" onClick={onClick} className="group relative h-full min-h-[250px] cursor-pointer rounded-xl pt-3 text-left outline-none focus-visible:ring-2 focus-visible:ring-accent-blue/60">
    <span className="pointer-events-none absolute left-0 top-0 h-5 w-24 rounded-t-lg border border-b-0 border-dashed border-bg-border bg-bg-card transition-colors group-hover:border-accent-blue/60"/>
    <span className="card relative flex h-full min-h-[250px] flex-col items-center justify-center rounded-tl-none border-dashed p-6 text-center transition-[border-color,background-color,transform,box-shadow] duration-200 group-hover:-translate-y-0.5 group-hover:border-accent-blue/60 group-hover:bg-bg-hover/30 group-hover:shadow-card"><span className="flex h-14 w-14 items-center justify-center rounded-xl border border-accent-blue/25 bg-accent-blue/10 text-accent-blue transition-transform duration-200 group-hover:scale-105"><Plus size={24}/></span><span className="mt-4 text-base font-semibold text-text-primary">New Project</span><span className="mt-1 max-w-[210px] text-xs leading-5 text-text-tertiary">Create another governed pipeline project.</span></span>
  </button>
}

export function ProjectForm({ initial, connections, connectionsLoading, busy, onClose, onSave }) {
  const [form, setForm] = useState({...EMPTY, ...initial})
  const databaseConnections = connections.filter(c => c.sourceType !== 'data_lake')
  const lakeConnections = connections.filter(c => c.sourceType === 'data_lake')
  const set = (key, value) => setForm(current => ({...current, [key]: value}))
  const valid = form.name.trim() && form.description.trim() && form.connectionType && (form.connectionType === 'database' ? form.dbType && form.databaseName : form.integrationType)
  return <motion.div initial={{opacity:0}} animate={{opacity:1}} exit={{opacity:0}} onClick={onClose} className="fixed inset-0 z-[100] flex justify-end bg-black/60 backdrop-blur-sm">
    <motion.aside initial={{x:440}} animate={{x:0}} exit={{x:440}} onClick={e => e.stopPropagation()} className="flex h-full w-full max-w-md flex-col border-l border-bg-border bg-bg-card shadow-2xl">
      <header className="flex items-center justify-between border-b border-bg-border p-5"><div><h2 className="text-base font-bold">{initial ? 'Edit Project' : 'New Project'}</h2><p className="mt-1 text-xs text-text-tertiary">Project settings are reused for every run.</p></div><button onClick={onClose}><X size={17}/></button></header>
      <div className="flex-1 space-y-4 overflow-y-auto p-5">
        <Field label="Project Name"><input className="input-field" value={form.name} onChange={e => set('name', e.target.value)} placeholder="Claims modernization"/></Field>
        <Field label="Description"><textarea className="input-field min-h-24 resize-none" value={form.description} onChange={e => set('description', e.target.value)} placeholder="Describe the project scope"/></Field>
        <Field label="Target"><select className="input-field" value={form.target} onChange={e => set('target', e.target.value)}><option>Databricks</option><option>Snowflake</option><option disabled>Fabric</option></select></Field>
        <div className="border-t border-bg-border pt-4"><h3 className="text-sm font-semibold">Source Data Configuration</h3><p className="mt-1 text-xs text-text-tertiary">This read-only source context will be inherited by runs.</p></div>
        {connectionsLoading ? <p className="text-xs text-text-tertiary">Loading configurations...</p> : <>
          <Field label="Source Type"><select className="input-field" value={form.connectionType} onChange={e => {setForm({...form, connectionType:e.target.value, connectionName:'', dbType:'', databaseName:'', integrationType:'', dataLakeName:''})}}><option value="">Select source type...</option><option value="database">Database</option><option value="data_lake">Data Lake</option></select></Field>
          {form.connectionType === 'database' && <><Field label="Database Type"><select className="input-field" value={form.dbType} onChange={e => {const c=databaseConnections.find(x=>x.dbType===e.target.value); setForm({...form,dbType:e.target.value,connectionName:String(c?.id||''),databaseName:c?.databaseName||''})}}><option value="">Select database type...</option>{[...new Set(databaseConnections.map(c=>c.dbType))].map(value=><option key={value} value={value}>{value === 'azure_sql' ? 'Azure SQL DB' : value}</option>)}</select></Field><Field label="Database Name"><select className="input-field" value={form.connectionName} onChange={e => {const c=databaseConnections.find(x=>String(x.id)===e.target.value); setForm({...form,connectionName:e.target.value,databaseName:c?.databaseName||''})}}><option value="">Select database...</option>{databaseConnections.filter(c=>c.dbType===form.dbType).map(c=><option key={c.id} value={String(c.id)}>{c.databaseName||c.name}</option>)}</select></Field>
          <label className="flex items-center gap-3 rounded-lg border border-bg-border bg-bg-base p-3 text-sm"><input type="checkbox" checked={form.useDomainKB} onChange={e=>set('useDomainKB',e.target.checked)}/><BookOpen size={15}/>Use Domain Knowledge Base</label>{form.useDomainKB && <Field label="Domain Profile"><select className="input-field" value={form.domainProfile} onChange={e=>setForm({...form,domainProfile:e.target.value,knowledgeBaseId:KB[e.target.value]||''})}><option value="">Select domain...</option>{Object.keys(KB).map(k=><option key={k}>{k}</option>)}</select></Field>}</>}
          {form.connectionType === 'data_lake' && <><Field label="Ingestion Type"><select className="input-field" value={form.integrationType} onChange={e=>{const c=lakeConnections.find(x=>x.integrationType===e.target.value); setForm({...form,integrationType:e.target.value,connectionName:String(c?.id||''),dataLakeType:c?.dataLakeSourceType||'',dataLakeName:c?.name||''})}}><option value="">Select ingestion...</option><option>SFTP</option><option>API</option></select></Field>{form.integrationType==='SFTP' && <Field label="Data Lake Name"><select className="input-field" value={form.connectionName} onChange={e=>{const c=lakeConnections.find(x=>String(x.id)===e.target.value);setForm({...form,connectionName:e.target.value,dataLakeName:c?.name||'',dataLakeType:c?.dataLakeSourceType||''})}}><option value="">Select data lake...</option>{lakeConnections.map(c=><option key={c.id} value={String(c.id)}>{c.name}</option>)}</select></Field>}</>}
        </>}
      </div>
      <footer className="space-y-3 border-t border-bg-border p-5"><div className="flex gap-3"><button className="btn-secondary h-11 flex-1" onClick={onClose}>Cancel</button><button className="btn-primary flex h-11 flex-1 items-center justify-center gap-2" disabled={!valid||busy} onClick={()=>onSave(form,false)}><Save size={14}/>Save</button></div>{!initial && <button className="btn-primary flex h-11 w-full items-center justify-center gap-2" disabled={!valid||busy} onClick={()=>onSave(form,true)}><Play size={14}/>Save & Start Run</button>}</footer>
    </motion.aside>
  </motion.div>
}

function Field({label,children}) { return <div><label className="label">{label} <span className="text-red-400">*</span></label>{children}</div> }
