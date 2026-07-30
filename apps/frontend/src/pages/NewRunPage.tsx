// @ts-nocheck
import React from 'react'
import { Navigate, useNavigate, useParams } from 'react-router-dom'
import { AlertCircle, Loader2, RefreshCw } from 'lucide-react'
import NewRunModal from '../components/shared/NewRunModal'
import { useProject } from '../hooks/useProjects'

function NewRunPage() {
  const navigate = useNavigate()
  const { projectId } = useParams()
  const { data: project, isLoading, isError, error, refetch, isFetching } = useProject(projectId)

  if (!projectId) return <Navigate to="/app/project" replace />
  if (isLoading) {
    return (
      <div className="flex h-full items-center justify-center gap-2 text-sm text-text-tertiary">
        <Loader2 size={16} className="animate-spin" />
        Loading project...
      </div>
    )
  }
  if (isError && !project) {
    return (
      <div className="flex h-full items-center justify-center p-6">
        <div className="w-full max-w-md rounded-lg border border-amber-500/30 bg-amber-500/5 p-5 text-center">
          <AlertCircle size={22} className="mx-auto text-amber-400" />
          <h2 className="mt-3 text-sm font-semibold text-white">Project is taking longer than expected</h2>
          <p className="mt-1 text-xs text-text-tertiary">
            {error?.message || 'The project details could not be loaded.'}
          </p>
          <div className="mt-4 flex justify-center gap-2">
            <button type="button" className="btn-secondary" onClick={() => navigate('/app/project')}>
              Back to Projects
            </button>
            <button type="button" className="btn-primary inline-flex items-center gap-2" onClick={() => refetch()} disabled={isFetching}>
              {isFetching ? <Loader2 size={13} className="animate-spin" /> : <RefreshCw size={13} />}
              Retry
            </button>
          </div>
        </div>
      </div>
    )
  }
  if (!project) return <Navigate to="/app/project" replace />

  return (
    <div className="h-full min-h-0 overflow-hidden">
      <NewRunModal
        isOpen
        pageMode
        project={project}
        onClose={() => navigate('/app/project')}
      />
    </div>
  )
}

export default NewRunPage
