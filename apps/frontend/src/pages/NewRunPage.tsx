// @ts-nocheck
import React from 'react'
import { Navigate, useNavigate, useParams } from 'react-router-dom'
import NewRunModal from '../components/shared/NewRunModal'
import { useProject } from '../hooks/useProjects'

function NewRunPage() {
  const navigate = useNavigate()
  const { projectId } = useParams()
  const { data: project, isLoading } = useProject(projectId)

  if (!projectId) return <Navigate to="/app/project" replace />
  if (isLoading) return <div className="flex h-full items-center justify-center text-sm text-text-tertiary">Loading project...</div>
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
