import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { projectService, ProjectInput } from '../services/projectService'

export const PROJECTS_KEY = ['projects']
export const useProjects = () => useQuery({ queryKey: PROJECTS_KEY, queryFn: projectService.getAll })
export const useProject = (id?: string) => useQuery({
  queryKey: [...PROJECTS_KEY, id],
  queryFn: () => projectService.getOne(id!),
  enabled: !!id,
})

export const useCreateProject = () => {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (data: ProjectInput) => projectService.create(data),
    onSuccess: () => client.invalidateQueries({ queryKey: PROJECTS_KEY }),
  })
}

export const useUpdateProject = () => {
  const client = useQueryClient()
  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: ProjectInput }) => projectService.update(id, data),
    onSuccess: () => client.invalidateQueries({ queryKey: PROJECTS_KEY }),
  })
}

export const useDeleteProject = () => {
  const client = useQueryClient()
  return useMutation({
    mutationFn: projectService.remove,
    onSuccess: () => client.invalidateQueries({ queryKey: PROJECTS_KEY }),
  })
}
