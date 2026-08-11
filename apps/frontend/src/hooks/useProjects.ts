import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { projectService, ProjectInput } from '../services/projectService'

export const PROJECTS_KEY = ['projects']
export const useProjects = () => useQuery({ queryKey: PROJECTS_KEY, queryFn: projectService.getAll })
export const useProject = (id?: string) => {
  const client = useQueryClient()
  return useQuery({
    queryKey: [...PROJECTS_KEY, id],
    queryFn: () => projectService.getOne(id!),
    enabled: !!id,
    retry: false,
    initialData: () => {
      const cachedProjects = client.getQueryData(PROJECTS_KEY) as any[] | undefined
      return cachedProjects?.find((project) => String(project.id) === String(id))
    },
    initialDataUpdatedAt: () => client.getQueryState(PROJECTS_KEY)?.dataUpdatedAt,
  })
}

export const useProjectRuns = (id?: string) => useQuery({
  queryKey: [...PROJECTS_KEY, id, 'runs'],
  queryFn: () => projectService.getRuns(id!),
  enabled: !!id,
  retry: false,
  staleTime: 30 * 1000,
})

export const useCreateProject = () => {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (data: ProjectInput) => projectService.create(data),
    onSuccess: (project) => {
      client.setQueryData([...PROJECTS_KEY, project.id], project)
      client.setQueryData(PROJECTS_KEY, (current: any[] | undefined) => (
        current ? [project, ...current.filter((item) => item.id !== project.id)] : [project]
      ))
      client.invalidateQueries({ queryKey: PROJECTS_KEY })
    },
  })
}

export const useUpdateProject = () => {
  const client = useQueryClient()
  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: ProjectInput }) => projectService.update(id, data),
    onSuccess: (project) => {
      client.setQueryData([...PROJECTS_KEY, project.id], project)
      client.setQueryData(PROJECTS_KEY, (current: any[] | undefined) => (
        current?.map((item) => item.id === project.id ? project : item) || [project]
      ))
      client.invalidateQueries({ queryKey: PROJECTS_KEY })
    },
  })
}

export const useDeleteProject = () => {
  const client = useQueryClient()
  return useMutation({
    mutationFn: projectService.remove,
    onSuccess: () => client.invalidateQueries({ queryKey: PROJECTS_KEY }),
  })
}
