import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { dbConfigService, DbConnection } from '../services/dbConfigService'

export const DB_CONFIGS_KEY = ['db-configurations']

export const useDbConfigurations = () =>
  useQuery({
    queryKey: DB_CONFIGS_KEY,
    queryFn: dbConfigService.getAll,
  })

export const useCreateDbConfig = () => {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (data: DbConnection) => dbConfigService.create(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: DB_CONFIGS_KEY })
    },
  })
}

export const useUpdateDbConfig = () => {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, data }: { id: string | number; data: DbConnection }) =>
      dbConfigService.update(id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: DB_CONFIGS_KEY })
    },
  })
}

export const useDeleteDbConfig = () => {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: string | number) => dbConfigService.remove(id),
    onSuccess: (_, id) => {
      queryClient.setQueryData<DbConnection[]>(DB_CONFIGS_KEY, (prev = []) =>
        prev.filter((c) => c.id !== id)
      )
    },
  })
}

export const useTestDbConnection = () =>
  useMutation({
    mutationFn: (data: DbConnection) => dbConfigService.testConnection(data),
  })
