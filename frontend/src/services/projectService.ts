import { createProject, deleteProject, getProject, getProjects, updateProject } from '../api/athenaApi'

export interface AthenaProject {
  id: string
  name: string
  description: string
  target: 'Databricks' | 'Snowflake' | 'Fabric'
  status: 'ACTIVE' | 'ARCHIVED'
  ownerEmail?: string
  connectionType: 'database' | 'data_lake'
  connectionName?: string
  dbType?: string
  databaseName?: string
  integrationType?: string
  dataLakeType?: string
  dataLakeName?: string
  useDomainKB?: boolean
  domainProfile?: string
  knowledgeBaseId?: string
  createdAt?: string
  updatedAt?: string
}

export type ProjectInput = Omit<AthenaProject, 'id' | 'ownerEmail' | 'createdAt' | 'updatedAt'>

const fromApi = (raw: any): AthenaProject => ({
  id: String(raw.id),
  name: raw.name,
  description: raw.description,
  target: raw.target,
  status: raw.status,
  ownerEmail: raw.owner_email,
  connectionType: raw.connection_type,
  connectionName: raw.connection_name,
  dbType: raw.db_type,
  databaseName: raw.database_name,
  integrationType: raw.integration_type,
  dataLakeType: raw.data_lake_type,
  dataLakeName: raw.data_lake_name,
  useDomainKB: !!raw.use_domain_knowledge_base,
  domainProfile: raw.domain_profile,
  knowledgeBaseId: raw.knowledge_base_id,
  createdAt: raw.created_at,
  updatedAt: raw.updated_at,
})

const toApi = (project: ProjectInput) => ({
  name: project.name,
  description: project.description,
  target: project.target,
  status: project.status,
  connection_type: project.connectionType,
  connection_name: project.connectionName,
  db_type: project.dbType,
  database_name: project.databaseName,
  integration_type: project.integrationType,
  data_lake_type: project.dataLakeType,
  data_lake_name: project.dataLakeName,
  use_domain_knowledge_base: project.useDomainKB,
  domain_profile: project.domainProfile,
  knowledge_base_id: project.knowledgeBaseId,
})

export const projectService = {
  getAll: async () => ((await (getProjects() as unknown as Promise<any[]>))).map(fromApi),
  getOne: async (id: string) => fromApi(await (getProject(id) as unknown as Promise<any>)),
  create: async (project: ProjectInput) => fromApi(await (createProject(toApi(project)) as unknown as Promise<any>)),
  update: async (id: string, project: ProjectInput) => fromApi(await (updateProject(id, toApi(project)) as unknown as Promise<any>)),
  remove: (id: string) => deleteProject(id) as unknown as Promise<void>,
}
