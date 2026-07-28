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
  executionEngine?: 'native' | 'dbt'
  dbtDeploymentMode?: 'generate_only' | 'generate_and_deploy'
  dbtTargetName?: string
  dbtThreads?: number
  dbtCommandTimeoutSecs?: number
  forceDbtDeploy?: boolean
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
  executionEngine: raw.execution_engine || 'native',
  dbtDeploymentMode: raw.dbt_deployment_mode || 'generate_only',
  dbtTargetName: raw.dbt_target_name,
  dbtThreads: raw.dbt_threads,
  dbtCommandTimeoutSecs: raw.dbt_command_timeout_secs,
  forceDbtDeploy: !!raw.force_dbt_deploy,
  createdAt: raw.created_at,
  updatedAt: raw.updated_at,
})

const toApi = (project: ProjectInput) => {
  const snowflakeDatabaseTarget =
    String(project.target || '').toLowerCase() === 'snowflake' &&
    project.connectionType === 'database'
  const executionEngine = snowflakeDatabaseTarget && project.executionEngine === 'dbt' ? 'dbt' : 'native'
  return ({
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
  execution_engine: executionEngine,
  dbt_deployment_mode: executionEngine === 'dbt' ? 'generate_and_deploy' : 'generate_only',
  dbt_target_name: project.dbtTargetName,
  dbt_threads: project.dbtThreads,
  dbt_command_timeout_secs: project.dbtCommandTimeoutSecs,
  force_dbt_deploy: false,
  })
}

export const projectService = {
  getAll: async () => ((await (getProjects() as unknown as Promise<any[]>))).map(fromApi),
  getOne: async (id: string) => fromApi(await (getProject(id) as unknown as Promise<any>)),
  create: async (project: ProjectInput) => fromApi(await (createProject(toApi(project)) as unknown as Promise<any>)),
  update: async (id: string, project: ProjectInput) => fromApi(await (updateProject(id, toApi(project)) as unknown as Promise<any>)),
  remove: (id: string) => deleteProject(id) as unknown as Promise<void>,
}
