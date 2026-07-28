import {
  getConfigurations,
  createConfiguration,
  updateConfiguration,
  deleteConfiguration,
  testConnection,
} from '../api/athenaApi'

export interface DbConnection {
  id?: string | number | null
  name: string
  sourceType: string
  dbType: string
  jdbcUrl: string
  driverClass: string
  username: string
  password: string
  host: string
  port: string
  databaseName: string
  schema: string
  integrationType: string
  dataLakeSourceType: string
  basePath: string
  directoryName: string
  secret: string
  baseUrl: string
  apiKey: string
}

// Convert snake_case API response → camelCase frontend model
function fromApi(raw: any): DbConnection {
  return {
    id: raw.id,
    name: raw.name ?? raw.data_lake_name ?? raw.dataLakeName ?? '',
    sourceType: raw.source_type ?? raw.sourceType ?? 'database',
    dbType: raw.db_type ?? raw.dbType ?? 'custom',
    jdbcUrl: raw.jdbc_url ?? raw.jdbcUrl ?? '',
    driverClass: raw.driver_class ?? raw.driverClass ?? '',
    username: raw.username ?? '',
    password: raw.password ?? '',
    host: raw.host ?? '',
    port: raw.port ?? '',
    databaseName: raw.database_name ?? raw.databaseName ?? '',
    schema: raw.schema ?? '',
    integrationType:
      raw.integration_type ??
      raw.integrationType ??
      raw.data_lake_integration_type ??
      raw.dataLakeIntegrationType ??
      'SFTP',
    dataLakeSourceType:
      raw.data_lake_source_type ??
      raw.dataLakeSourceType ??
      raw.lake_source_type ??
      raw.lakeSourceType ??
      'ADLS',
    basePath: raw.base_path ?? raw.basePath ?? '',
    directoryName: raw.directory_name ?? raw.directoryName ?? '',
    secret: raw.secret ?? '',
    baseUrl: raw.base_url ?? raw.baseUrl ?? '',
    apiKey: raw.api_key ?? raw.apiKey ?? '',
  }
}

// Convert camelCase frontend model → API request body
// Data Lake and database connections intentionally use separate payload shapes.
function toApi(data: DbConnection): object {
  if (data.sourceType === 'data_lake') {
    if (data.integrationType === 'API') {
      return {
        name: data.name,
        sourceType: data.sourceType,
        source_type: data.sourceType,
        integrationType: data.integrationType,
        integration_type: data.integrationType,
        baseUrl: data.baseUrl,
        base_url: data.baseUrl,
        ...(data.apiKey ? { apiKey: data.apiKey, api_key: data.apiKey } : {}),
      }
    }

    return {

      name: data.name,
      integrationType: data.integrationType || 'SFTP',
      integration_type: data.integrationType || 'SFTP',
      dataLakeSourceType: data.dataLakeSourceType,
      data_lake_source_type: data.dataLakeSourceType,
      basePath: data.basePath,
      base_path: data.basePath,
      directoryName: data.directoryName,
      directory_name: data.directoryName,
      sourceType: data.sourceType,
      source_type: data.sourceType,
      ...(data.secret ? { secret: data.secret } : {}),
    }
  }

  return {
    id: data.id,
    name: data.name,
    // snake_case
    source_type: data.sourceType,
    db_type: data.dbType,
    jdbc_url: data.jdbcUrl,
    driver_class: data.driverClass,
    database_name: data.databaseName,
    // camelCase (keep both in case the backend expects camelCase on writes)
    sourceType: data.sourceType,
    dbType: data.dbType,
    jdbcUrl: data.jdbcUrl,
    driverClass: data.driverClass,
    databaseName: data.databaseName,
    // shared keys
    username: data.username,
    password: data.password,
    host: data.host,
    port: data.port,
    schema: data.schema,
  }
}

export const dbConfigService = {
  getAll: async (): Promise<DbConnection[]> => {
    const raw = await (getConfigurations() as unknown as Promise<any[]>)
    return Array.isArray(raw) ? raw.map(fromApi) : []
  },

  create: async (data: DbConnection): Promise<DbConnection> => {
    const raw = await (createConfiguration(toApi(data)) as unknown as Promise<any>)
    return fromApi(raw)
  },

  update: async (id: string | number, data: DbConnection): Promise<DbConnection> => {
    const raw = await (updateConfiguration(id, toApi(data)) as unknown as Promise<any>)
    return fromApi(raw)
  },

  remove: (id: string | number) => deleteConfiguration(id) as unknown as Promise<void>,

  testConnection: (data: DbConnection) =>
    testConnection(toApi(data)) as unknown as Promise<{ success: boolean; message?: string }>,
}
