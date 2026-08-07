jest.mock('react-router-dom', () => ({
  useNavigate: () => jest.fn(),
}), { virtual: true })

import { buildInitialForm } from './NewRunModal'
import { isProjectFormValid } from '../../pages/ProjectInitiation'

test('loads the saved ADLS project connection instead of showing the transactions default', () => {
  const form = buildInitialForm(
    {},
    null,
    {
      name: 'Insurance',
      description: 'Insurance feeds',
      connectionType: 'data_lake',
      integrationType: 'SFTP',
      dataLakeType: 'ADLS',
      dataLakeName: 'Insurance ADLS Gen2',
      target: 'Databricks',
    },
  )

  expect(form).toMatchObject({
    source: 'adls_gen2',
    sftpEntity: 'auto',
    integrationType: 'SFTP',
    dataLakeType: 'ADLS',
    dataLakeName: 'Insurance ADLS Gen2',
  })
})

test('requires an SFTP data lake selection before a project can be saved', () => {
  const project = {
    name: 'Insurance',
    description: 'Insurance feeds',
    connectionType: 'data_lake',
    integrationType: 'SFTP',
    connectionName: '',
  }

  expect(isProjectFormValid(project)).toBe(false)
  expect(isProjectFormValid({ ...project, connectionName: 'sftp_adls_insurance' })).toBe(true)
})

test('restores target metadata selection when restarting a database run', () => {
  const form = buildInitialForm(
    {},
    {
      source: 'database',
      target_warehouse: 'databricks',
      target_environment: 'qa',
      source_system_id: '7499026347042686646',
      source_connection_id: '3358264270364792816',
    },
    null,
  )

  expect(form).toMatchObject({
    targetEnvironment: 'qa',
    sourceSystemId: '7499026347042686646',
    sourceConnectionId: '3358264270364792816',
  })
})
