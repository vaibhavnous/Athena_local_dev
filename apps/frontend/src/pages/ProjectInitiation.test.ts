jest.mock('react-router-dom', () => ({ useNavigate: () => jest.fn() }))

import { getProjectFormErrors, isProjectFormValid } from './ProjectInitiation'

const validProject = {
  name: 'Claims',
  description: 'Claims pipeline',
  target: 'Databricks',
  connectionType: 'database',
  dbType: 'azure_sql',
  connectionName: 'connection-1',
  databaseName: 'insurance',
  integrationType: '',
  useDomainKB: false,
}

test('requires every visible project field before save actions are enabled', () => {
  expect(isProjectFormValid(validProject)).toBe(true)
  expect(getProjectFormErrors({ ...validProject, description: '' })).toEqual({
    description: 'Description is required.',
  })
})

test('requires the conditional SFTP and knowledge-base selections', () => {
  const errors: any = getProjectFormErrors({
    ...validProject,
    connectionType: 'data_lake',
    integrationType: 'SFTP',
    connectionName: '',
    useDomainKB: true,
    domainProfile: '',
    knowledgeBaseId: '',
  })

  expect(errors.dataLakeName).toBe('Data lake name is required.')
  expect(errors.domainProfile).toMatch(/Domain profile is required/)
})
