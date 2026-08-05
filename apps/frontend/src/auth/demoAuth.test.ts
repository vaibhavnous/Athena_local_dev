import { isDemoAuthEnabled } from './demoAuth'

afterEach(() => {
  delete process.env.REACT_APP_AUTH_MODE
  delete process.env.REACT_APP_AUTH_DEMO_MODE
})

test('enables automatic backend demo sessions only in demo mode', () => {
  expect(isDemoAuthEnabled()).toBe(false)

  process.env.REACT_APP_AUTH_MODE = 'demo'
  expect(isDemoAuthEnabled()).toBe(true)

  process.env.REACT_APP_AUTH_MODE = 'required'
  expect(isDemoAuthEnabled()).toBe(false)
})

test('accepts the legacy demo flag only when the new mode is absent', () => {
  process.env.REACT_APP_AUTH_DEMO_MODE = 'true'
  expect(isDemoAuthEnabled()).toBe(true)

  process.env.REACT_APP_AUTH_MODE = 'required'
  expect(isDemoAuthEnabled()).toBe(false)
})
