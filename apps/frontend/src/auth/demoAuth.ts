export function isDemoAuthEnabled() {
  const mode = String(process.env.REACT_APP_AUTH_MODE || '').trim().toLowerCase()
  if (mode) return mode === 'demo'
  // Keep the old flag working while deployments migrate to REACT_APP_AUTH_MODE.
  return String(process.env.REACT_APP_AUTH_DEMO_MODE || '').trim().toLowerCase() === 'true'
}
