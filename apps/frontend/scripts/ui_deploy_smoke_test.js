const { spawnSync } = require('child_process')
const { existsSync, readFileSync, readdirSync } = require('fs')
const { join } = require('path')
const https = require('https')

const frontendDir = join(__dirname, '..')
const args = process.argv.slice(2)

const parsed = args.reduce((acc, arg) => {
  if (arg.startsWith('--api-base-url=')) {
    acc.apiBaseUrl = arg.split('=')[1]
  }
  if (arg.startsWith('--deployed-url=')) {
    acc.deployedUrl = arg.split('=')[1]
  }
  return acc
}, {
  apiBaseUrl: process.env.REACT_APP_API_BASE_URL || 'http://127.0.0.1:8000',
  deployedUrl: process.env.FRONTEND_DEPLOY_URL || null,
})

function runCommand(command, args, options = {}) {
  const result = spawnSync(command, args, {
    stdio: 'inherit',
    shell: process.platform === 'win32',
    ...options,
  })

  if (result.error) {
    throw result.error
  }

  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} exited with code ${result.status}`)
  }
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message)
  }
}

function checkUrl(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        const statusCode = res.statusCode
        let body = ''
        res.on('data', (chunk) => {
          body += chunk
        })
        res.on('end', () => {
          resolve({ statusCode, body })
        })
      })
      .on('error', reject)
  })
}

console.log('=== UI Deployment Smoke Test ===')
console.log(`Frontend directory: ${frontendDir}`)
console.log(`API base URL used for build: ${parsed.apiBaseUrl}`)

assert(existsSync(join(frontendDir, 'package.json')), 'package.json not found in frontend directory')

console.log('Installing frontend dependencies...')
runCommand('npm', ['ci'], { cwd: frontendDir, env: { ...process.env, CI: 'true' } })

console.log('Building frontend with REACT_APP_API_BASE_URL...')
runCommand('npm', ['run', 'build'], {
  cwd: frontendDir,
  env: {
    ...process.env,
    CI: 'true',
    REACT_APP_API_BASE_URL: parsed.apiBaseUrl,
  },
})

const buildDir = join(frontendDir, 'build')
assert(existsSync(buildDir), 'Build output directory not found')

const jsDir = join(buildDir, 'static', 'js')
assert(existsSync(jsDir), 'Built JS directory not found; expected build/static/js')

const jsFiles = readdirSync(jsDir).filter((file) => file.endsWith('.js'))
assert(jsFiles.length > 0, 'No built JS files found in build/static/js')

let foundTargetUrl = false
let foundBadEnv = false

for (const file of jsFiles) {
  const content = readFileSync(join(jsDir, file), 'utf8')
  if (content.includes(parsed.apiBaseUrl)) {
    foundTargetUrl = true
  }
  if (content.includes('REACT_APP_API_ENDPOINT')) {
    foundBadEnv = true
  }
}

assert(foundTargetUrl, `Built output does not contain the injected API base URL: ${parsed.apiBaseUrl}`)
assert(!foundBadEnv, 'Built output contains REACT_APP_API_ENDPOINT; frontend code should use REACT_APP_API_BASE_URL')

console.log('Verifying source code references...')
const sourceFile = readFileSync(join(frontendDir, 'src', 'api', 'athenaApi.ts'), 'utf8')
assert(sourceFile.includes('REACT_APP_API_BASE_URL'), 'Source code does not reference REACT_APP_API_BASE_URL')
assert(!sourceFile.includes('REACT_APP_API_ENDPOINT'), 'Source code still references REACT_APP_API_ENDPOINT')

if (parsed.deployedUrl) {
  console.log(`Checking deployed frontend URL: ${parsed.deployedUrl}`)
  checkUrl(parsed.deployedUrl)
    .then(({ statusCode, body }) => {
      assert(statusCode === 200, `Deployed frontend returned ${statusCode}`)
      assert(body.includes('<div id="root">') || body.includes('React'), 'Deployed frontend HTML does not look like a React app')
      console.log('Deployed frontend returned 200 and appears to be a React app')
      console.log('UI Deployment Smoke Test Passed — build succeeded and deployed frontend is reachable')
      process.exit(0)
    })
    .catch((err) => {
      console.error('Failed to validate deployed frontend URL:', err.message)
      process.exit(2)
    })
} else {
  console.log('No deployed frontend URL provided; skipping live deployment verification')
  console.log('UI Deployment Smoke Test Passed — build succeeded and the frontend is wired to REACT_APP_API_BASE_URL')
  process.exit(0)
}
