import React, { useEffect, useRef, useState } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { motion } from 'framer-motion'
import { Mail, Lock, Eye, EyeOff, ArrowLeft } from 'lucide-react'
import { useAuth } from '../context/AuthContext'

const SINGLE_FLOW_DURATION_MS = 2800
const PAGE_SHIFT_DURATION_MS = 420
function LoginPage() {
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  const { login } = useAuth()

  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [errorMessage, setErrorMessage] = useState('')
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [isLeaving, setIsLeaving] = useState(false)
  const [showPassword, setShowPassword] = useState(false)

  const completeLogin = async (loginStartedAt: number) => {
    const elapsed = Date.now() - loginStartedAt
    const remainingDelay = Math.max(0, SINGLE_FLOW_DURATION_MS - elapsed)
    if (remainingDelay > 0) {
      await new Promise((resolve) => window.setTimeout(resolve, remainingDelay))
    }

    setIsLeaving(true)
    await new Promise((resolve) => window.setTimeout(resolve, PAGE_SHIFT_DURATION_MS))
    const requestedPath = searchParams.get('next')
    navigate(requestedPath?.startsWith('/app') ? requestedPath : '/app', { replace: true })
  }

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault()
    setErrorMessage('')
    setIsSubmitting(true)
    const loginStartedAt = Date.now()

    try {
      await login(email, password)
      await completeLogin(loginStartedAt)
    } catch (error: any) {
      setErrorMessage(
        error?.message === 'Network Error'
          ? 'Unable to reach the login API. Please confirm the backend is running.'
          : error?.message ?? 'Login failed. Please try again.'
      )
    } finally {
      setIsSubmitting(false)
    }
  }

  return (
    <motion.div
      className={`relative min-h-screen overflow-x-hidden bg-[#050914] px-4 py-6 transition-all duration-500 ease-out ${
        isLeaving ? 'scale-[0.99] opacity-0 blur-sm' : ''
      }`}
      initial={{ opacity: 0, y: 14, scale: 0.995 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      transition={{ duration: 0.45, ease: 'easeOut' }}
    >
      <LoginBackgroundCanvas />
      <div className="pointer-events-none absolute inset-0 bg-[linear-gradient(180deg,rgba(7,9,16,0.10)_0%,rgba(7,9,16,0.24)_56%,rgba(7,9,16,0.16)_100%)]" />

      <div className="relative z-10 flex min-h-[calc(100vh-48px)] items-start justify-center overflow-y-auto py-4 sm:items-center">
        <motion.div
          className="relative w-full max-w-md rounded-3xl border border-blue-300/15 bg-slate-950/55 shadow-[0_28px_90px_rgba(2,6,23,0.62)] backdrop-blur-2xl"
          initial={{ opacity: 0, y: 28, scale: 0.97 }}
          animate={{ opacity: 1, y: 0, scale: 1 }}
          transition={{ duration: 0.58, ease: 'easeOut' }}
        >
        <div className="p-6 sm:p-7">
          <button
            type="button"
            onClick={() => navigate('/')}
            className="absolute left-6 top-6 inline-flex h-10 w-10 items-center justify-center rounded-full border border-blue-400/20 bg-slate-950/45 text-blue-100 transition hover:border-blue-300/45 hover:bg-blue-500/10"
            aria-label="Back"
          >
            <ArrowLeft size={18} strokeWidth={2.2} />
          </button>

          {/* Logo */}
          <div className="flex flex-col items-center mb-5 pt-2">
            <img
              src="/astra-wordmark-white.png"
              alt="Astra"
              className="mb-3 h-auto w-56 max-w-[70%] object-contain"
            />

            <img
              src="/data-wordmark-white.png"
              alt="Data"
              className="mb-2 h-auto w-48 max-w-[60%] translate-x-3 object-contain"
            />

            <p className="text-slate-400 text-center">
              AI-Powered Data Engineering Platform
            </p>
          </div>

          <form onSubmit={handleLogin}>
            {/* Email */}
            <div className="mb-5">
              <label className="block text-slate-300 mb-2">
                Email
              </label>

              <div className="relative">
                <Mail
                  size={18}
                  className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-500"
                />

                <input
                  type="email"
                  required
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="admin@astra.local"
                  className="w-full h-14 rounded-xl bg-slate-900/60 border border-slate-700 pl-12 pr-4 text-white outline-none focus:border-blue-500"
                />
              </div>
            </div>

            {/* Password */}
            <div className="mb-4">
              <label className="block text-slate-300 mb-2">
                Password
              </label>

              <div className="relative">
                <Lock
                  size={18}
                  className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-500"
                />

                <input
                  type={showPassword ? 'text' : 'password'}
                  required
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="••••••••"
                  className="login-password-input w-full h-14 rounded-xl bg-slate-900/60 border border-slate-700 pl-12 pr-12 text-white outline-none focus:border-blue-500"
                />

                <button
                  type="button"
                  aria-label={showPassword ? 'Hide password' : 'Show password'}
                  aria-pressed={showPassword}
                  onClick={() => setShowPassword((current) => !current)}
                  className="absolute right-4 top-1/2 -translate-y-1/2 text-slate-500 transition hover:text-slate-300"
                >
                  {showPassword ? (
                    <EyeOff size={18} />
                  ) : (
                    <Eye size={18} />
                  )}
                </button>
              </div>
            </div>

            {errorMessage && (
              <div className="mb-4 rounded-xl border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-200">
                {errorMessage}
              </div>
            )}

            {/* Remember */}
            <div className="flex justify-between items-center mb-6 text-sm">
              <label className="flex items-center gap-2 text-slate-400">
                <input type="checkbox" />
                Remember me
              </label>

              {/* <button
                type="button"
                className="text-blue-400 hover:text-blue-300"
              >
                Forgot password?
              </button> */}
            </div>

            {/* Login Button */}
            <button
              type="submit"
              disabled={isSubmitting}
              className="relative h-14 w-full overflow-hidden rounded-xl text-white font-semibold text-lg transition-all disabled:cursor-wait"
              style={{
                background:
                  'linear-gradient(90deg,#3b82f6 0%, #2563eb 100%)'
              }}
            >
              {isSubmitting ? (
                <PipelineActivationLoader />
              ) : (
                'Login'
              )}
            </button>
          </form>
        </div>
        </motion.div>
      </div>
    </motion.div>
  )
}
function LoginBackgroundCanvas() {
  const canvasRef = useRef<HTMLCanvasElement | null>(null)

  useEffect(() => {
    const canvas = canvasRef.current
    const ctx = canvas?.getContext('2d')

    if (!canvas || !ctx) {
      return undefined
    }

    let width = 0
    let height = 0
    let frame = 0
    let animationId = 0
    const ripples: Array<{ x: number; y: number; life: number; strength: number }> = []
    let lastPulseAt = 0
    const pointer = { x: 0.58, y: 0.42, active: false }
    const scrollLight = { progress: 0, y: 0, viewportHeight: window.innerHeight }
    const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches

    const clamp = (value: number, min = 0, max = 1) => Math.min(max, Math.max(min, value))

    const updateScrollLight = () => {
      const parentTop = canvas.parentElement?.offsetTop || 0
      const scrollY = Math.max(0, window.scrollY - parentTop)
      const scrollRange = Math.max(1, height - window.innerHeight)

      scrollLight.y = scrollY
      scrollLight.viewportHeight = window.innerHeight
      scrollLight.progress = clamp(scrollY / scrollRange)
    }

    const resize = () => {
      const parentRect = canvas.parentElement?.getBoundingClientRect()
      const nextWidth = parentRect?.width || window.innerWidth
      const nextHeight = parentRect?.height || window.innerHeight
      const dpr = Math.min(window.devicePixelRatio || 1, 2)

      width = Math.max(1, Math.floor(nextWidth))
      height = Math.max(1, Math.floor(nextHeight))
      canvas.width = Math.floor(width * dpr)
      canvas.height = Math.floor(height * dpr)
      canvas.style.width = `${width}px`
      canvas.style.height = `${height}px`
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
      updateScrollLight()
    }

    const addRipple = (x: number, y: number, strength = 1) => {
      ripples.push({ x, y, life: 0, strength })

      if (ripples.length > 18) {
        ripples.shift()
      }
    }

    const handlePointerMove = (event: PointerEvent) => {
      const rect = canvas.getBoundingClientRect()
      const x = event.clientX - rect.left
      const y = event.clientY - rect.top

      if (x < 0 || y < 0 || x > rect.width || y > rect.height) {
        pointer.active = false
        return
      }

      pointer.x = x / Math.max(1, rect.width)
      pointer.y = y / Math.max(1, rect.height)
      pointer.active = true

      const now = performance.now()
      if (!reduceMotion && now - lastPulseAt > 300) {
        lastPulseAt = now
        addRipple(x, y, 0.38)
      }
    }

    const drawBackground = () => {
      const scrollGlowStrength = 0.045 + scrollLight.progress * 0.08
      const scrollLightX = pointer.active
        ? width * pointer.x
        : width * (0.26 + scrollLight.progress * 0.46)
      const scrollLightY = clamp(
        scrollLight.y + scrollLight.viewportHeight * 0.48,
        0,
        height
      )

      const gradient = ctx.createLinearGradient(0, 0, width, height)
      gradient.addColorStop(0, '#07111f')
      gradient.addColorStop(0.34, scrollLight.progress > 0.32 ? '#0a182c' : '#0a1420')
      gradient.addColorStop(0.68, scrollLight.progress > 0.56 ? '#111f38' : '#171426')
      gradient.addColorStop(1, scrollLight.progress > 0.72 ? '#10233d' : '#0d0a11')
      ctx.fillStyle = gradient
      ctx.fillRect(0, 0, width, height)

      ctx.fillStyle = `rgba(37,99,235,${0.015 + scrollLight.progress * 0.045})`
      ctx.fillRect(0, 0, width, height)

      const sheen = ctx.createRadialGradient(width * pointer.x, height * pointer.y, 20, width * pointer.x, height * pointer.y, Math.max(width, height) * 0.55)
      sheen.addColorStop(0, pointer.active ? 'rgba(96,165,250,0.22)' : 'rgba(59,130,246,0.14)')
      sheen.addColorStop(0.4, 'rgba(168,85,247,0.06)')
      sheen.addColorStop(1, 'rgba(7,9,16,0)')
      ctx.fillStyle = sheen
      ctx.fillRect(0, 0, width, height)

      const scrollGlow = ctx.createRadialGradient(
        scrollLightX,
        scrollLightY,
        10,
        scrollLightX,
        scrollLightY,
        Math.max(width, height) * 0.42
      )
      scrollGlow.addColorStop(0, `rgba(147,197,253,${scrollGlowStrength})`)
      scrollGlow.addColorStop(0.36, `rgba(59,130,246,${scrollGlowStrength * 0.62})`)
      scrollGlow.addColorStop(0.72, `rgba(168,85,247,${scrollGlowStrength * 0.28})`)
      scrollGlow.addColorStop(1, 'rgba(7,9,16,0)')
      ctx.fillStyle = scrollGlow
      ctx.fillRect(0, 0, width, height)
    }

    const drawWaterLines = () => {
      const lineCount = Math.max(12, Math.floor(height / 54))

      for (let i = 0; i < lineCount; i += 1) {
        const baseY = (height / (lineCount + 1)) * (i + 1)
        const alpha = 0.07 + (i % 4) * 0.018

        ctx.beginPath()

        for (let x = -40; x <= width + 40; x += 18) {
          const pointerX = pointer.x * width
          const pointerY = pointer.y * height
          const distanceX = x - pointerX
          const distanceY = baseY - pointerY
          const pointerPush = Math.exp(-(distanceX * distanceX + distanceY * distanceY) / 58000) * (pointer.active ? 16 : 6)
          let ripplePush = 0

          for (const ripple of ripples) {
            const rippleDistance = Math.hypot(x - ripple.x, baseY - ripple.y)
            ripplePush += Math.sin(rippleDistance * 0.048 - ripple.life * 4.3) * Math.exp(-rippleDistance / 260) * ripple.strength * 6 * (1 - ripple.life)
          }

          const wave = Math.sin(x * 0.012 + frame * 0.018 + i * 0.62) * 11
          const crossWave = Math.cos(x * 0.006 - frame * 0.012 + i) * 7
          const y = baseY + wave + crossWave + pointerPush + ripplePush

          if (x === -40) {
            ctx.moveTo(x, y)
          } else {
            ctx.lineTo(x, y)
          }
        }

        ctx.strokeStyle = i % 3 === 0 ? `rgba(96,165,250,${alpha + 0.05})` : `rgba(147,197,253,${alpha})`
        ctx.lineWidth = i % 3 === 0 ? 1.2 : 0.8
        ctx.stroke()
      }
    }

    const drawCursorPulses = () => {
      for (const ripple of ripples) {
        const radius = 12 + ripple.life * 54
        const alpha = Math.max(0, 0.1 * (1 - ripple.life))
        const pulseGlow = ctx.createRadialGradient(ripple.x, ripple.y, 0, ripple.x, ripple.y, radius)

        pulseGlow.addColorStop(0, `rgba(96,165,250,${alpha * 0.22})`)
        pulseGlow.addColorStop(0.58, `rgba(96,165,250,${alpha * 0.16})`)
        pulseGlow.addColorStop(1, 'rgba(96,165,250,0)')
        ctx.fillStyle = pulseGlow
        ctx.fillRect(ripple.x - radius, ripple.y - radius, radius * 2, radius * 2)

        ctx.beginPath()
        ctx.arc(ripple.x, ripple.y, radius, 0, Math.PI * 2)
        ctx.strokeStyle = `rgba(147,197,253,${alpha * 0.82})`
        ctx.lineWidth = 1
        ctx.stroke()
      }
    }

    const draw = () => {
      frame += reduceMotion ? 0 : 1
      updateScrollLight()
      drawBackground()
      drawWaterLines()
      drawCursorPulses()

      for (let index = ripples.length - 1; index >= 0; index -= 1) {
        ripples[index].life += 0.015

        if (ripples[index].life >= 1) {
          ripples.splice(index, 1)
        }
      }

      if (!reduceMotion) {
        animationId = window.requestAnimationFrame(draw)
      }
    }

    resize()
    draw()

    const handleScroll = () => {
      updateScrollLight()

      if (reduceMotion) {
        draw()
      }
    }

    if (!reduceMotion) {
      window.addEventListener('pointermove', handlePointerMove)
      window.addEventListener('scroll', handleScroll, { passive: true })
      window.addEventListener('resize', resize)
    } else {
      window.addEventListener('scroll', handleScroll, { passive: true })
      window.addEventListener('resize', resize)
    }

    return () => {
      window.cancelAnimationFrame(animationId)
      window.removeEventListener('pointermove', handlePointerMove)
      window.removeEventListener('scroll', handleScroll)
      window.removeEventListener('resize', resize)
    }
  }, [])

  return (
    <canvas
      ref={canvasRef}
      aria-hidden="true"
      className="pointer-events-none absolute inset-0 h-full w-full"
    />
  )
}

function PipelineActivationLoader() {
  return (
    <span className="absolute inset-0 flex items-center justify-center">
      <span className="absolute inset-0 bg-[linear-gradient(110deg,rgba(147,197,253,0.10)_0%,rgba(191,219,254,0.20)_42%,rgba(37,99,235,0.18)_100%)] animate-[athenaSingleFlowGlow_2.8s_ease-out_forwards]" />

      <span className="relative flex items-center gap-4">
        <span className="relative h-3 w-32">
          <span className="absolute left-1 right-1 top-1/2 h-px -translate-y-1/2 overflow-hidden rounded-full bg-white/30">
            <span className="absolute inset-y-0 left-0 w-full origin-left rounded-full bg-white/90 shadow-[0_0_14px_rgba(255,255,255,0.78)] animate-[athenaSingleFlowFill_2.8s_cubic-bezier(0.22,1,0.36,1)_forwards]" />
          </span>
          <span className="absolute top-1/2 h-2.5 w-2.5 rounded-full bg-white shadow-[0_0_14px_rgba(255,255,255,0.9)] animate-[athenaSingleFlowDot_2.8s_cubic-bezier(0.22,1,0.36,1)_forwards]" />
          {[0, 50, 100].map((position) => (
            <span
              key={position}
              className="absolute top-1/2 h-2.5 w-2.5 -translate-x-1/2 -translate-y-1/2 rounded-full border border-white/75 bg-white/35"
              style={{ left: `${position}%` }}
            />
          ))}
        </span>
        <span className="text-sm font-bold tracking-wide">Starting workspace</span>
      </span>
    </span>
  )
}

export default LoginPage
