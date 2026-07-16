// @ts-nocheck
import React, { useEffect, useRef, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { AnimatePresence, motion, useReducedMotion, useScroll, useSpring, useTransform } from 'framer-motion'
import {
  Activity,
  ArrowRight,
  Brain,
  Code2,
  Database,
  GitBranch,
  Play,
  ShieldCheck,
  Sparkles,
  Waves,
  Workflow,
  X
} from 'lucide-react'

const SINGLE_FLOW_DURATION_MS = 2800

const stats = [
  { value: '80%', label: 'faster pipeline delivery' },
  { value: '2', label: 'cloud data platforms' },
  { value: '24/7', label: 'run visibility and review' }
]

const heroFlowSteps = [
  {
    key: 'brd',
    title: 'BRD Intake',
    detail: 'business requirements captured'
  },
  {
    key: 'extract',
    title: 'Requirement Extraction',
    detail: 'objectives, KPIs, rules'
  },
  {
    key: 'source',
    title: 'Source Mapping',
    detail: 'schema and table context'
  },
  {
    key: 'medallion',
    title: 'Medallion Pipeline Architecture',
    detail: 'source to bronze, silver, gold',
    architecture: [
      { title: 'Source', detail: 'source tables', badge: 'mapped', accent: '#60a5fa' },
      { title: 'Bronze', detail: 'raw ingestion', badge: 'schema aligned', accent: '#38bdf8' },
      { title: 'Silver', detail: 'quality and joins', badge: 'validated', accent: '#93c5fd' },
      { title: 'Gold', detail: 'business KPIs', badge: 'KPI ready', accent: '#c084fc' },
      { title: 'Output', detail: 'curated workflow', badge: 'ready', accent: '#f0abfc' }
    ]
  },
  {
    key: 'code',
    title: 'Code Generation',
    detail: 'reviewable pipeline assets'
  },
  {
    key: 'review',
    title: 'Human Review',
    detail: 'KPI, table, semantic, code gates'
  },
  {
    key: 'monitor',
    title: 'Run & Monitor',
    detail: 'approvals, logs, recovery'
  }
]

const medallionPreviewRows = [
  { stage: 'Bronze', task: 'ingest source tables', state: 'schema aligned', width: 74 },
  { stage: 'Silver', task: 'quality and joins', state: 'review ready', width: 82 },
  { stage: 'Gold', task: 'business KPIs', state: 'KPI ready', width: 88 }
]

const workflowSteps = [
  {
    eyebrow: '01',
    title: 'Understand the request',
    description: 'Capture source context, quality rules, ownership, and transformation goals in one guided flow.',
    icon: Brain
  },
  {
    eyebrow: '02',
    title: 'Generate the path',
    description: 'Turn requirements into bronze, silver, and gold pipeline artifacts for Databricks, Snowflake and Fabric.',
    icon: GitBranch
  },
  {
    eyebrow: '03',
    title: 'Review with confidence',
    description: 'Bring data teams into semantic, table, KPI, and code review gates before promotion.',
    icon: ShieldCheck
  },
  {
    eyebrow: '04',
    title: 'Operate every run',
    description: 'Track execution, cost, logs, and recovery signals across the full data lifecycle.',
    icon: Activity
  }
]

const capabilityGroups = [
  {
    title: 'AI pipeline design',
    description: 'Requirements are translated into structured specs, lineage-aware tasks, and implementation-ready steps.',
    icon: Sparkles
  },
  {
    title: 'Code generation',
    description: 'Astra-Data produces PySpark assets with medallion architecture patterns and reviewable output.',
    icon: Code2
  },
  {
    title: 'Platform control',
    description: 'Databricks, Snowflake and Fabric, workflows stay visible from configuration through production runs.',
    icon: Database
  }
]

const platformRows = [
  { label: 'Source discovery', value: 'metadata, schema, profiling', accent: '#60a5fa' },
  { label: 'Transformation layer', value: 'bronze, silver, gold', accent: '#3b82f6' },
  { label: 'Review loop', value: 'business, data, code', accent: '#f0abfc' },
  { label: 'Run operations', value: 'logs, cost, recovery', accent: '#fbbf24' }
]

const lifecycleCheckpoints = [
  {
    id: '01',
    title: 'Intent to spec',
    detail: 'BRD, KPIs, rules, owners'
  },
  {
    id: '02',
    title: 'Spec to assets',
    detail: 'mappings, layers, generated code'
  },
  {
    id: '03',
    title: 'Run to recovery',
    detail: 'logs, status, cost, fixes'
  }
]

function LandingPage() {
  const [showDemo, setShowDemo] = useState(false)
  const [showIntro, setShowIntro] = useState(true)
  const [isLeaving, setIsLeaving] = useState(false)
  const navigationTimerRef = useRef(null)
  const navigate = useNavigate()
  const prefersReducedMotion = useReducedMotion()

  useEffect(() => {
    if (prefersReducedMotion) {
      setShowIntro(false)
      return undefined
    }

    const introTimer = window.setTimeout(() => setShowIntro(false), 2600)
    return () => window.clearTimeout(introTimer)
  }, [prefersReducedMotion])

  useEffect(() => {
    return () => {
      if (navigationTimerRef.current) {
        window.clearTimeout(navigationTimerRef.current)
      }
    }
  }, [])

  const fadeToRoute = (route) => {
    if (isLeaving) {
      return
    }

    if (prefersReducedMotion) {
      navigate(route)
      return
    }

    setIsLeaving(true)
    navigationTimerRef.current = window.setTimeout(() => navigate(route), 520)
  }

  const handleGetStarted = () => {
    fadeToRoute('/login')
  }

  return (
    <div className="min-h-screen overflow-x-hidden bg-[#070910] text-white">
      <AnimatePresence>{showIntro && <IntroSplash />}</AnimatePresence>

      <AnimatePresence>
        {showDemo && (
          <DemoModal onClose={() => setShowDemo(false)} />
        )}
      </AnimatePresence>

      <div
        className={`relative min-h-screen transition-all duration-500 ease-out ${
          isLeaving ? 'scale-[0.99] opacity-0 blur-sm' : ''
        } ${
          showDemo ? 'scale-[0.99] opacity-55 blur-sm' : ''
        }`}
      >
        <LiquidHeroCanvas />
        <div className="pointer-events-none absolute inset-0 bg-[linear-gradient(180deg,rgba(7,9,16,0.10)_0%,rgba(7,9,16,0.24)_56%,rgba(7,9,16,0.16)_100%)]" />

        <header className="relative z-20 border-b border-blue-300/[0.08] bg-[#07111f]/35 backdrop-blur-xl">
          <div className="mx-auto flex h-16 max-w-[1320px] items-center justify-between px-5 sm:px-8">
            <Link to="/" className="flex flex-col items-center gap-1" aria-label="Astra Data home">
              <img src="/astra-wordmark-white.png" alt="Astra" className="h-auto w-28 object-contain" />
              <img src="/data-wordmark-white.png" alt="Data" className="h-auto w-24 translate-x-1.5 object-contain" />
            </Link>

            <div className="flex items-center gap-3">
              <button
                type="button"
                onClick={() => fadeToRoute('/login')}
                className="inline-flex h-10 items-center justify-center rounded-lg border border-blue-200/[0.16] bg-white/[0.04] px-4 text-sm font-semibold text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.05)] transition hover:border-[#60a5fa]/50 hover:bg-blue-500/[0.12]"
              >
                Sign In
              </button>
            </div>
          </div>
        </header>

        <main className="relative z-10">
          <section className="mx-auto grid min-h-[calc(100vh-96px)] max-w-[1320px] items-start gap-8 px-5 pb-8 pt-6 sm:px-8 lg:grid-cols-[0.92fr_1.08fr] lg:pt-10">
            <motion.div
              initial={{ opacity: 0, y: 26 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.8, delay: 0.15, ease: 'easeOut' }}
              className="max-w-[580px]"
            >
              <div className="mb-5 inline-flex items-center gap-2 rounded-lg border border-[#60a5fa]/25 bg-[#081a33]/70 px-3 py-1.5 text-[11px] font-semibold uppercase text-[#bfdbfe]">
                <Waves className="h-3.5 w-3.5" strokeWidth={1.8} />
                AI-native data engineering control plane
              </div>

              <h1 className="text-[1.72rem] font-semibold leading-[1.12] text-white sm:text-[2.05rem] lg:text-[2.2rem] xl:text-[2.35rem] max-w-full">
                Turn business requirements into production-deployable workflows.
              </h1>

              <p className="mt-5 max-w-[520px] text-sm leading-7 text-[#c7d2dc] sm:text-base">
                Astra-Data helps teams move from discovery to governed pipeline runs with AI-assisted
                specifications, generated code, review gates, and live operational visibility.
              </p>

              <div className="mt-7 flex flex-col gap-3 sm:flex-row">
                <button
                  type="button"
                  onClick={handleGetStarted}
                  className="inline-flex h-11 items-center justify-center gap-2 rounded-lg bg-[#3b82f6] px-5 text-sm font-bold text-white shadow-[0_18px_50px_rgba(59,130,246,0.28)] transition hover:bg-[#60a5fa]"
                >
                  Get Started
                  <ArrowRight className="h-4 w-4" strokeWidth={2.3} />
                </button>

                <button
                  onClick={() => setShowDemo(true)}
                  className="inline-flex h-11 items-center justify-center gap-2 rounded-lg border border-white/[0.15] bg-white/[0.07] px-5 text-sm font-semibold text-white transition hover:border-white/30 hover:bg-white/[0.12]"
                >
                  <Play className="h-4 w-4" strokeWidth={2} />
                  View Demo
                </button>
              </div>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 28, scale: 0.98 }}
              animate={{ opacity: 1, y: 0, scale: 1 }}
              transition={{ duration: 0.8, delay: 0.28, ease: 'easeOut' }}
              className="w-full"
            >
              <HeroFlowSummary />
            </motion.div>
          </section>

          <section className="mx-auto max-w-[1320px] px-5 pb-14 sm:px-8">
            <div className="grid bg-[#0b1118]/70 md:grid-cols-3">
              {stats.map((stat) => (
                <div key={stat.label} className="border-white/10 px-5 py-5 md:border-r md:last:border-r-0">
                  <div className="text-3xl font-semibold text-white">{stat.value}</div>
                  <div className="mt-1.5 text-xs uppercase text-[#94a3b8]">{stat.label}</div>
                </div>
              ))}
            </div>
          </section>

          <PipelineFlowSection />

          <section>
            <div className="mx-auto grid max-w-[1320px] gap-10 px-5 py-16 sm:px-8 lg:grid-cols-[1fr_1fr]">
              <div>
                <p className="text-xs font-semibold uppercase text-[#fbbf24]">platform map</p>
                <h2 className="mt-3 text-3xl font-semibold leading-tight text-white sm:text-4xl">
                  Built around the full data lifecycle.
                </h2>
                <p className="mt-4 max-w-[600px] text-sm leading-7 text-[#b9c6d2]">
                  Astra-Data connects intake, transformation, validation, review, and monitoring into one
                  guided operating surface for data teams.
                </p>

                <div className="mt-8 grid max-w-[720px] gap-3 sm:grid-cols-3">
                  {lifecycleCheckpoints.map((checkpoint) => (
                    <div
                      key={checkpoint.id}
                      className="min-h-[92px] rounded-lg border border-[#60a5fa]/15 bg-[#07111f]/45 px-4 py-3.5 shadow-[inset_0_1px_0_rgba(255,255,255,0.04)] backdrop-blur-sm"
                    >
                      <div className="mb-2 flex items-center gap-2">
                        <span className="h-2 w-2 rounded-full bg-[#60a5fa] shadow-[0_0_14px_rgba(96,165,250,0.7)]" />
                        <span className="text-[11px] font-semibold text-[#93c5fd]">{checkpoint.id}</span>
                        <p className="text-sm font-semibold text-white">{checkpoint.title}</p>
                      </div>
                      <p className="mt-2 text-xs leading-5 text-[#aebcca]">{checkpoint.detail}</p>
                    </div>
                  ))}
                </div>
              </div>

              <div className="space-y-3">
                {platformRows.map((row) => (
                  <div key={row.label} className="grid min-h-[62px] grid-cols-[7px_1fr] overflow-hidden rounded-lg border border-white/10 bg-[#0c1218]/[0.86] shadow-[0_18px_45px_rgba(2,6,23,0.22)]">
                    <span style={{ backgroundColor: row.accent }} />
                    <div className="flex flex-col justify-center px-4 py-3 sm:flex-row sm:items-center sm:justify-between">
                      <span className="text-xs font-semibold uppercase text-white">{row.label}</span>
                      <span className="mt-1 text-xs text-[#9fb0bf] sm:mt-0">{row.value}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </section>

          <section className="mx-auto max-w-[1320px] px-5 pb-14 pt-10 sm:px-8">
            <div className="mb-7 max-w-[760px]">
              <p className="text-xs font-semibold uppercase text-[#93c5fd]">capabilities</p>
              <h2 className="mt-2 text-[2rem] font-semibold leading-[1.12] text-white sm:text-[2.35rem]">
                Built for teams that design, review, and operate together.
              </h2>
              <p className="mt-4 max-w-[640px] text-sm leading-6 text-[#b9c6d2]">
                Data engineers, reviewers, and platform owners get a shared view of how a request
                becomes a governed, observable pipeline run.
              </p>
            </div>

            <div className="grid gap-4 lg:grid-cols-3">
              {capabilityGroups.map((capability) => (
                <CapabilityCard key={capability.title} capability={capability} />
              ))}
            </div>
          </section>
        </main>

        <footer className="relative z-10 border-t border-white/10 bg-[#070910]">
          <div className="mx-auto flex max-w-[1320px] flex-col items-start justify-between gap-3 px-5 py-6 sm:flex-row sm:items-center sm:px-8">
            <Link to="/" className="flex flex-col items-center gap-1" aria-label="Astra Data home">
              <img src="/astra-wordmark-white.png" alt="Astra" className="h-auto w-28 object-contain" />
              <img src="/data-wordmark-white.png" alt="Data" className="h-auto w-24 translate-x-1.5 object-contain" />
            </Link>
            <div className="text-xs text-[#8fa0ae]">© 2026 Astra-Data. All rights reserved.</div>
          </div>
        </footer>
      </div>
    </div>
  )
}

function IntroSplash() {
  return (
    <motion.div
      className="fixed inset-0 z-[80] flex items-center justify-center overflow-hidden bg-[#06080d]"
      initial={{ opacity: 1 }}
      exit={{ opacity: 0, y: -28 }}
      transition={{ duration: 0.55, ease: 'easeInOut' }}
    >
      <motion.div
        className="absolute inset-x-0 top-1/2 h-px bg-[linear-gradient(90deg,transparent,rgba(96,165,250,0.95),transparent)]"
        initial={{ scaleX: 0, opacity: 0 }}
        animate={{ scaleX: [0, 1, 0.38], opacity: [0, 1, 0] }}
        transition={{ duration: 1.6, ease: 'easeInOut' }}
      />

      <motion.div
        className="relative flex flex-col items-center"
        initial={{ scale: 0.92 }}
        animate={{ scale: 1 }}
        transition={{ duration: 1.2, ease: 'easeOut' }}
      >
        <motion.div
          className="absolute h-36 w-72 rounded-full border border-[#60a5fa]/30"
          initial={{ scale: 0.55, opacity: 0 }}
          animate={{ scale: [0.55, 1.05, 1.24], opacity: [0, 0.75, 0] }}
          transition={{ duration: 1.8, ease: 'easeOut' }}
        />

        <motion.img
          src="/astra-wordmark-white.png"
          alt="Astra"
          className="h-auto w-64 max-w-[70vw] object-contain drop-shadow-[0_0_32px_rgba(96,165,250,0.30)]"
          initial={{ opacity: 0, y: 10, rotate: -8 }}
          animate={{ opacity: 1, y: 0, rotate: 0 }}
          transition={{ duration: 0.7, ease: 'easeOut' }}
        />

        <motion.img
          src="/data-wordmark-white.png"
          alt="Data"
          className="mt-4 h-auto w-52 max-w-[58vw] translate-x-3 object-contain"
          initial={{ y: 36, opacity: 0 }}
          animate={{ y: 0, opacity: 1 }}
          transition={{ duration: 0.5, delay: 0.78, ease: 'easeOut' }}
        />
      </motion.div>
    </motion.div>
  )
}

function LiquidHeroCanvas() {
  const canvasRef = useRef(null)

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
    const ripples = []
    let lastPulseAt = 0
    const pointer = { x: 0.58, y: 0.42, active: false }
    const scrollLight = { progress: 0, y: 0, viewportHeight: window.innerHeight }
    const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches

    const clamp = (value, min = 0, max = 1) => Math.min(max, Math.max(min, value))

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

    const addRipple = (x, y, strength = 1) => {
      ripples.push({ x, y, life: 0, strength })

      if (ripples.length > 18) {
        ripples.shift()
      }
    }

    const handlePointerMove = (event) => {
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
      if (!reduceMotion && now - lastPulseAt > 220) {
        lastPulseAt = now
        addRipple(x, y, 0.5)
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
          const pointerPush = Math.exp(-(distanceX * distanceX + distanceY * distanceY) / 52000) * (pointer.active ? 26 : 8)
          let ripplePush = 0

          for (const ripple of ripples) {
            const rippleDistance = Math.hypot(x - ripple.x, baseY - ripple.y)
            ripplePush += Math.sin(rippleDistance * 0.052 - ripple.life * 4.5) * Math.exp(-rippleDistance / 260) * ripple.strength * 10 * (1 - ripple.life)
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
        const radius = 16 + ripple.life * 72
        const alpha = Math.max(0, 0.14 * (1 - ripple.life))
        const pulseGlow = ctx.createRadialGradient(ripple.x, ripple.y, 0, ripple.x, ripple.y, radius)

        pulseGlow.addColorStop(0, `rgba(96,165,250,${alpha * 0.22})`)
        pulseGlow.addColorStop(0.58, `rgba(96,165,250,${alpha * 0.16})`)
        pulseGlow.addColorStop(1, 'rgba(96,165,250,0)')
        ctx.fillStyle = pulseGlow
        ctx.fillRect(ripple.x - radius, ripple.y - radius, radius * 2, radius * 2)

        ctx.beginPath()
        ctx.arc(ripple.x, ripple.y, radius, 0, Math.PI * 2)
        ctx.strokeStyle = `rgba(147,197,253,${alpha})`
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
        ripples[index].life += 0.016

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

function DemoModal({ onClose }) {
  const [isVideoLoading, setIsVideoLoading] = useState(true)

  useEffect(() => {
    const loaderTimer = window.setTimeout(() => {
      setIsVideoLoading(false)
    }, SINGLE_FLOW_DURATION_MS)

    return () => {
      window.clearTimeout(loaderTimer)
    }
  }, [])

  return (
    <motion.div
      className="fixed inset-0 z-50 flex items-center justify-center bg-[#020617]/68 p-4 backdrop-blur-md"
      onClick={onClose}
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.24, ease: 'easeOut' }}
    >
      <motion.div
        className="relative w-full max-w-5xl overflow-hidden rounded-lg border border-blue-300/[0.16] bg-[#050914] shadow-[0_30px_90px_rgba(2,6,23,0.68)]"
        onClick={(event) => event.stopPropagation()}
        initial={{ opacity: 0, y: 18, scale: 0.97 }}
        animate={{ opacity: 1, y: 0, scale: 1 }}
        exit={{ opacity: 0, y: 12, scale: 0.98 }}
        transition={{ duration: 0.3, ease: 'easeOut' }}
      >
        <button
          onClick={onClose}
          className="absolute right-3 top-3 z-30 flex h-9 w-9 items-center justify-center rounded-lg border border-blue-200/[0.16] bg-[#07111f]/90 text-white shadow-[0_0_22px_rgba(59,130,246,0.22)] transition hover:border-[#60a5fa]/60 hover:bg-[#0d1b2f]"
          aria-label="Close demo"
        >
          <X size={16} />
        </button>
        <div className="relative aspect-video w-full bg-black">
          <div className="pointer-events-none absolute inset-x-0 top-0 z-10 h-px bg-[linear-gradient(90deg,transparent,rgba(96,165,250,0.95),transparent)]" />
          <AnimatePresence>
            {isVideoLoading && <DemoVideoLoader />}
          </AnimatePresence>
          <iframe
            src="https://nousinfo-my.sharepoint.com/personal/kushalsl_nousinfo_com/_layouts/15/embed.aspx?UniqueId=5ccb6f25-e6b2-4c4c-acde-266da50e8c11&embed=%7B%22hvm%22%3Atrue%2C%22ust%22%3Afalse%7D&referrer=StreamWebApp&referrerScenario=EmbedDialog.Create"
            className={`h-full w-full transition-opacity duration-500 ${isVideoLoading ? 'opacity-0' : 'opacity-100'}`}
            frameBorder="0"
            scrolling="no"
            allowFullScreen
            title="Astra-Data Demo"
          />
        </div>
      </motion.div>
    </motion.div>
  )
}

function DemoVideoLoader() {
  return (
    <motion.div
      className="absolute inset-0 z-20 flex items-center justify-center overflow-hidden bg-[#050914]"
      initial={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.35, ease: 'easeOut' }}
    >
      <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_50%_42%,rgba(59,130,246,0.22),transparent_32%),linear-gradient(135deg,rgba(7,17,31,0.96),rgba(5,9,20,1))]" />
      <div className="pointer-events-none absolute inset-x-0 top-1/2 h-px bg-[linear-gradient(90deg,transparent,rgba(96,165,250,0.75),transparent)]" />

      <div className="relative h-14 w-[min(560px,calc(100%-48px))] overflow-hidden rounded-xl bg-[#2f75f5] text-white shadow-[0_24px_80px_rgba(37,99,235,0.38)]">
        <span className="absolute inset-0 bg-[linear-gradient(110deg,rgba(147,197,253,0.10)_0%,rgba(191,219,254,0.20)_42%,rgba(37,99,235,0.18)_100%)] animate-[athenaSingleFlowGlow_2.8s_ease-out_forwards]" />

        <div className="relative flex h-full items-center justify-center gap-4">
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
          <span className="text-sm font-bold tracking-wide">Loading Astra-Data demo</span>
        </div>
      </div>
    </motion.div>
  )
}

function LiquidCardFill({
  active = false,
  filled = false,
  duration = 1.15,
  delay = 0,
  roundedClass = 'rounded-lg',
  compact = false,
  boosted = false
}) {
  const prefersReducedMotion = useReducedMotion()
  const [settled, setSettled] = useState(filled && !active)

  useEffect(() => {
    if (!filled) {
      setSettled(false)
      return undefined
    }

    if (active && !prefersReducedMotion) {
      setSettled(false)
      const settleTimer = window.setTimeout(() => {
        setSettled(true)
      }, (delay + duration) * 1000 + 120)

      return () => window.clearTimeout(settleTimer)
    }

    setSettled(true)
    return undefined
  }, [active, delay, duration, filled, prefersReducedMotion])

  const canMove = filled && !prefersReducedMotion
  const isFilling = active && !settled && !prefersReducedMotion
  const waveTravel = (isFilling ? (compact ? 18 : 22) : compact ? 5 : 7) + (boosted ? 4 : 0)
  const waveLift = (isFilling ? (compact ? 2 : 3) : 1) + (boosted ? 1 : 0)
  const waveDuration = isFilling ? 1.65 : boosted ? 3.4 : 5.2
  const secondaryDuration = isFilling ? 2.05 : boosted ? 4.2 : 6.4
  const surfaceOpacity = isFilling ? 0.9 : boosted ? 0.58 : 0.46

  const waveTransition = (seconds, extraDelay = 0) => canMove
    ? {
        repeat: Infinity,
        repeatType: 'mirror',
        duration: seconds,
        delay: delay + extraDelay,
        ease: 'easeInOut'
      }
    : { duration: 0.2 }

  return (
    <motion.span
      className={`pointer-events-none absolute inset-x-0 bottom-0 overflow-hidden ${roundedClass}`}
      initial={{ height: active ? '0%' : filled ? '100%' : '0%' }}
      animate={{ height: filled ? '100%' : '0%' }}
      transition={{ duration: active ? duration : 0.28, delay: active ? delay : 0, ease: [0.22, 1, 0.36, 1] }}
    >
      <span className="absolute inset-0 bg-[linear-gradient(180deg,rgba(56,189,248,0.10),rgba(37,99,235,0.28),rgba(192,132,252,0.18))]" />
      <span className="absolute inset-x-0 bottom-0 h-2/3 bg-[radial-gradient(circle_at_32%_94%,rgba(14,165,233,0.22),transparent_44%),radial-gradient(circle_at_78%_82%,rgba(168,85,247,0.14),transparent_42%)]" />

      <motion.span
        className="absolute left-[-28%] top-[-18px] h-[36px] w-[156%] rounded-[42%_58%_48%_52%/62%_52%_48%_38%] bg-[radial-gradient(ellipse_at_center,rgba(219,234,254,0.74)_0%,rgba(147,197,253,0.52)_34%,rgba(56,189,248,0.28)_58%,rgba(37,99,235,0.08)_72%,transparent_84%)] blur-[1px]"
        animate={canMove ? {
          x: [-waveTravel, 0, waveTravel, 0, -waveTravel],
          y: [0, -waveLift, 0, waveLift, 0],
          scaleX: [0.9, 1.04, 0.92, 1, 0.9],
          opacity: [surfaceOpacity, surfaceOpacity + 0.06, surfaceOpacity, surfaceOpacity + 0.03, surfaceOpacity]
        } : { x: 0, y: 0, scaleX: 1, opacity: 0.54 }}
        transition={waveTransition(waveDuration)}
      />

      <motion.span
        className="absolute left-[-22%] top-[-11px] h-[24px] w-[144%] rounded-[58%_42%_56%_44%/48%_62%_38%_52%] bg-[radial-gradient(ellipse_at_center,rgba(186,230,253,0.42)_0%,rgba(59,130,246,0.20)_48%,rgba(192,132,252,0.11)_64%,transparent_78%)] blur-[1.4px]"
        animate={canMove ? {
          x: [waveTravel, 0, -waveTravel, 0, waveTravel],
          y: [0, waveLift, 0, -waveLift, 0],
          scaleX: [0.94, 1.08, 0.9, 1.02, 0.94],
          opacity: [surfaceOpacity * 0.74, surfaceOpacity * 0.92, surfaceOpacity * 0.7, surfaceOpacity * 0.86, surfaceOpacity * 0.74]
        } : { x: 0, y: 0, scaleX: 1, opacity: 0.28 }}
        transition={waveTransition(secondaryDuration, 0.08)}
      />

      <motion.span
        className="absolute left-[-18%] top-[-5px] h-[10px] w-[136%] rounded-[50%] border-t border-white/[0.28]"
        animate={canMove ? {
          x: [-waveTravel * 0.76, 0, waveTravel * 0.76, 0, -waveTravel * 0.76],
          scaleX: [0.92, 1.02, 0.94, 1, 0.92],
          opacity: [0.54, 0.84, 0.58, 0.74, 0.54]
        } : { x: 0, scaleX: 1, opacity: 0.36 }}
        transition={waveTransition(waveDuration, 0.04)}
      />

      <motion.span
        className="absolute inset-y-0 left-[-36%] w-[78%] rotate-12 bg-[linear-gradient(90deg,transparent,rgba(219,234,254,0.18),rgba(125,211,252,0.20),transparent)] blur-sm"
        initial={{ x: '-100%', opacity: 0 }}
        animate={isFilling ? { x: ['-100%', '220%'], opacity: [0, 0.52, 0] } : { x: '220%', opacity: 0 }}
        transition={{ duration, delay, ease: [0.22, 1, 0.36, 1] }}
      />

      <span className="absolute inset-x-0 top-0 h-1/3 bg-[linear-gradient(180deg,rgba(255,255,255,0.10),transparent)]" />
      <span className="absolute inset-x-0 bottom-0 h-1/2 bg-[linear-gradient(180deg,transparent,rgba(96,165,250,0.16))]" />
    </motion.span>
  )
}

function MedallionPipelinePreview({ reduced = false }) {
  const [phase, setPhase] = useState(reduced ? 7 : 0)

  useEffect(() => {
    if (reduced) {
      setPhase(7)
      return undefined
    }

    setPhase(0)
    const phaseTimers = [140, 520, 960, 1360, 1780, 2200, 2620].map((time, index) => (
      window.setTimeout(() => setPhase(index + 1), time)
    ))

    return () => {
      phaseTimers.forEach((timer) => window.clearTimeout(timer))
    }
  }, [reduced])

  return (
    <div className="relative">
      <div className="grid gap-2.5 sm:grid-cols-3">
        {medallionPreviewRows.map((row, index) => {
          const fillPhase = 2 + index * 2
          const rowActive = phase === fillPhase && !reduced
          const rowFilled = phase >= fillPhase || reduced

          return (
            <motion.div
              key={row.stage}
              className={`relative z-10 min-h-[82px] overflow-hidden rounded-lg border bg-white/[0.045] p-2.5 transition duration-300 hover:-translate-y-0.5 hover:border-[#60a5fa]/40 ${
                rowFilled
                  ? 'border-[#60a5fa]/25 shadow-[0_14px_32px_rgba(37,99,235,0.14)]'
                  : 'border-white/10'
              }`}
              initial={{ opacity: 0.46, y: reduced ? 0 : 8 }}
              animate={{ opacity: rowFilled || rowActive ? 1 : 0.58, y: 0 }}
              whileHover={reduced ? undefined : { y: -2 }}
              transition={{ duration: 0.28, ease: 'easeOut' }}
            >
              <LiquidCardFill
                active={rowActive}
                filled={rowFilled}
                duration={0.82}
                roundedClass="rounded-lg"
                compact
                boosted={rowActive}
              />
              <div className="relative z-10">
                <div className="mb-2 flex items-center justify-between gap-2">
                  <span className="flex h-6 w-6 items-center justify-center rounded-md border border-white/10 bg-[#0d1720] text-[10px] font-bold text-[#60a5fa]">
                    {index + 1}
                  </span>
                  <span className="rounded-md bg-white/[0.08] px-2 py-0.5 text-[10px] text-[#b9c6d2]">
                    {row.state}
                  </span>
                </div>
                <p className="text-xs font-semibold uppercase text-white">{row.stage}</p>
                <div className="mt-2 h-1.5 w-full overflow-hidden rounded-full bg-[#111b24]">
                  <motion.div
                    className="h-full rounded-full bg-[linear-gradient(90deg,#3b82f6,#93c5fd,#c084fc)]"
                    initial={{ width: 0 }}
                    animate={{ width: rowFilled ? `${row.width}%` : 0 }}
                    transition={{ duration: 0.55, ease: 'easeOut' }}
                  />
                </div>
                <p className="mt-1.5 text-[11px] text-[#9fb0bf]">{row.task}</p>
              </div>
            </motion.div>
          )
        })}
      </div>
    </div>
  )
}

function HeroFlowSummary() {
  const flowContainerRef = useRef(null)
  const stepRefs = useRef([])
  const [hoveredStep, setHoveredStep] = useState(null)
  const [flowIndex, setFlowIndex] = useState(0)
  const [activeConnectorIndex, setActiveConnectorIndex] = useState(null)
  const [medallionReady, setMedallionReady] = useState(false)
  const [workflowReady, setWorkflowReady] = useState(false)
  const [connectorLayout, setConnectorLayout] = useState({
    width: 1000,
    height: 400,
    flowPath: 'M159 48 C231 48 268 48 499 48 C570 48 609 48 841 48 C841 84 841 84 841 144 C770 144 731 144 499 144 C428 144 389 144 159 144 C159 180 159 180 159 288',
    segmentPaths: [
      'M159 48 C301.8 48 356.2 48 499 48',
      'M499 48 C642.6 48 697.4 48 841 48',
      'M841 48 C841 88.3 841 103.7 841 144',
      'M841 144 C697.4 144 642.6 144 499 144',
      'M499 144 C356.2 144 301.8 144 159 144',
      'M159 144 C159 204.5 159 227.5 159 288'
    ],
    cardRects: []
  })
  const prefersReducedMotion = useReducedMotion()
  const showMedallion = flowIndex === 3 && medallionReady
  const connectorFlowSeconds = 1.05
  const connectorFlowMs = connectorFlowSeconds * 1000
  const stepGridPositions = [
    'lg:col-start-1 lg:row-start-1',
    'lg:col-start-2 lg:row-start-1',
    'lg:col-start-3 lg:row-start-1',
    'lg:col-start-3 lg:row-start-2',
    'lg:col-start-2 lg:row-start-2',
    'lg:col-start-1 lg:row-start-2',
    'lg:col-start-1 lg:row-start-3'
  ]
  useEffect(() => {
    const container = flowContainerRef.current

    if (!container) {
      return undefined
    }

    let frameId
    const round = (value) => Math.round(value * 10) / 10

    const buildConnectorCurve = (previous, point) => {
      const deltaX = point.x - previous.x
      const deltaY = point.y - previous.y

      if (Math.abs(deltaX) >= Math.abs(deltaY)) {
        const handle = Math.max(18, Math.abs(deltaX) * 0.42)
        return `C${round(previous.x + Math.sign(deltaX || 1) * handle)} ${previous.y} ${round(point.x - Math.sign(deltaX || 1) * handle)} ${point.y} ${point.x} ${point.y}`
      }

      const handle = Math.max(18, Math.abs(deltaY) * 0.42)
      return `C${previous.x} ${round(previous.y + Math.sign(deltaY || 1) * handle)} ${point.x} ${round(point.y - Math.sign(deltaY || 1) * handle)} ${point.x} ${point.y}`
    }

    const buildStepCenters = (stepRects) => stepRects.map((rect) => ({
      x: round(rect.centerX),
      y: round(rect.centerY)
    }))

    const buildContinuousPath = (stepRects) => {
      const centers = buildStepCenters(stepRects)

      return centers.slice(1).reduce((path, point, index) => {
        const previous = centers[index]
        return `${path} ${buildConnectorCurve(previous, point)}`
      }, `M${centers[0].x} ${centers[0].y}`)
    }

    const buildSegmentPaths = (stepRects) => {
      const arrowClearance = 5

      return stepRects.slice(1).map((target, index) => {
        const source = stepRects[index]
        const deltaX = target.centerX - source.centerX
        const deltaY = target.centerY - source.centerY

        if (Math.abs(deltaX) >= Math.abs(deltaY)) {
          const movingRight = deltaX > 0
          const startX = movingRight ? source.right + arrowClearance : source.left - arrowClearance
          const endX = movingRight ? target.left - arrowClearance : target.right + arrowClearance
          return `M${round(startX)} ${source.centerY} L${round(endX)} ${target.centerY}`
        }

        const movingDown = deltaY > 0
        const startY = movingDown ? source.bottom + arrowClearance : source.top - arrowClearance
        const endY = movingDown ? target.top - arrowClearance : target.bottom + arrowClearance
        return `M${source.centerX} ${round(startY)} L${target.centerX} ${round(endY)}`
      })
    }

    const measureConnectors = () => {
      const containerRect = container.getBoundingClientRect()

      if (!containerRect.width || !containerRect.height) {
        return
      }

      const stepRects = stepRefs.current
        .slice(0, heroFlowSteps.length)
        .map((node) => node?.getBoundingClientRect())
        .filter(Boolean)
        .map((rect) => ({
          left: round(rect.left - containerRect.left),
          right: round(rect.right - containerRect.left),
          top: round(rect.top - containerRect.top),
          bottom: round(rect.bottom - containerRect.top),
          width: round(rect.width),
          height: round(rect.height),
          centerX: round(rect.left - containerRect.left + rect.width / 2),
          centerY: round(rect.top - containerRect.top + rect.height / 2)
        }))

      if (stepRects.length !== heroFlowSteps.length) {
        return
      }

      const maskPadding = 4

      setConnectorLayout({
        width: round(containerRect.width),
        height: round(containerRect.height),
        flowPath: buildContinuousPath(stepRects),
        segmentPaths: buildSegmentPaths(stepRects),
        cardRects: stepRects.map((rect) => ({
          x: round(rect.left - maskPadding),
          y: round(rect.top - maskPadding),
          width: round(rect.width + maskPadding * 2),
          height: round(rect.height + maskPadding * 2)
        }))
      })
    }

    const scheduleMeasure = () => {
      window.cancelAnimationFrame(frameId)
      frameId = window.requestAnimationFrame(measureConnectors)
    }

    scheduleMeasure()

    const resizeObserver = new ResizeObserver(scheduleMeasure)
    resizeObserver.observe(container)
    stepRefs.current.forEach((node) => {
      if (node) {
        resizeObserver.observe(node)
      }
    })
    window.addEventListener('resize', scheduleMeasure)

    return () => {
      window.cancelAnimationFrame(frameId)
      resizeObserver.disconnect()
      window.removeEventListener('resize', scheduleMeasure)
    }
  }, [showMedallion])

  useEffect(() => {
    if (prefersReducedMotion) {
      setFlowIndex(heroFlowSteps.length - 1)
      setActiveConnectorIndex(null)
      setWorkflowReady(true)
      return undefined
    }

    if (workflowReady) {
      const readyTimer = window.setTimeout(() => {
        setWorkflowReady(false)
        setFlowIndex(0)
        setActiveConnectorIndex(null)
      }, 2000)

      return () => window.clearTimeout(readyTimer)
    }

    if (activeConnectorIndex !== null) {
      const connectorTimer = window.setTimeout(() => {
        setActiveConnectorIndex(null)
        setFlowIndex((current) => Math.min(current + 1, heroFlowSteps.length - 1))
      }, connectorFlowMs)

      return () => window.clearTimeout(connectorTimer)
    }

    const flowTimer = window.setTimeout(() => {
      if (flowIndex === heroFlowSteps.length - 1) {
        setWorkflowReady(true)
        return
      }

      setActiveConnectorIndex(flowIndex)
    }, flowIndex === 3 ? 3900 : 1350)

    return () => window.clearTimeout(flowTimer)
  }, [activeConnectorIndex, connectorFlowMs, flowIndex, prefersReducedMotion, workflowReady])

  useEffect(() => {
    setMedallionReady(false)

    if (flowIndex !== 3) {
      return undefined
    }

    if (prefersReducedMotion) {
      setMedallionReady(true)
      return undefined
    }

    const drawerTimer = window.setTimeout(() => {
      setMedallionReady(true)
    }, 1150)

    return () => window.clearTimeout(drawerTimer)
  }, [flowIndex, prefersReducedMotion])

  return (
    <div
      ref={flowContainerRef}
      className="relative w-full"
      onMouseLeave={() => setHoveredStep(null)}
    >
      <HeroPipelineConnectors
        connectorLayout={connectorLayout}
        activeConnectorIndex={activeConnectorIndex}
        flowIndex={flowIndex}
        workflowReady={workflowReady}
        connectorFlowSeconds={connectorFlowSeconds}
        prefersReducedMotion={prefersReducedMotion}
      />

      <div className="relative z-10 grid gap-3 lg:grid-cols-3 lg:grid-rows-[96px_96px_auto] lg:gap-x-6 lg:gap-y-6">
        {heroFlowSteps.map((step, index) => {
          const isActive = !workflowReady && activeConnectorIndex === null && index === flowIndex
          const isConnectorSource = activeConnectorIndex === index
          const isReceiving = activeConnectorIndex === index - 1
          const isComplete = workflowReady || index < flowIndex || isConnectorSource
          const isLit = isActive || isComplete || isReceiving
          const isHovered = hoveredStep === step.key
          const isAutoCurrent = (isActive || isReceiving) && !prefersReducedMotion

          return (
            <div key={step.key} className={stepGridPositions[index]}>
              <button
                ref={(node) => {
                  stepRefs.current[index] = node
                }}
                type="button"
                className={`group relative h-20 w-full overflow-hidden rounded-lg border px-3.5 py-3 text-left transition duration-300 hover:-translate-y-0.5 focus:-translate-y-0.5 lg:h-24 lg:px-4 ${
                  workflowReady
                    ? 'border-emerald-300/35 bg-[#0b2030]/[0.78] shadow-[0_16px_38px_rgba(20,184,166,0.16)]'
                    : isReceiving
                    ? 'border-[#7dd3fc]/55 bg-[#0d2338]/[0.78] shadow-[0_0_34px_rgba(56,189,248,0.20),0_14px_34px_rgba(37,99,235,0.18)]'
                    : isLit
                    ? 'border-[#60a5fa]/50 bg-[#0d1d33]/[0.74] shadow-[0_14px_34px_rgba(37,99,235,0.18)]'
                    : isHovered
                      ? 'border-[#60a5fa]/30 bg-white/[0.06]'
                      : 'border-white/[0.10] bg-white/[0.035] hover:border-[#60a5fa]/30 hover:bg-white/[0.06]'
                }`}
                aria-expanded={step.key === 'medallion' ? showMedallion : undefined}
                onMouseEnter={() => setHoveredStep(step.key)}
                onFocus={() => setHoveredStep(step.key)}
                onClick={() => {
                  setWorkflowReady(false)
                  setActiveConnectorIndex(null)
                  setFlowIndex(index)
                }}
              >
                <LiquidCardFill
                  active={isAutoCurrent}
                  filled={isLit}
                  duration={isReceiving ? connectorFlowSeconds : 1.15}
                  delay={0}
                  boosted={isHovered || workflowReady || isReceiving}
                />

                <span className="relative z-10 flex items-center justify-between gap-3">
                  <span className="min-w-0">
                    <span className="mb-0.5 flex items-center gap-2">
                      <span className={`h-1.5 w-1.5 rounded-full ${isLit ? 'bg-[#60a5fa] shadow-[0_0_16px_rgba(96,165,250,0.75)]' : 'bg-[#60a5fa]/70'}`} />
                      <span className="text-[10px] font-semibold uppercase tracking-[0.08em] text-[#93c5fd]">
                        {String(index + 1).padStart(2, '0')}
                      </span>
                    </span>
                    <span className="block truncate text-[13px] font-semibold leading-5 text-white">{step.title}</span>
                    <span className="mt-0.5 block truncate text-xs leading-5 text-[#aebcca]">{step.detail}</span>
                  </span>
                </span>
              </button>

              {index < heroFlowSteps.length - 1 && (
                <div className="ml-5 flex h-4 items-center lg:hidden">
                  <ArrowRight className={`h-4 w-4 rotate-90 transition ${index < flowIndex || activeConnectorIndex === index ? 'text-[#3b82f6] drop-shadow-[0_0_8px_rgba(59,130,246,0.55)]' : 'text-[#3b82f6]/35'}`} strokeWidth={3} />
                </div>
              )}
            </div>
          )
        })}

        <div className="relative min-h-[148px] lg:col-span-2 lg:col-start-2 lg:row-start-3">
          <AnimatePresence mode="wait">
            {showMedallion ? (
              <motion.aside
                className="absolute inset-x-0 top-0 overflow-hidden rounded-lg border border-[#60a5fa]/25 bg-[#07111f]/[0.94] p-3 shadow-[0_24px_60px_rgba(2,6,23,0.42)] backdrop-blur-xl"
                initial={{ opacity: 0, x: 42, scale: 0.96 }}
                animate={{ opacity: 1, x: 0, scale: 1 }}
                exit={{ opacity: 0, x: 34, scale: 0.97 }}
                transition={{ duration: 0.34, ease: 'easeOut' }}
              >
                <div className="mb-3 flex items-center justify-between gap-3 border-b border-white/10 pb-3">
                  <div className="flex items-center gap-3">
                    <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-[#3b82f6] text-white">
                      <Workflow className="h-[18px] w-[18px]" strokeWidth={2.2} />
                    </div>
                    <div>
                      <p className="text-xs font-semibold text-white">Astra-Data run design</p>
                      <p className="text-[11px] text-[#8fa0ae]">medallion pipeline preview</p>
                    </div>
                  </div>
                  <span className="flex items-center gap-1.5 rounded-lg border border-[#60a5fa]/25 bg-[#07172f] px-2.5 py-1 text-[10px] font-semibold text-[#bfdbfe]">
                    <span className="h-1.5 w-1.5 rounded-full bg-[#60a5fa]" />
                    preview
                  </span>
                </div>

                <MedallionPipelinePreview reduced={prefersReducedMotion} />
              </motion.aside>
            ) : null}
          </AnimatePresence>
        </div>
      </div>
    </div>
  )
}

function HeroPipelineConnectors({
  connectorLayout,
  activeConnectorIndex,
  flowIndex,
  workflowReady,
  connectorFlowSeconds,
  prefersReducedMotion
}) {
  const segmentPaths = connectorLayout.segmentPaths || []
  const cardRects = connectorLayout.cardRects || []
  const maskId = 'hero-workflow-connector-mask'
  const completedSegmentCount = workflowReady ? segmentPaths.length : flowIndex

  return (
    <svg
      className="pointer-events-none absolute inset-0 hidden h-full w-full lg:block"
      viewBox={`0 0 ${connectorLayout.width} ${connectorLayout.height}`}
      preserveAspectRatio="none"
      aria-hidden="true"
    >
      <defs>
        <linearGradient id="hero-circuit-line" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stopColor="#2563eb" stopOpacity="0.72" />
          <stop offset="48%" stopColor="#38bdf8" stopOpacity="0.98" />
          <stop offset="100%" stopColor="#93c5fd" stopOpacity="0.78" />
        </linearGradient>
        <linearGradient id="hero-circuit-energy" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stopColor="#2563eb" stopOpacity="0" />
          <stop offset="30%" stopColor="#38bdf8" stopOpacity="0.76" />
          <stop offset="50%" stopColor="#ffffff" stopOpacity="0.98" />
          <stop offset="70%" stopColor="#7dd3fc" stopOpacity="0.8" />
          <stop offset="100%" stopColor="#2563eb" stopOpacity="0" />
        </linearGradient>
        <linearGradient id="hero-circuit-highlight" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stopColor="#bfdbfe" stopOpacity="0.24" />
          <stop offset="52%" stopColor="#ffffff" stopOpacity="0.76" />
          <stop offset="100%" stopColor="#bfdbfe" stopOpacity="0.22" />
        </linearGradient>
        <marker
          id="hero-flow-arrow-muted"
          markerWidth="11"
          markerHeight="11"
          refX="10"
          refY="5"
          orient="auto"
          markerUnits="userSpaceOnUse"
        >
          <path
            d="M2 1 L10 5 L2 9"
            fill="none"
            stroke="#3b82f6"
            strokeWidth="2.8"
            strokeLinecap="round"
            strokeLinejoin="round"
            opacity="0.62"
          />
        </marker>
        <marker
          id="hero-flow-arrow-active"
          markerWidth="12"
          markerHeight="12"
          refX="11"
          refY="5.5"
          orient="auto"
          markerUnits="userSpaceOnUse"
        >
          <path
            d="M2 1 L11 5.5 L2 10"
            fill="none"
            stroke="#60a5fa"
            strokeWidth="3.4"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </marker>
        <filter id="hero-circuit-glow" x="-80%" y="-180%" width="260%" height="460%">
          <feGaussianBlur stdDeviation="3.2" result="blur1" />
          <feGaussianBlur stdDeviation="1.2" result="blur2" />
          <feMerge>
            <feMergeNode in="blur1" />
            <feMergeNode in="blur2" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
        <filter id="hero-circuit-inner-glow" x="-50%" y="-120%" width="200%" height="340%">
          <feGaussianBlur stdDeviation="1.1" result="blur" />
          <feMerge>
            <feMergeNode in="blur" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
        <mask id={maskId} maskUnits="userSpaceOnUse">
          <rect x="0" y="0" width={connectorLayout.width} height={connectorLayout.height} fill="white" />
          {cardRects.map((rect, index) => (
            <rect
              key={`${rect.x}-${rect.y}-${index}`}
              x={rect.x}
              y={rect.y}
              width={rect.width}
              height={rect.height}
              rx="8"
              ry="8"
              fill="black"
            />
          ))}
        </mask>
      </defs>

      <g mask={`url(#${maskId})`}>
        {segmentPaths.map((path, index) => (
          <path
            key={`base-arrow-${index}`}
            d={path}
            fill="none"
            stroke="#3b82f6"
            strokeWidth="2.8"
            strokeLinecap="round"
            strokeLinejoin="round"
            markerEnd="url(#hero-flow-arrow-muted)"
            opacity="0.5"
          />
        ))}

        {segmentPaths.map((path, index) => {
          const isComplete = index < completedSegmentCount
          const isActive = activeConnectorIndex === index

          if (!isComplete && !isActive && !workflowReady) {
            return null
          }

          return (
            <g key={`${path}-${index}`}>
              <path
                d={path}
                fill="none"
                stroke="#38bdf8"
                strokeWidth="4.8"
                strokeLinecap="round"
                strokeLinejoin="round"
                filter="url(#hero-circuit-glow)"
                opacity={isActive ? 0.62 : 0.36}
              />
              <motion.path
                d={path}
                fill="none"
                stroke="url(#hero-circuit-line)"
                strokeWidth="4.2"
                strokeLinecap="round"
                strokeLinejoin="round"
                markerEnd="url(#hero-flow-arrow-active)"
                initial={isActive && !prefersReducedMotion ? { pathLength: 0, opacity: 0.54 } : false}
                animate={{ pathLength: 1, opacity: isActive ? 1 : 0.78 }}
                transition={{
                  pathLength: {
                    duration: isActive && !prefersReducedMotion ? connectorFlowSeconds : 0.24,
                    ease: [0.22, 1, 0.36, 1]
                  },
                  opacity: { duration: 0.24, ease: 'easeOut' }
                }}
              />
              <path
                d={path}
                fill="none"
                stroke="url(#hero-circuit-highlight)"
                strokeWidth="1.35"
                strokeLinecap="round"
                strokeLinejoin="round"
                opacity={isActive ? 0.8 : 0.54}
              />
            </g>
          )
        })}

        {!prefersReducedMotion && activeConnectorIndex !== null && segmentPaths[activeConnectorIndex] && (
          <>
            <motion.path
              key={`energy-${activeConnectorIndex}`}
              d={segmentPaths[activeConnectorIndex]}
              fill="none"
              stroke="url(#hero-circuit-energy)"
              strokeWidth="3.4"
              strokeLinecap="round"
              strokeLinejoin="round"
              filter="url(#hero-circuit-inner-glow)"
              strokeDasharray="18 26"
              initial={{ pathLength: 0, strokeDashoffset: 28, opacity: 0.35 }}
              animate={{ pathLength: 1, strokeDashoffset: -118, opacity: [0.45, 1, 0.7] }}
              transition={{
                pathLength: { duration: connectorFlowSeconds, ease: [0.22, 1, 0.36, 1] },
                strokeDashoffset: { duration: connectorFlowSeconds, ease: 'linear' },
                opacity: { duration: connectorFlowSeconds, ease: 'easeInOut' }
              }}
            />
            <motion.path
              key={`highlight-${activeConnectorIndex}`}
              d={segmentPaths[activeConnectorIndex]}
              fill="none"
              stroke="#e0f2fe"
              strokeWidth="1.2"
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeDasharray="7 30"
              initial={{ pathLength: 0, strokeDashoffset: 22, opacity: 0 }}
              animate={{ pathLength: 1, strokeDashoffset: -92, opacity: [0.1, 0.78, 0.18] }}
              transition={{
                pathLength: { duration: connectorFlowSeconds, ease: [0.22, 1, 0.36, 1] },
                strokeDashoffset: { duration: connectorFlowSeconds, ease: 'linear' },
                opacity: { duration: connectorFlowSeconds, ease: 'easeInOut' }
              }}
            />
          </>
        )}

      </g>
    </svg>
  )
}

function PipelineFlowSection() {
  const sectionRef = useRef(null)
  const prefersReducedMotion = useReducedMotion()
  const { scrollYProgress } = useScroll({
    target: sectionRef,
    offset: ['start 78%', 'end 38%']
  })
  const flowProgress = useSpring(scrollYProgress, {
    stiffness: 82,
    damping: 24,
    mass: 0.75
  })

  return (
    <section ref={sectionRef} className="relative mx-auto max-w-[1320px] overflow-visible px-5 pb-[4.5rem] sm:px-8">
      <PipelineFlowGraphic progress={flowProgress} reduced={prefersReducedMotion} />

      <div className="relative z-10 grid gap-8 pt-10 lg:grid-cols-[0.76fr_1.24fr] lg:pt-16">
        <motion.div
          initial={{ opacity: 0, y: 26, scale: 0.98 }}
          whileInView={{ opacity: 1, y: 0, scale: 1 }}
          viewport={{ once: false, amount: 0.4 }}
          transition={{ duration: 0.62, ease: 'easeOut' }}
        >
          <p className="text-xs font-semibold uppercase text-[#60a5fa]">workflow current</p>
          <h2 className="mt-3 text-3xl font-semibold leading-tight text-white sm:text-4xl">
            One path from idea to monitored run.
          </h2>
          <p className="mt-4 text-sm leading-7 text-[#b9c6d2]">
            Astra-Data keeps each handoff visible, from requirement capture to generated assets,
            review decisions, execution status, and recovery signals.
          </p>
          <div className="mt-10 grid gap-2 text-xs text-[#9fb0bf] sm:grid-cols-2 lg:mt-14">
            {['Intake', 'Generate', 'Review', 'Run'].map((stage) => (
              <div key={stage} className="flex items-center gap-2">
                <span className="h-2 w-2 rounded-full bg-[#60a5fa] shadow-[0_0_16px_rgba(96,165,250,0.7)]" />
                {stage}
              </div>
            ))}
          </div>
        </motion.div>

        <div className="grid gap-3 md:grid-cols-2">
          {workflowSteps.map((step, index) => (
            <motion.div
              key={step.title}
              initial={{ opacity: 0, y: 34, scale: 0.96 }}
              whileInView={{ opacity: 1, y: 0, scale: 1 }}
              viewport={{ once: false, amount: 0.28 }}
              transition={{ duration: 0.58, delay: Math.min(index * 0.07, 0.24), ease: 'easeOut' }}
            >
              <WorkflowCard step={step} />
            </motion.div>
          ))}
        </div>

      </div>
    </section>
  )
}

function PipelineFlowGraphic({ progress, reduced }) {
  const pathProgress = reduced ? 1 : progress
  const dotProgress = progress
  const pipelinePath = 'M-24 26 C12 20 47 18 78 25 C128 37 176 52 227 46 C286 39 325 21 384 13 C468 2 568 0 648 13 C708 22 742 58 748 108 C758 168 734 221 692 258 C648 301 548 318 450 324 C417 326 396 329 380 340 C350 360 278 354 220 374 C175 397 150 431 148 468 C146 514 189 535 238 528 C273 523 293 513 300 502 C330 462 360 529 290 562 C221 596 93 574 33 554 C10 548 -8 540 -24 532'

  return (
    <div className="pointer-events-none absolute inset-x-0 top-0 z-0 hidden h-full lg:block">
      <svg className="h-full w-full overflow-visible" viewBox="0 0 1000 620" preserveAspectRatio="none" aria-hidden="true">
        <defs>
          {/* Outer glow filter — applied to the main lit stroke */}
          <filter id="pipe-outer-glow" x="-40%" y="-40%" width="180%" height="180%">
            <feGaussianBlur stdDeviation="7" result="blur1" />
            <feGaussianBlur stdDeviation="3" result="blur2" />
            <feMerge>
              <feMergeNode in="blur1" />
              <feMergeNode in="blur2" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>

          {/* Tight inner highlight glow — applied to the bright center stroke */}
          <filter id="pipe-inner-glow" x="-20%" y="-20%" width="140%" height="140%">
            <feGaussianBlur stdDeviation="2.5" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>

          {/* Leading dot glow */}
          <filter id="dot-glow" x="-60%" y="-60%" width="220%" height="220%">
            <feGaussianBlur stdDeviation="2.8" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>

          {/* Gradient along the path for the outer colored tube */}
          <linearGradient id="pipe-tube-gradient" x1="0%" y1="20%" x2="100%" y2="80%">
            <stop offset="0%" stopColor="#1d4ed8" stopOpacity="0.9" />
            <stop offset="45%" stopColor="#38bdf8" stopOpacity="1" />
            <stop offset="100%" stopColor="#93c5fd" stopOpacity="0.95" />
          </linearGradient>

          {/* Bright inner highlight stripe */}
          <linearGradient id="pipe-highlight-gradient" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#bfdbfe" stopOpacity="0.6" />
            <stop offset="50%" stopColor="white" stopOpacity="0.95" />
            <stop offset="100%" stopColor="#bfdbfe" stopOpacity="0.7" />
          </linearGradient>
        </defs>

        <path
          d={pipelinePath}
          fill="none"
          stroke="rgba(30,58,138,0.35)"
          strokeWidth="10"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <path
          d={pipelinePath}
          fill="none"
          stroke="rgba(96,165,250,0.06)"
          strokeWidth="4"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <motion.path
          d={pipelinePath}
          fill="none"
          stroke="url(#pipe-tube-gradient)"
          strokeWidth="9"
          strokeLinecap="round"
          strokeLinejoin="round"
          filter="url(#pipe-outer-glow)"
          style={{ pathLength: pathProgress }}
        />
        <motion.path
          d={pipelinePath}
          fill="none"
          stroke="url(#pipe-highlight-gradient)"
          strokeWidth="3"
          strokeLinecap="round"
          strokeLinejoin="round"
          filter="url(#pipe-inner-glow)"
          style={{ pathLength: pathProgress }}
        />
        <LeadingDot pathData={pipelinePath} progress={dotProgress} reduced={reduced} />

        <FlowSvgNode progress={progress} reduced={reduced} threshold={0.04} x={78} y={25} label="Intake" labelX={105} labelY={54} />
        <FlowSvgNode progress={progress} reduced={reduced} threshold={0.2} x={384} y={13} label="Generate" labelX={430} labelY={40} />
        <FlowSvgNode progress={progress} reduced={reduced} threshold={0.52} x={380} y={340} label="Review" labelX={380} labelY={366} />
        <FlowSvgNode progress={progress} reduced={reduced} threshold={0.86} x={328} y={508} label="Run" labelX={372} labelY={528} />
        <FlowSvgNode progress={progress} reduced={reduced} threshold={0.985} x={33} y={554} label="Monitor" labelX={61} labelY={589} />
      </svg>
    </div>
  )
}

function LeadingDot({ pathData, progress, reduced }) {
  const pathRef = useRef(null)
  const [dotPoint, setDotPoint] = useState({ x: 78, y: 25, opacity: 0 })

  useEffect(() => {
    if (reduced || !pathRef.current) return undefined

    const path = pathRef.current
    const length = path.getTotalLength()

    const updateDotPoint = (latest) => {
      const clampedProgress = Math.min(Math.max(latest, 0), 1)
      const point = path.getPointAtLength(clampedProgress * length)
      const entryOpacity = Math.min(clampedProgress / 0.012, 1)
      const exitOpacity = Math.min((1 - clampedProgress) / 0.012, 1)

      setDotPoint({
        x: point.x,
        y: point.y,
        opacity: Math.max(0, Math.min(entryOpacity, exitOpacity))
      })
    }

    updateDotPoint(progress.get ? progress.get() : 0)

    return progress.on('change', updateDotPoint)
  }, [pathData, progress, reduced])

  if (reduced) {
    return (
      <g opacity="0.95" filter="url(#dot-glow)">
        <circle cx={33} cy={554} r="10" fill="none" stroke="rgba(147,197,253,0.5)" strokeWidth="1.5" />
        <circle cx={33} cy={554} r="6.5" fill="rgba(7,17,31,0.9)" stroke="rgba(255,255,255,0.9)" strokeWidth="2" />
        <circle cx={33} cy={554} r="3" fill="white" />
      </g>
    )
  }

  return (
    <>
      <path ref={pathRef} d={pathData} fill="none" stroke="none" />
      <g
        style={{ opacity: dotPoint.opacity }}
        transform={`translate(${dotPoint.x} ${dotPoint.y})`}
        filter="url(#dot-glow)"
      >
      <circle
        cx="0"
        cy="0"
        r="10"
        fill="none"
        stroke="rgba(147,197,253,0.5)"
        strokeWidth="1.5"
      />
      <circle
        cx="0"
        cy="0"
        r="6.5"
        fill="rgba(7,17,31,0.9)"
        stroke="rgba(255,255,255,0.9)"
        strokeWidth="2"
      />
      <circle
        cx="0"
        cy="0"
        r="3"
        fill="white"
      />
      </g>
    </>
  )
}

function FlowSvgNode({ progress, reduced, threshold, x, y, label, labelX, labelY }) {
  const liveOpacity = useTransform(progress, [Math.max(0, threshold - 0.045), threshold], [0, 1])
  const opacity = reduced ? 1 : liveOpacity

  return (
    <motion.g style={{ opacity }}>
      <circle cx={x} cy={y} r="13" fill="none" stroke="rgba(96,165,250,0.25)" strokeWidth="1" />
      <circle cx={x} cy={y} r="9" fill="rgba(7,17,31,0.95)" stroke="rgba(255,255,255,0.85)" strokeWidth="2" />
      <circle cx={x} cy={y} r="4" fill="#dbeafe" filter="url(#pipe-inner-glow)" />
      <text
        x={labelX}
        y={labelY}
        fill="#dbeafe"
        fontFamily="inherit"
        fontSize="12"
        fontWeight="600"
        letterSpacing="0"
        style={{ textShadow: '0 2px 10px rgba(2,6,23,0.82)' }}
        textAnchor="middle"
      >
        {label}
      </text>
    </motion.g>
  )
}

function WorkflowCard({ step }) {
  const Icon = step.icon

  return (
    <div className="min-h-[150px] rounded-lg border border-white/10 bg-[#0c1218]/[0.86] p-4 transition hover:border-[#60a5fa]/[0.35] hover:bg-[#101a21]">
      <div className="mb-3 flex items-center justify-between gap-3">
        <h3 className="min-w-0 text-base font-semibold text-white">
          <span className="mr-2 text-sm font-semibold text-[#8fa0ae]">{step.eyebrow}</span>
          {step.title}
        </h3>
        <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-white/[0.08] text-[#60a5fa]">
          <Icon className="h-4 w-4" strokeWidth={1.8} />
        </div>
      </div>
      <p className="text-sm leading-5 text-[#aebcca]">{step.description}</p>
    </div>
  )
}

function CapabilityCard({ capability }) {
  const Icon = capability.icon

  return (
    <div className="min-h-[154px] rounded-lg border border-white/10 bg-[#0d121a] p-4 shadow-[0_16px_38px_rgba(2,6,23,0.20)]">
      <div className="mb-4 flex items-center gap-3 border-b border-white/[0.06] pb-3">
        <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-[#15232c] text-[#93c5fd]">
          <Icon className="h-5 w-5" strokeWidth={1.8} />
        </div>
        <h3 className="text-base font-semibold leading-5 text-white">{capability.title}</h3>
      </div>
      <p className="text-sm leading-6 text-[#aebcca]">{capability.description}</p>
    </div>
  )
}

export default LandingPage
