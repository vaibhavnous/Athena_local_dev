// @ts-nocheck
import React from 'react'
import { Link } from 'react-router-dom'
import { ChevronDown, Code2, Database, Play, Sparkles, Workflow, Zap, Brain } from 'lucide-react'

const stats = [
  { value: '80%', label: 'Faster Pipeline Development', gradient: 'from-[#668cff] to-[#8b82ff]' },
  { value: '100%', label: 'Code Quality Assurance', gradient: 'from-[#d56bff] to-[#ff76bd]' },
  { value: '50+', label: 'Enterprise Customers', gradient: 'from-[#b388ff] to-[#cf86ff]' }
]

const features = [
  {
    title: 'AI-Powered Workflow',
    description: 'Transform your requirements into detailed pipeline specifications with intelligent AI assistance.',
    icon: Sparkles
  },
  {
    title: 'Multi-Platform Support',
    description: 'Seamlessly work with Databricks and Microsoft Fabric platforms from a single interface.',
    icon: Database
  },
  {
    title: 'Auto-Code Generation',
    description: 'Generate production-ready PySpark code following medallion architecture best practices.',
    icon: Code2
  },
  {
    title: 'Real-Time Orchestration',
    description: 'Monitor and manage your data pipelines with live execution tracking and alerts.',
    icon: Zap
  }
]

function LandingPage() {
  return (
    <div className="min-h-screen overflow-x-hidden bg-[#0a0f28] text-white">
      <div className="relative min-h-screen bg-[radial-gradient(circle_at_top,_rgba(108,123,255,0.16),_transparent_30%),linear-gradient(180deg,#141b46_0%,#1d275a_48%,#1d275a_100%)]">
        <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(9,14,35,0.2)_0%,rgba(9,14,35,0.5)_100%)]" />

        <header className="relative z-10 border-b border-white/8 bg-[#121833]/95 backdrop-blur">
          <div className="mx-auto flex max-w-[1280px] items-center justify-between px-6 py-4">
            <div className="flex items-center gap-2">
              <Brain className="h-5 w-5 text-white" strokeWidth={2.2} />
              <span className="text-lg font-semibold tracking-tight text-white">
                Athena
              </span>
            </div>

            <Link
              to="/app"
              className="inline-flex h-9 items-center justify-center rounded-xl border border-white/10 bg-[#0a1020] px-4 text-xs font-semibold text-white transition hover:border-white/20 hover:bg-[#10182c]"
            >
              Sign In
            </Link>
          </div>
        </header>

        <main className="relative z-10">
          <section className="mx-auto flex min-h-[calc(100vh-80px)] max-w-[1280px] flex-col items-center px-6 pb-20 pt-20 text-center sm:pt-28">
            <div className="mb-6 inline-flex items-center gap-2 rounded-full border border-[#294785] bg-[#1f3a73]/45 px-4 py-2 text-xs font-medium text-[#61a6ff] shadow-[inset_0_1px_0_rgba(255,255,255,0.05)] sm:text-sm">
              <Sparkles className="h-4 w-4" strokeWidth={1.8} />
              AI-Powered Data Engineering Platform
            </div>

            <h1 className="max-w-[900px] text-3xl font-semibold leading-[1.1] tracking-[-0.02em] text-white sm:text-5xl lg:text-5xl">
              Streamline your{' '}
              <span className="bg-gradient-to-r from-[#61a6ff] via-[#9d7dff] to-[#ff6eb2] bg-clip-text text-transparent">
                Data Life Cycle
              </span>{' '}
              with AI-Powered Data Engineering
            </h1>

            <p className="mt-6 max-w-[700px] text-sm leading-[1.6] text-[#d7deef] sm:text-lg">
              Transform your data engineering requirements into production-ready artifacts in minutes.
              Our AI-powered platform automates pipeline design, code generation, and orchestration
              across Databricks and Microsoft Fabric.
            </p>

            <div className="mt-10 flex flex-col items-center gap-4 sm:flex-row">
              <Link
                to="/app"
                className="inline-flex h-12 min-w-[160px] items-center justify-center gap-2 rounded-xl bg-gradient-to-r from-[#2e66f5] to-[#a034ef] px-6 text-sm font-medium text-white shadow-[0_12px_30px_rgba(87,78,255,0.24)] transition hover:scale-[1.02] hover:shadow-[0_15px_35px_rgba(108,92,255,0.28)]"
              >
                Get Started
                <Zap className="h-4 w-4" strokeWidth={2.2} />
              </Link>

              <a
                href="#features"
                className="inline-flex h-12 min-w-[160px] items-center justify-center gap-2 rounded-xl border border-white/15 bg-[#060d1d] px-6 text-sm font-medium text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.06)] transition hover:border-white/25 hover:bg-[#081123]"
              >
                <Play className="h-4 w-4" strokeWidth={2} />
                View Demo
                <ChevronDown className="h-4 w-4" strokeWidth={2} />
              </a>
            </div>
          </section>

          <section className="mx-auto max-w-[1280px] px-6 pb-20">
            <div className="grid gap-10 py-10 text-center md:grid-cols-3">
              {stats.map((stat) => (
                <div key={stat.label}>
                  <div className={`bg-gradient-to-r ${stat.gradient} bg-clip-text text-4xl font-semibold leading-none text-transparent sm:text-5xl`}>
                    {stat.value}
                  </div>
                  <div className="mt-2 text-sm text-[#aeb8d5] sm:text-base">{stat.label}</div>
                </div>
              ))}
            </div>
          </section>

          <section id="features" className="mx-auto max-w-[1280px] px-6 pb-20 pt-10">
            <div className="text-center">
              <h2 className="text-2xl font-semibold tracking-tight text-white sm:text-4xl">
                Everything You Need to Build Better Pipelines
              </h2>
              <p className="mx-auto mt-3 max-w-[700px] text-sm text-[#9dadcf] sm:text-base">
                Accelerate your data engineering workflow with intelligent automation
              </p>
            </div>

            <div className="mt-12 grid gap-6 lg:grid-cols-2">
              {features.map((feature) => (
                <FeatureCard key={feature.title} feature={feature} />
              ))}
            </div>
          </section>
        </main>

        <footer className="relative z-10 border-t border-white/8 bg-[linear-gradient(90deg,#111834_0%,#080d1d_100%)]">
          <div className="mx-auto flex max-w-[1280px] flex-col items-start justify-between gap-3 px-6 py-4 sm:flex-row sm:items-center">
            <span className="text-sm font-semibold tracking-tight text-white">Athena</span>
            <div className="text-xs text-[#aeb8d5]">© 2025 Athena. All rights reserved.</div>
          </div>
        </footer>
      </div>
    </div>
  )
}

function FeatureCard({ feature }) {
  const Icon = feature.icon

  return (
    <div className="rounded-2xl border border-white/10 bg-[linear-gradient(180deg,rgba(29,39,90,0.96)_0%,rgba(18,26,60,0.96)_100%)] p-6 shadow-[inset_0_1px_0_rgba(255,255,255,0.03)] sm:p-8">
      <div className="mb-4 inline-flex h-12 w-12 items-center justify-center rounded-xl bg-gradient-to-br from-[#4f8dff] to-[#9d34ec] text-white shadow-lg">
        <Icon className="h-6 w-6" strokeWidth={1.8} />
      </div>
      <h3 className="text-lg font-semibold tracking-tight text-white sm:text-xl">
        {feature.title}
      </h3>
      <p className="mt-2 max-w-[700px] text-sm leading-relaxed text-[#9aa6c5]">
        {feature.description}
      </p>
    </div>
  )
}

function BrandMark({ className }) {
  return (
    <div className={`flex items-center justify-center bg-gradient-to-br from-[#4d8cff] to-[#8e34ef] shadow-[0_18px_45px_rgba(98,84,255,0.28)] ${className}`}>
      <Workflow className="h-8 w-8 text-white" strokeWidth={2.2} />
    </div>
  )
}

function AthenaLogo({ size = 42 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden="true">
      <polygon
        points="16,2 28,9 28,23 16,30 4,23 4,9"
        fill="url(#landingHexGrad)"
        stroke="rgba(59,130,246,0.45)"
        strokeWidth="1"
      />
      <text
        x="16"
        y="21"
        textAnchor="middle"
        fill="white"
        fontSize="12"
        fontWeight="700"
        fontFamily="Inter, system-ui, sans-serif"
      >
        A
      </text>
      <defs>
        <linearGradient id="landingHexGrad" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stopColor="#3b82f6" />
          <stop offset="100%" stopColor="#8b5cf6" />
        </linearGradient>
      </defs>
    </svg>
  )
}

export default LandingPage

