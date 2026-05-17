// @ts-nocheck
import React from 'react'
import { Plus } from 'lucide-react'

function Header() {
  return (
    <section className="flex flex-col gap-4 border-b border-bg-border pb-6 md:flex-row md:items-end md:justify-between">
      <div className="space-y-2">
        <h1 className="text-3xl font-semibold tracking-[-0.04em] text-text-primary sm:text-4xl">
          Pipeline Dashboard
        </h1>
        <p className="text-sm text-text-secondary sm:text-base">
          Monitor pipeline health, team activity, and delivery readiness across your workspace.
        </p>
      </div>

      <button className="inline-flex h-10 items-center justify-center gap-2 rounded-lg border border-bg-border bg-bg-hover text-text-primary font-medium text-sm transition-colors hover:bg-bg-border">
        <Plus size={16} strokeWidth={2} />
        Create Pipeline
      </button>
    </section>
  )
}

export default Header

