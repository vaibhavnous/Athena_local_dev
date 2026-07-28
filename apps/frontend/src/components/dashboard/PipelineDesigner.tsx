// @ts-nocheck
import React from 'react'
import { ArrowRight } from 'lucide-react'

function PipelineDesigner({ capabilities, actions }) {
  return (
    <section className="grid gap-6 rounded-xl border border-bg-border bg-bg-card p-6 lg:grid-cols-[1.7fr_1fr]">
      <div className="space-y-5">
        <div className="space-y-2">
          <h2 className="text-xl font-semibold tracking-[-0.03em] text-text-primary sm:text-2xl">
            Pipeline Designer & Builder
          </h2>
            <p className="text-sm text-text-secondary sm:text-base">
            Design, validate, and refine production-grade pipelines with AI-assisted workflows.
          </p>
        </div>

        <div className="flex flex-wrap gap-2">
          {capabilities.map((capability) => (
            <span
              key={capability}
              className="rounded-full border border-bg-border bg-bg-hover px-3 py-2 text-sm text-text-secondary"
            >
              {capability}
            </span>
          ))}
        </div>
      </div>

      <div className="space-y-3">
        <h3 className="text-sm font-medium text-text-secondary">Quick Actions</h3>
        <div className="divide-y divide-bg-border rounded-xl border border-bg-border bg-bg-base">
          {actions.map((action) => (
            <button
              key={action}
              className="flex w-full items-center justify-between px-4 py-4 text-left text-sm text-text-secondary transition-colors hover:bg-bg-hover"
            >
              <span>{action}</span>
              <ArrowRight size={16} className="text-gray-500" />
            </button>
          ))}
        </div>
      </div>
    </section>
  )
}

export default PipelineDesigner

