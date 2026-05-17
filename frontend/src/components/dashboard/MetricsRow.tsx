// @ts-nocheck
import React from 'react'

function MetricsRow({ metrics }) {
  return (
    <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
      {metrics.map((metric) => (
        <article
          key={metric.label}
          className="rounded-xl border border-bg-border bg-bg-card px-4 py-4 transition-colors hover:border-accent-blue/30"
        >
          <p className="text-sm text-text-tertiary">{metric.label}</p>
          <div className="mt-3 flex items-end justify-between gap-3">
            <p className="text-3xl font-semibold tracking-[-0.04em] text-text-primary">{metric.value}</p>
            <p className={`text-xs font-medium ${metric.trendColor}`}>
              {metric.change}
            </p>
          </div>
        </article>
      ))}
    </section>
  )
}

export default MetricsRow

