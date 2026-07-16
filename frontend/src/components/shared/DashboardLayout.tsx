import React from 'react'
import { LucideIcon } from 'lucide-react'

export function cx(...classes: Array<string | false | null | undefined>) {
  return classes.filter(Boolean).join(' ')
}

type PageFrameProps = {
  children: React.ReactNode
  maxWidth?: string
  className?: string
}

export function PageFrame({ children, maxWidth = 'max-w-[1500px]', className }: PageFrameProps) {
  return (
    <div className={cx('min-h-full text-text-primary', className)}>
      <div className={cx('mx-auto flex w-full flex-col gap-3 pb-6', maxWidth)}>
        {children}
      </div>
    </div>
  )
}

type PageHeaderProps = {
  eyebrow?: string
  title: string
  description?: React.ReactNode
  icon?: LucideIcon
  leadingAction?: React.ReactNode
  actions?: React.ReactNode
  compact?: boolean
}

export function PageHeader({ eyebrow, title, description, icon: Icon, leadingAction, actions, compact = true }: PageHeaderProps) {
  const iconBadge = eyebrow && Icon ? (
    <div
      className={cx(
        'flex shrink-0 items-center justify-center rounded-lg border border-accent-blue/20 bg-accent-blue/10 text-accent-blue',
        compact ? 'h-8 w-8' : 'h-10 w-10',
      )}
      aria-label={eyebrow}
      title={eyebrow}
    >
      <Icon size={compact ? 15 : 18} />
    </div>
  ) : null

  return (
    <section className="overflow-hidden rounded-lg border border-bg-border bg-bg-card shadow-card">
      <div className={cx('relative', compact ? 'p-3 sm:p-4' : 'p-5 sm:p-6')}>
        {!actions && iconBadge && <div className="absolute right-3 top-3">{iconBadge}</div>}
        <div className={cx('relative z-10 flex flex-col lg:flex-row lg:items-center lg:justify-between', compact ? 'gap-3' : 'gap-5')}>
          <div className="flex min-w-0 items-start gap-3">
            {leadingAction}
            <div className={cx('max-w-3xl min-w-0', !actions && iconBadge ? 'pr-12 lg:pr-0' : '')}>
              <h1 className={cx(
                'font-semibold text-text-primary',
                compact ? 'text-xl sm:text-2xl' : 'text-2xl sm:text-3xl',
              )}>
                {title}
              </h1>
              {description && (
                <div className={cx(
                  'max-w-3xl text-text-tertiary',
                  compact ? 'mt-2 text-xs leading-5 sm:text-sm' : 'mt-3 text-sm leading-6 sm:text-base',
                )}>
                  {description}
                </div>
              )}
            </div>
          </div>
          {actions && (
            <div className={cx('flex flex-col sm:flex-row sm:items-center lg:justify-end', compact ? 'gap-2' : 'gap-3')}>
              {iconBadge}
              {actions}
            </div>
          )}
        </div>
      </div>
    </section>
  )
}

type PanelProps = {
  children: React.ReactNode
  className?: string
}

export function DashboardPanel({ children, className }: PanelProps) {
  return (
    <section className={cx('rounded-lg border border-bg-border bg-bg-card p-4 shadow-card', className)}>
      {children}
    </section>
  )
}

type SectionTitleProps = {
  eyebrow?: string
  title: string
  description?: string
  action?: React.ReactNode
}

export function SectionTitle({ eyebrow, title, description, action }: SectionTitleProps) {
  return (
    <div className="mb-3 flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
      <div>
        {eyebrow && <p className="text-xs font-semibold uppercase text-accent-blue">{eyebrow}</p>}
        <h2 className={cx(eyebrow ? 'mt-1' : '', 'text-base font-semibold text-text-primary')}>{title}</h2>
        {description && <p className="mt-2 text-sm leading-5 text-text-tertiary">{description}</p>}
      </div>
      {action}
    </div>
  )
}

type StatTileProps = {
  label: string
  value: string | number
  detail?: string
  icon?: LucideIcon
  tone?: 'blue' | 'green' | 'amber' | 'red' | 'purple'
}

const toneClasses = {
  blue: 'border-accent-blue/20 bg-accent-blue/10 text-accent-blue',
  green: 'border-accent-green/20 bg-accent-green/10 text-accent-green',
  amber: 'border-accent-amber/20 bg-accent-amber/10 text-accent-amber',
  red: 'border-accent-red/20 bg-accent-red/10 text-accent-red',
  purple: 'border-accent-purple/20 bg-accent-purple/10 text-accent-purple',
}

export function StatTile({ label, value, detail, icon: Icon, tone = 'blue' }: StatTileProps) {
  return (
    <div className="relative overflow-hidden rounded-lg border border-bg-border bg-bg-card p-3 shadow-card">
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <p className="text-[11px] font-medium uppercase text-text-muted">{label}</p>
          <p className="mt-2 truncate text-2xl font-semibold text-text-primary">{value}</p>
        </div>
        {Icon && (
          <div className={cx('rounded-md border p-2', toneClasses[tone])}>
            <Icon size={16} />
          </div>
        )}
      </div>
      {detail && <p className="mt-2 text-xs leading-5 text-text-tertiary">{detail}</p>}
    </div>
  )
}

type StatusPillProps = {
  children: React.ReactNode
  tone?: 'blue' | 'green' | 'amber' | 'red' | 'neutral' | 'purple'
}

const pillClasses = {
  blue: 'border-accent-blue/20 bg-accent-blue/10 text-accent-blue',
  green: 'border-accent-green/20 bg-accent-green/10 text-accent-green',
  amber: 'border-accent-amber/20 bg-accent-amber/10 text-accent-amber',
  red: 'border-accent-red/20 bg-accent-red/10 text-accent-red',
  purple: 'border-accent-purple/20 bg-accent-purple/10 text-accent-purple',
  neutral: 'border-bg-border bg-bg-base text-text-tertiary',
}

export function StatusPill({ children, tone = 'neutral' }: StatusPillProps) {
  return (
    <span className={cx('inline-flex items-center gap-1 rounded-full border px-2.5 py-1 text-xs font-semibold', pillClasses[tone])}>
      {children}
    </span>
  )
}
