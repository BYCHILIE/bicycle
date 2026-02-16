'use client'

import { useParams, usePathname } from 'next/navigation'
import Link from 'next/link'
import { cn } from '@/lib/utils'
import { useJob } from '@/lib/hooks/useApi'

const tabs = [
  { href: '', label: 'Overview' },
  { href: '/graph', label: 'Graph' },
  { href: '/tasks', label: 'Tasks' },
  { href: '/checkpoints', label: 'Checkpoints' },
  { href: '/exceptions', label: 'Exceptions' },
]

export default function JobLayout({ children }: { children: React.ReactNode }) {
  const params = useParams()
  const pathname = usePathname()
  const jobId = params.jobId as string
  const basePath = `/jobs/${jobId}`
  const { data: job } = useJob(jobId)

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">{job?.name || 'Job Details'}</h1>
        <p className="text-sm text-muted-foreground font-mono">{jobId}</p>
      </div>

      <nav className="flex gap-1 border-b">
        {tabs.map((tab) => {
          const href = `${basePath}${tab.href}`
          const isActive = pathname === href || (tab.href === '' && pathname === basePath)

          return (
            <Link
              key={tab.href}
              href={href}
              className={cn(
                'px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors',
                isActive
                  ? 'border-primary text-primary'
                  : 'border-transparent text-muted-foreground hover:text-foreground'
              )}
            >
              {tab.label}
            </Link>
          )
        })}
      </nav>

      {children}
    </div>
  )
}
