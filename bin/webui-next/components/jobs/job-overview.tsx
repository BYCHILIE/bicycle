'use client'

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { JobStatusBadge } from './job-status-badge'
import { useJob } from '@/lib/hooks/useApi'
import { formatBytes, formatNumber, formatTimestamp, formatDuration } from '@/lib/utils'

interface JobOverviewProps {
  jobId: string
}

export function JobOverview({ jobId }: JobOverviewProps) {
  const { data: job, isLoading, error } = useJob(jobId)

  if (isLoading) {
    return (
      <div className="grid gap-4 md:grid-cols-2">
        {[...Array(4)].map((_, i) => (
          <Card key={i}>
            <CardHeader>
              <Skeleton className="h-5 w-24" />
            </CardHeader>
            <CardContent>
              <Skeleton className="h-8 w-32" />
            </CardContent>
          </Card>
        ))}
      </div>
    )
  }

  if (error) {
    return (
      <div className="text-center py-12 text-destructive">
        Failed to load job details: {error.message}
      </div>
    )
  }

  if (!job) {
    return (
      <div className="text-center py-12 text-muted-foreground">
        Job not found
      </div>
    )
  }

  // Safely handle timestamps - they might be in seconds or milliseconds
  const startTime = job.start_time > 1e12 ? job.start_time : job.start_time * 1000
  const endTime = job.end_time ? (job.end_time > 1e12 ? job.end_time : job.end_time * 1000) : null
  const duration = endTime ? endTime - startTime : Date.now() - startTime

  // Safely access metrics with defaults
  const metrics = job.metrics || { records_in: 0, records_out: 0, bytes_in: 0, bytes_out: 0 }

  return (
    <div className="space-y-6">
      {/* Status and Timing */}
      <div className="grid gap-4 md:grid-cols-3">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Status</CardTitle>
          </CardHeader>
          <CardContent>
            <JobStatusBadge state={job.state} />
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Start Time</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-lg font-semibold">{formatTimestamp(startTime)}</div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Duration</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-lg font-semibold">{formatDuration(duration)}</div>
          </CardContent>
        </Card>
      </div>

      {/* Metrics */}
      <Card>
        <CardHeader>
          <CardTitle>Metrics</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
            <div>
              <div className="text-sm text-muted-foreground">Records In</div>
              <div className="text-2xl font-bold">{formatNumber(metrics.records_in)}</div>
            </div>
            <div>
              <div className="text-sm text-muted-foreground">Records Out</div>
              <div className="text-2xl font-bold">{formatNumber(metrics.records_out)}</div>
            </div>
            <div>
              <div className="text-sm text-muted-foreground">Bytes In</div>
              <div className="text-2xl font-bold">{formatBytes(metrics.bytes_in)}</div>
            </div>
            <div>
              <div className="text-sm text-muted-foreground">Bytes Out</div>
              <div className="text-2xl font-bold">{formatBytes(metrics.bytes_out)}</div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Configuration */}
      <Card>
        <CardHeader>
          <CardTitle>Configuration</CardTitle>
        </CardHeader>
        <CardContent>
          <dl className="grid grid-cols-2 gap-4 text-sm">
            <div>
              <dt className="text-muted-foreground">Name</dt>
              <dd className="font-medium">{job.name}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Job ID</dt>
              <dd className="font-mono break-all text-muted-foreground">{job.job_id}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">End Time</dt>
              <dd>{endTime ? formatTimestamp(endTime) : '-'}</dd>
            </div>
          </dl>
        </CardContent>
      </Card>

      {/* Debug info - remove in production */}
      {process.env.NODE_ENV === 'development' && (
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Debug: Raw API Response</CardTitle>
          </CardHeader>
          <CardContent>
            <pre className="text-xs overflow-auto bg-muted p-2 rounded">
              {JSON.stringify(job, null, 2)}
            </pre>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
