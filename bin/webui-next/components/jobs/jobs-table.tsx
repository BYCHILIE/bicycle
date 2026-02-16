'use client'

import { useState } from 'react'
import { useRouter } from 'next/navigation'
import { StopCircle } from 'lucide-react'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Button } from '@/components/ui/button'
import { Select } from '@/components/ui/select'
import { Skeleton } from '@/components/ui/skeleton'
import { JobStatusBadge } from './job-status-badge'
import { useJobs, useCancelJob } from '@/lib/hooks/useApi'
import { formatTimestamp } from '@/lib/utils'
import type { JobState } from '@/types'

const JOB_STATES: (JobState | 'All')[] = ['All', 'Running', 'Created', 'Finished', 'Failed', 'Canceled']

export function JobsTable() {
  const router = useRouter()
  const { data: jobs, isLoading } = useJobs()
  const cancelJob = useCancelJob()
  const [filter, setFilter] = useState<JobState | 'All'>('All')

  const filteredJobs = jobs?.filter((job) => filter === 'All' || job.state === filter) ?? []

  const handleCancel = async (e: React.MouseEvent, jobId: string) => {
    e.stopPropagation() // Prevent row click
    if (confirm('Are you sure you want to cancel this job?')) {
      await cancelJob.mutateAsync(jobId)
    }
  }

  const handleRowClick = (jobId: string) => {
    router.push(`/jobs/${jobId}`)
  }

  if (isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-10 w-48" />
        <div className="space-y-2">
          {[...Array(5)].map((_, i) => (
            <Skeleton key={i} className="h-16 w-full" />
          ))}
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2">
          <span className="text-sm text-muted-foreground">Filter by state:</span>
          <Select
            value={filter}
            onValueChange={(v) => setFilter(v as JobState | 'All')}
            className="w-36"
          >
            {JOB_STATES.map((state) => (
              <option key={state} value={state}>
                {state}
              </option>
            ))}
          </Select>
        </div>
        <span className="text-sm text-muted-foreground">
          {filteredJobs.length} job{filteredJobs.length !== 1 ? 's' : ''}
        </span>
      </div>

      {filteredJobs.length === 0 ? (
        <div className="text-center py-12 text-muted-foreground">No jobs found</div>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Tasks</TableHead>
              <TableHead>Start Time</TableHead>
              <TableHead className="text-right">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {filteredJobs.map((job) => (
              <TableRow
                key={job.job_id}
                onClick={() => handleRowClick(job.job_id)}
                className="cursor-pointer"
              >
                <TableCell>
                  <div className="font-medium text-primary">{job.name}</div>
                  <div className="text-xs text-muted-foreground font-mono">{job.job_id}</div>
                </TableCell>
                <TableCell>
                  <JobStatusBadge state={job.state} />
                </TableCell>
                <TableCell>
                  <span className="text-green-500">{job.tasks_running}</span>
                  <span className="text-muted-foreground"> / {job.tasks_total}</span>
                </TableCell>
                <TableCell className="text-muted-foreground">
                  {formatTimestamp(job.start_time)}
                </TableCell>
                <TableCell className="text-right">
                  {(job.state === 'Running' || job.state === 'Created') && (
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={(e) => handleCancel(e, job.job_id)}
                      disabled={cancelJob.isPending}
                    >
                      <StopCircle className="h-4 w-4 text-destructive" />
                    </Button>
                  )}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </div>
  )
}
