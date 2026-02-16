'use client'

import { useRouter } from 'next/navigation'
import { ArrowRight } from 'lucide-react'
import Link from 'next/link'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Skeleton } from '@/components/ui/skeleton'
import { Button } from '@/components/ui/button'
import { JobStatusBadge } from '@/components/jobs/job-status-badge'
import { useJobs } from '@/lib/hooks/useApi'
import { formatTimestamp } from '@/lib/utils'

export function RecentJobs() {
  const router = useRouter()
  const { data: jobs, isLoading } = useJobs()

  const recentJobs = jobs?.slice(0, 10) ?? []

  const handleRowClick = (jobId: string) => {
    router.push(`/jobs/${jobId}`)
  }

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between">
        <CardTitle>Recent Jobs</CardTitle>
        <Link href="/jobs">
          <Button variant="ghost" size="sm" className="gap-1">
            View All <ArrowRight className="h-4 w-4" />
          </Button>
        </Link>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="space-y-3">
            {[...Array(5)].map((_, i) => (
              <Skeleton key={i} className="h-12 w-full" />
            ))}
          </div>
        ) : recentJobs.length === 0 ? (
          <div className="text-center py-8 text-muted-foreground">No jobs found</div>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Tasks</TableHead>
                <TableHead>Start Time</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {recentJobs.map((job) => (
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
                    {job.tasks_running}/{job.tasks_total}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {formatTimestamp(job.start_time)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </CardContent>
    </Card>
  )
}
