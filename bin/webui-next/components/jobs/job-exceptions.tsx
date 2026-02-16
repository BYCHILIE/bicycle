'use client'

import { useState } from 'react'
import { ChevronDown, ChevronRight, AlertTriangle, Copy, Check } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useJobExceptions } from '@/lib/hooks/useApi'
import { formatTimestamp } from '@/lib/utils'
import type { JobException } from '@/types'

interface JobExceptionsProps {
  jobId: string
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false)

  const handleCopy = async (e: React.MouseEvent) => {
    e.stopPropagation()
    try {
      await navigator.clipboard.writeText(text)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {
      // Fallback for non-secure contexts
      const textarea = document.createElement('textarea')
      textarea.value = text
      document.body.appendChild(textarea)
      textarea.select()
      document.execCommand('copy')
      document.body.removeChild(textarea)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    }
  }

  return (
    <button
      onClick={handleCopy}
      className="inline-flex items-center gap-1 px-2 py-1 text-xs rounded border bg-background hover:bg-muted transition-colors"
      title="Copy to clipboard"
    >
      {copied ? (
        <>
          <Check className="h-3 w-3 text-green-500" />
          <span className="text-green-500">Copied</span>
        </>
      ) : (
        <>
          <Copy className="h-3 w-3" />
          <span>Copy</span>
        </>
      )}
    </button>
  )
}

function buildCopyText(exception: JobException): string {
  const parts: string[] = []
  parts.push(`Operator: ${exception.operator_name}`)
  if (exception.task_index !== null) {
    parts.push(`Subtask: #${exception.task_index}`)
  }
  parts.push(`Task ID: ${exception.task_id}`)
  parts.push(`Timestamp: ${formatTimestamp(exception.timestamp)}`)
  if (exception.location) {
    parts.push(`Location: ${exception.location}`)
  }
  if (exception.root_cause) {
    parts.push(`Root Cause: ${exception.root_cause}`)
  }
  parts.push('')
  parts.push('Error Message:')
  parts.push(exception.message)
  if (exception.stack_trace && exception.stack_trace !== exception.message) {
    parts.push('')
    parts.push('Stack Trace:')
    parts.push(exception.stack_trace)
  }
  return parts.join('\n')
}

export function JobExceptions({ jobId }: JobExceptionsProps) {
  const { data, isLoading, error } = useJobExceptions(jobId)
  const [expandedIds, setExpandedIds] = useState<Set<number>>(new Set())

  const toggleExpand = (index: number) => {
    setExpandedIds((prev) => {
      const next = new Set(prev)
      if (next.has(index)) {
        next.delete(index)
      } else {
        next.add(index)
      }
      return next
    })
  }

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Exceptions</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {[...Array(3)].map((_, i) => (
              <Skeleton key={i} className="h-20 w-full" />
            ))}
          </div>
        </CardContent>
      </Card>
    )
  }

  if (error) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Exceptions</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center py-8 text-destructive">Failed to load exceptions</div>
        </CardContent>
      </Card>
    )
  }

  const exceptions = data?.exceptions ?? []

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <AlertTriangle className="h-5 w-5 text-destructive" />
          Exceptions ({exceptions.length})
        </CardTitle>
      </CardHeader>
      <CardContent>
        {exceptions.length === 0 ? (
          <div className="text-center py-8 text-muted-foreground">No exceptions</div>
        ) : (
          <div className="space-y-3">
            {exceptions.map((exception, index) => {
              const isExpanded = expandedIds.has(index)
              return (
                <div
                  key={index}
                  className="border rounded-lg overflow-hidden"
                >
                  <button
                    className="w-full px-4 py-3 flex items-start gap-3 text-left hover:bg-muted/50 transition-colors"
                    onClick={() => toggleExpand(index)}
                  >
                    {isExpanded ? (
                      <ChevronDown className="h-4 w-4 mt-0.5 flex-shrink-0" />
                    ) : (
                      <ChevronRight className="h-4 w-4 mt-0.5 flex-shrink-0" />
                    )}
                    <div className="flex-1 min-w-0">
                      <div className="font-semibold text-sm">
                        {exception.operator_name}
                        {exception.task_index !== null && (
                          <span className="text-muted-foreground font-normal ml-1">
                            #{exception.task_index}
                          </span>
                        )}
                      </div>
                      <div className="font-medium text-destructive text-sm mt-1 break-words">
                        {exception.message.split('\n')[0]}
                      </div>
                      <div className="text-xs text-muted-foreground mt-1.5 flex flex-wrap gap-x-4 gap-y-1">
                        {exception.location && (
                          <span className="font-medium">
                            {exception.location}
                          </span>
                        )}
                        <span>{formatTimestamp(exception.timestamp)}</span>
                      </div>
                    </div>
                  </button>
                  {isExpanded && (
                    <div className="px-4 pb-4 pt-2 border-t bg-muted/30 space-y-3">
                      {/* Metadata row */}
                      <div className="flex flex-wrap gap-x-6 gap-y-1 text-xs text-muted-foreground">
                        <span>
                          <span className="font-medium">Task ID:</span>{' '}
                          <code className="text-foreground">{exception.task_id}</code>
                        </span>
                        {exception.task_index !== null && (
                          <span>
                            <span className="font-medium">Subtask:</span>{' '}
                            <code className="text-foreground">#{exception.task_index}</code>
                          </span>
                        )}
                      </div>

                      {/* Location */}
                      {exception.location && (
                        <div>
                          <div className="text-xs font-medium text-muted-foreground mb-1">
                            Location
                          </div>
                          <div className="text-sm font-mono text-orange-500 dark:text-orange-400">
                            {exception.location}
                          </div>
                        </div>
                      )}

                      {/* Root Cause */}
                      {exception.root_cause && (
                        <div>
                          <div className="text-xs font-medium text-muted-foreground mb-1">
                            Root Cause
                          </div>
                          <div className="text-sm text-destructive">
                            {exception.root_cause}
                          </div>
                        </div>
                      )}

                      {/* Error / Stack Trace - copyable */}
                      <div>
                        <div className="flex items-center justify-between mb-1">
                          <div className="text-xs font-medium text-muted-foreground">
                            Stack Trace
                          </div>
                          <CopyButton text={buildCopyText(exception)} />
                        </div>
                        <pre className="text-xs font-mono bg-background p-3 rounded border overflow-x-auto max-h-80 whitespace-pre-wrap break-words select-all">
                          {exception.stack_trace}
                        </pre>
                      </div>
                    </div>
                  )}
                </div>
              )
            })}
            {data?.truncated && (
              <div className="text-center text-sm text-muted-foreground">
                Some exceptions have been truncated
              </div>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  )
}
