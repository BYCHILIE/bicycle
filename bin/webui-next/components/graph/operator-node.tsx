'use client'

import { memo } from 'react'
import { Handle, Position } from '@xyflow/react'
import { getOperatorTypeColor, formatNumber } from '@/lib/utils'
import type { JobVertex } from '@/types'

interface OperatorNodeData extends JobVertex {
  label: string
}

interface OperatorNodeProps {
  data: OperatorNodeData
}

export const OperatorNode = memo(function OperatorNode({ data }: OperatorNodeProps) {
  const borderColor = getOperatorTypeColor(data.operator_type)
  const hasBackpressure = data.backpressure > 0.5

  return (
    <div
      className={`px-4 py-3 shadow-md rounded-lg bg-card border-2 min-w-[180px] ${
        hasBackpressure ? 'border-red-500' : ''
      }`}
      style={{ borderColor: hasBackpressure ? undefined : borderColor }}
    >
      <Handle type="target" position={Position.Top} className="!bg-muted-foreground" />

      <div className="flex flex-col gap-1">
        <div className="flex items-center justify-between gap-2">
          <span className="font-semibold text-sm truncate">{data.name}</span>
          <span
            className="text-xs px-1.5 py-0.5 rounded text-white"
            style={{ backgroundColor: borderColor }}
          >
            {data.operator_type}
          </span>
        </div>

        {data.uid && data.uid !== data.id && (
          <div className="text-xs text-muted-foreground/60 font-mono truncate">
            {data.uid}
          </div>
        )}

        <div className="text-xs text-muted-foreground">
          Parallelism: {data.parallelism}
        </div>

        <div className="flex gap-4 text-xs mt-1">
          <div>
            <span className="text-muted-foreground">In: </span>
            <span className="font-mono">{formatNumber(data.records_in)}</span>
          </div>
          <div>
            <span className="text-muted-foreground">Out: </span>
            <span className="font-mono">{formatNumber(data.records_out)}</span>
          </div>
        </div>

        {hasBackpressure && (
          <div className="mt-1">
            <div className="h-1.5 bg-muted rounded-full overflow-hidden">
              <div
                className="h-full bg-red-500 transition-all"
                style={{ width: `${data.backpressure * 100}%` }}
              />
            </div>
            <div className="text-xs text-red-500 mt-0.5">
              Backpressure: {(data.backpressure * 100).toFixed(0)}%
            </div>
          </div>
        )}
      </div>

      <Handle type="source" position={Position.Bottom} className="!bg-muted-foreground" />
    </div>
  )
})
