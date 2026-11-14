import type { HTMLAttributes } from 'react'
import { cn } from '../../lib/utils'

type BadgeProps = HTMLAttributes<HTMLSpanElement> & {
  tone?: 'green' | 'amber' | 'blue' | 'slate'
}

export function Badge({ className, tone = 'slate', ...props }: BadgeProps) {
  return <span className={cn('status-badge', `status-badge-${tone}`, className)} {...props} />
}
