import { Slot } from '@radix-ui/react-slot'
import { cva, type VariantProps } from 'class-variance-authority'
import type { ButtonHTMLAttributes } from 'react'
import { cn } from '../../lib/utils'

const buttonVariants = cva('ui-button', {
  variants: {
    variant: {
      primary: 'ui-button-primary',
      secondary: 'ui-button-secondary',
      ghost: 'ui-button-ghost',
      outline: 'ui-button-outline',
    },
    size: {
      default: 'ui-button-default',
      sm: 'ui-button-sm',
      icon: 'ui-button-icon',
    },
  },
  defaultVariants: { variant: 'primary', size: 'default' },
})

type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> &
  VariantProps<typeof buttonVariants> & { asChild?: boolean }

export function Button({ className, variant, size, asChild = false, ...props }: ButtonProps) {
  const Component = asChild ? Slot : 'button'
  return <Component className={cn(buttonVariants({ variant, size }), className)} {...props} />
}
