import { useId, useRef, useState, type ReactNode } from 'react'
import { ChevronDown, PanelLeft } from 'lucide-react'

export function WorkspaceRail({ label, children }: {
  label: string
  children: (close: () => void) => ReactNode
}) {
  const [open, setOpen] = useState(false)
  const id = useId()
  const toggle = useRef<HTMLButtonElement>(null)
  function close() {
    if (open && toggle.current?.getClientRects().length) toggle.current.focus()
    setOpen(false)
  }
  return <aside className="workspace-rail" aria-label={label} data-open={open}>
    <button ref={toggle} className="workspace-menu-toggle" type="button" aria-expanded={open} aria-controls={id}
      onClick={() => setOpen(value => !value)}>
      <PanelLeft size={17}/><span>{label}</span><ChevronDown size={16}/>
    </button>
    <div id={id} className="workspace-rail-content">{children(close)}</div>
  </aside>
}
