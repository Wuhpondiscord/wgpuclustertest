import { Boxes, Layers3 } from 'lucide-react'

export type Workspace = 'clusters' | 'applications'

export function WorkspaceNav({ current, onChange }: { current: Workspace; onChange?: (value: Workspace) => void }) {
  return <nav className="workspace-nav" aria-label="Workspace">
    {(['clusters', 'applications'] as const).map(value => <a key={value}
      href={`/#${value}`} aria-current={current === value ? 'page' : undefined}
      onClick={onChange ? event => { event.preventDefault(); location.hash = value; onChange(value) } : undefined}>
      {value === 'clusters' ? <><Layers3 size={16}/>Clusters</> : <><Boxes size={16}/>Applications</>}
    </a>)}
  </nav>
}
