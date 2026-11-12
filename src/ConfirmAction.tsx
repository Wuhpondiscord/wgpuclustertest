import { useEffect, useId, useRef, type ReactNode } from 'react'

export function ConfirmAction({ title, children, action, danger = false, onConfirm, onClose }: {
  title: string
  children: ReactNode
  action: string
  danger?: boolean
  onConfirm: () => void
  onClose: () => void
}) {
  const dialog = useRef<HTMLDialogElement>(null)
  const id = useId()
  useEffect(() => {
    const element = dialog.current!
    element.showModal()
    return () => element.close()
  }, [])
  return <dialog ref={dialog} className="credentials-dialog confirm-action" aria-labelledby={id}
    onCancel={event => { event.preventDefault(); onClose() }}>
    <h2 id={id}>{title}</h2>
    {children}
    <div className="heading-buttons dialog-actions">
      <button className="manage-button" type="button" autoFocus onClick={onClose}>Cancel</button>
      <button className={`manage-button primary${danger ? ' danger' : ''}`} type="button"
        onClick={() => { onClose(); onConfirm() }}>{action}</button>
    </div>
  </dialog>
}
