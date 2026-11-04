import type { ContributionSettings } from './contribution-settings'
import type { RunnerState } from './compute-runner'

export function ContributorControls({ settings, state, remembered, onToggle, onChange, onRemember }: {
  settings: ContributionSettings; state?: RunnerState; remembered: boolean;
  onToggle: () => void; onChange: (patch: Partial<ContributionSettings>) => void; onRemember: (remember: boolean) => void;
}) {
  const running = Boolean(state && !['stopped', 'error'].includes(state.phase))
  return <div className="contributor-controls">
    <div className="contributor-heading">
      <button className={`manage-button small ${settings.enabled ? '' : 'primary'}`} onClick={onToggle}>
        {settings.enabled ? 'Stop contributing' : running ? 'Stop worker' : 'Contribute compute'}
      </button>
      <span role="status">{state?.message ?? 'Compute participation is off'}</span>
    </div>
    <div className="contributor-options">
      <label>Duty cycle <select aria-label="Contribution duty cycle" value={settings.dutyCycle} onChange={event => onChange({ dutyCycle: Number(event.target.value) })}>
        {[10, 25, 50, 100].map(value => <option key={value} value={value}>{value}%</option>)}
      </select></label>
      <label><input type="checkbox" checked={settings.allowCpuFallback} onChange={event => onChange({ allowCpuFallback: event.target.checked })}/>Allow CPU fallback</label>
      <label><input type="checkbox" checked={settings.allowInference} onChange={event => onChange({ allowInference: event.target.checked })}/>ML inference (downloads a ~90 MB model plus runtime)</label>
      <label><input type="checkbox" checked={settings.pauseWhenHidden} onChange={event => onChange({ pauseWhenHidden: event.target.checked })}/>Pause new work when hidden</label>
      <label><input type="checkbox" checked={remembered} onChange={event => onRemember(event.target.checked)}/>Remember this device</label>
    </div>
    <small>One batch at a time per node. Keep the tab open; browsers may suspend background work.</small>
    {remembered && <small>This browser stores a bearer credential and contribution settings. Forgetting removes the local saved copy; Remove node revokes access.</small>}
    {state && <small>{state.completed} batches completed · {state.retries} retries{state.executionMode ? ` · ${state.executionMode}` : ''}{state.fallbackReason ? ` · ${state.fallbackReason}` : ''}</small>}
    {state?.lastError && <small>Last worker error: {state.lastError}</small>}
  </div>
}
