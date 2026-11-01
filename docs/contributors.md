# Persistent browser contributors

Register a browser node, wait for **Connected · storage verified**, and click **Contribute compute** on that device. Opt-in is per node. Participation continues while the tab is open, including when the queue is empty or every remaining batch is leased elsewhere. **Stop contributing** cancels its current attempt and releases the exact lease when the coordinator can be reached.

The runner discovers jobs in that node's cluster and processes one batch at a time. The manual **Run on this node** control runs a selected job using the same recovery machinery. Multiple participating nodes can claim independent batches. Their immutable checkpoints and the Rust generation/attempt/epoch checks prevent a late previous worker from replacing an accepted result.

## Device controls

- **Duty cycle** inserts a cooldown based on the preceding batch's active wall time, including checkpoint work. This is a scheduling target, not a measured CPU/GPU utilization percentage.
- **Allow CPU fallback** permits a deterministic CPU implementation when WebGPU is unavailable or its device is lost. Initialization/device-loss reasons are shown. Numerical validation failures are rejected rather than silently substituted.
- **Pause new work when hidden** stops new claims while hidden. A current batch can finish; storage and node connectivity remain active. Background execution is best effort: a browser or operating system can freeze or suspend the page. Frozen or sleeping workers rely on server lease expiry and survivor takeover.
- **Remember this device** explicitly saves the node bearer credential and settings in localStorage so reopening the site can reconnect and resume contribution. It cannot run a closed tab. Unchecking removes the persistent copy and preserves only the current tab's session credential. **Remove node** revokes access on the coordinator. Ephemeral holder signing keys are not saved. Same-origin Web Locks prevent two tabs from repeatedly replacing the same node session.

## Retry and cancellation semantics

Idle waits have no fixed lifetime. A `work_available` WebSocket hint wakes the runner; a jittered two-to-five-second polling fallback covers missed hints. Temporary failures use a separate exponential backoff, so a worker's own release hint cannot trigger a tight retry loop.

Lease expiry/revocation cancels further transfers. Already submitted GPU work may finish, but its result is checked against the attempt signal before placement/commit. Recoverable failures abandon only that attempt; current authorization failures stop the contributor. Invalid job/validation requests are quarantined for that runner until it is restarted, allowing other jobs to proceed.

Transient commit failures first reconcile the job's committed digest. Retries keep the same immutable artifact and fenced attempt. A lost success response therefore does not create a new output. An acknowledged holder that disappears before commit causes one fresh full-copy placement attempt. Disconnected/failed targets are excluded temporarily, stable target ordering is rotated per worker, and explicit receiver-busy responses receive bounded jittered retries.

## Delivery and diagnostics

Critical coordinator messages use a bounded outbound queue. Queue failure invalidates that session and wakes its socket loop to close; reconnect rebuilds presence, signaling and holder-key registration. Outbound socket writes have deadlines. Only optional work hints may be coalesced/dropped because polling supplies their information. The server emits a secret-free session-resync event on delivery failure.

Malformed artifact starts with a usable transfer ID receive a NACK. Busy reads receive `artifact_busy`; neither path silently waits for the normal transfer deadline. Cancellation removes pending sender state and tells the receiver to reclaim an unfinished transfer. WebSocket handler failures are surfaced and trigger resynchronization.

Checkpoint diagnostics expose the computing node, lease generation, previous worker, and coordinator-observed claim/reassignment/failure-detection timestamps. A repeated generation may be a same-node retry; a different-worker takeover is labeled separately. Failure-detection time can be unknown, for example after lease expiry without a detected transport loss or after a coordinator restart. `committed_at_ms` records logical acceptance before snapshot persistence completes.

Execution mode, fallback reason, and compute/validation/encoding/transfer durations are **self-reported**. Compute duration includes input preparation and GPU submission/readback; CPU validation is reported separately. These fields are bounded metadata and are not proof of honest computation, useful throughput or contribution credits.

## Automated coverage and remaining limits

The browser suite covers participation beyond the old one-minute idle cap, automatic takeover after a compute-browser close, transient and ambiguous commits, remembered-device restoration, bounded receiver-busy retries, GPU-loss fallback, immediate lease cancellation, and a late result rejected after suspended renewal timers. Existing lease, receipt, sharding and repair scenarios remain in the suite. Rust tests cover queue overflow, exact-attempt release authorization, telemetry bounds and attempt observations.

The timer-suspension scenario uses Playwright clock controls to stop renewals without relying on real-time pages being eligible for browser freezing. It does not replace tests on real backgrounded/mobile/sleeping devices or different networks. Storage possession audits, artifact garbage collection, TURN configuration, binary transport, and execution in a dedicated Web Worker remain separate work. The current networking, signing and bounded executor continue to live on the main browser thread.
