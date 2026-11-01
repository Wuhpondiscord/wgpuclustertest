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

Critical coordinator messages use a bounded outbound queue. Queue failure invalidates that session and wakes its socket loop to close; reconnect rebuilds presence, signaling and holder-key registration. Outbound socket writes have deadlines. Optional work hints may be coalesced/dropped because polling supplies their information; peer signaling is also non-critical, as detailed below. The server emits a secret-free session-resync event on delivery failure.

Work hints and peer signaling leave 16 slots free in the production 128-slot queue (smaller test queues reserve a quarter, at least one). The capacity check follows permit reservation, including concurrent optional producers. Untrusted peer signals never invalidate the destination on congestion. Each sending node's active session has token buckets allowing a burst of 128 messages/512 KiB, refilling at 32 messages/128 KiB per second; replacing its active socket preserves that budget. Disconnected sessions discard their runtime budget. These limits constrain forwarding, not raw WebSocket parsing or connection creation. Excess/congested signals are dropped; peer negotiation may need rediscovery/reconnection. Critical messages can still exhaust a queue independently and trigger resync.

Reconnect authorization checks only forget saved credentials after a successful node list confirms removal or an access denial has the coordinator's JSON error envelope. A bare proxy 401/403/404 preserves the saved device and retries. This envelope distinguishes coordinator errors from ordinary intermediary errors; it is not a cryptographic authentication scheme.

Contribution control changes propagate across same-origin tabs using storage events for remembered settings and BroadcastChannel for session-only nodes. Incoming settings replace stale session overrides without rebroadcasting. **Stop contributing** in a tab waiting for the node lock also stops the lock-holding tab. A delayed/frozen tab can only process that command when its JavaScript resumes; lease fencing still protects against expired work.

Malformed artifact starts with a usable transfer ID receive a NACK. Busy reads receive `artifact_busy`; neither path silently waits for the normal transfer deadline. Cancellation removes pending sender state and tells the receiver to reclaim an unfinished transfer. WebSocket handler failures are surfaced and trigger resynchronization.

Checkpoint diagnostics expose the computing node, lease generation, previous worker, and coordinator-observed claim/reassignment/failure-detection timestamps. A repeated generation may be a same-node retry; a different-worker takeover is labeled separately. Failure-detection time can be unknown in older checkpoints or after a coordinator restart without an observed predecessor. `committed_at_ms` records logical acceptance before snapshot persistence completes.

New observations include the previous attempt's end reason: `disconnect`, `expiry`, `release`, or `revoked` (pause/removal). Only disconnect and expiry populate failure detection. For expiry without a transport-loss event, detection is the first successful reclaim, not the estimated moment the device stopped. The UI shows coordinator detection → commit for those failures; voluntary release/revocation have separately labeled end → commit timing. Older records without a reason remain unknown rather than being relabeled as failures. The final timestamp is logical acceptance, so this metric excludes the remaining GitHub write and response-delivery time.

Execution mode, fallback reason, and compute/validation/encoding/transfer durations are **self-reported**. Compute duration includes input preparation and GPU submission/readback; CPU validation is reported separately. These fields are bounded metadata and are not proof of honest computation, useful throughput or contribution credits.

## Automated coverage and remaining limits

The browser suite covers participation beyond the old one-minute idle cap, automatic takeover after a compute-browser close, transient and ambiguous commits, remembered-device restoration, bounded receiver-busy retries, GPU-loss fallback, immediate lease cancellation, and a late result rejected after suspended renewal timers. Existing lease, receipt, sharding and repair scenarios remain in the suite. Rust tests cover queue overflow, exact-attempt release authorization, telemetry bounds and attempt observations.

The timer-suspension scenario uses Playwright clock controls to stop renewals without relying on real-time pages being eligible for browser freezing. It does not replace tests on real backgrounded/mobile/sleeping devices or different networks. Storage possession audits, artifact garbage collection, TURN configuration, binary transport, and execution in a dedicated Web Worker remain separate work. The current networking, signing and bounded executor continue to live on the main browser thread.

GPU numerical rejection uses a typed error. The invalid result is discarded and that job is quarantined on the runner until restart, even when CPU fallback is enabled; fallback remains for unavailable/lost devices. Other jobs continue. A temporary job-list refresh failure after an accepted commit does not release the completed lease or count as a compute retry.

Scheduling remains intended for small test clusters: a commit can wake every idle contributor, and each may scan/claim several jobs. Claims serialize with durable writes through the mutation lock. Per-node signaling limits do not fix that scheduling bottleneck; targeted work allocation and independent runtime claim serialization need a separate scale-focused change.
