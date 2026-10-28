# Runtime review follow-up

The review was checked against main `6fdad9a`. This batch prioritizes correctness and reproducible disconnect trials. A passing trial does not imply production readiness.

| Finding | Result in this batch |
| --- | --- |
| 500k JSON vector artifacts exceed 8 MiB | Confirmed. Version 3 checkpoints encode f32 little-endian bytes as base64, under 3 MiB including metadata; every value is tested for exact round-trip preservation. |
| Unknown invite previews serialize writes and amplify GitHub reads | Confirmed. Invalid token shapes do no remote work. A coordinator-wide monotonic 60-second cooldown bounds refreshes, including different unknown tokens. GitHub reads run outside the mutation lock; a revision check fences the eventual swap. Locally known valid invites bypass the cooldown. This is not a general HTTP denial-of-service defense. |
| Contents API large-file limit | Confirmed against [GitHub documentation](https://docs.github.com/en/rest/repos/contents#get-repository-content). Object metadata supplies the file revision; large snapshots are fetched through the immutable blob SHA with the raw media type. Both reads and writes enforce a 16 MiB metadata budget. |
| Unbounded retained jobs | Added limits of 32 jobs and 2,048 retained batches per cluster, plus owner-authorized job deletion and the global serialized-state budget. Deletion removes task/manifest metadata; IndexedDB garbage collection remains outstanding. |
| Persisted holder proofs | Retained as evidence of the commit-time acknowledgements. They do not prove current possession, honest compute, or validity of a new connection. Current signing readiness is tracked independently. Removing or compacting historical proofs should be a schema migration, not silently changing existing UI audit counts. |
| Stale socket cluster selection | Open/close/holder-ready/repair handlers use the current selection ref. |
| ACK timeout starts during transfer | Confirmed and fixed. The receipt timeout starts after the final chunk; stalled backpressure still fails. Incoming reads extend their inactivity deadline when chunks arrive. |
| Cross-peer transfer injection | Confirmed and fixed. Starts cannot replace an existing transfer ID; chunks must match the original peer. Artifact names use the same ASCII allowlist in JS and Rust. |
| Rust formatting and duplicate modules | Formatted the crate, added a shared library for recovery/checkpoint/erasure modules, and removed benchmark path-based copies. Strict Clippy passes locally. The main API and frontend still need a broader module split. |
| Runtime merges / removed holder sessions | Runtime merge logic is shared. Durable pauses/removals fence leases instead of being undone by runtime preservation. Node/cluster/member removal clears holder sessions and shortens removed workers' leases. |
| Throwaway checkpoint catalog validation | Removed from live receipt verification. The registry directly checks manifest structure and independently signed matching holder acknowledgements. The policy catalog remains available to the simulations. |
| Legacy commit input | The older synthesized-name compatibility path remains; refreshed clients send explicit signed manifests. Retiring it and updating legacy-specific tests is deferred. |
| Rust/TS erasure drift | Added shared fixed coding vectors loaded by both test suites, plus a maximum-payload reconstruction test. Removed the tautological length check. |
| Shader recompile / sparse GPU validation | Pipeline creation is reused for a worker session; CPU/GPU inputs share one generator. Every returned f32 is compared, including rejection of non-finite outputs. This still does not attest an untrusted worker's computation to the server. |
| Peer ordering / production E2E hook / extractable key | Plain string ordering replaces locale comparison; local-network hooks require Vite development mode; WebCrypto signing private keys are non-extractable. XSS could still invoke signing operations, so this does not replace CSP/XSS defenses. |
| Dependency/build reproducibility | Added npm/Cargo lockfiles and .gitignore; CI/Docker use locked dependencies. Docker builds only the coordinator executable; the browser job has a Rust cache; deployments are serialized. |

## Remaining work, in recommended order

1. Validate all three physical network pairs. If direct ICE fails, add supported configurable ICE servers and short-lived TURN credentials. Do not use GitHub to relay frequent artifact data. Add connection diagnostics, ICE restart/retry, and worker session resynchronization.
2. Add structured, secret-redacted coordinator logging, explicit backpressure for critical WebSocket messages, and graceful shutdown. `try_send` can still drop control/reply messages under queue saturation; overload behavior needs targeted tests.
3. Move computation, encoding and hashing into a Web Worker; split the API/node lifecycle/job runner and dashboard components. Replace polling with versioned push invalidations. Binary transfer frames and cached IndexedDB connections can reduce overhead after correctness is established.
4. Add storage possession audits and local artifact garbage collection. Online status and a historical receipt alone cannot prove the bytes still exist. Quotas are bounded in this batch but GitHub still rewrites a snapshot per durable mutation and is not a high-throughput transactional database.
5. Harden WebSocket authentication so bearer tokens are not in URLs, add CSP, improve identity recovery, and audit lock poison recovery and lock ordering. Removing the repository fallback requires setting `MESH_STORAGE_REPO` on existing deployments first.
6. Add full tooling type coverage, lint/format checks for TypeScript, accessible dialog focus/Escape handling, pinned action SHAs and Docker dependency caching. Synchronize removed managed source files during deployment. Choose an explicit license as a maintainer decision.

The [three-device test guide](three-device-test.md) gives pass criteria and separates shard-holder loss from compute-worker loss. Automated same-host browser tests supplement that trial; they do not replace it.
