---
title: Mesh WebGPU Cluster
emoji: 🕸️
colorFrom: green
colorTo: purple
sdk: docker
app_port: 7860
pinned: false
---

# Mesh — cluster management prototype

Mesh has a Rust coordinator for cluster and node management, plus bounded leased compute jobs whose results are replicated directly to peers. The React dashboard includes a deterministic CPU workload and a constrained WebGPU vector-add workload.

## GitHub → Hugging Face

GitHub Actions builds the React UI, runs Rust unit tests, and builds the Docker image on pull requests and pushes to `main`. A successful `main` push uploads the tested source to this Docker Space, which then builds the `Dockerfile`.

Repository Actions secrets:

- `HF_TOKEN`: a Hugging Face token with write access to the Space.
- `repo`: the Space URL, for example `https://huggingface.co/spaces/wuhp/webgputest` (or `owner/name`).

Deployment status and the Space link appear in the workflow run summary.

## Start locally

Requirements: Node.js 22+, npm, Rust stable.

```sh
npm install
npm run dev
```

In another terminal, run `cargo run --manifest-path server/Cargo.toml`. The Vite development server proxies `/api` to the Rust server on port 7860.

## Browser integration tests

Install the Playwright Chromium runtime with `npx playwright install chromium`, then run `npm run test:e2e`. Playwright starts an isolated Rust coordinator and Vite server, creates three browser contexts with separate Mesh identities, and exchanges a real vector-add checkpoint over WebRTC. The recovery scenario pauses one shard node and verifies that the other two shards reconstruct the artifact and match its full SHA-256 digest. The test uses host ICE candidates between same-machine contexts and does not exercise NAT traversal, TURN, or cross-network behavior. GitHub Actions runs this test before allowing a main-branch deployment.

## Holder receipts and automatic repair

Each connected browser registers an ephemeral ECDSA P-256 public key on its authenticated node WebSocket. The coordinator issues a fresh session nonce. After hashing and committing artifact bytes to IndexedDB, the holder signs the UTF-8 JSON array `["mesh-holder-v1", nonce, node_id, artifact_name, sha256, bytes]` with ECDSA/SHA-256. Public keys use raw SEC1 encoding and signatures use raw `r || s`, as specified by [WebCrypto](https://www.w3.org/TR/WebCryptoAPI/) and supported by [RustCrypto p256](https://docs.rs/p256/0.13.2/p256/ecdsa/index.html). Private signing keys stay in the holder browser for that connection.

The coordinator checks the authenticated connection, session nonce, registered key, signature, artifact name, digest, and size. The peer transfer acknowledgement waits for that confirmation. A sender cannot publish a checkpoint merely by naming online nodes: `CheckpointCatalog` requires an independently confirmed receipt for every assigned holder before the manifest commits. Receipt caching is bounded to 8,192 entries for ten minutes and stays in memory; only the committed manifest and its verified proofs enter durable state. Reconnecting invalidates cached receipts from the previous session. Refresh existing worker tabs after upgrading so they register signing sessions; the node list reports storage readiness. Historical checkpoints retain their prior metadata and are not retroactively signed.

Every five seconds, the coordinator checks committed manifests. A missing holder must remain unavailable for 20 seconds before repair is scheduled (`MESH_REPAIR_GRACE_SECS`, clamped to 1–300). A repair requires enough surviving shards plus a distinct online spare with a registered signing session. A surviving holder fetches the remaining artifacts directly, verifies their hashes, reconstructs and verifies the full checkpoint, regenerates the missing shard, verifies its original digest, and sends it directly to the spare. Two-copy manifests are repaired by copying the remaining full artifact. Artifact bytes never pass through GitHub or the coordinator.

The coordinator keeps the old manifest until the spare's signed storage receipt is verified and the updated manifest is durably saved. Repair attempts expire after 180 seconds, failed attempts back off for ten seconds, and completions are fenced by worker session and manifest revision. A failure during transfer or persistence leaves the prior checkpoint unchanged. Scheduling is bounded to eight dispatches per tick, one repair per worker and replacement target. Restarting rebuilds repair needs from durable manifests. The dashboard shows available holders, verified receipt counts, and healthy/degraded/unavailable status. Without a spare, the checkpoint stays degraded; without enough source shards, repair waits for holders to return.

Artifact names are immutable in IndexedDB; fallback full copies and repairs get distinct names. Full receive queues explicitly reject new transfers, and fresh traffic reclaims incoming transfer slots after 45 seconds of inactivity. A signed receipt identifies which authenticated holder confirmed which bytes at write time. It does not prove that an untrusted holder will retain those bytes indefinitely, and it does not attest to correct computation. Periodic possession challenges, storage quotas and garbage collection, and redundant compute verification remain future work.

Browser tests exercise immutable storage, rejection of fabricated holder receipts, and repair of both full replicas and 2+1 shards followed by loss of a second original holder and a successful full-hash restore. Rust tests cover signature/session/content tampering, missing confirmations, insufficient recovery sources, expired repair ownership, and persistence failure before manifest replacement.

## Current control plane

- A browser creates an anonymous Mesh identity key and stores it in local storage. The server stores only its SHA-256 pseudonymous ID. Clearing local site data loses that identity; there is no email/password login or account recovery yet.
- Users can create multiple clusters, invite others with seven-day bearer links, join clusters, rename/delete their own clusters, leave clusters they joined, view members, remove members, and revoke invite links as the owner.
- A member can register a node. The current browser tab connects it over an outbound WebSocket, sends heartbeats, and receives pause/resume controls. Keep that tab open to keep the browser node online. The returned private WebSocket URL/token can also be used by a future native worker agent.
- When `GH_TOKEN` is present, state is loaded from and committed to the private `MESH_STORAGE_REPO` GitHub repository (default `Wuhpondiscord/gpuclustertest-state`), file `MESH_STORAGE_PATH` (default `mesh-state.json`) on `MESH_STORAGE_BRANCH` (default `main`). Invite and node bearer tokens are SHA-256 hashed before storage. For local development without `GH_TOKEN`, JSON is written to `MESH_DATA_FILE` (default `/tmp/mesh-state.json`).
- GitHub snapshot storage is intended for one low-volume coordinator. Set the fine-grained token as the Hugging Face Space secret named `GH_TOKEN`, restricted to the private data repository with Contents read/write permission. The empty data repo can be initialized by the app on its first write. Optionally set the Space variable `MESH_STORAGE_REPO` to another `owner/repo`; the default is `Wuhpondiscord/gpuclustertest-state`. Do not put state in the public source repository. Each accepted update is one GitHub commit guarded by the saved file revision; if another writer wins first, the coordinator reloads the latest file and retries the mutation up to three times. This is best-effort optimistic concurrency for a single low-volume coordinator, not SQL transactions or a multi-replica database. Git history gives recovery points, but frequent writes grow the repository.

## Recovery prototype and benchmark

`server/src/recovery.rs` provides per-task generations, attempt IDs, lease expiry, stale-result rejection, idempotent result commits, and coordinator-epoch invalidation on restart. The coordinator exposes a bounded batch-job API and persists committed progress with the cluster snapshot. Claims and renewals stay in memory and do not write GitHub or local snapshot storage. Active leases are excluded from snapshots; a fresh coordinator session fences attempts from before a restart while keeping committed results.

A worker must be an online node owned by the authenticated user. Leases last 45 seconds, and the browser renews them every 10 seconds through `POST /api/jobs/:id/renew` during computation, transfer, fallback, and commit. Transient renewal errors retry after two seconds; stale or unauthorized attempts stop. Renewal remains available during durable saves, and snapshot swaps preserve concurrent renewals. A disconnected worker retains its leases for at most a 10-second reconnect grace period; reconnecting before expiry restores the normal lease window. Pausing or deleting a node makes its unfinished leases immediately reclaimable. Recovery still requires another worker to be running the job; this does not automatically start compute in idle browser tabs.

Peer signaling is serialized for each connection, with early ICE candidates buffered until its remote description is installed. Replacing a connection discards queued work for the previous connection. Browser tests cover candidates arriving before and during description setup, a commit delayed beyond the original lease deadline, temporary renewal failure, checkpoint transfer interruption, and recovery from two surviving shards. Rust tests cover renewal authorization, storage-free claims and renewals, snapshot replacement, reconnect grace, reassignment fencing, and restart fencing.

Run `cargo run --manifest-path server/Cargo.toml --bin recovery-bench` to emit a deterministic discrete-event simulation of lease retries, two-copy checkpoint replication, and 2+1 / 2+2 erasure-coded checkpoints. It simulates work rates, LAN/broadband/constrained links, checkpoint intervals, checkpoint uploads and restores, parity encode/decode costs, and additional artifact-node failures. Output includes recovery and total simulated wall time, rework, transferred/stored MB, codec time, and whether recovery succeeds. GitHub Actions runs the simulation after Rust tests and includes its CSV in the workflow summary. These are modeled values; worker/network rates and coding costs must be calibrated against real devices before choosing a policy.

`server/src/checkpoint.rs` contains the adaptive checkpoint planner and a metadata-only two-phase commit catalog. The planner estimates intervals from observed transfer cost and worker mean time to failure, prefers deltas with a compatible base, chooses storage by size/frequency/link risk, and spreads shards across failure domains. The catalog validates assigned peer receipts and SHA-256 digests, requires the complete desired shard set before publishing a checkpoint, preserves the last committed head when staging fails, and returns a verifiable base-to-delta restore chain. A full checkpoint can compact that chain.

`checkpoint-bench` compares fixed full replication with adaptive plans. `checkpoint-montecarlo` runs seeded repeated-failure trials and reports average, median, and p95 job time, traffic, rework, and shard-loss rates. These simulations use assumed worker, network, and codec costs, not hardware measurements. Browser nodes exchange same-cluster WebRTC signaling through Rust and move artifact bytes directly over ordered data channels. Small job artifacts are replicated to two nodes. Artifacts at least 64 KiB are split into two data shards and one bytewise XOR parity shard; the worker stores one shard and transfers the other two to distinct online peers. The coordinator commits the shard manifest only when all assigned nodes are active. **Test restore** requests available shards from peers, reconstructs from any two, and verifies the full artifact SHA-256. The coordinator stores job and shard metadata, not artifact bytes, and cannot independently attest that browser storage remains available.

The dashboard can create deterministic CPU hash jobs or vector-add jobs. Vector-add runs a bounded WGSL compute kernel in the browser when WebGPU is available, validates sample outputs, and includes the result values and digest in the peer-replicated batch artifact. Browsers without a usable WebGPU adapter run the same vector-add workload through a CPU fallback. Batches are capped at 500,000 elements by the worker and the current UI creates 100,000-element batches. WebGPU device loss fails the attempt; its lease expires and can be retried by a worker.

Each independently committed batch is the current checkpoint/recovery boundary; checkpoint cadence and batch size are fixed for a job. `server/src/erasure.rs` and `src/sharding.ts` implement matching 2+1 XOR encoding and reconstruction; this is not encryption. Per-shard hashes detect transfer corruption, and the reconstructed artifact hash is checked before restore. The adaptive `CheckpointCatalog` remains a metadata-only prototype and is not yet wired into delta checkpoints or interval selection. The browser worker validates only a small sample of vector results. The scheduler cannot attest to the claimed execution mode or verify the whole computation, so it must not issue credits or treat results as trusted. Distinct node IDs do not guarantee independent physical or network failure domains. GitHub snapshots receive each job/lease/commit mutation, which is suitable for this low-volume prototype but will not support high-throughput queues. There is no TURN relay fallback, so some restrictive NAT/firewall combinations will fail to establish a direct channel.

The identity and node connection URLs are bearer credentials. Do not share screenshots or logs containing them. This prototype does not yet have a full security review, rate limits, or login recovery.

## Compute architecture recommendation

The Hugging Face Space should coordinate work, not execute users’ programs. It can remain a small control plane that validates membership, issues bounded job leases, tracks heartbeats, and records accepted results. Avoid routing large input/output blobs through it.

For useful work beyond browser-only kernels, build an opt-in native node agent. Start with sandboxed WASM jobs (for example, WASI without ambient filesystem or network access) and a small host API for approved operations. Each node should enforce CPU, memory, GPU, wall-time, and bandwidth caps locally. Later, support a small set of well-defined GPU kernels through wgpu/WebGPU on capable devices. Browser WebGPU is a useful optional worker, but it cannot run arbitrary desktop programs and has browser lifecycle/device limits.

Good first workloads are independent batches with small inputs and outputs. Tightly coupled multi-node jobs and general distributed training are a poor fit for a low-cost coordinator and unpredictable home-device networks. Job results must be checked with deterministic validation, redundant samples, or trusted challenges; a node’s claimed speed or uptime is never proof of useful work.

The Space provides authenticated signaling, cluster control, bounded job leases, and low-rate durable metadata snapshots; artifact bytes travel over direct WebRTC data channels. This uses a free STUN service for NAT discovery but has no TURN fallback, so direct connections are not universal. Worker nodes connect outbound to the Space, avoiding inbound ports and peer-to-peer trust for control. For larger scale, move hot job queues and high-rate state to managed services; keep the Space as the control frontend and GitHub snapshots for low-rate durability.

Hugging Face’s current overview describes CPU Basic as 2 vCPU/16 GB with 50 GB non-persistent disk and says free Spaces sleep when unused; its storage docs say restarts discard ephemeral files unless durable storage is attached. That is suitable for a prototype control plane, not a reliable always-on scheduler or data relay. See [Space limits](https://huggingface.co/docs/hub/en/spaces-overview) and [Space storage](https://huggingface.co/docs/hub/en/spaces-storage).

