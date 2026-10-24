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

## Current control plane

- A browser creates an anonymous Mesh identity key and stores it in local storage. The server stores only its SHA-256 pseudonymous ID. Clearing local site data loses that identity; there is no email/password login or account recovery yet.
- Users can create multiple clusters, invite others with seven-day bearer links, join clusters, rename/delete their own clusters, leave clusters they joined, view members, remove members, and revoke invite links as the owner.
- A member can register a node. The current browser tab connects it over an outbound WebSocket, sends heartbeats, and receives pause/resume controls. Keep that tab open to keep the browser node online. The returned private WebSocket URL/token can also be used by a future native worker agent.
- When `GH_TOKEN` is present, state is loaded from and committed to the private `MESH_STORAGE_REPO` GitHub repository (default `Wuhpondiscord/gpuclustertest-state`), file `MESH_STORAGE_PATH` (default `mesh-state.json`) on `MESH_STORAGE_BRANCH` (default `main`). Invite and node bearer tokens are SHA-256 hashed before storage. For local development without `GH_TOKEN`, JSON is written to `MESH_DATA_FILE` (default `/tmp/mesh-state.json`).
- GitHub snapshot storage is intended for one low-volume coordinator. Set the fine-grained token as the Hugging Face Space secret named `GH_TOKEN`, restricted to the private data repository with Contents read/write permission. The empty data repo can be initialized by the app on its first write. Optionally set the Space variable `MESH_STORAGE_REPO` to another `owner/repo`; the default is `Wuhpondiscord/gpuclustertest-state`. Do not put state in the public source repository. Each accepted update is one GitHub commit guarded by the saved file revision; if another writer wins first, the coordinator reloads the latest file and retries the mutation up to three times. This is best-effort optimistic concurrency for a single low-volume coordinator, not SQL transactions or a multi-replica database. Git history gives recovery points, but frequent writes grow the repository.

## Recovery prototype and benchmark

`server/src/recovery.rs` provides per-task generations, attempt IDs, lease expiry, stale-result rejection, idempotent result commits, and coordinator-epoch invalidation on restart. The coordinator now exposes a bounded batch-job API and persists job progress with the cluster snapshot. A worker must be an online node owned by the authenticated user. Batch leases expire after 45 seconds; an unfinished batch can then be claimed by another worker. Coordinator restarts invalidate in-flight leases and keep committed batches.

Run `cargo run --manifest-path server/Cargo.toml --bin recovery-bench` to emit a deterministic discrete-event simulation of lease retries, two-copy checkpoint replication, and 2+1 / 2+2 erasure-coded checkpoints. It simulates work rates, LAN/broadband/constrained links, checkpoint intervals, checkpoint uploads and restores, parity encode/decode costs, and additional artifact-node failures. Output includes recovery and total simulated wall time, rework, transferred/stored MB, codec time, and whether recovery succeeds. GitHub Actions runs the simulation after Rust tests and includes its CSV in the workflow summary. These are modeled values; worker/network rates and coding costs must be calibrated against real devices before choosing a policy.

`server/src/checkpoint.rs` contains the adaptive checkpoint planner and a metadata-only two-phase commit catalog. The planner estimates intervals from observed transfer cost and worker mean time to failure, prefers deltas with a compatible base, chooses storage by size/frequency/link risk, and spreads shards across failure domains. The catalog validates assigned peer receipts and SHA-256 digests, requires the complete desired shard set before publishing a checkpoint, preserves the last committed head when staging fails, and returns a verifiable base-to-delta restore chain. A full checkpoint can compact that chain.

`checkpoint-bench` compares fixed full replication with adaptive plans. `checkpoint-montecarlo` runs seeded repeated-failure trials and reports average, median, and p95 job time, traffic, rework, and shard-loss rates. These simulations use assumed worker, network, and codec costs, not hardware measurements. Browser nodes exchange same-cluster WebRTC signaling through Rust and move artifact bytes directly over ordered data channels. The **Create demo job** action creates a deterministic CPU batch workload; **Run on this node** claims batches, computes them, sends each checkpoint to a second cluster node, and commits only after the peer acknowledges its SHA-256 and both browsers save local IndexedDB copies. The coordinator stores job metadata and client-reported artifact metadata, not artifact bytes; it cannot independently attest that browser storage remains available.

The dashboard can create deterministic CPU hash jobs or vector-add jobs. Vector-add runs a bounded WGSL compute kernel in the browser when WebGPU is available, validates sample outputs, and includes the result values and digest in the peer-replicated batch artifact. Browsers without a usable WebGPU adapter run the same vector-add workload through a CPU fallback. Batches are capped at 500,000 elements by the worker and the current UI creates 100,000-element batches. WebGPU device loss fails the attempt; its lease expires and can be retried by a worker.

Each independently committed batch is the current checkpoint/recovery boundary; checkpoint cadence and batch size are fixed for a job. The adaptive `CheckpointCatalog` remains a metadata-only prototype and is not yet wired into durable batch commits, deltas, or erasure coding. The browser worker validates only a small sample of vector results. The scheduler cannot attest to the claimed execution mode or verify the whole output, so it must not issue credits or treat results as trusted. GitHub snapshots receive each job/lease/commit mutation, which is suitable for this low-volume prototype but will not support high-throughput queues. There is no TURN relay fallback, so some restrictive NAT/firewall combinations will fail to establish a direct channel.

The identity and node connection URLs are bearer credentials. Do not share screenshots or logs containing them. This prototype does not yet have a full security review, rate limits, or login recovery.

## Compute architecture recommendation

The Hugging Face Space should coordinate work, not execute users’ programs. It can remain a small control plane that validates membership, issues bounded job leases, tracks heartbeats, and records accepted results. Avoid routing large input/output blobs through it.

For useful work beyond browser-only kernels, build an opt-in native node agent. Start with sandboxed WASM jobs (for example, WASI without ambient filesystem or network access) and a small host API for approved operations. Each node should enforce CPU, memory, GPU, wall-time, and bandwidth caps locally. Later, support a small set of well-defined GPU kernels through wgpu/WebGPU on capable devices. Browser WebGPU is a useful optional worker, but it cannot run arbitrary desktop programs and has browser lifecycle/device limits.

Good first workloads are independent batches with small inputs and outputs. Tightly coupled multi-node jobs and general distributed training are a poor fit for a low-cost coordinator and unpredictable home-device networks. Job results must be checked with deterministic validation, redundant samples, or trusted challenges; a node’s claimed speed or uptime is never proof of useful work.

The Space provides authenticated signaling, cluster control, bounded job leases, and low-rate durable metadata snapshots; artifact bytes travel over direct WebRTC data channels. This uses a free STUN service for NAT discovery but has no TURN fallback, so direct connections are not universal. Worker nodes connect outbound to the Space, avoiding inbound ports and peer-to-peer trust for control. For larger scale, move hot job queues and high-rate state to managed services; keep the Space as the control frontend and GitHub snapshots for low-rate durability.

Hugging Face’s current overview describes CPU Basic as 2 vCPU/16 GB with 50 GB non-persistent disk and says free Spaces sleep when unused; its storage docs say restarts discard ephemeral files unless durable storage is attached. That is suitable for a prototype control plane, not a reliable always-on scheduler or data relay. See [Space limits](https://huggingface.co/docs/hub/en/spaces-overview) and [Space storage](https://huggingface.co/docs/hub/en/spaces-storage).

