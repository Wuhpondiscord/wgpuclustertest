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

Mesh now has a working Rust coordinator for cluster creation, invite links, membership, node registration, and node presence/control over outbound WebSockets. The React dashboard calls that API instead of using the old sample cluster data. Compute jobs and WebGPU execution are not implemented yet.

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

`server/src/recovery.rs` contains an isolated Rust task-lease ledger prototype. It provides per-task generations, attempt IDs, lease expiry, stale-result rejection, idempotent result commits, and coordinator-epoch invalidation on restart. It is not wired into the current cluster API; the app still does not dispatch compute jobs.

Run `cargo run --manifest-path server/Cargo.toml --bin recovery-bench` to emit a deterministic CSV scenario model comparing lease retries, duplicate execution, replicated checkpoints, 2+1 and 2+2 erasure-coded checkpoints, and a Lattice-inspired replicated event log. The model uses normalized work/storage/network units and the same worker failure positions for each method. It compares expected recovery behavior, not real network or GPU throughput. GitHub Actions runs this model after Rust tests and includes the CSV in the workflow summary. Real-device benchmarks are a later step once a worker can execute tasks and transfer artifacts.

The identity and node connection URLs are bearer credentials. Do not share screenshots or logs containing them. This prototype does not yet have a full security review, rate limits, or login recovery.

## Compute architecture recommendation

The Hugging Face Space should coordinate work, not execute users’ programs. It can remain a small control plane that validates membership, issues bounded job leases, tracks heartbeats, and records accepted results. Avoid routing large input/output blobs through it.

For useful work beyond browser-only kernels, build an opt-in native node agent. Start with sandboxed WASM jobs (for example, WASI without ambient filesystem or network access) and a small host API for approved operations. Each node should enforce CPU, memory, GPU, wall-time, and bandwidth caps locally. Later, support a small set of well-defined GPU kernels through wgpu/WebGPU on capable devices. Browser WebGPU is a useful optional worker, but it cannot run arbitrary desktop programs and has browser lifecycle/device limits.

Good first workloads are independent batches with small inputs and outputs. Tightly coupled multi-node jobs and general distributed training are a poor fit for a low-cost coordinator and unpredictable home-device networks. Job results must be checked with deterministic validation, redundant samples, or trusted challenges; a node’s claimed speed or uptime is never proof of useful work.

Peer-to-peer WebRTC could move larger data directly between nodes, but still needs a rendezvous/signaling service and often a TURN fallback. Keep the Space authoritative for identity, membership, job leases, validation, and credits. Worker nodes should connect outbound to it, avoiding inbound ports and peer-to-peer trust for control. For larger scale, move job queues/identity and durable state to managed services; the Space should remain a replaceable web/control frontend.

Hugging Face’s current overview describes CPU Basic as 2 vCPU/16 GB with 50 GB non-persistent disk and says free Spaces sleep when unused; its storage docs say restarts discard ephemeral files unless durable storage is attached. That is suitable for a prototype control plane, not a reliable always-on scheduler or data relay. See [Space limits](https://huggingface.co/docs/hub/en/spaces-overview) and [Space storage](https://huggingface.co/docs/hub/en/spaces-storage).

