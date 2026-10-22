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
- A member can register a node and receive a private WebSocket URL/token. A node agent connects outbound to `/api/nodes/connect`; the coordinator tracks online state and sends pause/resume controls. A sample Rust or browser node agent still needs to be built.
- State is written as JSON to `MESH_DATA_FILE` (default `/tmp/mesh-state.json`). On Hugging Face that path is ephemeral. Attach durable storage and set `MESH_DATA_FILE` to its mount before using this with real clusters.

The identity and node connection URLs are bearer credentials. Do not share screenshots or logs containing them. This prototype does not yet have a full security review, rate limits, or login recovery.

## Compute architecture recommendation

The Hugging Face Space should coordinate work, not execute users’ programs. It can remain a small control plane that validates membership, issues bounded job leases, tracks heartbeats, and records accepted results. Avoid routing large input/output blobs through it.

For useful work beyond browser-only kernels, build an opt-in native node agent. Start with sandboxed WASM jobs (for example, WASI without ambient filesystem or network access) and a small host API for approved operations. Each node should enforce CPU, memory, GPU, wall-time, and bandwidth caps locally. Later, support a small set of well-defined GPU kernels through wgpu/WebGPU on capable devices. Browser WebGPU is a useful optional worker, but it cannot run arbitrary desktop programs and has browser lifecycle/device limits.

Good first workloads are independent batches with small inputs and outputs. Tightly coupled multi-node jobs and general distributed training are a poor fit for a low-cost coordinator and unpredictable home-device networks. Job results must be checked with deterministic validation, redundant samples, or trusted challenges; a node’s claimed speed or uptime is never proof of useful work.

Peer-to-peer WebRTC could move larger data directly between nodes, but still needs a rendezvous/signaling service and often a TURN fallback. Keep the Space authoritative for identity, membership, job leases, validation, and credits. Worker nodes should connect outbound to it, avoiding inbound ports and peer-to-peer trust for control. For larger scale, move job queues/identity and durable state to managed services; the Space should remain a replaceable web/control frontend.

Hugging Face’s current overview describes CPU Basic as 2 vCPU/16 GB with 50 GB non-persistent disk and says free Spaces sleep when unused; its storage docs say restarts discard ephemeral files unless durable storage is attached. That is suitable for a prototype control plane, not a reliable always-on scheduler or data relay. See [Space limits](https://huggingface.co/docs/hub/en/spaces-overview) and [Space storage](https://huggingface.co/docs/hub/en/spaces-storage).
