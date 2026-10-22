---
title: Mesh WebGPU Cluster
emoji: 🕸️
colorFrom: green
colorTo: purple
sdk: docker
app_port: 7860
pinned: false
---

# Mesh — browser compute pools

Mesh is an early prototype for sharing WebGPU compute across cluster members. The React dashboard is served by a Rust/Axum coordinator. The coordinator currently keeps state in memory and the dashboard still shows sample data; it is not yet a production compute scheduler.

## GitHub → Hugging Face deployment

The GitHub Actions workflow builds the React app, runs Rust unit tests, and builds the Docker image on pull requests and pushes to `main`. A successful push to `main` uploads the tested repository source to this Docker Space; Hugging Face then builds the `Dockerfile`.

Configure these GitHub repository Actions secrets:

- `HF_TOKEN`: a Hugging Face token with write access to the Space.
- `repo`: the Space URL, for example `https://huggingface.co/spaces/wuhp/webgputest` (or `owner/name`).

Deployment status and the Space link appear in the GitHub Actions run summary. Fork pull requests run tests but do not deploy.

## Local development

Requirements: Node.js 22+, npm, and Rust stable.

```sh
npm install
npm run dev
```

In another terminal:

```sh
cargo run --manifest-path server/Cargo.toml
```

The Vite UI runs on port 5173 and the Rust API on port 7860. Build the container locally with `docker build -t mesh-cluster .`.

## Coordinator and nodes

The Hugging Face Space is the coordinator and public app endpoint. For an initial version, nodes should make outbound authenticated WebSocket connections to this coordinator for registration, heartbeats, job assignment, and result submission. That keeps cluster membership, invitations, job ownership, and the credit ledger in one place. The current API is an early scaffold: authentication, WebSockets, persistent storage, and node registration still need implementation.

Direct peer-to-peer WebRTC can reduce relay bandwidth later, but it does not remove the need for a signaling/rendezvous service, and restrictive NATs often need a TURN relay. WebRTC encrypts transport; it does not make peer results trustworthy. Keep the coordinator authoritative for identities, job leases, validation, and credits. Do not award credits from client-reported uptime or raw TFLOPS. Award them for independently verified useful work and make the redemption rate explicit.

For a free initial deployment, using the existing Space as the coordinator is simpler and easier to secure than letting workers control each other. Free hosting can sleep, restart, and lose in-memory state, so persistent cluster membership and accounting require durable storage before real users rely on it.

## Safety and limitations

- Invite links should be signed, revocable, expiring, and require the user to authenticate and consent before joining.
- Device owners need explicit opt-in, resource limits, visible pause/leave controls, and a clear job allowlist. Never execute arbitrary code from a cluster member in a browser worker.
- Client-reported compute benchmarks and submitted results are untrusted. Use bounded job manifests, replay protection, deterministic checks or redundant verification, and server-side accounting.
- WebGPU needs a supported browser, device, and secure context. This prototype does not yet schedule WebGPU work.
