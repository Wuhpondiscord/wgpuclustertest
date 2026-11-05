# Browser application runtime v1

Mesh accepts bounded jobs, schedules them onto opted-in browser nodes, and exposes
the same queue through a JavaScript SDK and an asynchronous inference API. There
is no native worker installation. Application paths such as `/alice/search` are
private management pages, not DNS subdomains or always-on HTTP application servers.

## Try it

1. Create/activate a named account, create a cluster and a private application.
2. Register a browser node and select **Contribute compute**. Keep the tab open.
3. Open the application, choose **Matrix multiply**, and queue the example. The
   result is `[19, 22, 43, 50]`, shape `[2, 2]`.
4. For embeddings, enable **ML inference** on each contributor. First use downloads
   approximately 90 MB of model weights plus ONNX runtime assets. Downloads are
   opt-in; old remembered nodes default to ML disabled.
5. Choose the embedding template. Default `verification: "replicated"` needs two
   distinct approved node IDs. For one-device testing use `"single"`; the result
   is explicitly labeled `single_worker_unverified`.

Other users must first join the cluster, then receive an application **operator**
grant by username. Cluster members (including its owner) have no implicit app
access. A viewer can read app inputs/results, but cannot submit or execute work.

## Supported operations and bounds

| Operation | Input | Limit | Verification |
| --- | --- | --- | --- |
| `vector_add` | `a`, `b` arrays | equal lengths 1–4096 | every output checked in Rust |
| `matmul` | row-major `a`, `b`, `rows`, `inner`, `cols` | dimensions 1–128; at most 8192 input values, 4096 output values, 262144 multiply-adds | every output checked in Rust with float tolerance |
| `embedding` | `model: "minilm-l6-v2"`, `texts`, optional `verification` | 1–4 texts, each ≤2048 UTF-8 bytes, truncated to 128 tokens | two-node agreement by default; optional unverified single worker |

Numerical input values must be finite, absolute value ≤1e6. Outputs are row-major
`{values: number[], shape: number[]}`. No arbitrary Python, JavaScript, WGSL, ONNX
model upload, filesystem, sockets, Dockerfile or shell execution is supported.

Numerical kernels run on WebGPU, with an optional CPU fallback. Text embeddings
use Transformers.js and ONNX Runtime Web on WebGPU or single-threaded WASM. The
dedicated module Web Worker isolates execution from UI/renewal timers. Its serial
executor is terminated on Stop, lease loss or the five-minute task deadline.
Only fixed bounded kernels are executed; worker termination is not a general
guarantee of preempting an arbitrary GPU workload.

The curated model is `Xenova/all-MiniLM-L6-v2`, revision
`751bff37182d3f1213fa05d7196b954e230abad9`, fp32, 384-dimensional mean-pooled,
normalized embeddings. `onnx/model.onnx` SHA-256:
`759c3cd2b7fe7e93933ad23c4c9181b7396442a2ed746ec7c1d46192c469c46e`.
Both downloaded and cached ONNX bytes are checked. Only this immutable model
revision is fetched; public model assets are cached, never task text or output.
WASM assets are served from `/ort/`, copied during build from the locked npm
dependency. Model/runtime assets are software downloads inside the browser, not a
native installation.

## SDK and API

The app owner can generate separate 256-bit CSPRNG API keys. Save the `mak_…` key
when shown. Only its SHA-256 digest is persisted. It expires after 30 days and
can be revoked independently. Keys are app-scoped; the read-only option cannot
submit work. Keys cannot manage grants, delete jobs or manage other keys. They
also stop working if their issuer loses app/operator or cluster access. Account
key rotation does not automatically revoke independently issued app API keys;
revoke those explicitly when responding to a compromise.

In a trusted browser tool, import `/sdk/mesh-client.mjs`. For Node, download that
module and import the local file (Node does not normally import HTTPS modules).
Type declarations are provided alongside it. Keep a developer key in a secret
store or environment variable; embedding it in a public website exposes it.

```javascript
import { MeshClient } from './mesh-client.mjs';
const mesh = new MeshClient({
  endpoint: 'https://YOUR-SPACE.hf.space',
  applicationId: 'YOUR-APPLICATION-UUID',
  token: process.env.MESH_APPLICATION_KEY,
});
const job = await mesh.embeddings(['Semantic search starts here.']);
const result = await mesh.wait(job, { timeoutMs: 300000 });
console.log(result.output.shape, result.output.values);
```

`vectorAdd(a, b)` and `matmul({a, b, rows, inner, cols})` submit numerical jobs.
Submission returns immediately; `wait()` polls. Aborting an SDK request or wait
stops that client's request/polling; it does **not** cancel the coordinator job.
An app owner/operator can delete the job in the UI to revoke outstanding work.

HTTP routes, all scoped to `/api/applications/:application_id`:

| Method/path | Body/action |
| --- | --- |
| `POST /jobs` | tagged `operation` and its inputs → **202**, job summary |
| `GET /jobs` | retained private job summaries |
| `GET /jobs/:job_id` | status, verification label, output only when completed |
| `DELETE /jobs/:job_id` | owner/operator account session only; removes data and leases |
| `POST /inference` | `{model:"minilm-l6-v2", input:["text"], verification:"replicated"}` → **202**, same queue |
| `GET /runtime` | supported operations/model/bounds, retained completion measurements |
| `GET/POST /keys`, `DELETE /keys/:key_id` | application owner account session only |

Developer requests use `Authorization: Bearer mak_…`, never a token in a URL.
The coordinator allows cross-origin Bearer requests without credential cookies.
Its own preflight excludes account-session headers, but the Hugging Face edge
proxy may answer preflights itself and reflect additional requested headers. Do
not treat CORS or an origin allowlist as authorization: every request still needs
an explicit valid credential and application permission. Mesh does not use
ambient credential cookies; the browser UI sends its short-lived account session
explicitly from sessionStorage, which other origins cannot read.

POST supports `Idempotency-Key` (1–64 alphanumeric, hyphen or underscore). Reuse
the same key and payload when retrying an ambiguous submission; a different
payload with that key returns 409. The SDK generates one per submission. Calling
a submission method again without retaining its key is a new job. It does not
silently retry submissions. Idempotency only lasts while the job is retained;
deletion permits reuse.

States: `queued`, `running`, `awaiting_verification`, `completed`, `failed`,
`expired`. Jobs expire if unfinished after 30 minutes. Three reported execution
failures stop a job; disconnects are recovered via leases rather than counted as
runtime failures. Retention is bounded to 16 jobs/app and 128 jobs/coordinator;
delete old jobs to make room. Request bodies ≤64 KiB, results ≤128 KiB. Oversized
JSON can hit the body limit before reaching an operation's element limit.

## Recovery, verification and privacy

The existing recovery ledger provides 45-second renewable leases, generation,
attempt and coordinator-epoch fencing. Disconnect and node pause/removal use the
existing lease invalidation. A retry must acknowledge the exact accepted worker,
epoch, generation, attempt and result digest; a late old worker cannot substitute
even the same output. Worker commits are only acknowledged after encrypted state
persistence succeeds. Lost responses are reconciled before treating accepted work
as a failed attempt. Claims and renewals do not rewrite GitHub state.

These small v1 jobs restart their entire bounded operation after worker failure.
They do not checkpoint model execution or divide one matrix/model across devices.
Distribution is across independent jobs and the two inference replicas. Existing
demo checkpoint/shard recovery is unchanged. Bigger artifact/model sharding and
incremental task checkpoints require a separate measured data-plane extension.

Two normalized embedding results must have cosine ≥0.9995 and per-element absolute
difference ≤0.01. Disagreement withholds the output and fails the job. Distinct
node IDs may be controlled by the same owner; agreement is **not** an attestation,
Sybil-resistant verification or proof of honest compute. Single-worker embeddings
only receive shape, finite-value and normalization checks. No credits or mining
rewards are awarded. Numerical comparisons use tolerances, not cross-GPU bitwise
equality.

Application inputs and results are excluded from cluster-wide demo jobs, peer
artifact transfer, custody APIs and work hints. The coordinator and approved
worker hosts see task plaintext. This is access control and encrypted storage at
rest, **not** confidential computing against a worker, browser extension, XSS,
server operator or compromised runtime. Don't send secrets to untrusted workers.
The bounded private payloads use the encrypted coordinator snapshot for this
low-volume prototype. GitHub is not a scalable hot inference/artifact database;
high-volume deployment needs a different data plane before limits are raised.

## Measurements and testing

Runtime metadata reports completed retained samples' queue-to-result-**receipt**
p50/p95. This is measured before durable commit finishes and excludes client
polling/round trip. Worker load/compute times are self-reported. There is no uptime,
availability or latency SLO, OpenAI synchronous API compatibility, streaming chat,
distributed training, pooled VRAM or economic settlement in this release.

`npm run test:unit` tests the SDK. Rust router tests cover private access, key
hashing/revocation, idempotency/caps/body limits, fenced commits and persistence
failure, opt-in, replica agreement/disagreement and cleanup. Playwright tests run
real matrix/vector jobs, ambiguous commit recovery, Stop/restart, scoped SDK reads
and a real pinned MiniLM model with semantic-similarity assertions. Model downloads
require access to Hugging Face's model/CDN endpoints. Same-machine browser tests
do not measure real cross-network latency or GPU-vendor variation.

Local Chromium measurements on 2026-11-04 (not a performance guarantee): first
three-text WASM run loaded in about 61.4 s and computed in 63 ms; a later warm,
one-text run on the same contributor loaded in 0 ms and computed in 9 ms. A
production-bundle run completed two-worker agreement and recovered an application
job after a browser closed. This Windows test host exposed no usable WebGPU
adapter; the GPU-only test explicitly skipped. Real hardware performance,
cross-vendor inference agreement and cross-network behavior remain unmeasured.

CI runs both the development suite and the application-runtime tests against the
built static site served by Rust (`MESH_TEST_PRODUCTION=1`). Optional GPU adapter
flags are available with `MESH_TEST_GPU=1`; they do not guarantee adapter support.
Native ONNX/CUDA postinstall downloads are skipped in Docker and CI because only
the browser runtime is used. Model weights and runtime assets can consume several
hundred MB of browser memory; not every mobile/backgrounded device will fit or
stay available.

Next measurements should compare cold/warm model load, WebGPU/WASM latency,
multiple independent devices and networks, background tab suspension, simultaneous
jobs, disconnect during inference, and two-vendor embedding agreement. Establish
those baselines before adding more model families, Python graph frontends, large
artifacts, streaming endpoints or mining workloads.
