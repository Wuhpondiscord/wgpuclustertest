import type { TaskLease } from './compute-types'
export type ApplicationInput =
  | { operation: 'vector_add'; a: number[]; b: number[] }
  | { operation: 'matmul'; a: number[]; b: number[]; rows: number; inner: number; cols: number }
  | { operation: 'embedding'; model: 'minilm-l6-v2'; texts: string[]; verification: 'replicated' | 'single' }
export type ApplicationOutput = { values: number[]; shape: number[] }
export type ApplicationRuntimeReport = { backend: 'webgpu' | 'wasm' | 'cpu'; compute_ms: number; load_ms: number; fallback_reason: string | null }
export type ApplicationWork = { job_id: string; application_id: string; slot: number; input: ApplicationInput; lease: TaskLease; timeout_ms: number }
export type ApplicationJob = {
  id: string; application_id: string; status: string; input: ApplicationInput; verification: string;
  created_at_ms: number; replicas_required: number; replicas_received: number;
  output: ApplicationOutput | null; error: string | null; status_url: string;
  workers: Array<{ node_id: string; attempt_id: number; coordinator_epoch: number; generation: number; backend: string; compute_ms: number; load_ms: number }>;
  queue_wait_ms: number | null; result_received_elapsed_ms: number | null;
}
