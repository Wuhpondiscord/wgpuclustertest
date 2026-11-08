export type Input = { operation: 'vector_add'; a: number[]; b: number[] } | { operation: 'matmul'; a: number[]; b: number[]; rows: number; inner: number; cols: number } | { operation: 'embedding'; model: 'minilm-l6-v2'; texts: string[]; verification?: 'replicated' | 'single' }
export type Job = { id: string; application_id: string; status: 'queued' | 'running' | 'awaiting_verification' | 'completed' | 'failed' | 'expired' | 'cancelled'; verification: string; output: { values: number[]; shape: number[] } | null; error: string | null; result_received_elapsed_ms: number | null; deployment?: { name: string; revision_id: string } }
export type Deployment = {
  id: string;
  deployed_at_ms: number;
  manifest: { version: 1; name: string; jobs: Array<{ name: string; input: Input }> };
  source: { kind: 'upload' } | { kind: 'github'; repository: string; commit: string };
}
export type Options = { signal?: AbortSignal; idempotencyKey?: string }
export class MeshClient {
  constructor(options: { endpoint: string; applicationId: string; token: string; fetch?: typeof fetch });
  submit(input: Input, options?: Options): Promise<Job>;
  getDeployment(options?: { signal?: AbortSignal }): Promise<Deployment | null>;
  runDeployment(revisionId: string, options?: Options): Promise<Job[]>;
  getJob(id: string, options?: { signal?: AbortSignal }): Promise<Job>;
  wait(job: Job | string, options?: { signal?: AbortSignal; timeoutMs?: number; pollMs?: number }): Promise<Job>;
  vectorAdd(a: Iterable<number>, b: Iterable<number>, options?: Options): Promise<Job>;
  matmul(input: { a: Iterable<number>; b: Iterable<number>; rows: number; inner: number; cols: number }, options?: Options): Promise<Job>;
  embeddings(input: Iterable<string>, options?: Options & { verification?: 'replicated' | 'single'; model?: 'minilm-l6-v2' }): Promise<Job>;
}
