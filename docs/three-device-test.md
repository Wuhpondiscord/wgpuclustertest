# Three-device, three-network recovery test

Use three separate browser devices, A, B and C. Join with the cluster invite, then register and connect a node on each device. Membership alone does not create a worker.

## Before disconnecting anything

1. Refresh every node tab after deployment. Wait until all three nodes say **Connected · storage verified**.
2. On **each** device, run **Test P2P**. It now checks every other active verified holder. All three devices must report success before a disconnect trial. A successful transfer from A to B alone does not verify B to C.
3. Keep the node tabs open and devices awake. Create a CPU demo, click **Run on this node**, let it finish, then run **Test restore**.
4. Create a WebGPU job and run it on A. The completion notice identifies `webgpu` or `cpu-fallback`. A CPU fallback validates transport and recovery but does not validate your GPU or driver.

The current workloads execute bounded independent batches on the browser where **Run on this node** is clicked. The coordinator schedules leases; other devices store checkpoint shards. This is not one GPU kernel spread across three devices.

## Trial 1: lose a storage holder while A continues computing

Start a fresh WebGPU job on A and disconnect B while batches are running. Disable B's network or close its node tab; do not use **Remove node**, which tests administrative deletion instead.

Expected results:

- A keeps completing batches using two full copies on A and C when B is unavailable.
- A transfer or pre-commit holder loss triggers a bounded re-placement attempt. There must be no commit claiming an unacknowledged holder.
- Previously committed 2+1 checkpoints remain recoverable with two holders and show **Degraded · 2/3** until a third holder can be assigned. New two-copy checkpoints can be **Healthy · 2/2**.
- After completion, **Test restore** reports that the full-artifact SHA-256 matches.

Also finish one job with all three connected, disconnect B afterward, then run **Test restore**. That specifically tests reconstruction of a committed 2+1 checkpoint with one missing shard, rather than restoration of a newer two-copy fallback.

## Trial 2: lose the device doing the computation

Reconnect all three devices, create a fresh job, and run it on A. Disconnect A during an unfinished batch. Wait until A is marked offline, then click **Run on this node** on B.

Expected results:

- Already committed batches remain committed.
- B completes independent pending batches and reclaims the interrupted batch after its old lease expires. Two connected devices are sufficient to resume with full copies.
- There is no automatic compute takeover until a survivor is told to run the job. The storage repair loop is automatic, but compute participation is currently explicit.

A clean browser socket close is usually detected promptly. A network blackhole can take roughly a minute to be declared offline, followed by the worker's ten-second reconnect grace. Allow this time before treating a reclaim wait as a failure.

## Limits and evidence to collect

- With three holders and one offline, there is no distinct fourth node onto which to repair a missing 2+1 shard. Recoverability with two surviving shards is the expected result. Testing repair while the failed holder remains offline needs an additional spare.
- Direct connections use STUN. Restrictive NAT/firewall combinations can require a TURN relay; STUN alone cannot guarantee every cross-network pair. See [MDN's WebRTC protocol explanation](https://developer.mozilla.org/en-US/docs/Web/API/WebRTC_API/Protocols). This deployment does not include TURN credentials.
- Record the execution mode, completed batch count before/after disconnect, latest checkpoint health, restore result, and whether you disconnected a storage holder or the compute worker. Do not include invite tokens, identity values, or node connection URLs in screenshots or logs.
- Automated browser tests exchange real bytes between isolated contexts on one host. They cover abrupt socket closes, storage loss, repair onto a spare, transfer failures, holder changes before commit, and lease renewal. They do not simulate three physical networks or prove hardware WebGPU performance.
