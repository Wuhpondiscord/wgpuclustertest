# Database disclosure and integrity review

Scope: the serialized `Store` and its GitHub history, account authentication, node credentials, and snapshot import. The review inspected the production schema without copying credentials or identifiers into reports. This is a code review and regression testing, not an independent security audit.

## Impact of a leaked snapshot

| Material | Before this change | After this change |
| --- | --- | --- |
| Usernames, stable principal IDs, cluster/member relationships, application grants and names | Readable JSON; API permissions do not hide it from repository readers | Inside an authenticated encrypted snapshot |
| Node names/status, job configuration, checkpoint locations/hashes and recovery history | Readable when present | Inside the same encrypted snapshot; ciphertext length and commit timing remain observable |
| Account, node, invite and recovery credentials | High-entropy credential digests, not usable bearer keys | Digests remain; the entire snapshot additionally hides them |
| Authenticator seeds | Separately encrypted with account-bound AES-256-GCM | Inner encryption preserved, plus outer snapshot encryption with a different key |
| Account sessions and pending authenticator setup secrets | Coordinator memory only | Still excluded from snapshots |

The raw 256-bit account/node/invite keys and 128-bit recovery codes are randomly generated. Knowing their SHA-256 digest does not substitute for presenting the original credential. No user-chosen passwords are supported; this analysis must be revisited if passwords are introduced. An exposed snapshot does not itself disclose the separate Space secrets or the user's saved key. If the account key is separately stolen, TOTP still protects sign-in when enabled. Recovery codes require that key too.

Before snapshot authentication, repository write access could replace an account's credential digest, remove its authenticator, or alter grants/ownership. AES-GCM now rejects unauthenticated edits, a wrong key, an unsupported envelope format, and copying ciphertext to another repository/branch/path. A repository writer can still delete or withhold data, deny service, or replay an older valid encrypted snapshot from the same storage location. GitHub revision comparison prevents conflicting writes; it is not an independent anti-rollback authority. Replaying old state can revive old credentials/grants/recovery-code state. Strong rollback protection needs a trusted monotonic record outside this repository. A compromised coordinator can read plaintext and use its keys; storage encryption cannot protect against that compromise.

## Storage and migration

All production durable writes (GitHub and local development files) use `mesh-state-aes256gcm-v1`: AES-256-GCM, a fresh OS-random 96-bit nonce, and authenticated context containing the format plus the repository/branch/path (or the local-storage namespace). The only visible JSON fields are `format`, `nonce`, and `ciphertext`. The ciphertext includes the authentication tag. The plaintext metadata limit stays 16 MiB; the transport bound allows Base64 expansion. The serializer still excludes active leases/runtime sessions. Expired invite entries are pruned during subsequent mutations.

`MESH_STATE_ENCRYPTION_KEY` is a separate 32-byte lowercase hexadecimal Space secret. Deployment creates it only if missing and never logs or replaces it. Keep a secure independent backup: losing this key makes the whole snapshot unreadable. Keep `MESH_AUTH_ENCRYPTION_KEY` too, because authenticator seeds retain their separate encryption. [Operator key recovery](operator-key-recovery.md) can encrypt both actual values to an operator-pinned public recipient for offline recovery with a separately saved private identity. Changing the configured repository, branch or path requires an explicit authenticated export/re-encryption procedure, not copying the encrypted file unchanged. Do not deploy an older server version that expects plaintext after migration.

The first encryption deployment issues a fifteen-minute `MESH_STATE_PLAINTEXT_MIGRATION_UNTIL` permission. Startup imports a valid existing plaintext snapshot only inside that window and persists it encrypted before opening the listener. Failure to persist prevents startup. The process closes its import gate immediately, including conflict/refresh loads. The workflow waits for the uploaded container revision and encrypted-state health header, then removes the migration permission and checks readiness after the configuration restart. It never renews this permission automatically on later deploys. Encrypted files never fall back to plaintext after authentication failure. Missing/malformed keys, corrupt files and unreadable local files fail closed instead of silently creating an empty replacement. A genuinely absent state file remains valid for a new installation.

Deployment configuration changes restart the coordinator and expire its temporary account sessions; sign in again with the saved account key and authenticator. No account key or authenticator reset is required by this migration. The original cluster/account identifiers and grants are preserved.

## Historical data and incident response

Encrypting the current file **does not encrypt old commits, clones, cached files or prior leaks**. Treat all pre-migration history as readable metadata. Raw account keys were not written there by the account feature, and authenticator seeds were already separately encrypted. History removal needs its own deliberate maintenance operation; it cannot recall other copies and is not performed automatically.

For a confirmed disclosure, identify whether it included only snapshots, repository write access, a saved account/node key, or Space/coordinator secrets. Revoke exposed invites/nodes and rotate exposed account keys. Re-enroll an authenticator if its seed or the authenticator encryption key was exposed; recovery-code renewal comes from re-enrollment. If a state encryption key leaked, re-encrypt under a fresh key and assess all historical ciphertext accessible with the old key. Rotate compromised GitHub/Hugging Face credentials at their source. Do not restore old snapshots casually, since that can undo revocations and consumed-code records. Keep the storage repository private and tightly scope its access.

Application API permissions and encrypted storage do not hide assigned plaintext code/data from the device that executes a workload. Private runtime placement and isolated application hosting remain separate work.

## Validation

Tests exercise randomized encryption round trips, metadata absence from ciphertext, wrong-key/context rejection, tampering/version rejection, bounded import expiry and closure, oversized files, and account/TOTP/recovery behavior restored from encrypted files. Existing routing, persistence-failure, fencing, sharding and browser recovery tests run against the changed persistence path. Browser integration uses explicit test-only state/authenticator keys, never production keys.

References: [OWASP cryptographic storage guidance](https://cheatsheetseries.owasp.org/cheatsheets/Cryptographic_Storage_Cheat_Sheet.html), [Hugging Face secret management](https://huggingface.co/docs/huggingface_hub/main/en/guides/manage-spaces).
