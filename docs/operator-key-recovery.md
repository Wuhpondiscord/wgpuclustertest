# Recovering the Space encryption values

The operator can back up and recover the actual `MESH_STATE_ENCRYPTION_KEY` and `MESH_AUTH_ENCRYPTION_KEY` values without resetting the database. This preserves account keys, authenticators, memberships and application grants. It does not recover an individual's lost account key or bypass TOTP.

The Space stores only a public age recipient in `MESH_KEY_RECOVERY_RECIPIENT`. At startup the coordinator encrypts its actual in-use encryption values and storage location into a standard age file. The separate recovery private identity stays on the operator's computer. The backup includes the state key and the authenticator encryption key when configured, not GitHub/Hugging Face tokens or user account credentials. Temporary plaintext serialization buffers are zeroed after use; the running coordinator still necessarily holds its operational encryption keys.

## First setup

On a trusted computer, from the repository root:

```sh
cargo run --manifest-path server/Cargo.toml --locked --bin mesh-key-recovery -- init /private/path/mesh-recovery
```

The tool creates `mesh-recovery-identity.txt` (private) and `mesh-recovery-recipient.txt` (public). It prints only the public recipient. Save the private identity in an encrypted password manager or offline backup. The private file is created with mode 0600 on Unix; on Windows its ACL is restricted to the creating user, SYSTEM and Administrators before any private bytes are written. The tool refuses to overwrite existing private files. Protect the containing directory and computer as well. A local administrator or compromised operator computer can still access these files.

Add **only the public recipient** to `ops/key-recovery-recipients.json`, under the exact Space ID, for example `{"owner/space":"age1..."}`. CI validates the recipient/checksum using the age library. Deployment pins it to the Space variable only if absent and refuses to silently replace a different configured recipient. Forks targeting another Space ID do not inherit this configuration. Only trusted operators with deployment or Space-settings access should change the pin; an attacker who controls deployment source can already execute code with the server's secrets.

Alternatively set `MESH_KEY_RECOVERY_RECIPIENT` directly in the Space Settings. Use the public `age1...` value, never `AGE-SECRET-KEY-...`. Space configuration changes restart the coordinator and expire account sessions. Preserve both original encryption secrets.

## Download and recover

After deployment, save `/api/operator/key-backup` from your Space as `mesh-operator-keys.age`. The response is a download with `Cache-Control: no-store`, an octet-stream content type, and an `x-mesh-recovery-recipient` header containing the public pin. It is generated once at startup, not re-encrypted per request. The HTTP API never returns raw encryption values and never accepts a caller-selected recipient. A query parameter cannot redirect the export to another key; POST is unsupported.

**The encrypted download is public to fetch.** Its confidentiality depends on the private recovery identity, not cluster/account permissions. A cluster owner/member or anonymous caller gets only the same age ciphertext. This avoids creating another server-wide bearer credential or giving application roles access to master keys. With no configured recipient the endpoint returns 404. A malformed configured recipient prevents startup instead of exposing a fallback export.

Decrypt on the trusted computer:

```sh
cargo run --manifest-path server/Cargo.toml --locked --bin mesh-key-recovery -- decrypt /private/path/mesh-recovery/mesh-recovery-identity.txt /private/path/mesh-operator-keys.age /private/path/recovered-space-keys.json
```

The output JSON contains the **actual encryption values**, the creation time, and the repository/branch/path needed to interpret the snapshot's authenticated context. The tool writes a new private file, never prints the values, and refuses to overwrite an existing output. Wrong identities, modified ciphertext, invalid schemas and oversized backups are rejected. Standard age/rage tooling can also decrypt the binary age file with the identity; the repository tool adds output-permission and schema checks.

Keep private identities and recovered JSON out of source repositories, Space uploads, chat, screenshots and logs. The project ignores the standard private identity/output names, but ignore patterns are not a substitute for storing them outside the source tree. The public recipient and encrypted `.age` file do not reveal the encryption values. An authenticated age file protects against modification, but anyone knowing the public recipient can create a different age file: fetch the backup from your trusted Space over HTTPS and verify the values against the expected encrypted database before a disaster restore. This is an encrypted export, not a digital signature proving the operator's provenance.

## Restore and rotation

To restore the same database, put the recovered exact values back into the corresponding Hugging Face secrets. Preserve the storage repository/branch/path (or explicitly re-encrypt for a new location), then restart. Do not generate replacement encryption values for an existing database. Never restore an older database casually; it can undo revocations and consumed recovery-code state. This export does not erase historical plaintext commits or add an external anti-rollback authority.

Rotating the recovery identity changes only who can decrypt future key-backup files; it does not rotate the database or authenticator keys. Preserve an old identity for old backups. Update the Space variable and matching reviewed public pin explicitly, redeploy, download a new backup and test decryption before retiring the old identity. Anyone retaining an old identity and its matching backup still has the exported encryption values. If the recovery identity was compromised, rotating the recipient alone is insufficient: plan an authenticated rotation/re-encryption of the exposed operational keys too.

If the Space loses its encryption secrets but you retain the matching encrypted backup and private identity, recover and restore the originals. If both the operational key and every decryptable backup are lost, encryption cannot be bypassed; a database reset is then a separate destructive operation. No reset is part of this feature.

References: [age file format and CLI](https://github.com/FiloSottile/age), [Rust age library](https://docs.rs/age/0.12.1/age/), [Hugging Face Space configuration](https://huggingface.co/docs/huggingface_hub/main/en/guides/manage-spaces).
