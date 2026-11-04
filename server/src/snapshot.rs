//! Encrypted durable snapshots. Keys live in Space secrets, never beside data.
use crate::{now, Store, MAX_STATE_BYTES};
use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Nonce,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};

pub const FORMAT: &str = "mesh-state-aes256gcm-v1";
pub const MAX_FILE_BYTES: usize = MAX_STATE_BYTES.div_ceil(3) * 4 + 1024;
const INVALID: &str =
    "State snapshot authentication failed; check the encryption key and storage location";

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    format: String,
    nonce: String,
    ciphertext: String,
}

pub struct Codec {
    key: [u8; 32],
    aad: Vec<u8>,
    migration_until: Option<u64>,
    migration_open: AtomicBool,
}
impl Codec {
    pub fn from_env(context: &str) -> Result<Self, String> {
        let key = std::env::var("MESH_STATE_ENCRYPTION_KEY")
            .ok()
            .and_then(|v| data_encoding::HEXLOWER.decode(v.as_bytes()).ok())
            .and_then(|v| v.try_into().ok())
            .ok_or("Set MESH_STATE_ENCRYPTION_KEY to a private 32-byte lowercase hex key")?;
        let until = std::env::var("MESH_STATE_PLAINTEXT_MIGRATION_UNTIL")
            .ok()
            .map(|v| {
                v.parse::<u64>()
                    .map_err(|_| "Invalid state migration deadline")
            })
            .transpose()?;
        Self::new(key, context, until, now())
    }
    fn new(key: [u8; 32], context: &str, until: Option<u64>, at: u64) -> Result<Self, String> {
        if until.is_some_and(|until| until > at.saturating_add(15 * 60)) {
            return Err("State migration window cannot exceed fifteen minutes".into());
        }
        Ok(Self {
            key,
            aad: serde_json::to_vec(&(FORMAT, context)).expect("static snapshot context"),
            migration_until: until,
            migration_open: AtomicBool::new(until.is_some_and(|until| until > at)),
        })
    }
    /// Close the compatibility gate after startup, including all conflict reloads.
    pub fn finish_migration(&self) {
        self.migration_open.store(false, Ordering::Release);
    }
    pub fn encode(&self, store: &Store) -> Result<Vec<u8>, String> {
        let bytes = serde_json::to_vec(store).map_err(|_| "Could not serialize state")?;
        if bytes.len() > MAX_STATE_BYTES {
            return Err("Cluster metadata exceeds the 16 MiB limit; delete old jobs".into());
        }
        let mut nonce = [0; 12];
        getrandom::getrandom(&mut nonce).map_err(|_| "State encryption randomness unavailable")?;
        let ciphertext = Aes256Gcm::new_from_slice(&self.key)
            .map_err(|_| INVALID)?
            .encrypt(
                &Nonce::from(nonce),
                Payload {
                    msg: &bytes,
                    aad: &self.aad,
                },
            )
            .map_err(|_| INVALID)?;
        serde_json::to_vec(&Envelope {
            format: FORMAT.into(),
            nonce: BASE64.encode(nonce),
            ciphertext: BASE64.encode(ciphertext),
        })
        .map_err(|_| "Could not encode encrypted state".into())
    }
    pub fn decode(&self, bytes: &[u8]) -> Result<(Store, bool), String> {
        self.decode_at(bytes, now())
    }
    fn decode_at(&self, bytes: &[u8], at: u64) -> Result<(Store, bool), String> {
        if bytes.len() > MAX_FILE_BYTES {
            return Err("Encrypted state exceeds the storage size limit".into());
        }
        let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| INVALID)?;
        if value.get("format").is_none() {
            if !self.migration_open.load(Ordering::Acquire)
                || !self.migration_until.is_some_and(|until| at < until)
            {
                return Err(
                    "Plaintext state refused; use the controlled one-time migration".into(),
                );
            }
            if bytes.len() > MAX_STATE_BYTES {
                return Err("Legacy state exceeds the 16 MiB limit".into());
            }
            let store = serde_json::from_value(value).map_err(|_| "Invalid legacy Mesh state")?;
            return Ok((store, true));
        }
        let envelope: Envelope = serde_json::from_value(value).map_err(|_| INVALID)?;
        if envelope.format != FORMAT {
            return Err(INVALID.into());
        }
        let nonce: [u8; 12] = BASE64
            .decode(envelope.nonce)
            .map_err(|_| INVALID)?
            .try_into()
            .map_err(|_| INVALID)?;
        let ciphertext = BASE64.decode(envelope.ciphertext).map_err(|_| INVALID)?;
        let plaintext = Aes256Gcm::new_from_slice(&self.key)
            .map_err(|_| INVALID)?
            .decrypt(
                &Nonce::from(nonce),
                Payload {
                    msg: &ciphertext,
                    aad: &self.aad,
                },
            )
            .map_err(|_| INVALID)?;
        if plaintext.len() > MAX_STATE_BYTES {
            return Err("State exceeds the 16 MiB limit".into());
        }
        Ok((
            serde_json::from_slice(&plaintext).map_err(|_| INVALID)?,
            false,
        ))
    }
    #[cfg(test)]
    pub fn for_test() -> Self {
        Self::new([73; 32], "test", None, 0).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Cluster;
    use std::collections::HashMap;
    use uuid::Uuid;

    #[test]
    fn encrypted_roundtrip_hides_metadata_and_randomizes_each_snapshot() {
        let codec = Codec::for_test();
        let id = Uuid::new_v4();
        let mut store = Store::default();
        store.clusters.insert(
            id,
            Cluster {
                id,
                name: "private-cluster-name".into(),
                owner_id: "private-owner-principal".into(),
                members: HashMap::new(),
            },
        );
        let first = codec.encode(&store).unwrap();
        let second = codec.encode(&store).unwrap();
        assert_ne!(first, second);
        let text = String::from_utf8(first.clone()).unwrap();
        assert!(!text.contains("private-cluster-name"));
        assert!(!text.contains("private-owner-principal"));
        assert!(!text.contains("clusters"));
        let (restored, migrated) = codec.decode(&first).unwrap();
        assert!(!migrated);
        assert_eq!(restored.clusters[&id].name, "private-cluster-name");
    }

    #[test]
    fn tampering_wrong_key_context_version_and_plaintext_downgrades_fail_closed() {
        let codec = Codec::for_test();
        let bytes = codec.encode(&Store::default()).unwrap();
        assert!(Codec::new([74; 32], "test", None, 0)
            .unwrap()
            .decode(&bytes)
            .is_err());
        assert!(Codec::new([73; 32], "other-repo-or-path", None, 0)
            .unwrap()
            .decode(&bytes)
            .is_err());
        for field in ["nonce", "ciphertext", "format"] {
            let mut v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            v[field] = "AAAA".into();
            assert!(codec.decode(&serde_json::to_vec(&v).unwrap()).is_err());
        }
        assert!(codec
            .decode(&serde_json::to_vec(&Store::default()).unwrap())
            .is_err());
        assert!(codec.decode(b"broken JSON").is_err());
        assert!(codec.decode(&vec![0; MAX_FILE_BYTES + 1]).is_err());
    }

    #[test]
    fn migration_expires_and_closes_without_leaving_a_plaintext_fallback() {
        let bytes = serde_json::to_vec(&Store::default()).unwrap();
        let codec = Codec::new([73; 32], "test", Some(100), 0).unwrap();
        let (store, migration) = codec.decode_at(&bytes, 99).unwrap();
        assert!(migration);
        assert!(codec.decode_at(&bytes, 100).is_err());
        let encrypted = codec.encode(&store).unwrap();
        codec.finish_migration();
        assert!(codec.decode_at(&bytes, 99).is_err());
        assert!(codec.decode(&encrypted).is_ok());
        assert!(Codec::new([73; 32], "test", Some(901), 0).is_err());
        assert!(Codec::new([73; 32], "test", Some(99), 100)
            .unwrap()
            .decode_at(&bytes, 100)
            .is_err());
    }
}
