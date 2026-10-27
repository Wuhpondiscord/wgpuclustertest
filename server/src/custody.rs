//! Independently signed holder acknowledgements. Artifact bytes never enter here.
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use p256::ecdsa::{signature::Verifier, Signature, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;
use crate::checkpoint::{ArtifactManifest, ArtifactShard, CheckpointCatalog, ShardReceipt, StorageStrategy};

const RECEIPT_TTL_SECONDS: u64 = 600;
const MAX_RECEIPTS: usize = 8192;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HolderProof {
    pub node_id: Uuid,
    pub artifact_name: String,
    pub sha256: String,
    pub bytes: u64,
    pub nonce: String,
    pub public_key: String,
    pub signature: String,
    pub observed_at: u64,
}

pub fn signing_input(nonce: &str, node_id: Uuid, name: &str, sha256: &str, bytes: u64) -> Vec<u8> {
    serde_json::to_vec(&("mesh-holder-v1", nonce, node_id, name, sha256, bytes)).expect("receipt tuple serializes")
}

pub fn public_key(encoded: &str) -> Result<VerifyingKey, String> {
    if encoded.len() > 100 { return Err("Invalid holder public key".into()); }
    let bytes = BASE64.decode(encoded).map_err(|_| "Invalid holder public key")?;
    VerifyingKey::from_sec1_bytes(&bytes).map_err(|_| "Invalid holder public key".into())
}

#[derive(Default)]
pub struct Registry {
    receipts: HashMap<(Uuid, String), HolderProof>,
    sessions: HashMap<Uuid, (String, Option<String>)>,
}

impl Registry {
    pub fn remove_node(&mut self, node_id: Uuid) {
        self.receipts.retain(|(id, _), _| *id != node_id);
        self.sessions.remove(&node_id);
    }

    pub fn begin_session(&mut self, node_id: Uuid, nonce: String) {
        self.remove_node(node_id);
        self.sessions.insert(node_id, (nonce, None));
    }

    pub fn end_session(&mut self, node_id: Uuid, nonce: &str) {
        if self.sessions.get(&node_id).is_some_and(|(current, _)| current == nonce) { self.remove_node(node_id); }
    }

    pub fn register_key(&mut self, node_id: Uuid, nonce: &str, key: &str) -> Result<(), String> {
        let (current_nonce, current_key) = self.sessions.get_mut(&node_id).ok_or("The holder session ended")?;
        if current_nonce != nonce || current_key.as_ref().is_some_and(|current| current != key) { return Err("The holder signing session changed".into()); }
        *current_key = Some(key.to_owned());
        Ok(())
    }

    pub fn accept(&mut self, proof: HolderProof, authenticated_node: Uuid, session_nonce: &str, session_key: &str, now: u64) -> Result<(), String> {
        if proof.node_id != authenticated_node || proof.nonce != session_nonce || proof.public_key != session_key {
            return Err("Receipt is not bound to the authenticated holder session".into());
        }
        if !self.sessions.get(&authenticated_node).is_some_and(|(nonce, key)| nonce == session_nonce && key.as_deref() == Some(session_key)) {
            return Err("The holder signing session is no longer current".into());
        }
        if proof.artifact_name.is_empty() || proof.artifact_name.len() > 128 || proof.bytes == 0 || proof.bytes > 8 * 1024 * 1024
            || proof.sha256.len() != 64 || !proof.sha256.bytes().all(|b| b.is_ascii_hexdigit()) || proof.signature.len() > 100 {
            return Err("Invalid holder receipt metadata".into());
        }
        let key = public_key(session_key)?;
        let signature = BASE64.decode(&proof.signature).map_err(|_| "Invalid holder signature")?;
        let signature = Signature::from_slice(&signature).map_err(|_| "Invalid holder signature")?;
        key.verify(&signing_input(&proof.nonce, proof.node_id, &proof.artifact_name, &proof.sha256, proof.bytes), &signature)
            .map_err(|_| "Holder signature verification failed")?;
        self.receipts.retain(|_, receipt| now.saturating_sub(receipt.observed_at) <= RECEIPT_TTL_SECONDS);
        let id = (proof.node_id, proof.artifact_name.clone());
        if let Some(existing) = self.receipts.get(&id) {
            if existing.sha256 != proof.sha256 || existing.bytes != proof.bytes { return Err("Artifact names are immutable".into()); }
        } else if self.receipts.len() >= MAX_RECEIPTS { return Err("Holder receipt cache is full; retry later".into()); }
        self.receipts.insert(id, HolderProof { observed_at: now, ..proof });
        Ok(())
    }

    pub fn proof(&self, shard: &crate::ShardReceipt, now: u64) -> Result<HolderProof, String> {
        self.receipts.get(&(shard.node_id, shard.artifact_name.clone()))
            .filter(|proof| proof.sha256 == shard.sha256 && proof.bytes == shard.bytes && now.saturating_sub(proof.observed_at) <= RECEIPT_TTL_SECONDS)
            .cloned().ok_or_else(|| "A shard holder has not independently confirmed storage; refresh its node client and retry".into())
    }

    pub fn verify_manifest(&self, id: &str, logical_bytes: u64, shards: &[crate::ShardReceipt], now: u64) -> Result<Vec<HolderProof>, String> {
        let strategy = match shards.len() { 2 => StorageStrategy::Replication2x, 3 => StorageStrategy::Erasure2Plus1, _ => return Err("Unsupported checkpoint manifest".into()) };
        let mut catalog = CheckpointCatalog::default();
        catalog.begin(ArtifactManifest {
            checkpoint_id: id.into(), generation: 1, base_checkpoint_id: None,
            logical_bytes, payload_bytes: logical_bytes, strategy,
            shards: shards.iter().map(|shard| ArtifactShard { index: shard.shard_index as usize, node_id: shard.node_id.to_string(), failure_domain: String::new(), bytes: shard.bytes }).collect(),
        }, shards.iter().map(|shard| shard.sha256.clone()).collect()).map_err(|_| "Invalid checkpoint catalog manifest")?;
        let mut proofs = Vec::new();
        for shard in shards {
            let proof = self.proof(shard, now)?;
            catalog.acknowledge(ShardReceipt { checkpoint_id: id.into(), shard_index: shard.shard_index as usize, node_id: proof.node_id.to_string(), sha256: proof.sha256.clone() })
                .map_err(|_| "Holder confirmation does not match its assigned shard")?;
            proofs.push(proof);
        }
        catalog.commit(id).map_err(|_| "The checkpoint has not reached its durability target")?;
        Ok(proofs)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use p256::ecdsa::{signature::Signer, SigningKey};

    pub fn confirm(registry: &mut Registry, shard: &crate::ShardReceipt, now: u64) {
        let key = SigningKey::from_slice(&[7; 32]).unwrap();
        let public_key = BASE64.encode(key.verifying_key().to_encoded_point(false).as_bytes());
        let nonce = "unit-test-session".to_string();
        if !registry.sessions.get(&shard.node_id).is_some_and(|(current, _)| current == &nonce) { registry.begin_session(shard.node_id, nonce.clone()); }
        registry.register_key(shard.node_id, &nonce, &public_key).unwrap();
        let signature: Signature = key.sign(&signing_input(&nonce, shard.node_id, &shard.artifact_name, &shard.sha256, shard.bytes));
        let proof = HolderProof { node_id: shard.node_id, artifact_name: shard.artifact_name.clone(), sha256: shard.sha256.clone(), bytes: shard.bytes, nonce: nonce.clone(), public_key: public_key.clone(), signature: BASE64.encode(signature.to_bytes()), observed_at: now };
        registry.accept(proof, shard.node_id, &nonce, &public_key, now).unwrap();
    }

    #[test]
    fn signatures_bind_holder_session_content_and_artifact_name() {
        let node_id = Uuid::new_v4();
        let shard = crate::ShardReceipt { shard_index: 0, node_id, artifact_name: "immutable-artifact".into(), sha256: "a".repeat(64), bytes: 123 };
        let mut registry = Registry::default();
        confirm(&mut registry, &shard, 10);
        let proof = registry.proof(&shard, 10).unwrap();
        assert!(registry.accept(proof.clone(), Uuid::new_v4(), &proof.nonce, &proof.public_key, 11).is_err());
        assert!(registry.accept(proof.clone(), node_id, "replayed-session", &proof.public_key, 11).is_err());
        let mut corrupt = proof.clone(); corrupt.bytes += 1;
        assert!(registry.accept(corrupt, node_id, &proof.nonce, &proof.public_key, 11).is_err());
        assert!(registry.proof(&shard, 611).is_err());
        registry.begin_session(node_id, "replacement-session".into());
        registry.end_session(node_id, &proof.nonce);
        assert!(registry.sessions.contains_key(&node_id));
        assert!(registry.accept(proof.clone(), node_id, &proof.nonce, &proof.public_key, 11).is_err());
        assert!(registry.proof(&shard, 11).is_err());
    }

    #[test]
    fn catalog_requires_independent_matching_confirmations_for_every_holder() {
        let shards: Vec<_> = (0..3).map(|index| crate::ShardReceipt { shard_index: index, node_id: Uuid::new_v4(), artifact_name: format!("s{index}"), sha256: format!("{index}").repeat(64), bytes: 32_768 }).collect();
        let mut registry = Registry::default();
        confirm(&mut registry, &shards[0], 10);
        confirm(&mut registry, &shards[1], 10);
        assert!(registry.verify_manifest("checkpoint", 65_536, &shards, 11).is_err());
        confirm(&mut registry, &shards[2], 10);
        assert_eq!(registry.verify_manifest("checkpoint", 65_536, &shards, 11).unwrap().len(), 3);
    }
}

