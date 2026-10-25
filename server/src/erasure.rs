//! Systematic 2+1 XOR coding for bounded checkpoint artifacts.
//!
//! This is byte-level erasure coding, not encryption or tamper protection.
//! Callers must authenticate the shard digests and verify the reconstructed
//! full-artifact digest before using restored bytes.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Shards {
    pub data_a: Vec<u8>,
    pub data_b: Vec<u8>,
    pub parity: Vec<u8>,
    pub logical_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErasureError {
    EmptyArtifact,
    InvalidLogicalLength,
    ShardsTooShort,
}

pub fn encode_2_plus_1(input: &[u8]) -> Result<Shards, ErasureError> {
    if input.is_empty() { return Err(ErasureError::EmptyArtifact); }
    let shard_len = input.len().div_ceil(2);
    let mut data_a = vec![0; shard_len];
    let mut data_b = vec![0; shard_len];
    data_a.copy_from_slice(&input[..shard_len]);
    if input.len() > shard_len { data_b[..input.len() - shard_len].copy_from_slice(&input[shard_len..]); }
    let parity = data_a.iter().zip(&data_b).map(|(a, b)| a ^ b).collect();
    Ok(Shards { data_a, data_b, parity, logical_bytes: input.len() })
}

/// Restore from any two indexed shards: 0=data A, 1=data B, 2=parity.
pub fn reconstruct_2_plus_1(shards: &[(u8, &[u8])], logical_bytes: usize) -> Result<Vec<u8>, ErasureError> {
    if logical_bytes == 0 { return Err(ErasureError::EmptyArtifact); }
    let shard_len = logical_bytes.div_ceil(2);
    if shards.len() != 2 || logical_bytes > shard_len.saturating_mul(2) {
        return Err(ErasureError::InvalidLogicalLength);
    }
    let (first_index, first) = shards[0];
    let (second_index, second) = shards[1];
    if first_index > 2 || second_index > 2 || first_index == second_index
        || first.len() != shard_len || second.len() != shard_len
    {
        return Err(ErasureError::ShardsTooShort);
    }
    let mut data_a = None;
    let mut data_b = None;
    let mut parity = None;
    for (index, bytes) in shards {
        match index {
            0 => data_a = Some(bytes.to_vec()),
            1 => data_b = Some(bytes.to_vec()),
            2 => parity = Some(bytes.to_vec()),
            _ => return Err(ErasureError::ShardsTooShort),
        }
    }
    match (data_a.is_some(), data_b.is_some(), parity.is_some()) {
        (true, true, _) => {},
        (true, false, true) => {
            let a = data_a.as_ref().unwrap();
            let p = parity.as_ref().unwrap();
            data_b = Some(a.iter().zip(p).map(|(a, p)| a ^ p).collect());
        },
        (false, true, true) => {
            let b = data_b.as_ref().unwrap();
            let p = parity.as_ref().unwrap();
            data_a = Some(b.iter().zip(p).map(|(b, p)| b ^ p).collect());
        },
        _ => return Err(ErasureError::ShardsTooShort),
    }
    let mut restored = data_a.unwrap();
    restored.extend(data_b.unwrap());
    restored.truncate(logical_bytes);
    Ok(restored)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_single_missing_shard_reconstructs_even_and_odd_artifacts() {
        for bytes in [b"abcdef".as_slice(), b"abcde".as_slice()] {
            let coded = encode_2_plus_1(bytes).unwrap();
            let all = [coded.data_a.as_slice(), coded.data_b.as_slice(), coded.parity.as_slice()];
            for missing in 0..3 {
                let available: Vec<(u8, &[u8])> = (0..3).filter(|index| *index != missing)
                    .map(|index| (index as u8, all[index])).collect();
                assert_eq!(reconstruct_2_plus_1(&available, bytes.len()).unwrap(), bytes);
            }
        }
    }

    #[test]
    fn rejects_duplicate_or_wrong_sized_shards() {
        assert_eq!(encode_2_plus_1(b""), Err(ErasureError::EmptyArtifact));
        assert_eq!(reconstruct_2_plus_1(&[(0, b"ab"), (0, b"ab")], 4), Err(ErasureError::ShardsTooShort));
        assert_eq!(reconstruct_2_plus_1(&[(0, b"a"), (2, b"a")], 4), Err(ErasureError::ShardsTooShort));
    }
}

