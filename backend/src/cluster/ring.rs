//! Consistent-hash ring with virtual nodes.
//!
//! Every storage node is hashed onto a 64-bit ring many times ("virtual nodes").
//! To place a chunk we hash its id onto the ring and walk clockwise; the distinct
//! nodes we meet, in order, form the chunk's *preference list*.
//!
//! Why consistent hashing? When a node is added/removed only ~1/N of the keys move,
//! instead of nearly all of them as with `hash(key) % N`.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
pub struct HashRing {
    ring: BTreeMap<u64, String>,
    distinct_nodes: usize,
}

fn hash64(input: &str) -> u64 {
    let digest = Sha256::digest(input.as_bytes());
    u64::from_be_bytes(digest[..8].try_into().unwrap())
}

impl HashRing {
    pub fn new(node_ids: &[String], vnodes_per_node: usize) -> Self {
        let mut ring = BTreeMap::new();
        for id in node_ids {
            for v in 0..vnodes_per_node {
                ring.insert(hash64(&format!("{}#{}", id, v)), id.clone());
            }
        }
        HashRing { ring, distinct_nodes: node_ids.len() }
    }

    /// All distinct nodes, ordered by ring position starting at `key`.
    pub fn preference_list(&self, key: &str) -> Vec<String> {
        let start = hash64(key);
        let mut out: Vec<String> = Vec::with_capacity(self.distinct_nodes);
        for (_, node) in self.ring.range(start..).chain(self.ring.range(..start)) {
            if !out.contains(node) {
                out.push(node.clone());
                if out.len() == self.distinct_nodes {
                    break;
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lists_every_node_once_and_is_deterministic() {
        let ids: Vec<String> = (1..=5).map(|i| format!("s{}", i)).collect();
        let ring = HashRing::new(&ids, 64);
        let a = ring.preference_list("some-chunk");
        let b = ring.preference_list("some-chunk");
        assert_eq!(a, b);
        assert_eq!(a.len(), 5);
        let mut sorted = a.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 5);
    }

    #[test]
    fn load_is_roughly_balanced() {
        let ids: Vec<String> = (1..=5).map(|i| format!("s{}", i)).collect();
        let ring = HashRing::new(&ids, 128);
        let mut counts = std::collections::HashMap::new();
        for i in 0..10_000 {
            let first = ring.preference_list(&format!("chunk-{}", i))[0].clone();
            *counts.entry(first).or_insert(0usize) += 1;
        }
        for (_, c) in counts {
            assert!(c > 1_000 && c < 3_000, "unbalanced: {}", c);
        }
    }
}
