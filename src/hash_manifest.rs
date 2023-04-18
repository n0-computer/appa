use anyhow::{anyhow, Result};
use cid::Cid;
use libipld::{prelude::References, Ipld, IpldCodec};
use std::{collections::BTreeSet, io::Cursor};

use iroh::Hash;
use serde::{Deserialize, Serialize};

use crate::store::Store;

const BLAKE3_MC: u64 = 0x1e;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashManifest {
    hashes: Vec<Hash>,
}

impl HashManifest {
    pub fn new(hashes: impl IntoIterator<Item = Hash>) -> HashManifest {
        Self {
            hashes: hashes.into_iter().collect(),
        }
    }

    pub fn without(&self, manifest: &HashManifest) -> HashManifest {
        HashManifest::new(
            self.hashes
                .iter()
                .filter(|hash| !manifest.hashes.contains(hash))
                .cloned(),
        )
    }
}

pub fn walk_dag(store: Store, root: Cid) -> Result<HashManifest> {
    let mut visited: BTreeSet<Cid> = BTreeSet::new();
    let mut frontier = vec![root];
    while let Some(cid) = frontier.pop() {
        visited.insert(cid);
        let block = store.get_block_sync(cid)?;
        let codec = IpldCodec::try_from(cid.codec())?;
        frontier.extend(references(codec, block)?.filter(|cid| !visited.contains(cid)));
    }
    let mut hashes = Vec::new();
    for cid in visited.into_iter() {
        if cid.hash().code() != BLAKE3_MC {
            return Err(anyhow!("Expected blake3 only"));
        }
        let digest: [u8; 32] = cid.hash().digest().try_into()?;
        hashes.push(Hash::from(digest));
    }
    Ok(HashManifest { hashes })
}

fn references(codec: IpldCodec, block: Vec<u8>) -> Result<impl Iterator<Item = Cid>> {
    let mut refs = Vec::new();
    <Ipld as References<IpldCodec>>::references(codec, &mut Cursor::new(block), &mut refs)?;
    Ok(refs.into_iter())
}

#[test]
fn test_without() {
    let manifest_one = manifest_from(&["one", "two", "three"]);
    let manifest_two = manifest_from(&["two", "three", "four"]);
    let manifest_diff = manifest_one.without(&manifest_two);
    assert_eq!(manifest_diff, manifest_from(&["one"]));
}

#[cfg(test)]
fn manifest_from(stuff: &[&str]) -> HashManifest {
    HashManifest::new(stuff.iter().map(|s| Hash::new(s.as_bytes())))
}
