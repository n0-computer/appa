use std::io::Cursor;
use std::str::FromStr;

use crate::state::{DATA_ROOT, ROOT_DIR};
use crate::store::flatfs::Flatfs;
use anyhow::{anyhow, Context as _, Result};
use cid::Cid;
use libipld::codec::Decode;
use libipld::{Ipld, IpldCodec};

use wnfs::common::BlockStore;

pub async fn doctor(item: String) -> Result<()> {
    let store = Flatfs::new(ROOT_DIR)?;
    if let Ok(cid) = Cid::from_str(&item) {
        // Print blocks behind CIDs
        let block = store
            .get_block(&cid)
            .await
            .context("Block not found in store")?;
        let codec = IpldCodec::try_from(cid.codec())
            .context(format!("Unrecognized codec: {}", cid.codec()))?;
        println!("Codec: {codec:?}");
        println!("Length: {} bytes", block.len());
        if codec == IpldCodec::Raw {
            println!("{}", hex::encode(block));
        } else {
            let ipld =
                Ipld::decode(codec, &mut Cursor::new(block)).context("Couldn't parse block")?;
            println!("{ipld:#?}");
        }

        let data_root_entry = store
            .get(DATA_ROOT)?
            .ok_or(anyhow!("No data root for path analysis."))?;
        let data_root = Cid::read_bytes(Cursor::new(data_root_entry))?;
        for path in find_paths(&store, data_root, &cid, vec![]).await {
            println!("Found path: {path:?}");
        }
    } else {
        // Output values from store
        let value = store
            .get(&item)?
            .ok_or(anyhow!("No value with this key in store."))?;
        println!("Value hex: {}", hex::encode(&value));
        if let Ok(str) = String::from_utf8(value.clone()) {
            println!("Seems to be a string? {str}");
        }
        if let Ok(cid) = Cid::read_bytes(&mut Cursor::new(&value)) {
            println!("Seems to be a CID? {cid}");
        }
    }

    Ok(())
}

#[async_recursion::async_recursion]
async fn find_paths(
    store: &impl BlockStore,
    from: Cid,
    needle: &Cid,
    mut path_so_far: Vec<Cid>,
) -> Vec<Vec<Cid>> {
    if from == *needle {
        path_so_far.push(from);
        return vec![path_so_far];
    }

    let Ok(block) = store.get_block(&from).await else {
        return vec![];
    };
    let refs = car_mirror::common::references(from, block, Vec::new()).unwrap();
    let mut paths = vec![];
    for r in refs {
        let mut path_so_far = path_so_far.clone();
        path_so_far.push(from);
        paths.extend(find_paths(store, r, needle, path_so_far).await);
    }
    paths
}
