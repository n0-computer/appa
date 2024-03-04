use std::io::Cursor;

use crate::store::flatfs::Flatfs;
use anyhow::{anyhow, Result};
use cid::Cid;
use iroh_net::key::SecretKey;

use wnfs::private::AccessKey;
use wnfs::root_tree::RootTree;

/// Appa data directory
pub const ROOT_DIR: &str = ".appa";

/// Key storing the latest data root CID
pub const DATA_ROOT: &str = "DATA_ROOT_V1";

/// Key storing the access symmetric key for the private root directory
pub const PRIVATE_ACCESS_KEY: &str = "PRIVATE_ACCESS_KEY_V1";

/// Secret key used for the iroh-net magic endpoint
pub const PEER_SECRET_KEY: &str = "PEER_SECRET_KEY_V1";

/// ALPN protocol identifier for the car mirror pull protocol for appa
pub const ALPN_APPA_CAR_MIRROR_PULL: &[u8] = b"appa/car-mirror/pull/v0";

/// ALPN protocol identifier for fetching arbitrary key-value pairs from the store
pub const ALPN_APPA_KEY_VALUE_FETCH: &[u8] = b"appa/key-value/fetch/v0";

#[derive(Debug, Clone)]
pub struct Appa {
    pub fs: RootTree<Flatfs>,
    pub peer_key: SecretKey,
}

impl Appa {
    pub async fn init() -> Result<Self> {
        let mut root_tree = RootTree::empty(Flatfs::new(ROOT_DIR)?);

        let access_key = root_tree.create_private_root(&["private".into()]).await?;
        root_tree
            .store
            .put(PRIVATE_ACCESS_KEY, access_key.to_bytes()?)?;

        let peer_key = SecretKey::generate();
        root_tree
            .store
            .put(PEER_SECRET_KEY, peer_key.to_bytes().to_vec())?;

        Ok(Self {
            fs: root_tree,
            peer_key,
        })
    }

    pub async fn commit(&mut self) -> Result<()> {
        let cid = self.fs.store().await?;
        self.fs.store.put(DATA_ROOT, cid.to_bytes())?;
        Ok(())
    }

    pub async fn load() -> Result<Self> {
        let bs = Flatfs::new(ROOT_DIR)?;

        let data_root_entry = bs.get(DATA_ROOT)?.ok_or(anyhow!("No data root"))?;
        let data_root = Cid::read_bytes(Cursor::new(data_root_entry))?;

        let mut root_tree = RootTree::load(&data_root, bs).await?;

        let access_key_bytes = &root_tree
            .store
            .get(PRIVATE_ACCESS_KEY)?
            .ok_or(anyhow!("No access key"))?;
        let access_key = AccessKey::parse(access_key_bytes)?;

        root_tree
            .load_private_root(&["private".into()], &access_key)
            .await?;

        let peer_key_bytes = root_tree.store.get(PEER_SECRET_KEY)?.unwrap_or_else(|| {
            println!("No secret key for peering found - generating a new one.");
            SecretKey::generate().to_bytes().to_vec()
        });
        let peer_key = SecretKey::from_bytes(
            &peer_key_bytes
                .try_into()
                .map_err(|b: Vec<u8>| anyhow!("Wrong secret key len: {}", b.len()))?,
        );
        root_tree.store.put(PEER_SECRET_KEY, peer_key.to_bytes())?;

        Ok(Self {
            fs: root_tree,
            peer_key,
        })
    }
}
