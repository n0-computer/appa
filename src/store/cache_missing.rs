use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use cid::Cid;
use quick_cache::{sync::Cache, GuardResult};
use wnfs::common::{BlockStore, BlockStoreError};

#[derive(Debug, Clone)]
pub struct CacheMissing<B: BlockStore> {
    missing_cids_cache: Arc<Cache<Cid, ()>>,
    pub inner: B,
}

impl<B: BlockStore> CacheMissing<B> {
    pub fn new(approx_cache_capacity: usize, inner: B) -> Self {
        Self {
            missing_cids_cache: Arc::new(Cache::new(approx_cache_capacity)),
            inner,
        }
    }
}

#[async_trait::async_trait]
impl<B: BlockStore> BlockStore for CacheMissing<B> {
    async fn get_block(&self, cid: &Cid) -> Result<Bytes> {
        match self.missing_cids_cache.get_value_or_guard(cid, None) {
            GuardResult::Guard(placeholder) => match self.inner.get_block(&cid).await {
                Ok(b) => Ok(b),
                Err(e) => {
                    if matches!(e.downcast_ref(), Some(BlockStoreError::CIDNotFound(_))) {
                        // Concurrent removes may have happened.
                        // We *could* re-try the `get_block`, but I don't really think that's
                        // how this should work.
                        let _ignore_err_caused_by_concurrent_remove = placeholder.insert(());
                    }
                    Err(e)
                }
            },
            GuardResult::Value(()) => Err(BlockStoreError::CIDNotFound(*cid).into()),
            GuardResult::Timeout => self.inner.get_block(&cid).await,
        }
    }

    async fn put_block(&self, bytes: impl Into<Bytes> + Send, codec: u64) -> Result<Cid> {
        let cid = self.inner.put_block(bytes, codec).await?;
        // TODO: I'm a little worried about race conditions here...
        self.missing_cids_cache.remove(&cid);
        Ok(cid)
    }
}
