use std::{path::PathBuf, rc::Rc};

use anyhow::{Context as _, Result};
use chrono::Utc;
use cid::Cid;
use wnfs::{
    common::{BlockStore, HashOutput},
    namefilter::Namefilter,
    private::{AesKey, PrivateDirectory, PrivateForest, PrivateNode, PrivateRef, TemporalKey},
    public::PublicDirectory,
};

use crate::store;

const LATEST: &str = "LATEST_COMMIT";

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Commit {
    public: Cid,
    private_saturated_name_hash: HashOutput,
    private_temporal_key: TemporalKey,
    private_content_cid: Cid,
    private_forest: Cid,
}

impl Default for Commit {
    fn default() -> Self {
        Commit {
            public: Cid::default(),
            private_saturated_name_hash: HashOutput::default(),
            private_temporal_key: TemporalKey(AesKey::new([0u8; 32])),
            private_content_cid: Cid::default(),
            private_forest: Cid::default(),
        }
    }
}

impl Commit {
    fn to_vec(&self) -> Vec<u8> {
        postcard::to_stdvec(&self).unwrap()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let res = postcard::from_bytes(&bytes)?;
        Ok(res)
    }
}

#[derive(Debug)]
pub struct Fs {
    store: store::Store,
    public: Rc<PublicDirectory>,
    private_forest: Rc<PrivateForest>,
    private: Rc<PrivateDirectory>,
    commit: Commit,
}

impl Fs {
    pub async fn init(dir: &str) -> Result<Fs> {
        if PathBuf::from(dir).exists() {
            anyhow::bail!("already initialized");
        }
        let store = store::Store::new(dir)?;
        let public_root_dir = Rc::new(PublicDirectory::new(Utc::now()));
        let private_forest = Rc::new(PrivateForest::new());
        let private_dir = Rc::new(PrivateDirectory::new(
            Namefilter::default(),
            Utc::now(),
            &mut rand::thread_rng(),
        ));

        let mut fs = Fs {
            store,
            public: public_root_dir,
            private_forest,
            private: private_dir,
            commit: Commit::default(),
        };
        fs.commit().await?;

        Ok(fs)
    }

    pub async fn load(dir: &str) -> Result<Self> {
        let path = PathBuf::from(dir);
        anyhow::ensure!(
            path.exists(),
            "Appa is not initialized, please call 'appa init'"
        );

        let store = store::Store::new(dir).context("unable to open store")?;

        let latest_commit_raw = store
            .get(LATEST)
            .context("loading commit")?
            .ok_or_else(|| anyhow::anyhow!("bad state, missing commit"))?;
        let latest_commit = Commit::from_bytes(&latest_commit_raw)?;
        let dir: PublicDirectory = store
            .get_deserializable(&latest_commit.public)
            .await
            .context("public")?;
        let public_root_dir = Rc::new(dir);

        let private_forest: PrivateForest = store
            .get_deserializable(&latest_commit.private_forest)
            .await
            .context("private forest")?;
        let private_ref = PrivateRef {
            saturated_name_hash: latest_commit.private_saturated_name_hash,
            temporal_key: latest_commit.private_temporal_key.clone(),
            content_cid: latest_commit.private_content_cid,
        };
        let private_node = PrivateNode::load(&private_ref, &private_forest, &store)
            .await
            .context("private")?;
        let private_root_dir = private_node.as_dir()?;
        let private_forest = Rc::new(private_forest);

        Ok(Fs {
            store,
            public: public_root_dir,
            private_forest,
            private: private_root_dir,
            commit: latest_commit,
        })
    }

    pub async fn commit(&mut self) -> Result<()> {
        let public = self.public.store(&mut self.store).await?;
        let private_ref = self
            .private
            .store(
                &mut self.private_forest,
                &mut self.store,
                &mut rand::thread_rng(),
            )
            .await?;
        let private_forest = self
            .store
            .put_async_serializable(&self.private_forest)
            .await?;
        self.commit = Commit {
            public,
            private_forest,
            private_saturated_name_hash: private_ref.saturated_name_hash,
            private_temporal_key: private_ref.temporal_key,
            private_content_cid: private_ref.content_cid,
        };

        self.store.put(LATEST, self.commit.to_vec())?;

        Ok(())
    }

    pub async fn mkdir(&mut self, dir: String) -> Result<()> {
        let path = PathSegments::from_path(dir)?;

        match path {
            PathSegments::Root => {
                anyhow::bail!("cannot create a folder at /");
            }
            PathSegments::Public(path) => {
                self.public.mkdir(&path, Utc::now(), &self.store).await?;
            }
            PathSegments::Private(path) => {
                self.private
                    .mkdir(
                        &path,
                        true,
                        Utc::now(),
                        &self.private_forest,
                        &self.store,
                        &mut rand::thread_rng(),
                    )
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn add(&mut self, dir: String, content: String) -> Result<()> {
        let path = PathSegments::from_path(dir)?;

        match path {
            PathSegments::Root => {
                anyhow::bail!("cannot add files to /");
            }
            PathSegments::Public(path) => {
                let content_cid = self
                    .store
                    .put_block(content.into(), libipld::IpldCodec::Raw.into())
                    .await?;

                self.public
                    .write(&path, content_cid, Utc::now(), &self.store)
                    .await?;
            }
            PathSegments::Private(path) => {
                self.private
                    .write(
                        &path,
                        true,
                        Utc::now(),
                        content.into(),
                        &mut self.private_forest,
                        &mut self.store,
                        &mut rand::thread_rng(),
                    )
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn rm(&mut self, dir: String) -> Result<()> {
        let path = PathSegments::from_path(dir)?;

        match path {
            PathSegments::Root => {
                anyhow::bail!("cannot delete /");
            }
            PathSegments::Public(path) => {
                self.public.rm(&path, &self.store).await?;
            }
            PathSegments::Private(path) => {
                self.private
                    .rm(&path, true, &self.private_forest, &self.store)
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn ls(&self, dir: String) -> Result<Vec<(String, wnfs::common::Metadata)>> {
        let path = PathSegments::from_path(dir)?;

        match path {
            PathSegments::Root => Ok(vec![
                ("public".into(), wnfs::common::Metadata::new(Utc::now())),
                ("private".into(), wnfs::common::Metadata::new(Utc::now())),
            ]),
            PathSegments::Public(path) => self.public.ls(&path, &self.store).await,
            PathSegments::Private(path) => {
                self.private
                    .ls(&path, true, &self.private_forest, &self.store)
                    .await
            }
        }
    }

    pub async fn cat(&self, dir: String) -> Result<Vec<u8>> {
        let path = PathSegments::from_path(dir)?;

        match path {
            PathSegments::Root => {
                anyhow::bail!("not a file");
            }
            PathSegments::Public(path) => {
                let content_cid = self.public.read(&path, &self.store).await?;
                let content = self.store.get_block(&content_cid).await?;
                Ok(content.to_vec())
            }
            PathSegments::Private(path) => {
                let content = self
                    .private
                    .read(&path, true, &self.private_forest, &self.store)
                    .await?;
                Ok(content.to_vec())
            }
        }
    }

    pub fn manifest(&self) -> Result<()> {
        let manifest = crate::hash_manifest::walk_dag(&self.store, self.commit.public)?;
        println!("public manifest: {manifest:#?}");

        let manifest = crate::hash_manifest::walk_dag(&self.store, self.commit.private_forest)?;
        println!("private manifest: {manifest:#?}");

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSegments {
    Root,
    Public(Vec<String>),
    Private(Vec<String>),
}

impl PathSegments {
    pub fn from_path(path: String) -> Result<Self> {
        let mut parts = path
            .split("/")
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        match parts.next() {
            Some(root) => match root.as_str() {
                "public" => Ok(PathSegments::Public(parts.collect())),
                "private" => Ok(PathSegments::Private(parts.collect())),
                _ => anyhow::bail!("unknown path {}", path),
            },
            None => Ok(PathSegments::Root),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path() {
        assert_eq!(
            PathSegments::from_path("/public/foo/bar".into()).unwrap(),
            PathSegments::Public(vec!["foo".into(), "bar".into()])
        );

        assert_eq!(
            PathSegments::from_path("/private/foo/bar".into()).unwrap(),
            PathSegments::Private(vec!["foo".into(), "bar".into()])
        );

        assert_eq!(
            PathSegments::from_path("/".into()).unwrap(),
            PathSegments::Root,
        );

        assert!(PathSegments::from_path("/foo/bar".into()).is_err());
    }
}
