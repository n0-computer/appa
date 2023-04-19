use std::{
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    rc::Rc,
};

use anyhow::{Context as _, Result};
use chrono::Utc;
use cid::Cid;
use wnfs::{
    common::{BlockStore, HashOutput},
    namefilter::Namefilter,
    private::{AesKey, PrivateDirectory, PrivateForest, PrivateNode, PrivateRef, TemporalKey},
    public::{PublicDirectory, PublicNode},
};

use crate::{hash_manifest::HashManifest, store};

/// Key for the latest commit stored in the database.
pub const LATEST: &str = "LATEST_COMMIT";

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Commit {
    pub public: Cid,
    pub private_saturated_name_hash: HashOutput,
    pub private_temporal_key: TemporalKey,
    pub private_content_cid: Cid,
    pub private_forest: Cid,
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
    pub fn to_vec(&self) -> Vec<u8> {
        postcard::to_stdvec(&self).unwrap()
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
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
    pub async fn init(dir: impl AsRef<Path>) -> Result<Fs> {
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

    pub async fn load(dir: &impl AsRef<Path>) -> Result<Self> {
        let path = PathBuf::from(dir.as_ref());
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
        let private_forest = Rc::new(private_forest);

        // Update to the latest
        let private_node = private_node.search_latest(&private_forest, &store).await?;
        let private_root_dir = private_node.as_dir()?;

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

    pub async fn mv(&mut self, source: String, target: String) -> Result<()> {
        let source = PathSegments::from_path(source)?;
        let target = PathSegments::from_path(target)?;

        match (source, target) {
            (PathSegments::Root, _) | (_, PathSegments::Root) => {
                anyhow::bail!("cannot modify /");
            }
            (PathSegments::Public(source), PathSegments::Public(target)) => {
                self.public
                    .basic_mv(&source, &target, Utc::now(), &self.store)
                    .await?;
            }
            (PathSegments::Private(source), PathSegments::Private(target)) => {
                self.private
                    .basic_mv(
                        &source,
                        &target,
                        true,
                        Utc::now(),
                        &mut self.private_forest,
                        &mut self.store,
                        &mut rand::thread_rng(),
                    )
                    .await?;
            }
            _ => {
                anyhow::bail!("cannot move between /public and /private");
            }
        }

        Ok(())
    }

    pub fn manifest(&self) -> Result<HashManifest> {
        let mut public = self.manifest_public()?;
        let private = self.manifest_private()?;

        public.extend(private);
        Ok(public)
    }

    pub fn manifest_public(&self) -> Result<HashManifest> {
        crate::hash_manifest::walk_dag(&self.store, self.commit.public)
    }

    pub fn manifest_private(&self) -> Result<HashManifest> {
        crate::hash_manifest::walk_dag(&self.store, self.commit.private_forest)
    }

    pub fn store(&self) -> &store::Store {
        &self.store
    }

    pub fn current_commit(&self) -> &Commit {
        &self.commit
    }

    pub async fn get_node(&self, path: String) -> anyhow::Result<Option<Node>> {
        let path = PathSegments::from_path(path)?;
        let node = match path {
            PathSegments::Root => Some(Node::Root),
            PathSegments::Private(path) => {
                if path.is_empty() {
                    Some(Node::Private(PrivateNode::Dir(Rc::clone(&self.private))))
                } else {
                    self.private
                        .get_node(&path, false, &self.private_forest, &self.store)
                        .await?
                        .map(Node::Private)
                }
            }
            PathSegments::Public(path) => {
                if path.is_empty() {
                    Some(Node::Public(PublicNode::Dir(Rc::clone(&self.public))))
                } else {
                    self.public
                        .get_node(&path, &self.store)
                        .await?
                        .map(|node| Node::Public(node.clone()))
                }
            }
        };
        Ok(node)
    }

    pub async fn read_file_at(
        &self,
        path: String,
        offset: usize,
        size: usize,
    ) -> anyhow::Result<Vec<u8>> {
        let node = self.get_node(path).await?;
        match node {
            None => Err(anyhow::anyhow!("Not found")),
            Some(Node::Root)
            | Some(Node::Private(PrivateNode::Dir(_)))
            | Some(Node::Public(PublicNode::Dir(_))) => {
                Err(anyhow::anyhow!("Is a directory, not a file"))
            }
            Some(Node::Private(PrivateNode::File(file))) => {
                file.read_at(offset, size, &self.private_forest, &self.store)
                    .await
            }
            Some(Node::Public(PublicNode::File(file))) => {
                let cid = file.get_content_cid();
                let mut file = self
                    .store
                    .get_block_as_file(*cid)?
                    .ok_or_else(|| anyhow::anyhow!("Block not found"))?;
                tokio::task::spawn_blocking(move || {
                    let meta = file.metadata()?;
                    let max_size = (offset + size).min(meta.len() as usize - offset);
                    if max_size == 0 {
                        return Ok(vec![]);
                    }
                    let mut bytes = vec![0u8; max_size];
                    file.seek(SeekFrom::Start(offset as u64))?;
                    tracing::debug!("public read offset {offset} size {size} {file:?}");
                    file.read_exact(&mut bytes)?;
                    tracing::debug!("public read offset {offset} size {size} {file:?}");
                    Ok(bytes)
                })
                .await?
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    Root,
    Private(PrivateNode),
    Public(PublicNode),
}

#[derive(Debug, Clone, PartialEq)]
pub enum NodeKind {
    Directory,
    File,
}

impl Node {
    pub fn kind(&self) -> NodeKind {
        match self {
            Node::Root => NodeKind::Directory,
            Node::Private(PrivateNode::Dir(_)) => NodeKind::Directory,
            Node::Private(PrivateNode::File(_)) => NodeKind::File,
            Node::Public(PublicNode::Dir(_)) => NodeKind::Directory,
            Node::Public(PublicNode::File(_)) => NodeKind::File,
        }
    }

    pub fn is_dir(&self) -> bool {
        matches!(self.kind(), NodeKind::Directory)
    }
    pub fn is_file(&self) -> bool {
        matches!(self.kind(), NodeKind::File)
    }

    pub fn size(&self, fs: &Fs) -> anyhow::Result<u64> {
        let size = match self {
            Node::Private(PrivateNode::File(file)) => file.get_content_size_upper_bound() as u64,
            Node::Public(PublicNode::File(file)) => {
                let cid = file.get_content_cid();
                // TODO: Does this need spawn_blocking?
                let file = fs
                    .store
                    .get_block_as_file(*cid)?
                    .ok_or_else(|| anyhow::anyhow!("Block not found"))?;
                file.metadata()?.len()
            }
            Node::Root | Node::Public(PublicNode::Dir(_)) | Node::Private(PrivateNode::Dir(_)) => 0,
        };
        Ok(size)
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

    #[tokio::test]
    async fn test_basics() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();

        let mut fs = Fs::init(&dir).await?;

        // Private
        fs.mkdir("/private/foo".into()).await?;
        let list = fs.ls("/private".into()).await?;
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].0, "foo");

        fs.add("/private/foo/hello.txt".into(), "hello world".into())
            .await?;
        let text = fs.cat("/private/foo/hello.txt".into()).await?;
        assert_eq!(text, b"hello world");

        fs.mv("/private/foo".into(), "/private/bar".into()).await?;

        let text = fs.cat("/private/bar/hello.txt".into()).await?;
        assert_eq!(text, b"hello world");

        fs.commit().await?;

        // Public
        fs.mkdir("/public/foo".into()).await?;
        let list = fs.ls("/public".into()).await?;
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].0, "foo");

        fs.add("/public/foo/hello.txt".into(), "hello world".into())
            .await?;
        let text = fs.cat("/public/foo/hello.txt".into()).await?;
        assert_eq!(text, b"hello world");

        fs.mv("/public/foo".into(), "/public/bar".into()).await?;

        let text = fs.cat("/public/bar/hello.txt".into()).await?;
        assert_eq!(text, b"hello world");

        fs.commit().await?;
        Ok(())
    }
}
