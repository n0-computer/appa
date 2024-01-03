use crate::state::Appa;
use chrono::{DateTime, Utc};
use futures::lock::{Mutex, MutexGuard};
use nfsserve::{
    nfs::{fattr3, fileid3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3, specdata3},
    vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities},
};
use std::{collections::BTreeMap, fmt::Debug, sync::Arc};
use wnfs::public::PublicNode;

#[derive(Debug, Clone)]
pub struct AppaNfs {
    appa: Appa,
    path_map: Arc<Mutex<PathMap>>,
}

impl AppaNfs {
    pub fn new(appa: Appa) -> Self {
        Self {
            appa,
            path_map: Arc::new(Mutex::new(PathMap::new())),
        }
    }

    pub fn path_map(&self) -> Result<MutexGuard<'_, PathMap>, nfsstat3> {
        self.path_map
            .try_lock()
            .ok_or_else(|| server_fault("path map lock currently held"))
    }
}

#[derive(Debug)]
pub struct PathMap {
    next_id: fileid3,
    id_to_path: BTreeMap<fileid3, Vec<String>>,
    path_to_id: BTreeMap<Vec<String>, fileid3>,
}

impl PathMap {
    pub fn new() -> Self {
        Self {
            next_id: 2,
            id_to_path: BTreeMap::from([(1, vec![])]),
            path_to_id: BTreeMap::from([(vec![], 1)]),
        }
    }

    pub fn root_id() -> fileid3 {
        1
    }

    pub fn id_for(&mut self, path: &[String]) -> fileid3 {
        if let Some(id) = self.path_to_id.get(path) {
            return *id;
        }

        let id = self.next_id;
        self.next_id += 1;
        self.id_to_path.insert(id, path.to_vec());
        self.path_to_id.insert(path.to_vec(), id);
        id
    }

    pub fn path_for(&self, id: fileid3) -> Result<&Vec<String>, nfsstat3> {
        self.id_to_path.get(&id).ok_or(nfsstat3::NFS3ERR_BADHANDLE)
    }
}

#[async_trait::async_trait]
impl NFSFileSystem for AppaNfs {
    /// Returns the set of capabilities supported
    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadOnly
    }

    /// Returns the ID the of the root directory "/"
    fn root_dir(&self) -> fileid3 {
        PathMap::root_id()
    }

    /// Look up the id of a path in a directory
    ///
    /// i.e. given a directory dir/ containing a file a.txt
    /// this may call lookup(id_of("dir/"), "a.txt")
    /// and this should return the id of the file "dir/a.txt"
    ///
    /// This method should be fast as it is used very frequently.
    #[tracing::instrument(skip(self), ret)]
    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        tracing::debug!("lookup");
        let mut path_map = self.path_map()?;
        let mut path = path_map.path_for(dirid)?.clone();
        let filename =
            String::from_utf8(filename.0.clone()).map_err(return_err(nfsstat3::NFS3ERR_INVAL))?;
        path.push(filename);
        let id = path_map.id_for(&path);
        Ok(id)
    }

    /// Returns the attributes of an id.
    /// This method should be fast as it is used very frequently.
    #[tracing::instrument(skip(self), ret)]
    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        let path_map = self.path_map()?;
        let path = path_map.path_for(id)?;
        tracing::debug!(?path, "getattr");

        let node = if path.is_empty() {
            PublicNode::Dir(Arc::clone(&self.appa.fs.public_root))
        } else {
            self.appa
                .fs
                .public_root
                .get_node(&path, &self.appa.fs.store)
                .await
                .map_err(server_fault)?
                .ok_or(nfsstat3::NFS3ERR_NOENT)?
                .clone()
        };

        tracing::debug!("gettattr success");

        Ok(node_to_attr(id, &node))
    }

    /// Sets the attributes of an id
    /// this should return Err(nfsstat3::NFS3ERR_ROFS) if readonly
    #[tracing::instrument(skip(self), ret)]
    async fn setattr(&self, _id: fileid3, _setattr: sattr3) -> Result<fattr3, nfsstat3> {
        tracing::debug!("setattr");
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    /// Reads the contents of a file returning (bytes, EOF)
    /// Note that offset/count may go past the end of the file and that
    /// in that case, all bytes till the end of file are returned.
    /// EOF must be flagged if the end of the file is reached by the read.
    #[tracing::instrument(skip(self))]
    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        tracing::debug!("read");
        let count = count as usize;
        let path_map = self.path_map()?;
        let path = path_map.path_for(id)?;
        let file = self
            .appa
            .fs
            .public_root
            .get_node(&path, &self.appa.fs.store)
            .await
            .map_err(server_fault)?
            .ok_or(nfsstat3::NFS3ERR_NOENT)?
            .as_file()
            .map_err(|_| nfsstat3::NFS3ERR_ISDIR)?;

        let bytes = file
            .read_at(offset, Some(count), &self.appa.fs.store)
            .await
            .map_err(server_fault)?;

        // TODO uuuhm this needs to be better
        // I need a file.size() function in rs-wnfs
        let is_eof = bytes.len() < count;
        Ok((bytes, is_eof))
    }

    /// Writes the contents of a file returning (bytes, EOF)
    /// Note that offset/count may go past the end of the file and that
    /// in that case, the file is extended.
    /// If not supported due to readonly file system
    /// this should return Err(nfsstat3::NFS3ERR_ROFS)
    #[tracing::instrument(skip(self), ret)]
    async fn write(&self, _id: fileid3, _offset: u64, _data: &[u8]) -> Result<fattr3, nfsstat3> {
        tracing::debug!("write");
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    /// Creates a file with the following attributes.
    /// If not supported due to readonly file system
    /// this should return Err(nfsstat3::NFS3ERR_ROFS)
    #[tracing::instrument(skip(self), ret)]
    async fn create(
        &self,
        _dirid: fileid3,
        _filename: &filename3,
        _attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        tracing::debug!("create");
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    /// Creates a file if it does not already exist
    /// this should return Err(nfsstat3::NFS3ERR_ROFS)
    #[tracing::instrument(skip(self), ret)]
    async fn create_exclusive(
        &self,
        _dirid: fileid3,
        _filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        tracing::debug!("create_exclusive");
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    /// Makes a directory with the following attributes.
    /// If not supported dur to readonly file system
    /// this should return Err(nfsstat3::NFS3ERR_ROFS)
    #[tracing::instrument(skip(self), ret)]
    async fn mkdir(
        &self,
        _dirid: fileid3,
        _dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        tracing::debug!("mkdir");
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    /// Removes a file.
    /// If not supported due to readonly file system
    /// this should return Err(nfsstat3::NFS3ERR_ROFS)
    #[tracing::instrument(skip(self), ret)]
    async fn remove(&self, _dirid: fileid3, _filename: &filename3) -> Result<(), nfsstat3> {
        tracing::debug!("remove");
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    /// Removes a file.
    /// If not supported due to readonly file system
    /// this should return Err(nfsstat3::NFS3ERR_ROFS)
    #[tracing::instrument(skip(self), ret)]
    async fn rename(
        &self,
        _from_dirid: fileid3,
        _from_filename: &filename3,
        _to_dirid: fileid3,
        _to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        tracing::debug!("rename");
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    /// Returns the contents of a directory with pagination.
    /// Directory listing should be deterministic.
    /// Up to max_entries may be returned, and start_after is used
    /// to determine where to start returning entries from.
    ///
    /// For instance if the directory has entry with ids [1,6,2,11,8,9]
    /// and start_after=6, readdir should returning 2,11,8,...
    //
    #[tracing::instrument(skip(self), ret)]
    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        tracing::debug!("readdir");
        let mut path_map = self.path_map()?;
        let path = path_map.path_for(dirid)?.clone();
        let dir = if path.is_empty() {
            Arc::clone(&self.appa.fs.public_root)
        } else {
            self.appa
                .fs
                .public_root
                .get_node(&path, &self.appa.fs.store)
                .await
                .map_err(server_fault)?
                .ok_or(nfsstat3::NFS3ERR_NOENT)?
                .as_dir()
                .map_err(|_| nfsstat3::NFS3ERR_NOTDIR)?
        };

        let items = dir
            .ls(&[], &self.appa.fs.store)
            .await
            .map_err(server_fault)?;

        let mut entries = Vec::with_capacity(items.len());
        let mut start = start_after == 0;
        let mut end = true;

        for (name, _) in items {
            let mut path = path.clone();
            path.push(name.clone());

            let id = path_map.id_for(&path);

            if id == start_after {
                start = true;
            }

            let filename = filename3::from(name.as_bytes().to_vec());

            let node = dir
                .get_node(&[name], &self.appa.fs.store)
                .await
                .map_err(server_fault)?
                .ok_or(nfsstat3::NFS3ERR_SERVERFAULT)?;

            if start {
                entries.push(DirEntry {
                    fileid: id,
                    name: filename,
                    attr: node_to_attr(id, node),
                });

                if entries.len() == max_entries {
                    end = false;
                    break;
                }
            }
        }

        Ok(ReadDirResult { entries, end })
    }

    /// Makes a symlink with the following attributes.
    /// If not supported due to readonly file system
    /// this should return Err(nfsstat3::NFS3ERR_ROFS)
    #[tracing::instrument(skip(self), ret)]
    async fn symlink(
        &self,
        _dirid: fileid3,
        _linkname: &filename3,
        _symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        tracing::debug!("symlink");
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    /// Reads a symlink
    #[tracing::instrument(skip(self), ret)]
    async fn readlink(&self, _id: fileid3) -> Result<nfspath3, nfsstat3> {
        tracing::debug!("readlink");
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }
}

fn server_fault(e: impl Debug) -> nfsstat3 {
    return_err(nfsstat3::NFS3ERR_SERVERFAULT)(e)
}

fn return_err<E: Debug>(e: nfsstat3) -> impl Fn(E) -> nfsstat3 {
    move |err| {
        tracing::error!(?err, "Got error, returning {e:?}");
        e
    }
}

fn to_nfstime(ts: &DateTime<Utc>) -> nfstime3 {
    nfstime3 {
        seconds: ts.timestamp() as _,
        nseconds: ts.timestamp_nanos_opt().unwrap() as _,
    }
}

fn node_to_attr(id: fileid3, node: &PublicNode) -> fattr3 {
    let (ftype, mode, metadata) = match node {
        PublicNode::File(file) => (ftype3::NF3REG, 0o755, file.get_metadata()),
        PublicNode::Dir(dir) => (ftype3::NF3DIR, 0o777, dir.get_metadata()),
    };
    let created = metadata.get_created().unwrap_or_default();
    let modified = metadata.get_modified().unwrap_or_default();

    fattr3 {
        ftype,
        mode,
        nlink: 1,
        uid: 507,
        gid: 507,
        size: 1000, // TODO
        used: 1000, // TODO
        rdev: specdata3::default(),
        fsid: 0,
        fileid: id,
        atime: to_nfstime(&modified),
        mtime: to_nfstime(&modified),
        ctime: to_nfstime(&created),
    }
}
