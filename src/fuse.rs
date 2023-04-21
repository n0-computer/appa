//! FUSE support for WNFS
//!
//! Contains some code from
//! https://github.com/awslabs/mountpoint-s3/blob/main/mountpoint-s3/src/fs.rs
//! Licensed under the Apache-2.0 License.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::future::Future;
use std::os::unix::prelude::MetadataExt;
use std::path::Path;
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request, SessionUnmounter,
};
use futures::FutureExt;
use libc::ENOENT;
use tokio::io::{AsyncRead, AsyncWriteExt, DuplexStream};
use tokio::sync::oneshot;
use tokio::task::LocalSet;
use tracing::{debug, error, trace};
use wnfs::private::PrivateNode;
use wnfs::public::PublicNode;

use crate::fs::{Fs, Node, NodeKind};

const TTL: Duration = Duration::from_secs(1); // 1 second
const BLOCK_SIZE: usize = 512;

/// Mount a filesystem
///
/// Blocks forever until Ctrl-C.
/// TODO: use fuser::spawn_mount once wnfs is Send.
pub fn mount<F, Fut>(
    make_fs: F,
    mountpoint: impl AsRef<Path>,
    unmount_tx: Option<oneshot::Sender<SessionUnmounter>>,
) -> anyhow::Result<()>
where
    F: (FnOnce() -> Fut) + Send + 'static,
    Fut: Future<Output = anyhow::Result<Fs>>,
{
    let mountpoint = mountpoint.as_ref().to_owned();
    let res = std::thread::spawn::<_, anyhow::Result<()>>(move || {
        let rt = Runtime::new();
        let fs = rt.block_on(make_fs())?;
        // let mountpoint = mountpoint.as_ref();
        let mountpoint_meta = std::fs::metadata(&mountpoint)?;
        let config = FuseConfig {
            uid: mountpoint_meta.uid(),
            gid: mountpoint_meta.gid(),
        };
        let fs = FuseFs::new(rt, fs, config);
        let options = vec![
            // Change to RW once writing files works
            MountOption::RW,
            MountOption::FSName("appa-wnfs".to_string()),
            MountOption::AutoUnmount,
            MountOption::AllowRoot,
        ];
        debug!("mount FUSE at {mountpoint:?}");
        let mut session = fuser::Session::new(fs, &mountpoint, &options[..])?;
        let unmounter = session.unmount_callable();
        if let Some(tx) = unmount_tx {
            let _ = tx.send(unmounter);
        }
        session.run()?;
        // fuser::mount2(fs, mountpoint, &options)?;
        Ok(())
    });
    let res = res.join();

    res.map_err(|err| anyhow::anyhow!(format!("{:?}", err)))?
}

/// Inode index for a filesystem.
///
/// This is a partial view of the filesystem and contains only nodes that have been accessed
/// in the current session. Inode numbers are assigned sequentially on first use.
#[derive(Default, Debug)]
pub struct Inodes {
    inodes: HashMap<u64, Inode>,
    by_path: HashMap<String, u64>,
    counter: u64,
}

impl Inodes {
    pub fn push(&mut self, path: String) -> u64 {
        // pub fn push(&mut self, path: String, kind: FileType) -> u64 {
        self.counter += 1;
        let ino = self.counter;
        let inode = Inode::new(ino, path);
        self.by_path.insert(inode.path.clone(), ino);
        self.inodes.insert(ino, inode);
        ino
    }
    pub fn get(&self, ino: u64) -> Option<&Inode> {
        self.inodes.get(&ino)
    }

    pub fn get_path(&self, ino: u64) -> Option<&String> {
        self.get(ino).map(|node| &node.path)
    }

    pub fn get_child_path(&self, parent: u64, child_name: &str) -> Option<String> {
        let Some(parent_path) = self.get_path(parent) else {
            return None
        };
        Some(push_segment(parent_path, child_name))
    }

    pub fn get_by_path(&self, path: &str) -> Option<&Inode> {
        self.by_path.get(path).and_then(|ino| self.inodes.get(ino))
    }

    pub fn get_or_push(&mut self, path: &str) -> Inode {
        let id = if let Some(id) = self.by_path.get(path) {
            *id
        } else {
            self.push(path.to_string())
        };
        self.get(id).unwrap().clone()
    }
}

#[derive(Debug, Clone)]
pub struct Inode {
    pub path: String,
    pub ino: u64,
}

impl Inode {
    pub fn new(ino: u64, path: String) -> Self {
        Self { path, ino }
    }
}

pub struct Runtime {
    rt: tokio::runtime::Runtime,
    tasks: LocalSet,
}

impl Runtime {
    pub fn new() -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let tasks = LocalSet::new();
        Self { rt, tasks }
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.tasks.block_on(&self.rt, future)
    }
}
impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

pub struct FuseConfig {
    uid: u32,
    gid: u32,
}

pub struct FuseFs {
    fs: Fs,
    inodes: Inodes,
    config: FuseConfig,
    write_handles: HashMap<u64, WriteHandle>,
    rt: Runtime,
}

impl FuseFs {
    pub fn new(rt: Runtime, fs: Fs, config: FuseConfig) -> Self {
        let mut inodes = Inodes::default();
        // Init root inodes.
        inodes.push("".to_string());
        inodes.push("private".to_string());
        inodes.push("public".to_string());

        Self {
            fs,
            inodes,
            config,
            write_handles: Default::default(),
            rt,
        }
    }

    fn node_to_attr(&self, ino: u64, node: &Node) -> FileAttr {
        let uid = self.config.uid;
        let gid = self.config.gid;
        if matches!(node, Node::Root) {
            return FileAttr {
                ino: 1,
                size: 0,
                blocks: 0,
                nlink: 2,
                perm: 0o555,
                uid,
                gid,
                rdev: 0,
                flags: 0,
                blksize: BLOCK_SIZE as u32,
                kind: FileType::Directory,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
            };
        }
        let metadata = match node {
            Node::Private(PrivateNode::File(file)) => file.get_metadata(),
            Node::Private(PrivateNode::Dir(dir)) => dir.get_metadata(),
            Node::Public(PublicNode::File(file)) => file.get_metadata(),
            Node::Public(PublicNode::Dir(dir)) => dir.get_metadata(),
            Node::Root => unreachable!(),
        };
        let kind = match node.kind() {
            NodeKind::Directory => FileType::Directory,
            NodeKind::File => FileType::RegularFile,
        };
        let perm = match node.kind() {
            NodeKind::Directory => 0o755,
            NodeKind::File => 0o644,
        };
        let size = node.size(&self.fs).unwrap_or(0);
        let nlink = match node.kind() {
            NodeKind::Directory => 2,
            NodeKind::File => 1,
        };
        let blocks = size / BLOCK_SIZE as u64;
        let mtime = metadata
            .get_modified()
            .map(|x| x.into())
            .unwrap_or(UNIX_EPOCH);
        let ctime = metadata
            .get_created()
            .map(|x| x.into())
            .unwrap_or(UNIX_EPOCH);
        FileAttr {
            ino,
            size,
            blocks,
            nlink,
            perm,
            uid,
            gid,
            rdev: 0,
            flags: 0,
            blksize: BLOCK_SIZE as u32,
            kind,
            atime: mtime,
            mtime,
            ctime,
            crtime: ctime,
        }
    }
}

// fn block_on<F: Future>(future: F) -> F::Output {
//     futures::executor::block_on(future)
// }

impl Filesystem for FuseFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        trace!("lookup: i{parent} {name:?}");
        let Some(path) = self.inodes.get_child_path(parent, &name.to_string_lossy()) else {
            trace!("  ENOENT");
            reply.error(ENOENT);
            return;
        };
        let Inode { ino, .. } = self.inodes.get_or_push(&path);
        match self.rt.block_on(self.fs.get_node(path)) {
            Ok(Some(node)) => {
                let attr = self.node_to_attr(ino, &node);
                trace!("  ok {attr:?}");
                reply.entry(&TTL, &attr, 0);
            }
            Ok(None) => {
                trace!("  ENOENT (not found)");
                reply.error(ENOENT);
            }
            Err(err) => {
                trace!("  ENOENT ({err})");
                reply.error(ENOENT);
            }
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        trace!("getattr: i{ino}");

        let Some(path) = self.inodes.get_path(ino) else {
                trace!("  ENOENT (ino not found)");
                reply.error(ENOENT);
                return;
            };
        let Ok(Some(node)) = self.rt.block_on(self.fs.get_node(path.into())) else {
                trace!("  ENOENT (path not found)");
                reply.error(ENOENT);
                return;
            };
        let attr = self.node_to_attr(ino, &node);
        trace!("  ok {attr:?}");
        reply.attr(&TTL, &attr)
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        trace!("read: i{ino} offset {offset} size {size}");
        let Some(path) = self.inodes.get_path(ino) else {
              trace!("  ENOENT (ino not found)");
              reply.error(ENOENT);
              return;
        };
        let content = self.rt.block_on(self.fs.read_file_at(
            path.into(),
            offset as usize,
            size as usize,
        ));
        // let content = block_on(self.wnfs.read_file(&path));
        match content {
            Ok(data) => {
                trace!("  ok, len {}", data.len());
                reply.data(&data)
            }
            Err(err) => {
                trace!("  ENOENT ({err})");
                reply.error(ENOENT);
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        trace!("readdir: i{ino} offset {offset}");
        let path = {
            // We're cloning the path segments here to not keep an immutable borrow to self.inodes around.
            // TODO: Maybe always wrap Inode an Rc
            let Some(path) = self.inodes.get_path(ino) else {
                trace!("  ENOENT (ino not found)");
                reply.error(ENOENT);
                return;
            };
            path.clone()
        };

        let Ok(dir) = self.rt.block_on(self.fs.ls(path.clone())) else {
            trace!("  ENOENT (failed to get metadata)");
            reply.error(ENOENT);
            return;
        };

        let mut entries = vec![
            (ino, FileType::Directory, ".".to_string()),
            (ino, FileType::Directory, "..".to_string()),
        ];

        for (name, _metadata) in dir {
            let path = push_segment(&path, &name);

            // We need to know for each entry whether it's a file or a directory.
            // However, the metadata from `ls` does not have that info.
            // Therefore we fetch all nodes again.
            // TODO: Solve by making wnfs return nodes, not metadata, on ls
            let node = self.rt.block_on(self.fs.get_node(path.clone()));
            if let Ok(Some(node)) = node {
                let kind = match node.kind() {
                    NodeKind::File => FileType::Directory,
                    NodeKind::Directory => FileType::RegularFile,
                };
                let ino = self.inodes.get_or_push(&path);
                entries.push((ino.ino, kind, name));
            }
        }
        trace!("  ok {entries:?}");

        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            // i + 1 means the index of the next entry
            if reply.add(entry.0, (i + 1) as i64, entry.1, entry.2) {
                break;
            }
        }
        reply.ok();
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        trace!("mkdir : i{parent} {name:?}");
        let Some(path) = self.inodes.get_child_path(parent, &name.to_string_lossy()) else {
            trace!("  ENOENT: parent not found");
            reply.error(ENOENT);
            return;
        };
        match self.rt.block_on(self.fs.mkdir(path.clone())) {
            Ok(_) => match self.rt.block_on(self.fs.get_node(path.clone())) {
                Ok(Some(node)) => {
                    let ino = self.inodes.get_or_push(&path);
                    let attr = self.node_to_attr(ino.ino, &node);
                    trace!("  ok, created! ino {}", ino.ino);
                    reply.entry(&TTL, &attr, 0);
                }
                Err(_) | Ok(None) => {
                    trace!("  ENOENT, failed to find created dir");
                    reply.error(ENOENT);
                }
            },
            Err(err) => {
                trace!("  ENOENT, failed to create dir: {err}");
                reply.error(ENOENT);
            }
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        let Some(path) = self.inodes.get_path(ino) else {
            trace!("  ENOENT");
            reply.error(ENOENT);
            return;
        };

        if flags & libc::O_RDWR != 0 {
            error!("O_RDWR is unsupported");
            return reply.error(libc::EINVAL);
        } else if flags & libc::O_WRONLY != 0 {
            // Opened for writing
            let write_handle = WriteHandle::new(path.clone(), self.fs.clone());
            self.write_handles.insert(ino, write_handle);
        } else {
            // Opened for reading, nothing to do because reads
            // are stateless atm.
        };

        reply.opened(0, 0);
    }

    fn mknod(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        if mode & libc::S_IFMT != libc::S_IFREG {
            error!(
                ?parent,
                ?name,
                "invalid mknod type {}; only regular files are supported",
                mode & libc::S_IFMT
            );
            reply.error(libc::EINVAL);
            return;
        }
        let Some(path) = self.inodes.get_child_path(parent, &name.to_string_lossy()) else {
            trace!("  ENOENT");
            reply.error(ENOENT);
            return;
        };
        if let Ok(Some(_node)) = self.rt.block_on(self.fs.get_node(path.clone())) {
            trace!("  EEXISTS {path}");
            reply.error(libc::EEXIST);
            return;
        }
        let ino = self.inodes.get_or_push(&path);
        let attr = FileAttr {
            ino: ino.ino,
            size: 0,
            blocks: 0,
            nlink: 2,
            perm: 0o755,
            uid: self.config.uid,
            gid: self.config.gid,
            rdev: 0,
            flags: 0,
            blksize: BLOCK_SIZE as u32,
            kind: FileType::RegularFile,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            crtime: SystemTime::now(),
        };
        reply.entry(&TTL, &attr, 0);
    }

    // TODO: Writes are fully collected in memory at the moment and written on release.
    // This can be made streaming once wnfs is Send.
    // Only append-only writes are supported.
    // Partial writes are not cleared up until the file is released, so will lead to memory
    // overflows potentially.
    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        let Some(handle) = self.write_handles.get_mut(&ino) else {
            trace!("  Abort write: Not opened");
            reply.error(libc::EBADF);
            return;
        };
        match self.rt.block_on(handle.append(offset as u64, data)) {
            Ok(written) => {
                trace!("  Write ok: {written}");
                reply.written(written as u32);
            }
            Err(err) => {
                trace!("  Abort write: {err}");
                reply.error(libc::EINVAL)
            }
        }
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        let Some(old_path) = self.inodes.get_child_path(parent, &name.to_string_lossy()) else {
            trace!("  ENOENT (parent does not exist)");
            reply.error(ENOENT);
            return;
        };
        let Some(new_path) = self.inodes.get_child_path(newparent, &newname.to_string_lossy()) else {
            trace!("  ENOENT (new parent does not exist)");
            reply.error(ENOENT);
            return;
        };
        match self.rt.block_on(self.fs.mv(old_path, new_path)) {
            Ok(_) => reply.ok(),
            Err(err) => {
                trace!("  Error {err}");
                reply.error(libc::EINVAL);
            }
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        let Some(path) = self.inodes.get_child_path(parent, &name.to_string_lossy()) else {
            trace!("  ENOENT (parent does not exist)");
            reply.error(ENOENT);
            return;
        };
        match self.rt.block_on(self.fs.rm(path)) {
            Ok(_) => reply.ok(),
            Err(err) => {
                trace!("  Error {err}");
                reply.error(libc::EINVAL);
            }
        }
    }

    // TODO: Properly do this
    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let attr = FileAttr {
            ino,
            size: 0,
            blocks: 0,
            nlink: 2,
            perm: 0o755,
            uid: self.config.uid,
            gid: self.config.gid,
            rdev: 0,
            flags: 0,
            blksize: BLOCK_SIZE as u32,
            kind: FileType::RegularFile,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            crtime: SystemTime::now(),
        };
        reply.attr(&TTL, &attr)
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        if let Some(mut handle) = self.write_handles.remove(&ino) {
            let len = handle.size();
            match self.rt.block_on(async {
                // replace instance with the instance from the committed write.
                // TODO: We actually have to merge the instances. We can merge the private forest,
                // but not yet the private directories and public directories.
                let next_fs = handle.finish().await?;
                // self.fs = self.fs.merge(next_fs);
                self.fs = next_fs;
                Ok::<(), anyhow::Error>(())
            }) {
                Ok(()) => {
                    trace!("Wrote file {} (len {})", handle.path, len);
                    reply.ok();
                }
                Err(err) => {
                    trace!("Failed to write file {}: ({})", handle.path, err);
                    reply.error(libc::EIO);
                }
            }
        } else {
            reply.ok();
        }
    }
}

fn push_segment(path: &str, name: &str) -> String {
    if path.is_empty() {
        name.to_string()
    } else {
        format!("{}/{}", path, name)
    }
}

pub struct WriteHandle {
    path: String,
    pos: usize,
    wnfs_write_fut: Option<Pin<Box<dyn Future<Output = anyhow::Result<Fs>> + 'static>>>,
    pipe_writer: Option<DuplexStream>,
}

impl WriteHandle {
    pub fn new(path: String, fs: Fs) -> Self {
        let (pipe_writer, pipe_reader) = tokio::io::duplex(1024);
        let wnfs_write_fut = wnfs_write(fs, path.clone(), pipe_reader).boxed_local();
        Self {
            path,
            pos: 0,
            wnfs_write_fut: Some(wnfs_write_fut),
            pipe_writer: Some(pipe_writer),
        }
    }

    pub async fn append(&mut self, offset: u64, data: &[u8]) -> anyhow::Result<usize> {
        if offset as usize != self.pos {
            return Err(anyhow::anyhow!("Only append-only writes are supported"));
        }
        self.pos = offset as usize + data.len();
        let mut pipe_writer = self
            .pipe_writer
            .take()
            .expect("May not append after release");
        let res = {
            let pipe_writer_fut = pipe_writer.write_all(data).boxed_local();
            let mut wnfs_write_fut = self.wnfs_write_fut.take().unwrap();
            let res = tokio::select! {
                biased;
                _res = &mut wnfs_write_fut => {
                    panic!("drive fut may not complete in append");
                },
                res = pipe_writer_fut => res
            };
            self.pipe_writer = Some(pipe_writer);
            self.wnfs_write_fut = Some(wnfs_write_fut);
            res
        };
        res?;
        Ok(data.len())
    }

    pub async fn finish(&mut self) -> anyhow::Result<Fs> {
        // Drop the writer to signal shutdown.
        let pipe_writer = self
            .pipe_writer
            .take()
            .expect("May not finish more than once");
        drop(pipe_writer);
        // Complete the write
        let wnfs_write_fut = self.wnfs_write_fut.take().unwrap();
        let mut fs = wnfs_write_fut.await?;
        fs.commit().await?;
        // Return the fs instance
        Ok(fs)
    }

    pub fn size(&self) -> usize {
        self.pos
    }
}

pub async fn wnfs_write(
    mut fs: Fs,
    dir: String,
    content: impl AsyncRead + Send + Unpin + 'static,
) -> anyhow::Result<Fs> {
    fs.write(dir, content).await?;
    Ok(fs)
}

#[cfg(test)]
mod test {
    use std::{fs, time::Duration};

    use fuser::SessionUnmounter;
    use tokio::sync::oneshot;
    use tracing::warn;

    use crate::fs::Fs;
    use super::mount;

    #[tokio::test]
    async fn test_fuse_read_write() {
        tracing_subscriber::fmt::init();
        let store_dir = tempfile::tempdir().unwrap();
        let mountpoint = tempfile::tempdir().unwrap();
        let mountpoint_path = mountpoint.path().to_owned();
        let (unmount_tx, unmount_rx) = oneshot::channel();

        // create a thread for sync fs operations for testing
        let mountpoint_path_clone = mountpoint_path.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            let mut unmount: SessionUnmounter = unmount_rx.blocking_recv().unwrap();
            let dir = mountpoint_path_clone;
            let content = "helloworld".repeat(1000);
            let content = content.as_bytes();
            fs::write(dir.join("private/test.txt"), content).unwrap();
            let res = fs::read(dir.join("private/test.txt")).unwrap();
            assert_eq!(&content, &res);
            fs::write(dir.join("public/test.txt"), content).unwrap();
            let res = fs::read(dir.join("public/test.txt")).unwrap();
            assert_eq!(&content, &res);
            unmount.unmount().unwrap();
        });
        // let fs = Fs::init(&store_dir).await.unwrap();
        // TODO: This blocks the tokio runtime.. and will never finish
        warn!("now mount fs");
        let store_dir_clone = store_dir.path().to_owned();
        let fs = move || async move { Fs::init(&store_dir_clone).await };
        mount(fs, mountpoint_path, Some(unmount_tx)).unwrap();
        drop(store_dir);
        drop(mountpoint);
        assert!(true);
    }
}
