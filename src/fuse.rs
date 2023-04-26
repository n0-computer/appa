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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request, SessionUnmounter,
};
use libc::ENOENT;
use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, LocalSet};
use tracing::{debug, error, trace};
use wnfs::private::PrivateNode;
use wnfs::public::PublicNode;

use crate::fs::{DetachedWriter, Fs, Node, NodeKind};

const TTL: Duration = Duration::from_secs(1); // 1 second
const BLOCK_SIZE: usize = 512;

macro_rules! fuse_try {
    ($reply:ident, $err:expr, $e:expr) => {
        match $e {
            Err(err) => {
                trace!("FUSE operation failed {}: {}", $err, err);
                return $reply.error($err);
            }
            Ok(val) => val,
        }
    };
}

/// Mount a WNFS filesystem with FUSE
///
/// This spawns FUSE on a seperate thread. As WNFS is not Send, the WNFS instance is constructed
/// through a closure that is run on the FUSE thread.
///
/// Returns a FuseHandle that can be used to unmount the file system.
pub async fn mount<F, Fut>(make_fs: F, mountpoint: impl AsRef<Path>) -> anyhow::Result<FuseHandle>
where
    F: (FnOnce() -> Fut) + Send + 'static,
    Fut: Future<Output = anyhow::Result<Fs>>,
{
    let mountpoint = mountpoint.as_ref().to_owned();
    let (unmount_tx, unmount_rx) = oneshot::channel();
    let join_handle = std::thread::spawn::<_, anyhow::Result<()>>(move || {
        let rt = Runtime::new();
        let fs = rt.block_on(make_fs())?;
        let mountpoint_meta = std::fs::metadata(&mountpoint)?;
        let config = FuseConfig {
            uid: mountpoint_meta.uid(),
            gid: mountpoint_meta.gid(),
        };
        let fs = FuseFs::new(rt, fs, config);
        let options = vec![
            MountOption::RW,
            MountOption::FSName("appa-wnfs".to_string()),
            MountOption::AutoUnmount,
            MountOption::AllowRoot,
        ];
        debug!("mount FUSE at {mountpoint:?}");
        let mut session = fuser::Session::new(fs, &mountpoint, &options[..])?;
        let unmounter = session.unmount_callable();
        let _ = unmount_tx.send(unmounter);
        // This blocks until unmount is called.
        session.run()?;
        Ok(())
    });
    let unmounter = unmount_rx.await?;
    let handle = FuseHandle {
        unmounter,
        join_handle,
    };
    Ok(handle)
}

pub struct FuseHandle {
    unmounter: SessionUnmounter,
    join_handle: std::thread::JoinHandle<anyhow::Result<()>>,
}
impl FuseHandle {
    pub fn unmount(&mut self) -> anyhow::Result<()> {
        self.unmounter.unmount()?;
        Ok(())
    }
    pub fn join(self) -> anyhow::Result<()> {
        let res = self
            .join_handle
            .join()
            .map_err(|_| anyhow::anyhow!("Faile to join FUSE thread"))?;
        res
    }
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

    pub fn get_mut(&mut self, ino: u64) -> Option<&mut Inode> {
        self.inodes.get_mut(&ino)
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

    pub fn get_or_push(&mut self, path: &str) -> u64 {
        if let Some(ino) = self.by_path.get(path) {
            *ino
        } else {
            self.push(path.to_string())
        }
    }

    pub fn get_write_handle(&mut self, ino: u64) -> Option<&mut WriteHandle> {
        match self.get_mut(ino) {
            Some(inode) => inode.write_handle.as_mut(),
            None => None,
        }
    }
}

#[derive(Debug)]
pub struct Inode {
    pub path: String,
    pub ino: u64,
    pub write_handle: Option<WriteHandle>,
}

impl Inode {
    pub fn new(ino: u64, path: String) -> Self {
        Self {
            path,
            ino,
            write_handle: None,
        }
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

    fn spawn_local<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        self.tasks.spawn_local(future)
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
        let ino = self.inodes.get_or_push(&path);
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
                entries.push((ino, kind, name));
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
                    let attr = self.node_to_attr(ino, &node);
                    trace!("  ok, created! ino {}", ino);
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
        let Some(inode) = self.inodes.get_mut(ino) else {
            trace!("  ENOENT");
            reply.error(ENOENT);
            return;
        };

        if flags & libc::O_RDWR != 0 {
            error!("O_RDWR is unsupported");
            return reply.error(libc::EINVAL);
        } else if flags & libc::O_WRONLY != 0 {
            // Opened for writing
            let file_writer = fuse_try!(
                reply,
                libc::EINVAL,
                self.rt.block_on(self.fs.write_detached(inode.path.clone()))
            );
            let write_handle = WriteHandle::new(&self.rt, file_writer, self.fs.store.clone());
            inode.write_handle = Some(write_handle);
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
        reply.entry(&TTL, &attr, 0);
    }

    // Writes are streaming. Only append-only writes are supported.
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
        let Some(handle) = self.inodes.get_write_handle(ino) else {
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
        let Some(inode) = self.inodes.get_mut(ino) else {
            trace!("  ENOENT");
            reply.error(ENOENT);
            return;
        };
        if let Some(handle) = inode.write_handle.take() {
            let len = handle.size();
            let path = inode.path.clone();
            match self.rt.block_on(async {
                // replace instance with the instance from the committed write.
                // TODO: We actually have to merge the instances. We can merge the private forest,
                // but not yet the private directories and public directories.
                // Or, easier TODO: Let's wrap the fs in an RwLock
                handle.finalize(&mut self.fs).await
            }) {
                Ok(()) => {
                    trace!("Wrote file {} (len {})", path, len);
                    reply.ok();
                }
                Err(err) => {
                    trace!("Failed to write file {}: ({})", path, err);
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

#[derive(Debug)]
pub struct WriteHandle {
    pos: u64,
    pipe_writer: DuplexStream,
    fut: JoinHandle<anyhow::Result<DetachedWriter>>,
}

impl WriteHandle {
    pub fn new(
        rt: &Runtime,
        mut file_writer: DetachedWriter,
        mut store: crate::store::Store,
    ) -> Self {
        // Create a buffered pipe for bytes to flow from FUSE writes into WNFS.
        let (pipe_writer, pipe_reader) = tokio::io::duplex(1024);
        // Spawn a task on the thread-local runtime that actually writes the data from the pipe
        // into WNFS.
        let fut = rt.spawn_local(async move {
            file_writer.write_all(pipe_reader, &mut store).await?;
            Ok(file_writer)
        });
        Self {
            pos: 0,
            pipe_writer,
            fut,
        }
    }

    pub async fn append(&mut self, offset: u64, data: &[u8]) -> anyhow::Result<usize> {
        if offset != self.pos {
            return Err(anyhow::anyhow!(
                "Bad offset: Only linear append-only writes are supported"
            ));
        }
        self.pipe_writer.write_all(data).await?;
        self.pos += data.len() as u64;
        Ok(data.len())
    }

    pub async fn finalize(mut self, fs: &mut Fs) -> anyhow::Result<()> {
        self.pipe_writer.shutdown().await?;
        let file_writer = self.fut.await??;
        file_writer.finalize(fs).await?;
        fs.commit().await?;
        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.pos
    }
}

#[cfg(test)]
mod test {
    use std::{fs, time::Duration};

    use super::mount;
    use crate::fs::Fs;

    #[tokio::test]
    async fn test_fuse_read_write() {
        let store_dir = tempfile::tempdir().unwrap();
        let mountpoint = tempfile::tempdir().unwrap();
        let mountpoint_path = mountpoint.path().to_owned();

        let store_dir_clone = store_dir.path().to_owned();
        let fs = move || async move { Fs::init(&store_dir_clone).await };
        let mut handle = mount(fs, mountpoint_path.clone()).await.unwrap();

        // create a thread for sync fs operations for testing
        let dir = mountpoint_path.clone();
        tokio::task::spawn_blocking(move || {
            std::thread::sleep(Duration::from_millis(100));
            let content = "helloworld".repeat(1000);
            let content = content.as_bytes();
            fs::write(dir.join("private/test.txt"), content).unwrap();
            let res = fs::read(dir.join("private/test.txt")).unwrap();
            assert_eq!(&content, &res);
            fs::write(dir.join("public/test.txt"), content).unwrap();
            let res = fs::read(dir.join("public/test.txt")).unwrap();
            assert_eq!(&content, &res);
            handle.unmount().unwrap();
            drop(store_dir);
            drop(mountpoint);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_fuse_parallel_write() {
        let store_dir = tempfile::tempdir().unwrap();
        let mountpoint = tempfile::tempdir().unwrap();
        let mountpoint_path = mountpoint.path().to_owned();
        let store_dir_clone = store_dir.path().to_owned();
        let fs = move || async move { Fs::init(&store_dir_clone).await };
        let mut handle = mount(fs, mountpoint_path.clone()).await.unwrap();

        let (write_done_tx, write_done_rx) = std::sync::mpsc::channel();
        let dir = mountpoint_path.clone();
        // create a threads for sync fs operations
        for i in 1..=2 {
            let done_tx = write_done_tx.clone();
            let path = dir.clone();
            std::thread::spawn(move || {
                let content = format!("secret{i}").repeat(1024);
                let path = path.join(format!("private/secret{i}.txt"));
                fs::write(path, content.as_bytes()).unwrap();
                done_tx.send(()).unwrap();
            });
            let done_tx = write_done_tx.clone();
            let path = dir.clone();
            std::thread::spawn(move || {
                let content = format!("hello{i}").repeat(1024);
                let path = path.join(format!("public/hello{i}.txt"));
                fs::write(path, content.as_bytes()).unwrap();
                done_tx.send(()).unwrap();
            });
        }
        let dir = mountpoint_path.clone();
        tokio::task::spawn_blocking(move || {
            // wait for all writes to be finished.
            for _ in 0..4 {
                write_done_rx.recv().unwrap();
            }
            let res = fs::read(dir.join("public/hello1.txt")).unwrap();
            assert_eq!(&res, &b"hello1".repeat(1024));
            let res = fs::read(dir.join("public/hello2.txt")).unwrap();
            assert_eq!(&res, &b"hello2".repeat(1024));
            let res = fs::read(dir.join("private/secret1.txt")).unwrap();
            assert_eq!(&res, &b"secret1".repeat(1024));
            let res = fs::read(dir.join("private/secret2.txt")).unwrap();
            assert_eq!(&res, &b"secret2".repeat(1024));
            // unmount
            handle.unmount().unwrap();
            drop(store_dir);
            drop(mountpoint);
        })
        .await
        .unwrap();
    }
}
