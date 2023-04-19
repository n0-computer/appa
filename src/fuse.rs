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
    Request,
};
use libc::ENOENT;
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
pub fn mount(fs: Fs, mountpoint: impl AsRef<Path>) -> anyhow::Result<()> {
    let mountpoint = mountpoint.as_ref();
    let mountpoint_meta = std::fs::metadata(mountpoint)?;
    let config = FuseConfig {
        uid: mountpoint_meta.uid(),
        gid: mountpoint_meta.gid(),
    };
    let fs = FuseFs::new(fs, config);
    let mountpoint = mountpoint.to_owned();
    let options = vec![
        // Change to RW once writing files works
        MountOption::RW,
        MountOption::FSName("appa-wnfs".to_string()),
        MountOption::AutoUnmount,
        MountOption::AllowRoot,
    ];
    debug!("mount FUSE at {mountpoint:?}");
    fuser::mount2(fs, mountpoint, &options)?;
    Ok(())
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

pub struct FuseConfig {
    uid: u32,
    gid: u32,
}

pub struct FuseFs {
    fs: Fs,
    inodes: Inodes,
    config: FuseConfig,
    write_handles: HashMap<u64, WriteHandle>,
}

impl FuseFs {
    pub fn new(fs: Fs, config: FuseConfig) -> Self {
        let mut inodes = Inodes::default();
        // Init root inodes.
        inodes.push("/".to_string());
        inodes.push("/private".to_string());
        inodes.push("/public".to_string());

        Self {
            fs,
            inodes,
            config,
            write_handles: Default::default(),
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
            size: size as u64,
            blocks: blocks as u64,
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

fn block_on<F: Future>(future: F) -> F::Output {
    futures::executor::block_on(future)
}

impl Filesystem for FuseFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        trace!("lookup: i{parent} {name:?}");
        let Some(path) = self.inodes.get_path(parent) else {
            trace!("  ENOENT");
            reply.error(ENOENT);
            return;
        };
        let path = push_segment(&path, &name.to_str().unwrap());
        let Inode { ino, .. } = self.inodes.get_or_push(&path);
        match block_on(self.fs.get_node(path)) {
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
        let Ok(Some(node)) = block_on(self.fs.get_node(path.into())) else {
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
        let content = block_on(
            self.fs
                .read_file_at(path.into(), offset as usize, size as usize),
        );
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

        let Ok(dir) = block_on(self.fs.ls(path.clone())) else {
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
            let node = block_on(self.fs.get_node(path.clone()));
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
        let Some(path) = self.inodes.get_path(parent) else {
            trace!("  ENOENT: parent not found");
            reply.error(ENOENT);
            return;
        };
        let path = push_segment(path, name.to_str().unwrap());
        match block_on(self.fs.mkdir(path.clone())) {
            Ok(_) => match block_on(self.fs.get_node(path.clone())) {
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
            let write_handle = WriteHandle::new(path.clone());
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
        let Some(path) = self.inodes.get_path(parent) else {
            trace!("  ENOENT");
            reply.error(ENOENT);
            return;
        };
        let path = push_segment(&path, &name.to_string_lossy());
        match block_on(self.fs.get_node(path.clone())) {
            Ok(Some(_node)) => {
                trace!("  EEXISTS {path}");
                reply.error(libc::EEXIST);
                return;
            }
            _ => {}
        }
        let ino = self.inodes.push(path);
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
        match handle.append(offset as u64, data) {
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
        if let Some(handle) = self.write_handles.remove(&ino) {
            let len = handle.content.len();
            match block_on(async {
                self.fs.write(handle.path.clone(), handle.content).await?;
                self.fs.commit().await?;
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
    format!("{}/{}", path, name)
}

pub struct WriteHandle {
    path: String,
    pos: usize,
    content: Vec<u8>,
}

impl WriteHandle {
    pub fn new(path: String) -> Self {
        Self {
            path,
            pos: 0,
            content: Vec::new(),
        }
    }
    pub fn append(&mut self, offset: u64, data: &[u8]) -> anyhow::Result<usize> {
        if offset as usize != self.pos {
            return Err(anyhow::anyhow!("Only append-only writes are supported"));
        }
        self.pos = offset as usize + data.len();
        self.content.extend_from_slice(data);
        Ok(data.len())
    }
}

// TODO: Write tests once wnfs is Send
// #[cfg(test)]
// mod test {
//     use std::{time::Duration, fs};
//
//     use crate::{store::flatfs::FlatFsStore, fs::Wnfs};
//
//     use super::mount;
//
//     #[tokio::test]
//     async fn test_fuse_read() {
//         let dir = tempfile::tempdir().unwrap();
//         let mountpoint = tempfile::tempdir().unwrap();
//         let store = FlatFsStore::new(dir).unwrap();
//         let fs = Wnfs::with_store(store, "test").await.unwrap();
//         let path = &["foo".to_string()];
//         fs::write(path, "rev1".as_bytes().to_vec());
//         let mountpoint2 = mountpoint.
//         std::thread::spawn(move || {
//             std::thread::sleep(Duration::from_millis(100));
//             let read = fs::read_to_string(mountpoint2.join("foo")).unwrap();
//             assert_eq!("rev1", read.as_str(), "read ok");
//         });
//         mount(fs, mountpoint);
//     }
// }
