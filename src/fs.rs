//! Shared daemon state and the `fuser::Filesystem` implementation.

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, Metadata};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirEntryExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::SystemTime;

use crossbeam_channel::Sender;
use fuser::{
    AccessFlags, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation,
    INodeNo, InitFlags, KernelConfig, LockOwner, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate,
    ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite,
    ReplyXattr, Request, TimeOrNow, WriteFlags,
};
use log::{error, info, warn};
use parking_lot::{Mutex, RwLock};

use crate::config::{PassthroughMode, Settings};
use crate::files::{self, Opened};
use crate::flusher::FlushCmd;
use crate::inode::{FileState, Inode, InodeTable, Layer, lock_idle};
use crate::layers::{Roots, fuse_ino, merged_attr};
use crate::namespace;
use crate::readcache::{ReadIndex, drop_read_copy};
use crate::sys;

pub struct Handle {
    pub file: Arc<File>,
    /// Backing-file fd of a write-through handle on a read-cached file.
    pub dual: Option<Arc<File>>,
    pub writer: bool,
}

type DirEntries = Vec<(u64, FileType, OsString)>;

pub struct Shared {
    pub roots: Roots,
    pub settings: Settings,
    pub table: InodeTable,
    pub ns: RwLock<()>,
    pub handles: Mutex<HashMap<u64, Handle>>,
    pub dirs: Mutex<HashMap<u64, DirEntries>>,
    pub next_fh: AtomicU64,
    pub read_index: ReadIndex,
    pub write_used: AtomicU64,
    pub passthrough: AtomicBool,
    pub shutdown: AtomicBool,
    pub promote_tx: Sender<u64>,
    pub flush_tx: Sender<FlushCmd>,
}

impl Shared {
    pub fn new(
        settings: Settings,
        roots: Roots,
        read_index: ReadIndex,
        promote_tx: Sender<u64>,
        flush_tx: Sender<FlushCmd>,
    ) -> Shared {
        Shared {
            roots,
            settings,
            table: InodeTable::new(),
            ns: RwLock::new(()),
            handles: Mutex::new(HashMap::new()),
            dirs: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            read_index,
            write_used: AtomicU64::new(0),
            passthrough: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            promote_tx,
            flush_tx,
        }
    }

    pub fn inode(&self, ino: u64) -> Result<Arc<Inode>, Errno> {
        self.table.get(ino).ok_or(Errno::ENOENT)
    }

    pub fn child_path(&self, parent: u64, name: &OsStr) -> Result<PathBuf, Errno> {
        Ok(self.inode(parent)?.path().join(name))
    }
}

/// Attributes of `inode` given its backing metadata, overlaying the write copy when dirty.
/// Records the result as the inode's last known attributes.
pub fn attr_for(s: &Shared, inode: &Inode, st: &FileState, backing: &Metadata) -> FileAttr {
    let write_meta = if st.dirty {
        fs::symlink_metadata(s.roots.write_path(&inode.path())).ok()
    } else {
        None
    };
    let attr = merged_attr(backing, write_meta.as_ref(), inode.ino);
    *inode.last_attr.lock() = Some(attr);
    attr
}

/// Look up `rel` on the backing store and register a kernel reference. Caller holds `ns`.
pub fn entry_for(s: &Shared, rel: &Path) -> Result<(Arc<Inode>, FileAttr), Errno> {
    let meta = fs::symlink_metadata(s.roots.backing_path(rel))?;
    if meta.dev() != s.settings.backing_dev {
        warn!(
            "nested mount under backing not supported: {}",
            rel.display()
        );
        return Err(Errno::EIO);
    }
    let ino = fuse_ino(&s.settings, meta.ino())?;
    let inode = s.table.lookup_insert(ino, rel, meta.is_file(), &s.roots);
    let mut st = inode.state.lock();
    if st.deleted && st.active.is_none() {
        // The backing store reused the inode number of a deleted file.
        st.deleted = false;
    }
    let attr = attr_for(s, &inode, &st, &meta);
    drop(st);
    Ok((inode, attr))
}

/// Current attributes of a known inode. Caller holds `ns`.
pub fn entry_attr(s: &Shared, ino: u64) -> Result<FileAttr, Errno> {
    let inode = s.inode(ino)?;
    let st = inode.state.lock();
    if st.deleted {
        let mut attr = (*inode.last_attr.lock()).ok_or(Errno::ENOENT)?;
        attr.nlink = 0;
        if let Some(m) = st.active.as_ref().and_then(|a| a.file.metadata().ok()) {
            attr.size = m.len();
            attr.blocks = m.blocks();
        }
        return Ok(attr);
    }
    let meta = fs::symlink_metadata(s.roots.backing_path(&inode.path()))?;
    Ok(attr_for(s, &inode, &st, &meta))
}

/// Give a freshly created backing node the caller's ownership (root only).
pub fn chown_new(s: &Shared, uid: u32, gid: u32, path: &Path) -> Result<(), Errno> {
    if !s.settings.is_root {
        return Ok(());
    }
    let parent_sgid = path
        .parent()
        .and_then(|p| fs::metadata(p).ok())
        .is_some_and(|m| m.mode() & libc::S_ISGID != 0);
    std::os::unix::fs::lchown(path, Some(uid), if parent_sgid { None } else { Some(gid) })?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn setattr(
    s: &Shared,
    ino: u64,
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    size: Option<u64>,
    atime: Option<TimeOrNow>,
    mtime: Option<TimeOrNow>,
) -> Result<FileAttr, Errno> {
    let inode = s.inode(ino)?;
    let (_ns, mut guard) = lock_idle(s, &inode);
    let st = &mut *guard;
    let rel = inode.path();
    let bp = s.roots.backing_path(&rel);
    let meta = fs::symlink_metadata(&bp)?;
    if let Some(mode) = mode
        && !meta.file_type().is_symlink()
    {
        fs::set_permissions(&bp, fs::Permissions::from_mode(mode & 0o7777))?;
    }
    if uid.is_some() || gid.is_some() {
        std::os::unix::fs::lchown(&bp, uid, gid)?;
    }
    let mut touched_dirty = false;
    if let Some(size) = size {
        match &st.active {
            Some(a) => {
                a.file.set_len(size)?;
                if a.layer == Layer::Read {
                    // The backing store stays authoritative; the read copy keeps open readers consistent.
                    fs::OpenOptions::new()
                        .write(true)
                        .open(&bp)?
                        .set_len(size)?;
                }
            }
            None if st.dirty => fs::OpenOptions::new()
                .write(true)
                .open(s.roots.write_path(&rel))?
                .set_len(size)?,
            None => fs::OpenOptions::new()
                .write(true)
                .open(&bp)?
                .set_len(size)?,
        }
        let read_active = st.active.as_ref().is_some_and(|a| a.layer == Layer::Read);
        if !read_active && size != meta.len() {
            drop_read_copy(s, &rel);
        }
        st.content_gen += 1;
        if st.dirty {
            touched_dirty = true;
            if st.active.is_none() {
                s.resize_write_used(st, size);
            }
        }
    }
    if atime.is_some() || mtime.is_some() {
        sys::utimens_path(&bp, atime, mtime)?;
        if st.dirty {
            sys::utimens_path(&s.roots.write_path(&rel), atime, mtime)?;
            touched_dirty = true;
        }
    }
    if touched_dirty {
        st.bump_dirty();
        if st.writers() == 0 {
            s.schedule_flush(ino);
        }
    }
    let meta = fs::symlink_metadata(&bp)?;
    Ok(attr_for(s, &inode, st, &meta))
}

fn opendir(s: &Shared, ino: u64) -> Result<u64, Errno> {
    let _ns = s.ns.read();
    let rel = s.inode(ino)?.path();
    let parent_ino = match rel.parent() {
        None => 1,
        Some(p) => fuse_ino(
            &s.settings,
            fs::symlink_metadata(s.roots.backing_path(p))?.ino(),
        )?,
    };
    let mut entries: DirEntries = vec![
        (ino, FileType::Directory, ".".into()),
        (parent_ino, FileType::Directory, "..".into()),
    ];
    for e in fs::read_dir(s.roots.backing_path(&rel))? {
        let e = e?;
        let kind = e
            .file_type()
            .ok()
            .and_then(FileType::from_std)
            .unwrap_or(FileType::RegularFile);
        match fuse_ino(&s.settings, e.ino()) {
            Ok(i) => entries.push((i, kind, e.file_name())),
            Err(_) => warn!("skipping {:?} in {}", e.file_name(), rel.display()),
        }
    }
    let fh = s.next_fh.fetch_add(1, Ordering::Relaxed);
    s.dirs.lock().insert(fh, entries);
    Ok(fh)
}

fn with_path<T>(s: &Shared, ino: u64, f: impl FnOnce(&Path) -> io::Result<T>) -> Result<T, Errno> {
    let _ns = s.ns.read();
    let rel = s.inode(ino)?.path();
    Ok(f(&s.roots.backing_path(&rel))?)
}

fn reply_xattr(reply: ReplyXattr, size: u32, data: Result<Vec<u8>, Errno>) {
    match data {
        Err(e) => reply.error(e),
        Ok(v) if size == 0 => reply.size(v.len() as u32),
        Ok(v) if v.len() > size as usize => reply.error(Errno::ERANGE),
        Ok(v) => reply.data(&v),
    }
}

fn reply_entry(s: &Shared, reply: ReplyEntry, r: Result<FileAttr, Errno>) {
    match r {
        Ok(attr) => reply.entry(&s.settings.attr_ttl, &attr, Generation(0)),
        Err(e) => reply.error(e),
    }
}

fn reply_empty(reply: ReplyEmpty, r: Result<(), Errno>) {
    match r {
        Ok(()) => reply.ok(),
        Err(e) => reply.error(e),
    }
}

fn reply_opened(reply: ReplyOpen, o: Opened) {
    match &o.backing_id {
        Some(id) => reply.opened_passthrough(FileHandle(o.fh), o.flags, id),
        None => reply.opened(FileHandle(o.fh), o.flags),
    }
}

pub struct CacheFs(pub Arc<Shared>);

impl Filesystem for CacheFs {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> io::Result<()> {
        let s = &self.0;
        let _ = config.add_capabilities(InitFlags::FUSE_ATOMIC_O_TRUNC);
        let _ = config.add_capabilities(InitFlags::FUSE_PARALLEL_DIROPS);
        let mode = s.settings.passthrough;
        if mode != PassthroughMode::Off
            && s.settings.is_root
            && config.add_capabilities(InitFlags::FUSE_PASSTHROUGH).is_ok()
            && config.set_max_stack_depth(2).is_ok()
        {
            s.passthrough.store(true, Ordering::Relaxed);
            info!("passthrough enabled");
        } else if mode == PassthroughMode::On {
            return Err(io::Error::other(
                "passthrough requested but unavailable: requires root and a kernel with CONFIG_FUSE_PASSTHROUGH (>= 6.9)",
            ));
        } else {
            info!("passthrough disabled");
        }
        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let s = &self.0;
        let r = (|| {
            let _ns = s.ns.read();
            let rel = s.child_path(parent.0, name)?;
            Ok(entry_for(s, &rel)?.1)
        })();
        reply_entry(s, reply, r);
    }

    fn forget(&self, _req: &Request, ino: INodeNo, nlookup: u64) {
        self.0.table.forget(ino.0, nlookup);
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let s = &self.0;
        let r = {
            let _ns = s.ns.read();
            entry_attr(s, ino.0)
        };
        match r {
            Ok(attr) => reply.attr(&s.settings.attr_ttl, &attr),
            Err(e) => reply.error(e),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let s = &self.0;
        match setattr(s, ino.0, mode, uid, gid, size, atime, mtime) {
            Ok(attr) => reply.attr(&s.settings.attr_ttl, &attr),
            Err(e) => reply.error(e),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        match with_path(&self.0, ino.0, |p| fs::read_link(p)) {
            Ok(t) => reply.data(t.as_os_str().as_bytes()),
            Err(e) => reply.error(e),
        }
    }

    fn mknod(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let s = &self.0;
        reply_entry(
            s,
            reply,
            namespace::mknod(s, req.uid(), req.gid(), parent.0, name, mode, umask, rdev),
        );
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let s = &self.0;
        reply_entry(
            s,
            reply,
            namespace::mkdir(s, req.uid(), req.gid(), parent.0, name, mode, umask),
        );
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        reply_empty(reply, namespace::unlink(&self.0, parent.0, name));
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        reply_empty(reply, namespace::rmdir(&self.0, parent.0, name));
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let s = &self.0;
        reply_entry(
            s,
            reply,
            namespace::symlink(s, req.uid(), req.gid(), parent.0, link_name, target),
        );
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        reply_empty(
            reply,
            namespace::rename(&self.0, parent.0, name, newparent.0, newname, flags),
        );
    }

    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let s = &self.0;
        reply_entry(s, reply, namespace::link(s, ino.0, newparent.0, newname));
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let r = files::open_file(&self.0, ino.0, flags.0, &|f| reply.open_backing(f));
        match r {
            Ok(o) => reply_opened(reply, o),
            Err(e) => reply.error(e),
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        match files::read(&self.0, ino.0, fh.0, offset, size) {
            Ok(d) => reply.data(&d),
            Err(e) => reply.error(e),
        }
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        match files::write(&self.0, fh.0, offset, data) {
            Ok(n) => reply.written(n),
            Err(e) => reply.error(e),
        }
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        files::release(&self.0, ino.0, fh.0);
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply_empty(reply, files::fsync(&self.0, fh.0, datasync));
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        match opendir(&self.0, ino.0) {
            Ok(fh) => reply.opened(FileHandle(fh), FopenFlags::empty()),
            Err(e) => reply.error(e),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let dirs = self.0.dirs.lock();
        let Some(entries) = dirs.get(&fh.0) else {
            return reply.error(Errno::EBADF);
        };
        for (i, (ino, kind, name)) in entries.iter().enumerate().skip(offset as usize) {
            if reply.add(INodeNo(*ino), (i + 1) as u64, *kind, name) {
                break;
            }
        }
        drop(dirs);
        reply.ok();
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        self.0.dirs.lock().remove(&fh.0);
        reply.ok();
    }

    fn fsyncdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply_empty(
            reply,
            with_path(&self.0, ino.0, |p| File::open(p)?.sync_all()),
        );
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        match sys::statvfs(&self.0.roots.backing) {
            Ok(v) => reply.statfs(
                v.f_blocks,
                v.f_bfree,
                v.f_bavail,
                v.f_files,
                v.f_ffree,
                v.f_bsize as u32,
                v.f_namemax as u32,
                v.f_frsize as u32,
            ),
            Err(e) => reply.error(e.into()),
        }
    }

    fn setxattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply_empty(
            reply,
            with_path(&self.0, ino.0, |p| sys::lsetxattr(p, name, value, flags)),
        );
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        reply_xattr(
            reply,
            size,
            with_path(&self.0, ino.0, |p| sys::lgetxattr(p, name)),
        );
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        reply_xattr(reply, size, with_path(&self.0, ino.0, sys::llistxattr));
    }

    fn removexattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        reply_empty(
            reply,
            with_path(&self.0, ino.0, |p| sys::lremovexattr(p, name)),
        );
    }

    fn access(&self, _req: &Request, _ino: INodeNo, _mask: AccessFlags, reply: ReplyEmpty) {
        reply.ok();
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let s = &self.0;
        let r = files::create_file(
            s,
            req.uid(),
            req.gid(),
            parent.0,
            name,
            mode,
            umask,
            flags,
            &|f| reply.open_backing(f),
        );
        match r {
            Ok(c) => {
                let ttl = s.settings.attr_ttl;
                match &c.opened.backing_id {
                    Some(id) => reply.created_passthrough(
                        &ttl,
                        &c.attr,
                        Generation(0),
                        FileHandle(c.opened.fh),
                        c.opened.flags,
                        id,
                    ),
                    None => reply.created(
                        &ttl,
                        &c.attr,
                        Generation(0),
                        FileHandle(c.opened.fh),
                        c.opened.flags,
                    ),
                }
            }
            Err(e) => reply.error(e),
        }
    }

    fn fallocate(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        reply_empty(reply, files::fallocate(&self.0, fh.0, offset, length, mode));
    }
}

/// Log-and-continue helper for best-effort cache-mirror maintenance.
pub fn log_err(what: &str, path: &Path, r: io::Result<()>) {
    if let Err(e) = r {
        error!("{what} {}: {e}", path.display());
    }
}
