//! Namespace operations. All run synchronously on the backing store under `ns.write()` and
//! then mirror the change onto the cache directories.

use std::ffi::OsStr;
use std::fs::{self, DirBuilder, Metadata};
use std::io;
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::Path;

use fuser::{Errno, FileAttr, RenameFlags};

use crate::flusher::{flush_inline, prune_empty_dirs};
use crate::fs::{Shared, chown_new, entry_for, log_err};
use crate::inode::finalize_clean;
use crate::layers::{ensure_parent, fuse_ino};
use crate::readcache::drop_read_copy;
use crate::sys;

fn perm(mode: u32, umask: u32) -> u32 {
    mode & !umask & 0o7777
}

#[allow(clippy::too_many_arguments)]
pub fn mknod(
    s: &Shared,
    uid: u32,
    gid: u32,
    parent: u64,
    name: &OsStr,
    mode: u32,
    umask: u32,
    rdev: u32,
) -> Result<FileAttr, Errno> {
    let _ns = s.ns.write();
    let rel = s.child_path(parent, name)?;
    let bp = s.roots.backing_path(&rel);
    sys::mknod(
        &bp,
        (mode & libc::S_IFMT) | perm(mode, umask),
        u64::from(rdev),
    )?;
    chown_new(s, uid, gid, &bp)?;
    Ok(entry_for(s, &rel)?.1)
}

pub fn mkdir(
    s: &Shared,
    uid: u32,
    gid: u32,
    parent: u64,
    name: &OsStr,
    mode: u32,
    umask: u32,
) -> Result<FileAttr, Errno> {
    let _ns = s.ns.write();
    let rel = s.child_path(parent, name)?;
    let bp = s.roots.backing_path(&rel);
    DirBuilder::new().mode(perm(mode, umask)).create(&bp)?;
    chown_new(s, uid, gid, &bp)?;
    Ok(entry_for(s, &rel)?.1)
}

pub fn symlink(
    s: &Shared,
    uid: u32,
    gid: u32,
    parent: u64,
    name: &OsStr,
    target: &Path,
) -> Result<FileAttr, Errno> {
    let _ns = s.ns.write();
    let rel = s.child_path(parent, name)?;
    let bp = s.roots.backing_path(&rel);
    std::os::unix::fs::symlink(target, &bp)?;
    chown_new(s, uid, gid, &bp)?;
    Ok(entry_for(s, &rel)?.1)
}

/// Forget the cache state of a backing node that just lost the name `rel`.
fn forget_name(s: &Shared, rel: &Path, meta: &Metadata) {
    if let Ok(ino) = fuse_ino(&s.settings, meta.ino())
        && let Some(inode) = s.table.get(ino)
    {
        let mut st = inode.state.lock();
        if meta.is_dir() || meta.nlink() <= 1 {
            if st.dirty {
                finalize_clean(s, &inode, &mut st);
            }
            st.deleted = true;
        }
    }
    if meta.is_dir() {
        let rp = s.roots.read_path(rel);
        match fs::remove_dir_all(&rp) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => {
                log_err("removing read mirror", &rp, Err(e))
            }
            _ => {}
        }
        // A removed backing directory was empty, so its write mirror holds no dirty files.
        let wp = s.roots.write_path(rel);
        match prune_empty_dirs(&wp, true) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => {
                log_err("removing write mirror", &wp, Err(e))
            }
            _ => {}
        }
    } else {
        drop_read_copy(s, rel);
    }
}

pub fn unlink(s: &Shared, parent: u64, name: &OsStr) -> Result<(), Errno> {
    let _ns = s.ns.write();
    let rel = s.child_path(parent, name)?;
    let bp = s.roots.backing_path(&rel);
    let meta = fs::symlink_metadata(&bp)?;
    fs::remove_file(&bp)?;
    forget_name(s, &rel, &meta);
    Ok(())
}

pub fn rmdir(s: &Shared, parent: u64, name: &OsStr) -> Result<(), Errno> {
    let _ns = s.ns.write();
    let rel = s.child_path(parent, name)?;
    let bp = s.roots.backing_path(&rel);
    let meta = fs::symlink_metadata(&bp)?;
    fs::remove_dir(&bp)?;
    forget_name(s, &rel, &meta);
    Ok(())
}

fn exists(p: &Path) -> bool {
    fs::symlink_metadata(p).is_ok()
}

pub fn rename(
    s: &Shared,
    parent: u64,
    name: &OsStr,
    newparent: u64,
    newname: &OsStr,
    flags: RenameFlags,
) -> Result<(), Errno> {
    if flags.contains(RenameFlags::RENAME_WHITEOUT) {
        return Err(Errno::EINVAL);
    }
    let _ns = s.ns.write();
    let old = s.child_path(parent, name)?;
    let new = s.child_path(newparent, newname)?;
    let old_meta = fs::symlink_metadata(s.roots.backing_path(&old))?;
    let new_meta = fs::symlink_metadata(s.roots.backing_path(&new)).ok();
    sys::renameat2(
        &s.roots.backing_path(&old),
        &s.roots.backing_path(&new),
        flags.bits(),
    )?;

    if new_meta.as_ref().is_some_and(|m| m.ino() == old_meta.ino()) {
        // Two names of one inode: rename(2) is a no-op.
        return Ok(());
    }
    let mirrors = [&s.roots.read_data, &s.roots.write_data];
    if flags.contains(RenameFlags::RENAME_EXCHANGE) {
        for root in mirrors {
            let (a, b) = (root.join(&old), root.join(&new));
            let r = match (exists(&a), exists(&b)) {
                (true, true) => sys::renameat2(&a, &b, libc::RENAME_EXCHANGE),
                (true, false) => ensure_parent(&b).and_then(|()| fs::rename(&a, &b)),
                (false, true) => ensure_parent(&a).and_then(|()| fs::rename(&b, &a)),
                (false, false) => Ok(()),
            };
            log_err("exchanging cache mirror", &a, r);
        }
        s.read_index.exchange_prefix(&old, &new);
        s.table.exchange_prefix(&old, &new);
    } else {
        if let Some(m) = &new_meta {
            forget_name(s, &new, m);
        }
        for root in mirrors {
            let (a, b) = (root.join(&old), root.join(&new));
            if exists(&a) {
                log_err(
                    "renaming cache mirror",
                    &a,
                    ensure_parent(&b).and_then(|()| fs::rename(&a, &b)),
                );
            }
        }
        s.read_index.rename_prefix(&old, &new);
        s.table.rename_prefix(&old, &new);
    }
    Ok(())
}

pub fn link(s: &Shared, ino: u64, newparent: u64, newname: &OsStr) -> Result<FileAttr, Errno> {
    let _ns = s.ns.write();
    let inode = s.inode(ino)?;
    let new = s.child_path(newparent, newname)?;
    let src = inode.path();
    {
        let mut st = inode.state.lock();
        if st.dirty {
            if st.active.is_some() {
                return Err(Errno::EBUSY);
            }
            flush_inline(s, &inode, &mut st)?;
        }
        fs::hard_link(s.roots.backing_path(&src), s.roots.backing_path(&new))?;
        // Files with more than one link bypass both caches.
        drop_read_copy(s, &src);
    }
    Ok(entry_for(s, &new)?.1)
}
