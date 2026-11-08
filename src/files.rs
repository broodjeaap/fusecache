//! Regular-file open/create/IO/release: layer selection and handle bookkeeping.

use std::ffi::OsStr;
use std::fs::{self, File, Metadata, OpenOptions};
use std::io;
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use fuser::{BackingId, Errno, FileAttr, FopenFlags};
use log::{debug, error, warn};

use crate::fs::{Handle, Shared, attr_for, chown_new};
use crate::inode::{Active, FileState, Inode, Layer, finalize_clean, lock_idle, mark_dirty};
use crate::layers::{
    self, STAMP_XATTR, Stamp, ensure_parent, fuse_ino, open_nofollow, remove_quiet, times_of,
};
use crate::readcache::{drop_read_copy, enqueue_promotion, valid_read_copy};
use crate::sys;

pub type Register<'a> = &'a dyn Fn(&File) -> io::Result<BackingId>;

pub struct Opened {
    pub fh: u64,
    pub flags: FopenFlags,
    pub backing_id: Option<Arc<BackingId>>,
}

fn is_access_denied(e: &io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::EACCES | libc::EPERM | libc::EROFS | libc::ETXTBSY)
    )
}

/// Create (or truncate) a private write-cache file at `rel`.
fn create_write_copy(s: &Shared, rel: &Path) -> io::Result<File> {
    let wp = s.roots.write_path(rel);
    ensure_parent(&wp)?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&wp)
}

/// Copy `src` into a fresh write-cache tmp file carrying the backing file's timestamps.
fn copy_up(s: &Shared, src: &File, meta: &Metadata) -> io::Result<(PathBuf, File, u64)> {
    let tmp = s.roots.write_tmp.join(layers::unique_tmp_name());
    let dst = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)?;
    let r = (|| {
        let n = layers::copy_contents(src, &dst, &s.shutdown)?;
        dst.sync_all()?;
        let (a, m) = times_of(meta);
        sys::futimens(&dst, a, m)?;
        Ok(n)
    })();
    match r {
        Ok(n) => Ok((tmp, dst, n)),
        Err(e) => {
            let _ = remove_quiet(&tmp);
            Err(e)
        }
    }
}

fn new_fh(s: &Shared) -> u64 {
    s.next_fh.fetch_add(1, Ordering::Relaxed)
}

/// Install `file` as the active layer of a closed inode and register the first handle.
fn activate(
    s: &Shared,
    st: &mut FileState,
    ino: u64,
    (layer, file, writable): (Layer, File, bool),
    want_write: bool,
    register: Register<'_>,
) -> Result<Opened, Errno> {
    let file = Arc::new(file);
    let backing_id = if s.passthrough.load(Ordering::Relaxed) {
        match register(&file) {
            Ok(id) => Some(Arc::new(id)),
            Err(e) => {
                error!("registering passthrough backing for ino={ino}: {e}");
                if st.needs_flush() {
                    s.schedule_flush(ino);
                }
                return Err(Errno::EIO);
            }
        }
    } else {
        None
    };
    st.active = Some(Active {
        layer,
        file: file.clone(),
        writable,
        backing_id: backing_id.clone(),
        handles: 1,
        writers: u32::from(want_write),
        dual_writers: 0,
    });
    let fh = new_fh(s);
    s.handles.lock().insert(
        fh,
        Handle {
            file,
            dual: None,
            writer: want_write,
        },
    );
    debug!(
        "open ino={ino} layer={layer:?} passthrough={}",
        backing_id.is_some()
    );
    Ok(Opened {
        fh,
        flags: FopenFlags::FOPEN_NOFLUSH,
        backing_id,
    })
}

pub fn open_file(
    s: &Shared,
    ino: u64,
    flags: i32,
    register: Register<'_>,
) -> Result<Opened, Errno> {
    let inode = s.table.get(ino).ok_or(Errno::ENOENT)?;
    let trunc = flags & libc::O_TRUNC != 0;
    let want_write = flags & libc::O_ACCMODE != libc::O_RDONLY || trunc;
    let (mut ns, mut guard) = lock_idle(s, &inode);
    if guard.deleted {
        return Err(Errno::ENOENT);
    }
    let rel = inode.path();

    if guard.active.is_some() {
        let st = &mut *guard;
        let a = st.active.as_mut().expect("checked above");
        let mut dual = None;
        let mut fopen = FopenFlags::FOPEN_NOFLUSH;
        if want_write && a.layer == Layer::Read {
            // Writer on a read-cached file: write through to backing and the read copy.
            let b = open_nofollow(&s.roots.backing_path(&rel), true)?;
            if trunc {
                b.set_len(0)?;
                a.file.set_len(0)?;
            }
            a.dual_writers += 1;
            dual = Some(Arc::new(b));
            fopen |= FopenFlags::FOPEN_DIRECT_IO;
        } else if want_write {
            if !a.writable {
                return Err(Errno::EACCES);
            }
            match a.layer {
                Layer::Write => {
                    st.dirty_gen += 1;
                    st.flushed_gen = None;
                }
                Layer::Backing => drop_read_copy(s, &rel),
                Layer::Read => unreachable!(),
            }
            if trunc {
                a.file.set_len(0)?;
            }
        }
        if want_write {
            a.writers += 1;
            st.content_gen += 1;
        }
        a.handles += 1;
        let fh = new_fh(s);
        let layer = a.layer;
        let backing_id = a.backing_id.clone();
        s.handles.lock().insert(
            fh,
            Handle {
                file: a.file.clone(),
                dual,
                writer: want_write,
            },
        );
        debug!(
            "open ino={ino} layer={layer:?} passthrough={} (shared)",
            backing_id.is_some()
        );
        return Ok(Opened {
            fh,
            flags: fopen,
            backing_id,
        });
    }

    let bp = s.roots.backing_path(&rel);
    let meta = fs::symlink_metadata(&bp)?;
    let cacheable = meta.is_file() && meta.nlink() == 1;
    let mut chosen: Option<(Layer, File, bool)> = None;

    if guard.dirty {
        let f = open_nofollow(&s.roots.write_path(&rel), true)?;
        if want_write {
            guard.bump_dirty();
            guard.content_gen += 1;
        }
        if trunc {
            f.set_len(0)?;
        }
        chosen = Some((Layer::Write, f, true));
    } else if want_write && cacheable && trunc && s.write_admit(0) {
        let f = create_write_copy(s, &rel)?;
        mark_dirty(s, &inode, &mut guard);
        drop_read_copy(s, &rel);
        chosen = Some((Layer::Write, f, true));
    } else if want_write
        && cacheable
        && !trunc
        && meta.len() <= s.settings.copy_up_max
        && s.write_admit(meta.len())
    {
        guard.busy = true;
        let src = match valid_read_copy(s, &rel, &meta) {
            Some(f) => Ok(f),
            None => open_nofollow(&bp, false),
        };
        drop(guard);
        drop(ns);
        let copied = src.and_then(|src| copy_up(s, &src, &meta));
        ns = s.ns.read();
        guard = inode.state.lock();
        guard.busy = false;
        inode.cv.notify_all();
        match copied {
            Ok((tmp, f, len)) => {
                if guard.deleted {
                    let _ = remove_quiet(&tmp);
                    return Err(Errno::ENOENT);
                }
                let cur = inode.path();
                let wp = s.roots.write_path(&cur);
                if let Err(e) = ensure_parent(&wp).and_then(|()| fs::rename(&tmp, &wp)) {
                    let _ = remove_quiet(&tmp);
                    return Err(e.into());
                }
                mark_dirty(s, &inode, &mut guard);
                s.resize_write_used(&mut guard, len);
                drop_read_copy(s, &cur);
                debug!("copied up ino={ino} {} ({len} bytes)", cur.display());
                chosen = Some((Layer::Write, f, true));
            }
            Err(e) => warn!(
                "copy-up of {} failed, writing to backing directly: {e}",
                rel.display()
            ),
        }
    }

    let rel = inode.path();
    let bp = s.roots.backing_path(&rel);
    if chosen.is_none() && want_write {
        let f = open_nofollow(&bp, true)?;
        drop_read_copy(s, &rel);
        guard.content_gen += 1;
        if trunc {
            f.set_len(0)?;
        }
        chosen = Some((Layer::Backing, f, true));
    }
    if chosen.is_none()
        && cacheable
        && let Some(f) = valid_read_copy(s, &rel, &meta)
    {
        chosen = Some((Layer::Read, f, true));
    }
    let chosen = match chosen {
        Some(c) => c,
        None => {
            if cacheable {
                enqueue_promotion(s, &mut guard, ino, &meta);
            }
            match open_nofollow(&bp, true) {
                Ok(f) => (Layer::Backing, f, true),
                Err(e) if is_access_denied(&e) => {
                    (Layer::Backing, open_nofollow(&bp, false)?, false)
                }
                Err(e) => return Err(e.into()),
            }
        }
    };
    let r = activate(s, &mut guard, ino, chosen, want_write, register);
    drop(guard);
    drop(ns);
    r
}

pub struct Created {
    pub attr: FileAttr,
    pub opened: Opened,
}

#[allow(clippy::too_many_arguments)]
pub fn create_file(
    s: &Shared,
    uid: u32,
    gid: u32,
    parent: u64,
    name: &OsStr,
    mode: u32,
    umask: u32,
    flags: i32,
    register: Register<'_>,
) -> Result<Created, Errno> {
    let ns = s.ns.write();
    let rel = s.table.get(parent).ok_or(Errno::ENOENT)?.path().join(name);
    let bp = s.roots.backing_path(&rel);
    let created = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(mode & !umask & 0o7777)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&bp);
    let bfile = match created {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            if flags & libc::O_EXCL != 0 {
                return Err(Errno::EEXIST);
            }
            drop(ns);
            return create_existing(s, &rel, flags, register);
        }
        Err(e) => return Err(e.into()),
    };
    chown_new(s, uid, gid, &bp)?;
    let meta = bfile.metadata()?;
    let ino = fuse_ino(&s.settings, meta.ino())?;
    let inode = s.table.lookup_insert(ino, &rel, true, &s.roots);
    let mut st = inode.state.lock();
    reset_reused(&inode, &mut st)?;

    let chosen = if s.write_admit(0) {
        let f = create_write_copy(s, &rel)?;
        mark_dirty(s, &inode, &mut st);
        drop(bfile);
        (Layer::Write, f, true)
    } else {
        st.content_gen += 1;
        (Layer::Backing, bfile, true)
    };
    let want_write = flags & libc::O_ACCMODE != libc::O_RDONLY;
    let opened = activate(s, &mut st, ino, chosen, want_write, register)?;
    let attr = attr_for(s, &inode, &st, &meta);
    drop(st);
    drop(ns);
    Ok(Created { attr, opened })
}

/// `create` without `O_EXCL` on an existing name: plain lookup + open.
fn create_existing(
    s: &Shared,
    rel: &Path,
    flags: i32,
    register: Register<'_>,
) -> Result<Created, Errno> {
    let ino = {
        let _ns = s.ns.read();
        crate::fs::entry_for(s, rel)?.0.ino
    };
    let opened = open_file(s, ino, flags, register)?;
    let attr = {
        let _ns = s.ns.read();
        crate::fs::entry_attr(s, ino)
    };
    match attr {
        Ok(attr) => Ok(Created { attr, opened }),
        Err(e) => {
            release(s, ino, opened.fh);
            Err(e)
        }
    }
}

/// A backing inode number came back for a new file while an old, deleted inode with the
/// same number is still known: reset its state, unless it is still open.
pub fn reset_reused(inode: &Inode, st: &mut FileState) -> Result<(), Errno> {
    if st.deleted {
        if st.active.is_some() {
            warn!(
                "backing inode {} reused while a deleted file with that number is open",
                inode.ino
            );
            return Err(Errno::EIO);
        }
        st.deleted = false;
    }
    Ok(())
}

fn handle_files(s: &Shared, fh: u64) -> Result<(Arc<File>, Option<Arc<File>>), Errno> {
    let hs = s.handles.lock();
    let h = hs.get(&fh).ok_or(Errno::EBADF)?;
    Ok((h.file.clone(), h.dual.clone()))
}

pub fn read(s: &Shared, ino: u64, fh: u64, offset: u64, size: u32) -> Result<Vec<u8>, Errno> {
    debug!("daemon read ino={ino} off={offset} size={size}");
    let (file, _) = handle_files(s, fh)?;
    let mut buf = vec![0u8; size as usize];
    let mut got = 0;
    while got < buf.len() {
        match file.read_at(&mut buf[got..], offset + got as u64) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    buf.truncate(got);
    Ok(buf)
}

pub fn write(s: &Shared, fh: u64, offset: u64, data: &[u8]) -> Result<u32, Errno> {
    let (file, dual) = handle_files(s, fh)?;
    file.write_all_at(data, offset)?;
    if let Some(d) = dual {
        d.write_all_at(data, offset)?;
    }
    Ok(data.len() as u32)
}

pub fn fsync(s: &Shared, fh: u64, datasync: bool) -> Result<(), Errno> {
    let (file, dual) = handle_files(s, fh)?;
    for f in std::iter::once(&file).chain(dual.as_ref()) {
        if datasync {
            f.sync_data()?
        } else {
            f.sync_all()?
        }
    }
    Ok(())
}

pub fn fallocate(s: &Shared, fh: u64, offset: u64, len: u64, mode: i32) -> Result<(), Errno> {
    let (file, dual) = handle_files(s, fh)?;
    sys::fallocate(&file, mode, offset, len)?;
    if let Some(d) = dual {
        sys::fallocate(&d, mode, offset, len)?;
    }
    Ok(())
}

pub fn release(s: &Shared, ino: u64, fh: u64) {
    let Some(inode) = s.table.get(ino) else {
        s.handles.lock().remove(&fh);
        return;
    };
    let _ns = s.ns.read();
    let mut guard = inode.state.lock();
    let Some(h) = s.handles.lock().remove(&fh) else {
        return;
    };
    let st = &mut *guard;
    let Some(a) = st.active.as_mut() else { return };
    a.handles -= 1;
    if h.writer {
        a.writers -= 1;
    }
    if let Some(dual) = &h.dual {
        a.dual_writers -= 1;
        if a.dual_writers == 0 && a.layer == Layer::Read {
            let rel = inode.path();
            match (a.file.metadata(), dual.metadata()) {
                (Ok(rm), Ok(bm)) if rm.len() == bm.len() => {
                    if let Err(e) =
                        sys::fsetxattr(&a.file, STAMP_XATTR, Stamp::of(&bm).encode().as_bytes())
                    {
                        warn!("restamping read copy of {}: {e}", rel.display());
                        drop_read_copy(s, &rel);
                    }
                }
                _ => drop_read_copy(s, &rel),
            }
        }
    }
    let (writers, handles) = (a.writers, a.handles);
    if writers == 0 && st.needs_flush() {
        s.schedule_flush(ino);
    }
    if handles == 0 {
        let a = st.active.take().expect("checked above");
        if a.layer == Layer::Write && st.dirty {
            match a.file.metadata() {
                Ok(m) => s.resize_write_used(st, m.len()),
                Err(e) => warn!("stat of write copy for ino={ino}: {e}"),
            }
        }
        drop(a);
        if st.dirty && st.flushed_gen == Some(st.dirty_gen) {
            finalize_clean(s, &inode, st);
        }
    }
}
