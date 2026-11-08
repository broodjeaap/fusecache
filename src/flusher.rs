//! Write-back of dirty write-cache files to the backing store, and startup recovery.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossbeam_channel::{Receiver, RecvTimeoutError};
use log::{debug, error, info, warn};

use crate::fs::Shared;
use crate::inode::{FileState, Inode, finalize_clean};
use crate::layers::{self, ensure_parent, fuse_ino, open_nofollow, times_of};
use crate::readcache::drop_read_copy;
use crate::sys;

const RETRY_AFTER: Duration = Duration::from_secs(60);
const MAX_WAIT: Duration = Duration::from_millis(500);

pub enum FlushCmd {
    Schedule { ino: u64, due: Instant },
    Wake,
}

impl Shared {
    /// Whether `incoming` more bytes fit in the write cache.
    pub fn write_admit(&self, incoming: u64) -> bool {
        self.write_used
            .load(Ordering::Relaxed)
            .saturating_add(incoming)
            <= self.settings.write_cache_size
    }

    pub fn write_pressure(&self) -> bool {
        self.write_used.load(Ordering::Relaxed) > self.settings.write_cache_size / 10 * 8
    }

    /// Queue a flush `flush_delay` from now (immediately under write-cache pressure).
    pub fn schedule_flush(&self, ino: u64) {
        let due = if self.write_pressure() {
            Instant::now()
        } else {
            Instant::now() + self.settings.flush_delay
        };
        let _ = self.flush_tx.send(FlushCmd::Schedule { ino, due });
    }
}

pub fn spawn(s: &Arc<Shared>, rx: Receiver<FlushCmd>) -> io::Result<JoinHandle<()>> {
    let s = s.clone();
    std::thread::Builder::new()
        .name("flusher".into())
        .spawn(move || flusher(&s, &rx))
}

fn flusher(s: &Shared, rx: &Receiver<FlushCmd>) {
    let mut heap: BinaryHeap<Reverse<(Instant, u64)>> = BinaryHeap::new();
    loop {
        let now = Instant::now();
        let wait = match heap.peek() {
            Some(_) if s.write_pressure() => Duration::ZERO,
            Some(Reverse((due, _))) => due.saturating_duration_since(now).min(MAX_WAIT),
            None => MAX_WAIT,
        };
        let mut push = |cmd| {
            if let FlushCmd::Schedule { ino, due } = cmd {
                heap.push(Reverse((due, ino)));
            }
        };
        match rx.recv_timeout(wait) {
            Ok(cmd) => push(cmd),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
        while let Ok(cmd) = rx.try_recv() {
            push(cmd);
        }
        if s.shutdown.load(Ordering::Relaxed) {
            return;
        }
        let now = Instant::now();
        let mut batch = Vec::new();
        while let Some(&Reverse((due, ino))) = heap.peek() {
            if due > now {
                break;
            }
            heap.pop();
            batch.push(ino);
        }
        if batch.is_empty()
            && s.write_pressure()
            && let Some(Reverse((_, ino))) = heap.pop()
        {
            batch.push(ino);
        }
        for ino in batch {
            if s.shutdown.load(Ordering::Relaxed) {
                return;
            }
            flush_one(s, ino);
        }
    }
}

/// Copy `src` over `dst` in place and carry over its timestamps.
fn copy_back(s: &Shared, src: &File, dst: &File) -> io::Result<u64> {
    let n = layers::copy_contents(src, dst, &s.shutdown)?;
    let (a, m) = times_of(&src.metadata()?);
    sys::futimens(dst, a, m)?;
    dst.sync_data()?;
    Ok(n)
}

fn open_pair(s: &Shared, rel: &Path) -> io::Result<(File, File)> {
    let src = open_nofollow(&s.roots.write_path(rel), false)?;
    let dst = open_nofollow(&s.roots.backing_path(rel), true)?;
    Ok((src, dst))
}

pub fn flush_one(s: &Shared, ino: u64) {
    let Some(inode) = s.table.get(ino) else {
        return;
    };
    let (gen_, rel, src, dst) = {
        let _ns = s.ns.read();
        let st = inode.state.lock();
        if !st.needs_flush() || st.busy || st.writers() > 0 {
            return;
        }
        let rel = inode.path();
        match open_pair(s, &rel) {
            Ok((src, dst)) => (st.dirty_gen, rel, src, dst),
            Err(e) => {
                error!(
                    "flush of {}: {e}; retrying in {RETRY_AFTER:?}",
                    rel.display()
                );
                let _ = s.flush_tx.send(FlushCmd::Schedule {
                    ino,
                    due: Instant::now() + RETRY_AFTER,
                });
                return;
            }
        }
    };

    match copy_back(s, &src, &dst) {
        Ok(n) => debug!("flushed ino={ino} {} ({n} bytes)", rel.display()),
        Err(e) if e.kind() == io::ErrorKind::Interrupted => return,
        Err(e) => {
            error!(
                "flush of {}: {e}; retrying in {RETRY_AFTER:?}",
                rel.display()
            );
            let _ = s.flush_tx.send(FlushCmd::Schedule {
                ino,
                due: Instant::now() + RETRY_AFTER,
            });
            return;
        }
    }

    let _ns = s.ns.read();
    let mut st = inode.state.lock();
    if st.deleted || !st.dirty || st.dirty_gen != gen_ || st.writers() > 0 {
        return;
    }
    if st.active.is_none() {
        finalize_clean(s, &inode, &mut st);
    } else {
        st.flushed_gen = Some(gen_);
    }
    drop_read_copy(s, &inode.path());
}

/// Synchronous flush with `ns` (write) and the inode state held by the caller.
pub fn flush_inline(s: &Shared, inode: &Inode, st: &mut FileState) -> io::Result<()> {
    let rel = inode.path();
    let (src, dst) = open_pair(s, &rel)?;
    copy_back(s, &src, &dst)?;
    finalize_clean(s, inode, st);
    drop_read_copy(s, &rel);
    Ok(())
}

/// Register every write-cache file left by a previous run and queue it for flushing.
pub fn recover(s: &Shared) -> io::Result<()> {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let mut files = Vec::new();
    walk_files(&s.roots.write_data, PathBuf::new(), &mut files)?;
    for rel in files {
        let wp = s.roots.write_path(&rel);
        let target = fs::symlink_metadata(s.roots.backing_path(&rel));
        let registered = match &target {
            Ok(m) if m.is_file() => match fuse_ino(&s.settings, m.ino()) {
                Ok(ino) => {
                    let len = fs::symlink_metadata(&wp)?.len();
                    let ok = s.table.insert_recovered(ino, &rel, len);
                    if ok {
                        s.write_used.fetch_add(len, Ordering::Relaxed);
                        let _ = s.flush_tx.send(FlushCmd::Schedule {
                            ino,
                            due: Instant::now(),
                        });
                        info!("recovered dirty file {}", rel.display());
                    }
                    ok
                }
                Err(_) => false,
            },
            _ => false,
        };
        if !registered {
            let dest = s.roots.write_orphans.join(stamp.to_string()).join(&rel);
            warn!(
                "orphaned dirty file {} moved to {}",
                wp.display(),
                dest.display()
            );
            ensure_parent(&dest)?;
            fs::rename(&wp, &dest)?;
        }
    }
    prune_empty_dirs(&s.roots.write_data, false)?;
    prune_empty_dirs(&s.roots.read_data, false)?;
    Ok(())
}

fn walk_files(root: &Path, dir: PathBuf, out: &mut Vec<PathBuf>) -> io::Result<()> {
    for e in fs::read_dir(root.join(&dir))? {
        let e = e?;
        let rel = dir.join(e.file_name());
        let ft = e.file_type()?;
        if ft.is_dir() {
            walk_files(root, rel, out)?;
        } else if ft.is_file() {
            out.push(rel);
        } else {
            warn!(
                "ignoring non-regular file {} in write cache",
                root.join(&rel).display()
            );
        }
    }
    Ok(())
}

/// Remove empty directories below `dir` bottom-up (and `dir` itself if `remove_self`).
/// Returns whether `dir` ended up empty. Files are never touched.
pub fn prune_empty_dirs(dir: &Path, remove_self: bool) -> io::Result<bool> {
    let mut empty = true;
    for e in fs::read_dir(dir)? {
        let e = e?;
        if e.file_type()?.is_dir() && prune_empty_dirs(&e.path(), true)? {
            continue;
        }
        empty = false;
    }
    if empty && remove_self {
        fs::remove_dir(dir)?;
    }
    Ok(empty)
}
