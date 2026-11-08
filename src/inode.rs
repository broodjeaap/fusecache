//! Inode table and per-file cache state.
//!
//! Lock order, everywhere: `Shared::ns` → `Inode::state` → (`Inode::path` | `InodeTable` |
//! `ReadIndex` | `Shared::handles` | `Inode::last_attr`). `InodeTable` is taken before `Inode::path`.
//! Never take `ns.read()` twice in one thread (parking_lot RwLocks are writer-preferring).

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use fuser::{BackingId, FileAttr};
use log::warn;
use parking_lot::{Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard};

use crate::fs::Shared;
use crate::layers::{Roots, remove_quiet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layer {
    Backing,
    Read,
    Write,
}

/// The single layer serving every open handle of an inode (the kernel requires one passthrough
/// backing file per inode).
pub struct Active {
    pub layer: Layer,
    pub file: Arc<File>,
    pub writable: bool,
    /// Kept alive until the last handle closes; `Some` iff passthrough is on.
    pub backing_id: Option<Arc<BackingId>>,
    pub handles: u32,
    pub writers: u32,
    pub dual_writers: u32,
}

#[derive(Default)]
pub struct FileState {
    /// The write-cache copy exists and holds the authoritative content.
    pub dirty: bool,
    /// Bumped on every write-open / size / time change while dirty.
    pub dirty_gen: u64,
    /// `dirty_gen` already copied to the backing store.
    pub flushed_gen: Option<u64>,
    /// Bumped on any content change in any layer (promotion guard).
    pub content_gen: u64,
    /// Logical length of the write copy as counted in `Shared::write_used`.
    pub write_bytes: u64,
    pub deleted: bool,
    /// Copy-up in progress; waiters sleep on `Inode::cv`.
    pub busy: bool,
    pub promote_queued: bool,
    pub active: Option<Active>,
}

impl FileState {
    pub fn bump_dirty(&mut self) {
        self.dirty_gen += 1;
        self.flushed_gen = None;
    }

    pub fn needs_flush(&self) -> bool {
        self.dirty && !self.deleted && self.flushed_gen != Some(self.dirty_gen)
    }

    pub fn writers(&self) -> u32 {
        self.active.as_ref().map_or(0, |a| a.writers)
    }
}

pub struct Inode {
    pub ino: u64,
    path: RwLock<PathBuf>,
    pub state: Mutex<FileState>,
    pub cv: Condvar,
    pub last_attr: Mutex<Option<FileAttr>>,
}

impl Inode {
    fn new(ino: u64, rel: PathBuf, st: FileState) -> Inode {
        Inode {
            ino,
            path: RwLock::new(rel),
            state: Mutex::new(st),
            cv: Condvar::new(),
            last_attr: Mutex::new(None),
        }
    }

    /// Current path relative to every root.
    pub fn path(&self) -> PathBuf {
        self.path.read().clone()
    }
}

struct Entry {
    inode: Arc<Inode>,
    lookups: u64,
    pins: u64,
}

impl Entry {
    fn idle(&self) -> bool {
        self.lookups == 0 && self.pins == 0 && self.inode.ino != 1
    }
}

pub struct InodeTable {
    map: RwLock<HashMap<u64, Entry>>,
}

impl Default for InodeTable {
    fn default() -> Self {
        Self::new()
    }
}

impl InodeTable {
    pub fn new() -> InodeTable {
        let mut map = HashMap::new();
        map.insert(
            1,
            Entry {
                inode: Arc::new(Inode::new(1, PathBuf::new(), FileState::default())),
                lookups: 0,
                pins: 1,
            },
        );
        InodeTable {
            map: RwLock::new(map),
        }
    }

    pub fn get(&self, ino: u64) -> Option<Arc<Inode>> {
        self.map.read().get(&ino).map(|e| e.inode.clone())
    }

    /// Record a kernel lookup of `ino` at `rel`; the last looked-up path wins.
    pub fn lookup_insert(&self, ino: u64, rel: &Path, is_file: bool, roots: &Roots) -> Arc<Inode> {
        let mut map = self.map.write();
        if let Some(e) = map.get_mut(&ino) {
            e.lookups += 1;
            let mut p = e.inode.path.write();
            if *p != rel {
                *p = rel.to_path_buf();
            }
            return e.inode.clone();
        }
        let mut st = FileState::default();
        let mut pins = 0;
        if is_file && let Ok(m) = std::fs::symlink_metadata(roots.write_path(rel)) {
            st.dirty = true;
            st.write_bytes = m.len();
            pins = 1;
        }
        let inode = Arc::new(Inode::new(ino, rel.to_path_buf(), st));
        map.insert(
            ino,
            Entry {
                inode: inode.clone(),
                lookups: 1,
                pins,
            },
        );
        inode
    }

    /// Register a dirty file found in the write cache at startup. Returns false if `ino` is
    /// already known (another write-cache file maps to the same backing inode).
    pub fn insert_recovered(&self, ino: u64, rel: &Path, write_len: u64) -> bool {
        let mut map = self.map.write();
        if map.contains_key(&ino) {
            return false;
        }
        let st = FileState {
            dirty: true,
            dirty_gen: 1,
            write_bytes: write_len,
            ..FileState::default()
        };
        map.insert(
            ino,
            Entry {
                inode: Arc::new(Inode::new(ino, rel.to_path_buf(), st)),
                lookups: 0,
                pins: 1,
            },
        );
        true
    }

    pub fn forget(&self, ino: u64, n: u64) {
        let mut map = self.map.write();
        if let Some(e) = map.get_mut(&ino) {
            e.lookups = e.lookups.saturating_sub(n);
            if e.idle() {
                map.remove(&ino);
            }
        }
    }

    pub fn pin(&self, ino: u64) {
        if let Some(e) = self.map.write().get_mut(&ino) {
            e.pins += 1;
        }
    }

    pub fn unpin(&self, ino: u64) {
        let mut map = self.map.write();
        if let Some(e) = map.get_mut(&ino) {
            e.pins = e.pins.saturating_sub(1);
            if e.idle() {
                map.remove(&ino);
            }
        }
    }

    pub fn rename_prefix(&self, from: &Path, to: &Path) {
        for e in self.map.read().values() {
            let mut p = e.inode.path.write();
            if let Some(np) = rebase(&p, from, to) {
                *p = np;
            }
        }
    }

    pub fn exchange_prefix(&self, a: &Path, b: &Path) {
        for e in self.map.read().values() {
            let mut p = e.inode.path.write();
            if let Some(np) = rebase(&p, a, b).or_else(|| rebase(&p, b, a)) {
                *p = np;
            }
        }
    }
}

/// `path` with its leading `from` components replaced by `to`; `None` if `path` is not
/// `from` or below it (whole components only).
pub fn rebase(path: &Path, from: &Path, to: &Path) -> Option<PathBuf> {
    let rest = path.strip_prefix(from).ok()?;
    Some(if rest.as_os_str().is_empty() {
        to.to_path_buf()
    } else {
        to.join(rest)
    })
}

/// Make `st` dirty (write copy authoritative). Pins the inode on the clean → dirty transition.
pub fn mark_dirty(s: &Shared, inode: &Inode, st: &mut FileState) {
    if !st.dirty {
        st.dirty = true;
        s.table.pin(inode.ino);
    }
    st.bump_dirty();
    st.content_gen += 1;
}

/// Drop the write copy after its content reached the backing store (or the file died).
pub fn finalize_clean(s: &Shared, inode: &Inode, st: &mut FileState) {
    if !st.dirty {
        return;
    }
    let wp = s.roots.write_path(&inode.path());
    if let Err(e) = remove_quiet(&wp) {
        warn!("removing write copy {}: {e}", wp.display());
    }
    s.sub_write_used(st.write_bytes);
    st.write_bytes = 0;
    st.dirty = false;
    st.flushed_gen = None;
    s.table.unpin(inode.ino);
}

/// Lock `ns` (read) and the inode state, waiting out an in-progress copy-up.
pub fn lock_idle<'a>(
    s: &'a Shared,
    inode: &'a Inode,
) -> (RwLockReadGuard<'a, ()>, MutexGuard<'a, FileState>) {
    loop {
        let ns = s.ns.read();
        let mut st = inode.state.lock();
        if !st.busy {
            return (ns, st);
        }
        drop(ns);
        inode.cv.wait(&mut st);
        drop(st);
    }
}

impl Shared {
    pub fn sub_write_used(&self, n: u64) {
        let mut cur = self.write_used.load(Ordering::Relaxed);
        while let Err(v) = self.write_used.compare_exchange_weak(
            cur,
            cur.saturating_sub(n),
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            cur = v;
        }
    }

    /// Apply a change of a write copy's logical length to the write-cache usage.
    pub fn resize_write_used(&self, st: &mut FileState, new_len: u64) {
        if new_len >= st.write_bytes {
            self.write_used
                .fetch_add(new_len - st.write_bytes, Ordering::Relaxed);
        } else {
            self.sub_write_used(st.write_bytes - new_len);
        }
        st.write_bytes = new_len;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rebase_matches_whole_components() {
        let p = Path::new;
        assert_eq!(
            rebase(p("a/b/c"), p("a/b"), p("x")),
            Some(PathBuf::from("x/c"))
        );
        assert_eq!(rebase(p("a/bc"), p("a/b"), p("x")), None);
        assert_eq!(rebase(p("a/b"), p("a/b"), p("x")), Some(PathBuf::from("x")));
    }
}
