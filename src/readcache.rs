//! Read-cache LRU index and background promotion workers.

use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File, Metadata, OpenOptions};
use std::io;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError};
use log::{debug, error, warn};
use parking_lot::Mutex;

use crate::fs::Shared;
use crate::inode::{FileState, rebase};
use crate::layers::{self, STAMP_XATTR, Stamp, ensure_parent, open_nofollow, remove_quiet};
use crate::sys;

#[derive(Default)]
struct IndexInner {
    entries: HashMap<PathBuf, (u64, u64)>,
    lru: BTreeSet<(u64, PathBuf)>,
    used: u64,
    next_tick: u64,
}

impl IndexInner {
    fn insert(&mut self, rel: PathBuf, bytes: u64) {
        self.remove(&rel);
        let tick = self.next_tick;
        self.next_tick += 1;
        self.lru.insert((tick, rel.clone()));
        self.entries.insert(rel, (tick, bytes));
        self.used += bytes;
    }

    fn remove(&mut self, rel: &Path) -> bool {
        match self.entries.remove(rel) {
            Some((tick, bytes)) => {
                self.lru.remove(&(tick, rel.to_path_buf()));
                self.used -= bytes;
                true
            }
            None => false,
        }
    }

    fn pop_oldest(&mut self) -> Option<PathBuf> {
        let (_, rel) = self.lru.pop_first()?;
        if let Some((_, bytes)) = self.entries.remove(&rel) {
            self.used -= bytes;
        }
        Some(rel)
    }

    fn remap(&mut self, f: impl Fn(&Path) -> Option<PathBuf>) {
        let moved: Vec<(PathBuf, PathBuf)> = self
            .entries
            .keys()
            .filter_map(|k| f(k).map(|n| (k.clone(), n)))
            .collect();
        let mut readd = Vec::with_capacity(moved.len());
        for (old, new) in moved {
            let (tick, bytes) = self.entries.remove(&old).expect("key collected above");
            self.lru.remove(&(tick, old));
            readd.push((new, tick, bytes));
        }
        for (new, tick, bytes) in readd {
            if let Some((t, old_bytes)) = self.entries.insert(new.clone(), (tick, bytes)) {
                // Destination already indexed (should not happen after a rename); keep the moved one.
                self.lru.remove(&(t, new.clone()));
                self.used -= old_bytes;
            }
            self.lru.insert((tick, new));
        }
    }
}

/// LRU index of read-cache copies. Byte counts are logical file lengths.
pub struct ReadIndex {
    pub capacity: u64,
    inner: Mutex<IndexInner>,
}

impl ReadIndex {
    pub fn new(capacity: u64) -> ReadIndex {
        ReadIndex {
            capacity,
            inner: Mutex::new(IndexInner::default()),
        }
    }

    pub fn used(&self) -> u64 {
        self.inner.lock().used
    }

    pub fn insert(&self, rel: PathBuf, bytes: u64) {
        self.inner.lock().insert(rel, bytes);
    }

    pub fn touch(&self, rel: &Path) {
        let mut g = self.inner.lock();
        if let Some(&(_, bytes)) = g.entries.get(rel) {
            g.insert(rel.to_path_buf(), bytes);
        }
    }

    pub fn remove(&self, rel: &Path) -> bool {
        self.inner.lock().remove(rel)
    }

    /// Reserve `bytes`, evicting least-recently-used entries. Returns the evicted paths
    /// (already removed from the index; the caller unlinks them), or `None` if `bytes`
    /// exceeds the capacity.
    pub fn reserve(&self, bytes: u64) -> Option<Vec<PathBuf>> {
        if bytes > self.capacity {
            return None;
        }
        let mut g = self.inner.lock();
        let mut victims = Vec::new();
        while g.used + bytes > self.capacity {
            match g.pop_oldest() {
                Some(v) => victims.push(v),
                None => break,
            }
        }
        g.used += bytes;
        Some(victims)
    }

    pub fn commit(&self, rel: PathBuf, reserved: u64, actual: u64) {
        let mut g = self.inner.lock();
        g.used -= reserved;
        g.insert(rel, actual);
    }

    pub fn unreserve(&self, bytes: u64) {
        self.inner.lock().used -= bytes;
    }

    pub fn rename_prefix(&self, from: &Path, to: &Path) {
        self.inner.lock().remap(|p| rebase(p, from, to));
    }

    pub fn exchange_prefix(&self, a: &Path, b: &Path) {
        self.inner
            .lock()
            .remap(|p| rebase(p, a, b).or_else(|| rebase(p, b, a)));
    }

    /// Build the index from `read_data`, dropping unstamped files and evicting down to capacity.
    pub fn load(capacity: u64, read_data: &Path) -> io::Result<ReadIndex> {
        let mut found: Vec<(i64, i64, PathBuf, u64)> = Vec::new();
        let mut stack = vec![PathBuf::new()];
        while let Some(dir) = stack.pop() {
            for e in fs::read_dir(read_data.join(&dir))? {
                let e = e?;
                let rel = dir.join(e.file_name());
                let ft = e.file_type()?;
                if ft.is_dir() {
                    stack.push(rel);
                    continue;
                }
                let full = read_data.join(&rel);
                let stamped = ft.is_file()
                    && open_nofollow(&full, false)
                        .and_then(|f| sys::fgetxattr(&f, STAMP_XATTR))
                        .ok()
                        .and_then(|v| Stamp::decode(&v))
                        .is_some();
                if !stamped {
                    debug!("dropping unstamped read-cache file {}", full.display());
                    remove_quiet(&full)?;
                    continue;
                }
                let m = e.metadata()?;
                found.push((m.atime(), m.atime_nsec(), rel, m.len()));
            }
        }
        found.sort();
        let idx = ReadIndex::new(capacity);
        {
            let mut g = idx.inner.lock();
            for (_, _, rel, len) in found {
                g.insert(rel, len);
            }
            while g.used > capacity {
                let Some(v) = g.pop_oldest() else { break };
                remove_quiet(&read_data.join(&v))?;
            }
        }
        Ok(idx)
    }
}

/// Open the read-cache copy of `rel` if it matches the backing file; drop it otherwise.
pub fn valid_read_copy(s: &Shared, rel: &Path, backing: &Metadata) -> Option<File> {
    let rp = s.roots.read_path(rel);
    let f = match OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&rp)
    {
        Ok(f) => f,
        Err(e) => {
            if e.kind() == io::ErrorKind::NotFound {
                s.read_index.remove(rel);
            } else {
                warn!("opening read copy {}: {e}", rp.display());
            }
            return None;
        }
    };
    let ok = sys::fgetxattr(&f, STAMP_XATTR)
        .ok()
        .and_then(|v| Stamp::decode(&v))
        == Some(Stamp::of(backing))
        && f.metadata().is_ok_and(|m| m.len() == backing.len());
    if !ok {
        debug!("stale read copy {}", rp.display());
        drop(f);
        drop_read_copy(s, rel);
        return None;
    }
    if let Err(e) = sys::futimens(&f, sys::Timespec::NOW, sys::Timespec::OMIT) {
        debug!("touching read copy {}: {e}", rp.display());
    }
    s.read_index.touch(rel);
    Some(f)
}

/// Remove the read-cache copy of `rel` and its index entry.
pub fn drop_read_copy(s: &Shared, rel: &Path) {
    let rp = s.roots.read_path(rel);
    if let Err(e) = remove_quiet(&rp) {
        warn!("removing read copy {}: {e}", rp.display());
    }
    s.read_index.remove(rel);
}

pub fn enqueue_promotion(s: &Shared, st: &mut FileState, ino: u64, meta: &Metadata) {
    if st.promote_queued
        || meta.len() == 0
        || meta.len() > s.settings.read_max_file
        || meta.nlink() != 1
    {
        return;
    }
    st.promote_queued = true;
    if s.promote_tx.try_send(ino).is_err() {
        st.promote_queued = false;
        debug!("promotion queue full, skipping ino={ino}");
    }
}

pub fn spawn_promoters(s: &Arc<Shared>, rx: Receiver<u64>) -> io::Result<Vec<JoinHandle<()>>> {
    (0..s.settings.promote_workers)
        .map(|i| {
            let (s, rx) = (s.clone(), rx.clone());
            std::thread::Builder::new()
                .name(format!("promote-{i}"))
                .spawn(move || promoter(&s, &rx))
        })
        .collect()
}

fn promoter(s: &Shared, rx: &Receiver<u64>) {
    loop {
        if s.shutdown.load(Ordering::Relaxed) {
            return;
        }
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(ino) => promote(s, ino),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
    }
}

fn promote(s: &Shared, ino: u64) {
    let Some(inode) = s.table.get(ino) else {
        return;
    };
    // Phase 1: snapshot.
    let (gen_, rel) = {
        let _ns = s.ns.read();
        let mut st = inode.state.lock();
        st.promote_queued = false;
        if st.deleted || st.dirty || st.busy || st.writers() > 0 {
            return;
        }
        (st.content_gen, inode.path())
    };

    // Phase 2: copy without locks.
    let src = match open_nofollow(&s.roots.backing_path(&rel), false) {
        Ok(f) => f,
        Err(e) => {
            debug!("promotion of {}: {e}", rel.display());
            return;
        }
    };
    let Ok(meta) = src.metadata() else { return };
    if !meta.is_file() || meta.nlink() != 1 || meta.len() > s.settings.read_max_file {
        return;
    }
    let len = meta.len();
    s.read_index.remove(&rel);
    let Some(victims) = s.read_index.reserve(len) else {
        return;
    };
    if !victims.is_empty() {
        let _ns = s.ns.read();
        for v in &victims {
            debug!("evicting read copy {}", v.display());
            if let Err(e) = remove_quiet(&s.roots.read_path(v)) {
                warn!("evicting {}: {e}", v.display());
            }
        }
    }
    let tmp = s.roots.read_tmp.join(layers::unique_tmp_name());
    let abort = |why: &dyn std::fmt::Display| {
        let _ = remove_quiet(&tmp);
        s.read_index.unreserve(len);
        debug!("promotion of {} aborted: {why}", rel.display());
    };
    let copied = (|| -> io::Result<()> {
        let dst = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)?;
        layers::copy_contents(&src, &dst, &s.shutdown)?;
        dst.sync_all()?;
        sys::fsetxattr(&dst, STAMP_XATTR, Stamp::of(&meta).encode().as_bytes())?;
        Ok(())
    })();
    if let Err(e) = copied {
        if e.kind() != io::ErrorKind::Interrupted {
            error!("promoting {}: {e}", rel.display());
        }
        return abort(&e);
    }
    match src.metadata() {
        Ok(m) if Stamp::of(&m) == Stamp::of(&meta) => {}
        _ => return abort(&"backing file changed during copy"),
    }

    // Phase 3: install unless the file changed meanwhile.
    let _ns = s.ns.read();
    let st = inode.state.lock();
    let same_inode = s.table.get(ino).is_some_and(|i| Arc::ptr_eq(&i, &inode));
    if !same_inode || st.deleted || st.dirty || st.content_gen != gen_ || st.writers() > 0 {
        return abort(&"file changed");
    }
    let cur = inode.path();
    let rp = s.roots.read_path(&cur);
    if let Err(e) = ensure_parent(&rp).and_then(|()| fs::rename(&tmp, &rp)) {
        error!("installing read copy {}: {e}", rp.display());
        return abort(&e);
    }
    s.read_index.commit(cur, len, len);
    debug!("promoted ino={ino} ({len} bytes)");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_evicts_least_recently_used() {
        let idx = ReadIndex::new(10);
        idx.insert("A".into(), 4);
        idx.insert("B".into(), 4);
        idx.touch(Path::new("A"));
        assert_eq!(idx.reserve(4), Some(vec![PathBuf::from("B")]));
        assert_eq!(idx.used(), 8);
        assert_eq!(idx.reserve(11), None);
    }
}
