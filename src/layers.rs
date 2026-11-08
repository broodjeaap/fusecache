//! Cache directory layout, attribute conversion and copy helpers.

use std::fs::{self, File, Metadata, OpenOptions};
use std::io;
use std::os::unix::fs::{DirBuilderExt, FileExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{Errno, FileAttr, FileType, INodeNo};
use log::warn;

use crate::config::Settings;
use crate::sys;

pub const STAMP_XATTR: &str = "user.fusecache.stamp";
const PROBE_XATTR: &str = "user.fusecache.probe";
const COPY_CHUNK: usize = 1 << 20;

#[derive(Debug, Clone)]
pub struct Roots {
    pub backing: PathBuf,
    pub read_data: PathBuf,
    pub read_tmp: PathBuf,
    pub write_data: PathBuf,
    pub write_tmp: PathBuf,
    pub write_orphans: PathBuf,
}

pub fn mkdir_private(p: &Path) -> io::Result<()> {
    fs::DirBuilder::new().recursive(true).mode(0o700).create(p)
}

/// `create_dir_all` (mode 0700) for the parent of a cache path.
pub fn ensure_parent(p: &Path) -> io::Result<()> {
    match p.parent() {
        Some(parent) => mkdir_private(parent),
        None => Ok(()),
    }
}

/// Remove a file, treating "already gone" as success.
pub fn remove_quiet(p: &Path) -> io::Result<()> {
    match fs::remove_file(p) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        r => r,
    }
}

impl Roots {
    pub fn new(s: &Settings) -> Roots {
        Roots {
            backing: s.backing.clone(),
            read_data: s.read_cache.join("data"),
            read_tmp: s.read_cache.join("tmp"),
            write_data: s.write_cache.join("data"),
            write_tmp: s.write_cache.join("tmp"),
            write_orphans: s.write_cache.join("orphans"),
        }
    }

    pub fn backing_path(&self, rel: &Path) -> PathBuf {
        self.backing.join(rel)
    }
    pub fn read_path(&self, rel: &Path) -> PathBuf {
        self.read_data.join(rel)
    }
    pub fn write_path(&self, rel: &Path) -> PathBuf {
        self.write_data.join(rel)
    }

    pub fn prepare(&self) -> io::Result<()> {
        for d in [
            &self.read_data,
            &self.read_tmp,
            &self.write_data,
            &self.write_tmp,
            &self.write_orphans,
        ] {
            mkdir_private(d)?;
        }
        for tmp in [&self.read_tmp, &self.write_tmp] {
            for e in fs::read_dir(tmp)? {
                let e = e?;
                let p = e.path();
                if e.file_type()?.is_dir() {
                    fs::remove_dir_all(&p)?;
                } else {
                    fs::remove_file(&p)?;
                }
            }
        }
        let probe = self.read_tmp.join("xattr-probe");
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&probe)?;
        let r = sys::fsetxattr(&f, PROBE_XATTR, b"1");
        drop(f);
        fs::remove_file(&probe)?;
        r.map_err(|e| {
            io::Error::other(format!(
                "read cache filesystem must support user xattrs: {e}"
            ))
        })
    }
}

/// Map a backing `st_ino` to a FUSE inode number.
pub fn fuse_ino(s: &Settings, st_ino: u64) -> Result<u64, Errno> {
    if st_ino == s.backing_root_ino {
        Ok(1)
    } else if st_ino == 1 {
        warn!("backing inode 1 collides with the FUSE root inode");
        Err(Errno::EIO)
    } else {
        Ok(st_ino)
    }
}

fn to_time(sec: i64, nsec: i64) -> SystemTime {
    let nsec = nsec.clamp(0, 999_999_999) as u32;
    if sec >= 0 {
        UNIX_EPOCH + Duration::new(sec as u64, nsec)
    } else {
        UNIX_EPOCH - Duration::from_secs(sec.unsigned_abs()) + Duration::from_nanos(u64::from(nsec))
    }
}

fn clamp_u32(v: u64) -> u32 {
    u32::try_from(v).unwrap_or(u32::MAX)
}

pub fn attr_from_meta(m: &Metadata, ino: u64) -> FileAttr {
    FileAttr {
        ino: INodeNo(ino),
        size: m.size(),
        blocks: m.blocks(),
        atime: to_time(m.atime(), m.atime_nsec()),
        mtime: to_time(m.mtime(), m.mtime_nsec()),
        ctime: to_time(m.ctime(), m.ctime_nsec()),
        crtime: UNIX_EPOCH,
        kind: FileType::from_std(m.file_type()).unwrap_or(FileType::RegularFile),
        perm: (m.mode() & 0o7777) as u16,
        nlink: clamp_u32(m.nlink()),
        uid: m.uid(),
        gid: m.gid(),
        rdev: u32::try_from(m.rdev()).unwrap_or(0),
        blksize: clamp_u32(m.blksize()),
        flags: 0,
    }
}

/// Backing metadata with size/blocks/atime/mtime taken from the write-cache copy when dirty.
pub fn merged_attr(backing: &Metadata, write_copy: Option<&Metadata>, ino: u64) -> FileAttr {
    let mut a = attr_from_meta(backing, ino);
    if let Some(w) = write_copy {
        let wa = attr_from_meta(w, ino);
        a.size = wa.size;
        a.blocks = wa.blocks;
        a.atime = wa.atime;
        a.mtime = wa.mtime;
        a.ctime = a.ctime.max(wa.ctime);
    }
    a
}

/// Identity of a backing file's content, stored on read-cache copies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamp {
    pub ino: u64,
    pub size: u64,
    pub mtime_sec: i64,
    pub mtime_nsec: i64,
}

impl Stamp {
    pub fn of(m: &Metadata) -> Stamp {
        Stamp {
            ino: m.ino(),
            size: m.size(),
            mtime_sec: m.mtime(),
            mtime_nsec: m.mtime_nsec(),
        }
    }

    pub fn encode(&self) -> String {
        format!(
            "v1 {} {} {} {}",
            self.ino, self.size, self.mtime_sec, self.mtime_nsec
        )
    }

    pub fn decode(b: &[u8]) -> Option<Stamp> {
        let s = std::str::from_utf8(b).ok()?;
        let mut it = s.split(' ');
        if it.next()? != "v1" {
            return None;
        }
        let st = Stamp {
            ino: it.next()?.parse().ok()?,
            size: it.next()?.parse().ok()?,
            mtime_sec: it.next()?.parse().ok()?,
            mtime_nsec: it.next()?.parse().ok()?,
        };
        it.next().is_none().then_some(st)
    }
}

/// Copy `src` into `dst` from offset 0, then truncate `dst` to the copied length.
pub fn copy_contents(src: &File, dst: &File, shutdown: &AtomicBool) -> io::Result<u64> {
    let mut buf = vec![0u8; COPY_CHUNK];
    let mut off = 0u64;
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        let n = match src.read_at(&mut buf, off) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        dst.write_all_at(&buf[..n], off)?;
        off += n as u64;
    }
    dst.set_len(off)?;
    Ok(off)
}

pub fn open_nofollow(path: &Path, rw: bool) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(rw)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}

pub fn unique_tmp_name() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// atime and mtime of a file as `futimens` arguments.
pub fn times_of(m: &Metadata) -> (sys::Timespec, sys::Timespec) {
    (
        sys::Timespec {
            sec: m.atime(),
            nsec: m.atime_nsec(),
        },
        sys::Timespec {
            sec: m.mtime(),
            nsec: m.mtime_nsec(),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamp_roundtrip_and_rejects_garbage() {
        let s = Stamp {
            ino: 42,
            size: 7,
            mtime_sec: -3,
            mtime_nsec: 999,
        };
        assert_eq!(Stamp::decode(s.encode().as_bytes()), Some(s));
        assert_eq!(Stamp::decode(b"v1 1 2 3"), None);
        assert_eq!(Stamp::decode(b"v2 1 2 3 4"), None);
        assert_eq!(Stamp::decode(b"v1 1 2 3 4 5"), None);
    }
}
