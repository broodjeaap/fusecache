//! End-to-end tests: mount the real binary (passthrough off) and check where data lands.

use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

struct Mounted {
    _dir: tempfile::TempDir,
    root: PathBuf,
    child: Option<Child>,
}

fn fuse_available() -> Result<(), String> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/fuse")
        .map_err(|e| format!("/dev/fuse: {e}"))?;
    let found = std::env::var_os("PATH")
        .is_some_and(|p| std::env::split_paths(&p).any(|d| d.join("fusermount3").is_file()));
    if found {
        Ok(())
    } else {
        Err("fusermount3 not on PATH".into())
    }
}

fn is_mounted(mnt: &Path) -> bool {
    let info = fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    let mnt = mnt.to_str().unwrap();
    info.lines().any(|l| {
        let f: Vec<&str> = l.split(' ').collect();
        let sep = f.iter().position(|x| *x == "-");
        f.get(4) == Some(&mnt) && sep.and_then(|i| f.get(i + 1)) == Some(&"fuse.fusecache")
    })
}

fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if Instant::now() >= end {
            return false;
        }
        sleep(Duration::from_millis(50));
    }
}

impl Mounted {
    fn start(extra: &[&str]) -> Option<Mounted> {
        Self::start_with(|_| {}, extra)
    }

    /// Prepare dirs, let `setup` populate the backing dir, then mount.
    fn start_with(setup: impl FnOnce(&Path), extra: &[&str]) -> Option<Mounted> {
        if let Err(why) = fuse_available() {
            eprintln!("skipping: {why}");
            return None;
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        for d in ["backing", "rcache", "wcache", "mnt"] {
            fs::create_dir(root.join(d)).unwrap();
        }
        setup(&root.join("backing"));
        let mut m = Mounted {
            _dir: dir,
            root,
            child: None,
        };
        m.spawn(extra);
        Some(m)
    }

    fn spawn(&mut self, extra: &[&str]) {
        let r = &self.root;
        let child = Command::new(env!("CARGO_BIN_EXE_fusecache"))
            .args(["--backing", r.join("backing").to_str().unwrap()])
            .args(["--read-cache", r.join("rcache").to_str().unwrap()])
            .args(["--write-cache", r.join("wcache").to_str().unwrap()])
            .args(["--passthrough", "off", "--attr-ttl", "0s", "--threads", "4"])
            .args(if extra.contains(&"--flush-delay") {
                &[][..]
            } else {
                &["--flush-delay", "300ms"][..]
            })
            .args(extra)
            .arg(r.join("mnt"))
            .env("RUST_LOG", "fusecache=debug")
            .stdin(Stdio::null())
            .spawn()
            .unwrap();
        self.child = Some(child);
        let mnt = self.p("mnt");
        assert!(
            wait_until(Duration::from_secs(5), || is_mounted(&mnt)),
            "mount did not appear"
        );
    }

    fn p(&self, rel: &str) -> PathBuf {
        self.root.join(rel)
    }

    /// SIGKILL the daemon and clear the dead mount.
    fn crash(&mut self) {
        let mut c = self.child.take().unwrap();
        c.kill().unwrap();
        c.wait().unwrap();
        let _ = Command::new("fusermount3")
            .arg("-u")
            .arg("-z")
            .arg(self.p("mnt"))
            .status();
        let mnt = self.p("mnt");
        assert!(wait_until(Duration::from_secs(5), || !is_mounted(&mnt)));
    }
}

impl Drop for Mounted {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            // SAFETY: plain kill(2) on our own child.
            unsafe { libc::kill(c.id() as i32, libc::SIGTERM) };
            let exited = wait_until(Duration::from_secs(10), || c.try_wait().unwrap().is_some());
            if !exited {
                let _ = c.kill();
                let _ = c.wait();
            }
        }
        let _ = Command::new("fusermount3")
            .arg("-u")
            .arg("-z")
            .arg(self.p("mnt"))
            .stderr(Stdio::null())
            .status();
    }
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

fn count_files(dir: &Path) -> usize {
    let mut n = 0;
    for e in fs::read_dir(dir).unwrap() {
        let e = e.unwrap();
        if e.file_type().unwrap().is_dir() {
            n += count_files(&e.path());
        } else {
            n += 1;
        }
    }
    n
}

fn read_xattr(path: &Path, name: &str) -> Option<Vec<u8>> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let p = CString::new(path.as_os_str().as_bytes()).unwrap();
    let n = CString::new(name).unwrap();
    let mut buf = vec![0u8; 256];
    // SAFETY: valid strings and buffer.
    let r = unsafe { libc::getxattr(p.as_ptr(), n.as_ptr(), buf.as_mut_ptr().cast(), buf.len()) };
    if r < 0 {
        return None;
    }
    buf.truncate(r as usize);
    Some(buf)
}

const FLUSH_WAIT: Duration = Duration::from_secs(5);

#[test]
fn new_file_lands_in_write_cache_then_flushes() {
    let Some(m) = Mounted::start(&["--flush-delay", "1s"]) else {
        return;
    };
    let data = pattern(100 * 1024, 1);
    fs::create_dir(m.p("mnt/d")).unwrap();
    fs::write(m.p("mnt/d/f"), &data).unwrap();
    assert_eq!(fs::read(m.p("wcache/data/d/f")).unwrap(), data);
    assert_eq!(fs::metadata(m.p("backing/d/f")).unwrap().len(), 0);
    assert!(wait_until(FLUSH_WAIT, || fs::read(m.p("backing/d/f"))
        .unwrap()
        == data));
    assert!(wait_until(FLUSH_WAIT, || !m.p("wcache/data/d/f").exists()));
    assert_eq!(fs::read(m.p("mnt/d/f")).unwrap(), data);
}

#[test]
fn read_promotes_into_read_cache_and_detects_stale_copy() {
    let data = pattern(64 * 1024, 2);
    let Some(m) = Mounted::start_with(|b| fs::write(b.join("r"), &data).unwrap(), &[]) else {
        return;
    };
    assert_eq!(fs::read(m.p("mnt/r")).unwrap(), data);
    let cached = m.p("rcache/data/r");
    assert!(wait_until(FLUSH_WAIT, || fs::read(&cached).is_ok_and(|c| c == data)));
    assert!(
        read_xattr(&cached, "user.fusecache.stamp")
            .unwrap()
            .starts_with(b"v1 ")
    );
    // Served from the read cache now.
    assert_eq!(fs::read(m.p("mnt/r")).unwrap(), data);

    let fresh = pattern(10_000, 9);
    fs::write(m.p("backing/r"), &fresh).unwrap();
    assert_eq!(fs::read(m.p("mnt/r")).unwrap(), fresh);
}

#[test]
fn append_to_existing_file_copies_up() {
    let Some(m) = Mounted::start_with(
        |b| fs::write(b.join("a"), "hello").unwrap(),
        &["--flush-delay", "1s"],
    ) else {
        return;
    };
    let mut f = OpenOptions::new().append(true).open(m.p("mnt/a")).unwrap();
    f.write_all(b" world").unwrap();
    drop(f);
    assert_eq!(fs::read(m.p("wcache/data/a")).unwrap(), b"hello world");
    assert_eq!(fs::read(m.p("backing/a")).unwrap(), b"hello");
    assert!(wait_until(FLUSH_WAIT, || fs::read(m.p("backing/a"))
        .unwrap()
        == b"hello world"));
    assert!(wait_until(FLUSH_WAIT, || count_files(&m.p("wcache/data")) == 0));
}

#[test]
fn modify_above_copy_up_max_writes_backing_directly() {
    let data = pattern(4096, 3);
    let Some(m) = Mounted::start_with(
        |b| fs::write(b.join("big"), &data).unwrap(),
        &["--copy-up-max", "1KiB"],
    ) else {
        return;
    };
    let f = OpenOptions::new().write(true).open(m.p("mnt/big")).unwrap();
    f.write_all_at(b"ABCD", 0).unwrap();
    drop(f);
    let mut want = data.clone();
    want[..4].copy_from_slice(b"ABCD");
    assert_eq!(fs::read(m.p("backing/big")).unwrap(), want);
    assert!(!m.p("wcache/data/big").exists());
    assert_eq!(fs::read(m.p("mnt/big")).unwrap(), want);
}

#[test]
fn rename_dirty_file_flushes_to_new_path() {
    let Some(m) = Mounted::start(&["--flush-delay", "2s"]) else {
        return;
    };
    let data = pattern(20_000, 4);
    fs::write(m.p("mnt/x"), &data).unwrap();
    fs::create_dir(m.p("mnt/y")).unwrap();
    fs::rename(m.p("mnt/x"), m.p("mnt/y/x2")).unwrap();
    assert_eq!(fs::read(m.p("wcache/data/y/x2")).unwrap(), data);
    assert!(wait_until(FLUSH_WAIT, || fs::read(m.p("backing/y/x2"))
        .unwrap()
        == data));
    assert!(!m.p("backing/x").exists());
    assert!(wait_until(FLUSH_WAIT, || count_files(&m.p("wcache/data")) == 0));
}

#[test]
fn unlink_dirty_file_is_not_resurrected() {
    let Some(m) = Mounted::start(&["--flush-delay", "1s"]) else {
        return;
    };
    fs::write(m.p("mnt/u"), pattern(5000, 5)).unwrap();
    fs::remove_file(m.p("mnt/u")).unwrap();
    sleep(Duration::from_secs(2));
    assert!(!m.p("backing/u").exists());
    assert!(!m.p("wcache/data/u").exists());
}

#[test]
fn restart_flushes_leftover_dirty_files() {
    let Some(mut m) = Mounted::start(&["--flush-delay", "1h"]) else {
        return;
    };
    let data = pattern(30_000, 6);
    fs::write(m.p("mnt/p"), &data).unwrap();
    m.crash();
    assert_eq!(fs::metadata(m.p("backing/p")).unwrap().len(), 0);
    m.spawn(&["--flush-delay", "300ms"]);
    assert!(wait_until(FLUSH_WAIT, || fs::read(m.p("backing/p"))
        .unwrap()
        == data));
    assert!(wait_until(FLUSH_WAIT, || count_files(&m.p("wcache/data")) == 0));
    assert_eq!(fs::read(m.p("mnt/p")).unwrap(), data);
}

#[test]
fn read_cache_evicts_least_recently_used() {
    let setup = |b: &Path| {
        for (i, n) in ["a", "b", "c"].iter().enumerate() {
            fs::write(b.join(n), pattern(5 * 1024, i as u8)).unwrap();
        }
    };
    let Some(m) = Mounted::start_with(
        setup,
        &["--read-cache-size", "12KiB", "--read-max-file", "12KiB"],
    ) else {
        return;
    };
    for n in ["a", "b", "c"] {
        let mut buf = Vec::new();
        fs::File::open(m.p(&format!("mnt/{n}")))
            .unwrap()
            .read_to_end(&mut buf)
            .unwrap();
        let cached = m.p(&format!("rcache/data/{n}"));
        assert!(
            wait_until(FLUSH_WAIT, || cached.exists()),
            "{n} was not promoted"
        );
    }
    assert!(m.p("rcache/data/b").exists());
    assert!(m.p("rcache/data/c").exists());
    assert!(!m.p("rcache/data/a").exists());
}

#[test]
fn concurrent_writers_distinct_files() {
    let Some(m) = Mounted::start(&[]) else { return };
    let mnt = m.p("mnt");
    std::thread::scope(|sc| {
        for t in 0..8u8 {
            let mnt = &mnt;
            sc.spawn(move || {
                for i in 0..20u8 {
                    fs::write(
                        mnt.join(format!("t{t}-{i}")),
                        pattern(64 * 1024, t * 20 + i),
                    )
                    .unwrap();
                }
            });
        }
    });
    let all_flushed = || {
        (0..8u8).all(|t| {
            (0..20u8).all(|i| {
                fs::read(m.p(&format!("backing/t{t}-{i}")))
                    .is_ok_and(|d| d == pattern(64 * 1024, t * 20 + i))
            })
        })
    };
    assert!(wait_until(Duration::from_secs(15), all_flushed));
    assert!(wait_until(FLUSH_WAIT, || count_files(&m.p("wcache/data")) == 0));
}
