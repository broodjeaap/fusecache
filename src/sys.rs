//! Thin `libc` wrappers for syscalls std does not expose.

use std::ffi::{CString, OsStr};
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fuser::TimeOrNow;

fn cstr(b: &[u8]) -> io::Result<CString> {
    CString::new(b).map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))
}

fn cpath(p: &Path) -> io::Result<CString> {
    cstr(p.as_os_str().as_bytes())
}

fn cname(n: &OsStr) -> io::Result<CString> {
    cstr(n.as_bytes())
}

fn check(rc: libc::c_int) -> io::Result<()> {
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn check_len(rc: libc::ssize_t) -> io::Result<usize> {
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(rc as usize)
    }
}

pub fn renameat2(old: &Path, new: &Path, flags: u32) -> io::Result<()> {
    let (o, n) = (cpath(old)?, cpath(new)?);
    // SAFETY: valid NUL-terminated paths.
    check(unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            o.as_ptr(),
            libc::AT_FDCWD,
            n.as_ptr(),
            flags,
        )
    })
}

pub fn mknod(path: &Path, mode: u32, rdev: u64) -> io::Result<()> {
    let p = cpath(path)?;
    // SAFETY: valid NUL-terminated path.
    check(unsafe { libc::mknod(p.as_ptr(), mode as libc::mode_t, rdev as libc::dev_t) })
}

#[derive(Copy, Clone, Debug)]
pub struct Timespec {
    pub sec: i64,
    pub nsec: i64,
}

impl Timespec {
    pub const OMIT: Timespec = Timespec {
        sec: 0,
        nsec: libc::UTIME_OMIT,
    };
    pub const NOW: Timespec = Timespec {
        sec: 0,
        nsec: libc::UTIME_NOW,
    };

    pub fn from_system(t: SystemTime) -> Timespec {
        match t.duration_since(UNIX_EPOCH) {
            Ok(d) => Timespec {
                sec: d.as_secs() as i64,
                nsec: i64::from(d.subsec_nanos()),
            },
            Err(e) => {
                let d = e.duration();
                let mut sec = -(d.as_secs() as i64);
                let mut nsec = i64::from(d.subsec_nanos());
                if nsec > 0 {
                    sec -= 1;
                    nsec = 1_000_000_000 - nsec;
                }
                Timespec { sec, nsec }
            }
        }
    }

    fn from_opt(t: Option<TimeOrNow>) -> Timespec {
        match t {
            None => Timespec::OMIT,
            Some(TimeOrNow::Now) => Timespec::NOW,
            Some(TimeOrNow::SpecificTime(t)) => Timespec::from_system(t),
        }
    }

    fn raw(self) -> libc::timespec {
        libc::timespec {
            tv_sec: self.sec as libc::time_t,
            tv_nsec: self.nsec as _,
        }
    }
}

pub fn utimens_path(
    path: &Path,
    atime: Option<TimeOrNow>,
    mtime: Option<TimeOrNow>,
) -> io::Result<()> {
    let p = cpath(path)?;
    let ts = [
        Timespec::from_opt(atime).raw(),
        Timespec::from_opt(mtime).raw(),
    ];
    // SAFETY: valid path and two-element timespec array.
    check(unsafe {
        libc::utimensat(
            libc::AT_FDCWD,
            p.as_ptr(),
            ts.as_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    })
}

pub fn futimens(file: &File, atime: Timespec, mtime: Timespec) -> io::Result<()> {
    let ts = [atime.raw(), mtime.raw()];
    // SAFETY: valid fd and two-element timespec array.
    check(unsafe { libc::futimens(file.as_raw_fd(), ts.as_ptr()) })
}

/// Size-probe-then-read loop shared by the get/list xattr wrappers.
fn read_sized(
    mut call: impl FnMut(*mut libc::c_void, usize) -> libc::ssize_t,
) -> io::Result<Vec<u8>> {
    loop {
        let n = check_len(call(std::ptr::null_mut(), 0))?;
        if n == 0 {
            return Ok(Vec::new());
        }
        let mut buf = vec![0u8; n];
        match check_len(call(buf.as_mut_ptr().cast(), buf.len())) {
            Ok(m) => {
                buf.truncate(m);
                return Ok(buf);
            }
            // Value grew between the probe and the read; retry.
            Err(e) if e.raw_os_error() == Some(libc::ERANGE) => continue,
            Err(e) => return Err(e),
        }
    }
}

pub fn lgetxattr(path: &Path, name: &OsStr) -> io::Result<Vec<u8>> {
    let (p, n) = (cpath(path)?, cname(name)?);
    // SAFETY: valid strings; buffer pointer/len pairs come from read_sized.
    read_sized(|buf, len| unsafe { libc::lgetxattr(p.as_ptr(), n.as_ptr(), buf, len) })
}

pub fn llistxattr(path: &Path) -> io::Result<Vec<u8>> {
    let p = cpath(path)?;
    // SAFETY: valid path; buffer pointer/len pairs come from read_sized.
    read_sized(|buf, len| unsafe { libc::llistxattr(p.as_ptr(), buf.cast(), len) })
}

pub fn lsetxattr(path: &Path, name: &OsStr, value: &[u8], flags: i32) -> io::Result<()> {
    let (p, n) = (cpath(path)?, cname(name)?);
    // SAFETY: valid strings and value slice.
    check(unsafe {
        libc::lsetxattr(
            p.as_ptr(),
            n.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            flags,
        )
    })
}

pub fn lremovexattr(path: &Path, name: &OsStr) -> io::Result<()> {
    let (p, n) = (cpath(path)?, cname(name)?);
    // SAFETY: valid strings.
    check(unsafe { libc::lremovexattr(p.as_ptr(), n.as_ptr()) })
}

pub fn fgetxattr(file: &File, name: &str) -> io::Result<Vec<u8>> {
    let n = cstr(name.as_bytes())?;
    let fd = file.as_raw_fd();
    // SAFETY: valid fd and name; buffer pointer/len pairs come from read_sized.
    read_sized(|buf, len| unsafe { libc::fgetxattr(fd, n.as_ptr(), buf, len) })
}

pub fn fsetxattr(file: &File, name: &str, value: &[u8]) -> io::Result<()> {
    let n = cstr(name.as_bytes())?;
    // SAFETY: valid fd, name and value slice.
    check(unsafe {
        libc::fsetxattr(
            file.as_raw_fd(),
            n.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            0,
        )
    })
}

pub fn statvfs(path: &Path) -> io::Result<libc::statvfs> {
    let p = cpath(path)?;
    // SAFETY: statvfs is plain old data; zeroed is a valid initial value.
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: valid path and out-pointer.
    check(unsafe { libc::statvfs(p.as_ptr(), &mut st) })?;
    Ok(st)
}

pub fn fallocate(file: &File, mode: i32, offset: u64, len: u64) -> io::Result<()> {
    // SAFETY: valid fd.
    check(unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            mode,
            offset as libc::off_t,
            len as libc::off_t,
        )
    })
}

fn term_sigset() -> libc::sigset_t {
    // SAFETY: sigset_t is initialised by sigemptyset before use.
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGINT);
        libc::sigaddset(&mut set, libc::SIGTERM);
        set
    }
}

/// Block SIGINT/SIGTERM in the calling thread; threads spawned afterwards inherit the mask.
pub fn block_term_signals() -> io::Result<()> {
    let set = term_sigset();
    // SAFETY: valid sigset pointer.
    let rc = unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut()) };
    if rc != 0 {
        Err(io::Error::from_raw_os_error(rc))
    } else {
        Ok(())
    }
}

/// Wait for SIGINT or SIGTERM (which must be blocked) and return the signal number, or
/// `None` once `timeout` (if any) has elapsed.
pub fn wait_term_signal(timeout: Option<Duration>) -> Option<i32> {
    let set = term_sigset();
    let deadline = timeout.map(|t| Instant::now() + t);
    loop {
        let ts = deadline.map(|d| {
            let left = d.saturating_duration_since(Instant::now());
            libc::timespec {
                tv_sec: left.as_secs() as libc::time_t,
                tv_nsec: left.subsec_nanos() as _,
            }
        });
        let ts_ptr = ts
            .as_ref()
            .map_or(std::ptr::null(), |t| t as *const libc::timespec);
        // SAFETY: valid sigset; null siginfo is allowed; timeout is null or points to `ts`.
        let sig = unsafe { libc::sigtimedwait(&set, std::ptr::null_mut(), ts_ptr) };
        if sig > 0 {
            return Some(sig);
        }
        if io::Error::last_os_error().raw_os_error() == Some(libc::EAGAIN) {
            return None;
        }
    }
}
