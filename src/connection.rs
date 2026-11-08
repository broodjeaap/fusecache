//! Locating and aborting this mount's FUSE connection through fusectl
//! (`/sys/fs/fuse/connections`).
//!
//! Unmounting only detaches the mount from our mount namespace. A copy held by another
//! namespace (e.g. a systemd service with a private mount namespace started after mounting)
//! keeps the connection alive, and the session threads then never see it end.

use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

/// fusectl id of the fusecache connection mounted at `mountpoint`.
pub fn connection_id(mountpoint: &Path) -> Option<u64> {
    find_connection(
        &fs::read_to_string("/proc/self/mountinfo").ok()?,
        mountpoint,
    )
}

fn find_connection(mountinfo: &str, mountpoint: &Path) -> Option<u64> {
    let want = escape(mountpoint);
    // Later entries are mounted on top of earlier ones: search from the end.
    mountinfo.lines().rev().find_map(|line| {
        let f: Vec<&str> = line.split(' ').collect();
        let sep = f.iter().position(|x| *x == "-")?;
        let (fstype, source) = (*f.get(sep + 1)?, *f.get(sep + 2)?);
        let fuse = fstype == "fuse" || fstype.starts_with("fuse.");
        if *f.get(4)? != want || !fuse || !source.starts_with("fusecache:") {
            return None;
        }
        let (maj, min) = f.get(2)?.split_once(':')?;
        // fusectl names connections by the kernel's dev_t encoding.
        Some((maj.parse::<u64>().ok()? << 20) | min.parse::<u64>().ok()?)
    })
}

/// Mount point as written in mountinfo: space, tab, newline and backslash are octal-escaped.
fn escape(p: &Path) -> String {
    let mut out = String::new();
    for &b in p.as_os_str().as_bytes() {
        match b {
            b' ' | b'\t' | b'\n' | b'\\' => out.push_str(&format!("\\{b:03o}")),
            _ => out.push(b as char),
        }
    }
    out
}

/// Abort the connection: pending and future requests fail and the session threads exit.
pub fn abort(id: u64) -> io::Result<()> {
    fs::write(format!("/sys/fs/fuse/connections/{id}/abort"), "1")
}

#[cfg(test)]
mod tests {
    use super::*;

    const INFO: &str = "\
24 1 8:32 / / rw,relatime shared:1 - ext4 /dev/sdc rw
90 24 0:41 / /tmp rw shared:2 - tmpfs tmpfs rw
101 90 0:92 / /tmp/a\\040b rw,nosuid,nodev - fuse.fusecache fusecache:/tmp/b rw,user_id=1000
102 90 0:93 / /tmp/other rw - fuse.sshfs host:/ rw
103 101 0:94 / /tmp/a\\040b rw - fuse fusecache:/tmp/b2 rw
";

    #[test]
    fn finds_topmost_fusecache_mount_with_escaped_path() {
        assert_eq!(find_connection(INFO, Path::new("/tmp/a b")), Some(94));
        assert_eq!(find_connection(INFO, Path::new("/tmp/other")), None);
        assert_eq!(find_connection(INFO, Path::new("/tmp/a")), None);
    }
}
