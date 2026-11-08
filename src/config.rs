use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Parser, ValueEnum};

use crate::sys;

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum PassthroughMode {
    /// Use kernel passthrough when running as root, daemon-side I/O otherwise.
    Auto,
    /// Require kernel passthrough; fail to mount if unavailable.
    On,
    /// Always serve file I/O from the daemon.
    Off,
}

/// FUSE storage tiering: SSD read cache and SSD write-back cache over an HDD backing directory.
#[derive(Parser, Debug)]
#[command(version, about)]
pub struct Cli {
    /// Where to mount the tiered view.
    pub mountpoint: PathBuf,
    /// Backing directory (slow, authoritative storage).
    #[arg(long, value_name = "DIR")]
    pub backing: PathBuf,
    /// Read-cache directory (fast SSD).
    #[arg(long, value_name = "DIR")]
    pub read_cache: PathBuf,
    /// Write-cache directory (fast SSD).
    #[arg(long, value_name = "DIR")]
    pub write_cache: PathBuf,
    /// Read-cache capacity [default: 90% of the read-cache filesystem].
    #[arg(long, value_name = "SIZE", value_parser = parse_size)]
    pub read_cache_size: Option<u64>,
    /// Largest file promoted into the read cache [default: read-cache-size / 4].
    #[arg(long, value_name = "SIZE", value_parser = parse_size)]
    pub read_max_file: Option<u64>,
    /// Write-cache capacity [default: 90% of the write-cache filesystem].
    #[arg(long, value_name = "SIZE", value_parser = parse_size)]
    pub write_cache_size: Option<u64>,
    /// Existing files up to this size are copied to the write cache before modification;
    /// larger ones are modified in place on the backing store.
    #[arg(long, value_name = "SIZE", value_parser = parse_size, default_value = "1GiB")]
    pub copy_up_max: u64,
    /// Delay between the last writer closing a file and its write-back.
    #[arg(long, value_name = "DUR", value_parser = humantime::parse_duration, default_value = "30s")]
    pub flush_delay: Duration,
    /// Kernel attribute and entry cache TTL.
    #[arg(long, value_name = "DUR", value_parser = humantime::parse_duration, default_value = "1s")]
    pub attr_ttl: Duration,
    /// Kernel passthrough mode.
    #[arg(long, value_enum, default_value_t = PassthroughMode::Auto)]
    pub passthrough: PassthroughMode,
    /// FUSE session threads [default: available parallelism].
    #[arg(long, value_name = "N")]
    pub threads: Option<usize>,
    /// Background read-promotion threads.
    #[arg(long, value_name = "N", default_value_t = 2)]
    pub promote_workers: usize,
    /// Allow other users to access the mount.
    #[arg(long)]
    pub allow_other: bool,
    /// Unmount automatically when the daemon exits (requires --allow-other).
    #[arg(long)]
    pub auto_unmount: bool,
}

fn parse_size(s: &str) -> Result<u64, String> {
    s.parse::<bytesize::ByteSize>().map(|b| b.as_u64())
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub mountpoint: PathBuf,
    pub backing: PathBuf,
    pub read_cache: PathBuf,
    pub write_cache: PathBuf,
    pub read_cache_size: u64,
    pub read_max_file: u64,
    pub write_cache_size: u64,
    pub copy_up_max: u64,
    pub flush_delay: Duration,
    pub attr_ttl: Duration,
    pub passthrough: PassthroughMode,
    pub threads: usize,
    pub promote_workers: usize,
    pub allow_other: bool,
    pub auto_unmount: bool,
    pub backing_dev: u64,
    pub backing_root_ino: u64,
    pub is_root: bool,
}

fn canonical_dir(what: &str, p: &Path) -> Result<PathBuf, String> {
    let c = p
        .canonicalize()
        .map_err(|e| format!("{what} {}: {e}", p.display()))?;
    if !c.is_dir() {
        return Err(format!("{what} {} is not a directory", p.display()));
    }
    Ok(c)
}

fn fs_total_90pct(p: &Path) -> Result<u64, String> {
    let s = sys::statvfs(p).map_err(|e| format!("statvfs {}: {e}", p.display()))?;
    let total = s.f_blocks.saturating_mul(s.f_frsize);
    Ok(total / 10 * 9)
}

impl Settings {
    pub fn resolve(cli: Cli) -> Result<Settings, String> {
        let mountpoint = canonical_dir("mountpoint", &cli.mountpoint)?;
        let backing = canonical_dir("backing dir", &cli.backing)?;
        let read_cache = canonical_dir("read-cache dir", &cli.read_cache)?;
        let write_cache = canonical_dir("write-cache dir", &cli.write_cache)?;

        let named = [
            ("mountpoint", &mountpoint),
            ("backing dir", &backing),
            ("read-cache dir", &read_cache),
            ("write-cache dir", &write_cache),
        ];
        for (i, (na, a)) in named.iter().enumerate() {
            for (nb, b) in &named[i + 1..] {
                if a == b {
                    return Err(format!("{na} and {nb} are the same directory"));
                }
                if a.starts_with(b) || b.starts_with(a) {
                    return Err(format!("{na} and {nb} must not contain each other"));
                }
            }
        }
        if cli.auto_unmount && !cli.allow_other {
            return Err("--auto-unmount requires --allow-other".into());
        }
        if cli.threads == Some(0) {
            return Err("--threads must be at least 1".into());
        }
        if cli.promote_workers == 0 {
            return Err("--promote-workers must be at least 1".into());
        }

        let read_cache_size = match cli.read_cache_size {
            Some(v) => v,
            None => fs_total_90pct(&read_cache)?,
        };
        let write_cache_size = match cli.write_cache_size {
            Some(v) => v,
            None => fs_total_90pct(&write_cache)?,
        };
        let read_max_file = cli.read_max_file.unwrap_or(read_cache_size / 4);
        let threads = cli
            .threads
            .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, |n| n.get()));

        use std::os::unix::fs::MetadataExt;
        let bmeta = std::fs::symlink_metadata(&backing)
            .map_err(|e| format!("backing dir {}: {e}", backing.display()))?;

        Ok(Settings {
            mountpoint,
            backing,
            read_cache,
            write_cache,
            read_cache_size,
            read_max_file,
            write_cache_size,
            copy_up_max: cli.copy_up_max,
            flush_delay: cli.flush_delay,
            attr_ttl: cli.attr_ttl,
            passthrough: cli.passthrough,
            threads,
            promote_workers: cli.promote_workers,
            allow_other: cli.allow_other,
            auto_unmount: cli.auto_unmount,
            backing_dev: bmeta.dev(),
            backing_root_ino: bmeta.ino(),
            // SAFETY: geteuid has no preconditions.
            is_root: unsafe { libc::geteuid() } == 0,
        })
    }
}
