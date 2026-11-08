use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use clap::Parser;
use fuser::{Config, MountOption, SessionACL, SessionUnmounter};
use log::{error, info, warn};

use fusecache::config::{Cli, Settings};
use fusecache::connection;
use fusecache::flusher::{self, FlushCmd};
use fusecache::fs::{CacheFs, Shared};
use fusecache::layers::Roots;
use fusecache::readcache::{self, ReadIndex};
use fusecache::sys;

fn main() -> ExitCode {
    let settings = match Settings::resolve(Cli::parse()) {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("fusecache: {msg}");
            return ExitCode::from(2);
        }
    };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    match run(settings) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!("{e}");
            ExitCode::FAILURE
        }
    }
}

fn run(settings: Settings) -> std::io::Result<()> {
    // Signals are consumed by a dedicated thread; block them before any thread exists.
    sys::block_term_signals()?;
    // Modes requested through FUSE already have the caller's umask applied.
    // SAFETY: umask has no preconditions.
    unsafe { libc::umask(0) };

    let roots = Roots::new(&settings);
    roots.prepare()?;
    let read_index = ReadIndex::load(settings.read_cache_size, &roots.read_data)?;
    let (promote_tx, promote_rx) = crossbeam_channel::bounded(4096);
    let (flush_tx, flush_rx) = crossbeam_channel::unbounded();
    let shared = Arc::new(Shared::new(
        settings.clone(),
        roots,
        read_index,
        promote_tx,
        flush_tx,
    ));
    flusher::recover(&shared)?;
    info!(
        "read cache {} / {}, write cache {} / {}",
        bytesize::ByteSize(shared.read_index.used()),
        bytesize::ByteSize(settings.read_cache_size),
        bytesize::ByteSize(shared.write_used.load(Ordering::Relaxed)),
        bytesize::ByteSize(settings.write_cache_size),
    );

    let mut workers = readcache::spawn_promoters(&shared, promote_rx)?;
    workers.push(flusher::spawn(&shared, flush_rx)?);
    let stop_workers = |workers: Vec<std::thread::JoinHandle<()>>| {
        shared.shutdown.store(true, Ordering::Relaxed);
        let _ = shared.flush_tx.send(FlushCmd::Wake);
        for w in workers {
            let _ = w.join();
        }
    };

    let mut config = Config::default();
    config.mount_options = vec![
        MountOption::FSName(format!("fusecache:{}", settings.backing.display())),
        MountOption::Subtype("fusecache".into()),
        MountOption::DefaultPermissions,
    ];
    if settings.auto_unmount {
        config.mount_options.push(MountOption::AutoUnmount);
    }
    config.acl = if settings.allow_other {
        SessionACL::All
    } else {
        SessionACL::Owner
    };
    config.n_threads = Some(settings.threads);
    config.clone_fd = true;

    let mut session =
        match fuser::Session::new(CacheFs(shared.clone()), &settings.mountpoint, &config) {
            Ok(s) => s,
            Err(e) => {
                stop_workers(workers);
                return Err(std::io::Error::new(
                    e.kind(),
                    format!("mounting {}: {e}", settings.mountpoint.display()),
                ));
            }
        };
    info!(
        "mounted {} on {}",
        settings.backing.display(),
        settings.mountpoint.display()
    );

    let conn = connection::connection_id(&settings.mountpoint);
    if conn.is_none() {
        warn!("FUSE connection not found in /proc/self/mountinfo; shutdown cannot abort it");
    }
    let unmounter = session.unmount_callable();
    std::thread::Builder::new()
        .name("signals".into())
        .spawn(move || handle_signals(unmounter, conn))?;

    let r = session.run();
    stop_workers(workers);
    info!("unmounted");
    r
}

/// How long the session may outlive a successful unmount before its connection is aborted.
const ABORT_AFTER: Duration = Duration::from_secs(5);

/// First SIGINT/SIGTERM unmounts. If the session has not ended `ABORT_AFTER` later (the mount
/// is still referenced from another mount namespace), or on the next signal, the FUSE
/// connection is aborted; if even that does not end the session, the process exits.
/// Dirty data is safe either way: it stays in the write cache and is flushed on the next start.
fn handle_signals(mut unmounter: SessionUnmounter, conn: Option<u64>) {
    let Some(sig) = sys::wait_term_signal(None) else {
        return;
    };
    info!("signal {sig}, unmounting");
    let next = match unmounter.unmount() {
        Ok(()) => sys::wait_term_signal(Some(ABORT_AFTER)),
        Err(e) => {
            error!("unmount: {e}; send SIGINT/SIGTERM again to abort the filesystem");
            sys::wait_term_signal(None)
        }
    };
    match next {
        Some(sig) => info!("signal {sig}, aborting FUSE connection"),
        None => warn!(
            "FUSE connection still open {ABORT_AFTER:?} after unmount (the mount is still referenced, \
             e.g. from another mount namespace); aborting it"
        ),
    }
    if let Some(id) = conn
        && let Err(e) = connection::abort(id)
    {
        warn!("aborting FUSE connection {id}: {e}");
    }
    if sys::wait_term_signal(Some(ABORT_AFTER)).is_none() {
        error!("session did not end after aborting its connection; exiting");
    }
    std::process::exit(1);
}
