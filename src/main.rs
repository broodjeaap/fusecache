use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use clap::Parser;
use fuser::{Config, MountOption, SessionACL};
use log::{error, info};

use fusecache::config::{Cli, Settings};
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

    let mut unmounter = session.unmount_callable();
    std::thread::Builder::new()
        .name("signals".into())
        .spawn(move || {
            let sig = sys::wait_term_signal();
            info!("signal {sig}, unmounting");
            if let Err(e) = unmounter.unmount() {
                error!("unmount: {e}");
            }
        })?;

    let r = session.run();
    stop_workers(workers);
    info!("unmounted");
    r
}
