# fusecache

A FUSE module for storage tiering: an SSD read cache and an SSD write-back cache in front of a
slower backing directory (e.g. HDDs).

```
fusecache --backing /mnt/hdd --read-cache /mnt/ssd1/fc --write-cache /mnt/ssd2/fc /mnt/tiered
```

- The backing directory is authoritative for the namespace and all metadata; only regular-file
  data is cached.
- New or truncated files are written to the write cache and copied back `--flush-delay` (30s)
  after the last writer closes, or sooner once the write cache is above 80% full. Existing files
  up to `--copy-up-max` (1 GiB) are copied to the write cache before being modified; larger ones
  are modified in place on the backing store.
- The first read-only open of a file up to `--read-max-file` promotes the whole file into the
  read cache in the background; the cache is LRU-evicted down to `--read-cache-size`.
- Dirty data left behind by a crash is flushed at the next start. Write-cache files whose
  backing file vanished are moved to `<write-cache>/orphans/`.
- Running as root enables kernel FUSE passthrough (Linux ≥ 6.9 with `CONFIG_FUSE_PASSTHROUGH`):
  file reads and writes bypass the daemon. Otherwise I/O is served by the daemon.
  `--passthrough on|off|auto` controls this.
- Files with more than one hard link always bypass both caches.

Do not modify the backing or cache directories outside the mount while it is mounted.

`fusecache --help` lists every option. Unmount with `fusermount3 -u <mountpoint>` or SIGINT/SIGTERM.

## Building

```
cargo build --release
```

The binary (`target/release/fusecache`) links only against glibc (≥ 2.34). At runtime it needs a
kernel with FUSE; mounting as a non-root user also needs the setuid `fusermount3` helper
(package `fuse3`), as does `--auto-unmount`.
