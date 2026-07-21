# trackfs-rs

Fast FUSE filesystem that splits FLAC with CUE into individual track files, implemented in Rust.

Inspired by [andresch/trackfs](https://github.com/andresch/trackfs), and optimized to avoid re-encoding the full FLAC track.

## Features
- Split a single FLAC/WAV album into per-track virtual files using a CUE sheet
- Support both external CUE files and embedded CUE in FLAC (Vorbis comment cuesheet)
- Zero re-encoding for FLAC paths (fast, low CPU)
- Read-only FUSE filesystem
- CUE track size caching via PostgreSQL to avoid repeated scanning

## Supported input types
- FLAC + external CUE
- FLAC with embedded CUE (via Vorbis comment metadata; FLAC CUESHEET metadata block is not supported)
- WAV + external CUE (non-standard WAV metadata are passed through as-is)

## Requirements
- Linux with FUSE3 (libfuse >= 3)
- Rust toolchain (Rust 1.76+ recommended)
- PostgreSQL server (optional but recommended for persistent size cache)

## Build
```bash
# build in debug
cargo build

# build in release
cargo build --release
```

## Quick start
```bash
# Mount /music to /mnt/trackfs (read-only), using a local PostgreSQL for cache
export TRACKFS_DB_URL="postgres://user:pass@localhost:5432/postgres"
mkdir -p /mnt/trackfs
./target/release/trackfs-rs \
  --db-url "$TRACKFS_DB_URL" \
  --db-max-connections 20 \
  /music /mnt/trackfs

# Unmount when done
fusermount3 -u /mnt/trackfs
```

## Usage
```
Usage: trackfs-rs [OPTIONS] <BASE_DIR> <MOUNTPOINT>

Arguments:
  <BASE_DIR>    Base directory being converted into trackfs
  <MOUNTPOINT>  Mountpoint

Options:
  -s, --separator <SEPARATOR>
          The separator character used for differentiating cue filename and track name [default: #]
      --max-cache-entries <MAX_CACHE_ENTRIES>
          Max entries of kept flac frame caches (in memory, for fast flac processing) [default: 100]
      --flac-instances <FLAC_INSTANCES>
          Instances of flac decoders and encoders [default: <NUMBER OF CPU THREADS>]
      --db-url <DB_URL>
          Database URL for sqlx cache (can also be provided via env TRACKFS_DB_URL)
      --db-max-connections <DB_MAX_CONNECTIONS>
          Max DB connections for sqlx pool. Priority: CLI > env TRACKFS_DB_MAX_CONN > default 20
      --banned-exts <BANNED_EXTS>
          Comma-separated list of banned file extensions (case-insensitive). Files with these extensions
          will be hidden in the mounted FS. Example: "m3u8,log,tmp". Default: "m3u8" if not provided
  -o, --options <OPTIONS>
          Additional mount options. Defaults (besides this argument):
          default_permissions, nodev, nosuid, noexec, ro, async, allow_root, auto_unmount
      --allow-other
          Use allow_other instead of allow_root (may require fuse.conf; has security implications)
      --no-auto-unmount
          Omit allow_root/auto_unmount if desired (note: due to Rust/clap limitations, options have no defaults)
  -h, --help
          Print help
```

### Environment variables
- TRACKFS_DB_URL: Database URL for sqlx cache (alternative to --db-url)
- TRACKFS_DB_MAX_CONN: Max DB connections for sqlx pool (alternative to --db-max-connections)
- TRACKFS_BANNED_EXTS: Comma-separated list of banned file extensions (case-insensitive). Same semantics as --banned-exts

## Banned extensions (banned_exts)
This feature allows hiding files by their extensions.

- Scope: applies to regular passthrough files. It does not affect directories, symlinks, or generated virtual track files.
- Matching: case-insensitive, based on file extension only (characters after the last dot).
- Default: if not provided, defaults to a single banned extension: "m3u8".
- Configuration methods:
  - CLI: --banned-exts "m3u8,log,tmp"
  - Environment: TRACKFS_BANNED_EXTS="m3u8,log,tmp"
- Examples:
  - Hide playlist and log files: --banned-exts "m3u8,log"
  - Hide temporary files: TRACKFS_BANNED_EXTS="tmp,part,swp"

Notes:
- Whitespace around commas is ignored.
- Invalid/empty items are ignored.

## Database cache
trackfs-rs uses a PostgreSQL-backed persistent cache to store virtual CUE track sizes, avoiding repeated scanning and decoding.

- Table: `flac_track_size_cache`
  - `file_hash` BIGINT NOT NULL
  - `track_id` INTEGER NOT NULL
  - `file_size` BIGINT NOT NULL
  - PRIMARY KEY (`file_hash`, `track_id`)

- Invalidation strategy: a fast fingerprint derived from source file metadata is used as the key.
  - Fingerprint: (st_dev, st_ino, st_mtime, st_size) -> DefaultHasher (64-bit)
  - Pros: extremely fast, zero file IO reads
  - Note: weak consistency (very rare collisions), sufficient for caching

- Connection pool defaults (see source for details):
  - min_connections: 1
  - acquire_timeout: 5s
  - idle_timeout: 60s
  - max_lifetime: 3600s
  - max_connections: configurable via `--db-max-connections` or `TRACKFS_DB_MAX_CONN` (default 20)

- Upsert policy: only update when `file_size` changes (reduces WAL and table bloat)

## Performance tuning (optional)
- Conditional UPSERT (built-in): update only when size changes
- UNLOGGED table: if you can tolerate cache loss on crash, switching to UNLOGGED can greatly reduce WAL
- `synchronous_commit = off` (session-level): lowers write latency at the risk of losing the most recent transactions on crash
- Batch writes: for warming up, group updates in a single transaction
- Compile-time SQL checks: use `sqlx::query!` macros (requires setup)

## Logging
Use the `RUST_LOG` environment variable and a compatible subscriber to control verbosity, for example:
```bash
RUST_LOG=info ./target/release/trackfs-rs ...
```

## Troubleshooting
- Cannot connect to DB:
  - Validate `--db-url` / `TRACKFS_DB_URL`, network/firewall, and credentials
- Connection pool exhausted:
  - Increase `--db-max-connections` / `TRACKFS_DB_MAX_CONN` and check PostgreSQL server `max_connections`
- Permission or table errors:
  - The program auto-creates the table; ensure the DB user has privileges to create and update the cache table
- FUSE mount errors:
  - Ensure `fuse3` is installed and the mountpoint exists and is empty

## Development
- Code style: `cargo fmt`
- Lint: `cargo clippy`
- Test/build: `cargo check`, `cargo build`

## License
Dual-licensed under MIT or Apache-2.0.
