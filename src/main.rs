#![feature(seek_stream_len)]
#![feature(buf_read_has_data_left)]
#![feature(try_blocks)]

mod flac;
mod fs;
mod libflac_wrapper;
mod utils;
mod wav;

use std::path::PathBuf;

use clap::Parser;
use fuser::MountOption;

use crate::fs::TrackFS;

#[derive(Parser)]
struct Args {
    /// Database URL for sqlx cache (overrides env TRACKFS_DB_URL if set)
    #[clap(long, env = "TRACKFS_DB_URL")]
    db_url: Option<String>,
    /// Max DB connections for sqlx pool (overrides env TRACKFS_DB_MAX_CONN)
    #[clap(long)]
    db_max_connections: Option<usize>,
    #[clap(short, long, default_value_t = '#')]
    /// The separator character used for differentiating cue filename
    /// and track name
    separator: char,
    #[clap(long, default_value_t = 100)]
    /// Max entries of kept flac frame caches (in memory, for fast flac
    /// processing)
    max_cache_entries: usize,
    #[clap(long, default_value_t = num_cpus::get())]
    /// Instances of flac decoders and encoders
    flac_instances: usize,
    #[clap(long, env = "TRACKFS_BANNED_EXTS")]
    /// Comma-separated list of banned file extensions (case-insensitive), e.g. "m3u8,log,tmp"
    banned_exts: Option<String>,
    /// Base directory being converted into trackfs
    base_dir: PathBuf,
    /// Mountpoint
    mountpoint: PathBuf,
    #[clap(short, long)]
    #[arg(value_parser = mount_opt_from_str)]
    /// Additional mount options, default mount options for trackfs-rs (besides
    /// this argument): `default_permissions, nodev, nosuid, noexec, ro,
    /// async, allow_root, auto_unmount`
    options: Vec<MountOption>,
    /// Change the default `allow_root` to `allow_other`, enabling accounts
    /// other than mounting account to access the mounted FS, may cause security
    /// issues
    #[clap(long)]
    allow_other: bool,
    /// Some people may want to omit `allow_root` and `auto_unmount` option (as
    /// this requires `allow_other` in `fuse.conf`), it can be disabled with
    /// this flag. Note: due to limitation in `rust` and `clap`, we cannot
    /// give default values to `options`.
    #[clap(long)]
    no_auto_unmount: bool,
}

impl Args {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.max_cache_entries == 0 {
            anyhow::bail!("Max cache entries must be greater than 0");
        }

        if !self.base_dir.is_dir() {
            anyhow::bail!("{:?} is not a directory", self.base_dir)
        }
        if self.mountpoint.is_file() || self.mountpoint.is_symlink() {
            anyhow::bail!("{:?} is not a valid mountpoint", self.mountpoint)
        }

        if !self.mountpoint.exists() {
            std::fs::create_dir_all(&self.mountpoint)?;
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_target(true)
        .init();

    let args = Args::parse();
    args.validate()?;

    let banned_exts: std::collections::HashSet<String> = args
        .banned_exts
        .as_deref()
        .map(|s| s.split(',')
            .filter(|v| !v.trim().is_empty())
            .map(|v| v.trim().to_ascii_lowercase())
            .collect())
        .unwrap_or_else(|| {
            let mut set = std::collections::HashSet::new();
            set.insert("m3u8".to_string());
            set
        });

    let fs = TrackFS::new(
        args.max_cache_entries,
        args.base_dir,
        args.separator,
        tokio::runtime::Handle::current(),
        args.db_url.as_deref().unwrap_or("postgres://postgres:postgres@localhost:5432/postgres"),
        args.db_max_connections,
        banned_exts,
    )
    .await?;
    let mut mount_options = vec![
        MountOption::DefaultPermissions,
        MountOption::NoDev,
        MountOption::NoSuid,
        MountOption::NoExec,
        MountOption::RO,
        MountOption::Async,
    ];
    mount_options.extend(args.options);

    if !args.no_auto_unmount {
        mount_options.push(if args.allow_other {
            MountOption::AllowOther
        } else {
            MountOption::AllowRoot
        });
        mount_options.push(MountOption::AutoUnmount);
    }

    fuser::mount2(fs, args.mountpoint, &mount_options)?;
    Ok(())
}

pub fn mount_opt_from_str(s: &str) -> anyhow::Result<MountOption> {
    Ok(match s {
        "auto_unmount" => MountOption::AutoUnmount,
        "allow_other" => MountOption::AllowOther,
        "allow_root" => MountOption::AllowRoot,
        "default_permissions" => MountOption::DefaultPermissions,
        "dev" => MountOption::Dev,
        "nodev" => MountOption::NoDev,
        "suid" => MountOption::Suid,
        "nosuid" => MountOption::NoSuid,
        "ro" => MountOption::RO,
        "rw" => MountOption::RW,
        "exec" => MountOption::Exec,
        "noexec" => MountOption::NoExec,
        "atime" => MountOption::Atime,
        "noatime" => MountOption::NoAtime,
        "dirsync" => MountOption::DirSync,
        "sync" => MountOption::Sync,
        "async" => MountOption::Async,
        x if x.starts_with("fsname=") => MountOption::FSName(x[7..].into()),
        x if x.starts_with("subtype=") => MountOption::Subtype(x[8..].into()),
        x => MountOption::CUSTOM(x.into()),
    })
}
