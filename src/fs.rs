use std::{
    collections::HashSet,
    ffi::{OsStr, OsString},
    io::{SeekFrom},
    ops::{Deref, DerefMut},
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use dashmap::DashMap;
use flac_process::*;
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, Request,
};
use futures::stream::StreamExt;
use libc::{EINVAL, EIO, ENOENT, ENOTDIR};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    sync::RwLock,
};

use crate::libflac_wrapper::{FlacDecoder, FlacEncoder};

mod flac_process;
mod libc_wrappers;

/// As there are embedded cue in flac, it represents either cue or flac (with
/// cue embedded) path
type CUEPath = PathBuf;
type TrackID = usize;

#[derive(Clone, Default, Debug)]
struct VirtualFSEntry {
    #[allow(dead_code)]
    parent_inode: u64,
    virtual_path: PathBuf,
    origin: VirtualFSEntryOrigin,
}

#[derive(Clone, Debug)]
enum VirtualFSEntryOrigin {
    Directory(PathBuf),
    Symlink(PathBuf),
    Passthrough(PathBuf),
    CUEVirtualFile(CUEPath, TrackID),
}

impl Default for VirtualFSEntryOrigin {
    fn default() -> Self {
        Self::Directory(PathBuf::new())
    }
}

struct CUEInfoCache {
    info: Arc<CUEInfo>,
    mtime: i64,
}

#[derive(Clone, Debug)]
struct DirEntry {
    inode: u64,
    file_type: FileType,
    name: OsString,
}

struct DirEntryCache {
    entries: Vec<DirEntry>,
    mtime: i64,
}

pub struct TrackFS {
    handle: tokio::runtime::Handle,
    inner: Arc<TrackFSInner>,
    separator: char,
}

#[allow(dead_code)]
struct TrackFSInner {
    inode_table: RwLock<Vec<VirtualFSEntry>>,
    inode_lookup: DashMap<PathBuf, u64>,
    cue_info_cache: DashMap<CUEPath, CUEInfoCache>,
    childs_cache: DashMap<u64, DirEntryCache>,
    frames_cache: concurrent_lru::sharded::LruCache<CUEPath, FlacCacheData>,
    libflac_pool: deadpool::unmanaged::Pool<LibFlacDecEnc>,
    db_cache: db::CacheManager,
    banned_exts: std::collections::HashSet<String>,
}
// 黑名单通过 CLI/环境变量配置，保存为小写扩展名集合


#[derive(thiserror::Error, Debug)]
pub enum GetCUEError {
    #[error("IO error")]
    IOError(#[from] tokio::io::Error),
    #[error("no embedded cue")]
    NoEmbeddedCUEInFlac,
    #[error("invalid extension")]
    InvalidExtension,
    #[error("")]
    Other(#[from] anyhow::Error),
}


mod db {
    use sqlx::{PgPool, Row, postgres::PgPoolOptions};
    use std::{time::Duration, env};

    pub struct CacheManager {
        pool: PgPool,
    }

    impl CacheManager {
        pub async fn new(database_url: &str, max_conn_opt: Option<usize>) -> anyhow::Result<Self> {
            let max_conn = max_conn_opt
                .or_else(|| env::var("TRACKFS_DB_MAX_CONN").ok().and_then(|v| v.parse::<usize>().ok()))
                .unwrap_or(20);
            let pool = PgPoolOptions::new()
                .max_connections(max_conn as u32)
                .min_connections(1)
                .acquire_timeout(Duration::from_secs(15))
                .idle_timeout(Duration::from_secs(60))
                .max_lifetime(Duration::from_secs(3600))
                .connect(database_url)
                .await?;

            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS flac_track_size_cache (
                    file_hash BIGINT NOT NULL,
                    track_id INTEGER NOT NULL,
                    file_size BIGINT NOT NULL,
                    PRIMARY KEY (file_hash, track_id)
                )
                "#,
            )
            .execute(&pool)
            .await?;

            Ok(Self { pool })
        }

        pub async fn get_cached_track_size(&self, file_hash: i64, track_id: usize) -> anyhow::Result<Option<u64>> {
            let row = sqlx::query(
                "SELECT file_size FROM flac_track_size_cache WHERE file_hash = $1 AND track_id = $2",
            )
            .bind(file_hash)
            .bind(track_id as i32)
            .fetch_optional(&self.pool)
            .await?;
            Ok(row.map(|r| r.get::<i64, _>(0) as u64))
        }

        pub async fn update_track_cache(&self, file_hash: i64, track_id: usize, size: u64) -> anyhow::Result<()> {
            sqlx::query(
                r#"
                INSERT INTO flac_track_size_cache (file_hash, track_id, file_size)
                VALUES ($1, $2, $3)
                ON CONFLICT (file_hash, track_id) DO UPDATE
                SET file_size = EXCLUDED.file_size
                "#,
            )
            .bind(file_hash)
            .bind(track_id as i32)
            .bind(size as i64)
            .execute(&self.pool)
            .await?;
            Ok(())
        }
    }
}

async fn calculate_file_fingerprint(path: &Path) -> anyhow::Result<i64> {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
        os::unix::fs::MetadataExt,
    };

    let meta = tokio::fs::metadata(path).await?;
    let dev = meta.dev();
    let ino = meta.ino();
    let mtime = meta.mtime();
    let size = meta.size();

    let s = format!("{}:{}:{}:{}", dev, ino, mtime, size);
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    Ok(hasher.finish() as i64)
}

impl TrackFSInner {
    async fn get_cue_info(&self, cue_path: impl AsRef<Path>) -> Result<Arc<CUEInfo>, GetCUEError> {
        let cue_path_key = cue_path.as_ref().to_path_buf();
        // Use the real file's metadata for mtime checks to ensure cache validity
        let mtime = tokio::fs::metadata(cue_path.as_ref()).await?.mtime();

        if let Some(info) = self.cue_info_cache.get(&cue_path_key) {
            if info.mtime >= mtime {
                return Ok(info.info.clone());
            }
        }

        let parsed = match cue_path.as_ref().extension() {
            Some(cue) if cue == OsStr::new("cue") => process_cue(cue_path).await?,
            Some(flac) if flac == OsStr::new("flac") => process_flac_embedded_cue(cue_path)
                .await?
                .ok_or(GetCUEError::NoEmbeddedCUEInFlac)?,
            _ => Err(GetCUEError::InvalidExtension)?,
        };

        let info_arc = Arc::new(parsed);
        self.cue_info_cache
            .insert(cue_path_key.clone(), CUEInfoCache { info: info_arc.clone(), mtime });
        Ok(info_arc)
    }

    async fn update_dir_entries(
        //更新缓存
        &self,
        separator: char,
        parent_inode: u64,
        path: impl AsRef<Path>,
        real_path: impl AsRef<Path>,
    ) -> Result<Vec<DirEntry>, libc::c_int> {
        tracing::debug!(
            "update_dir_entries called with separator: {}, parent_inode: {}, path: {}, real_path: {},",
            separator,
            parent_inode,
            path.as_ref().display(),
            real_path.as_ref().display()
        );
        let path = path.as_ref().to_path_buf();
        // use async metadata on real_path to avoid blocking the async runtime
        let mtime = tokio::fs::metadata(real_path.as_ref()).await.ok().map(|m| m.mtime());
        if let Some(cache) = self.childs_cache.get(&parent_inode) {
            if mtime.unwrap_or(i64::MAX) <= cache.mtime {
                return Ok(cache.entries.clone());
            }
        }

        let childs: tokio::io::Result<Vec<PathBuf>> = try {
            let mut childs = vec![];
            let mut read_dir = tokio::fs::read_dir(real_path.as_ref()).await?;
            while let Some(entry) = read_dir.next_entry().await? {
                tracing::debug!("updatedir ,the entries is : {:?}", entry);
                childs.push(entry.path());
            }
            childs
        };
        let Ok(childs) = childs else {
            return Err(EIO);
        };

        let mut entries = Vec::new();

        let files = childs.iter().filter(|e| e.is_file());
        let dirs = childs.iter().filter(|e| e.is_dir());
        let symlinks = childs.iter().filter(|e| e.is_symlink());

        for dir in dirs {
            let name = dir.file_name().unwrap();
            let mut virtual_path = path.clone();
            virtual_path.push(name);

            let inode = self
                .add_or_update_entry(VirtualFSEntry {
                    parent_inode,
                    virtual_path,
                    origin: VirtualFSEntryOrigin::Directory(dir.clone()),
                })
                .await;

            let entry =
                DirEntry { inode, file_type: FileType::Directory, name: name.to_os_string() };
            entries.push(entry);
        }

        for symlink in symlinks {
            let name = symlink.file_name().unwrap();
            let mut virtual_path = path.clone();
            virtual_path.push(name);

            let inode = self
                .add_or_update_entry(VirtualFSEntry {
                    parent_inode,
                    virtual_path,
                    origin: VirtualFSEntryOrigin::Symlink(symlink.clone()),
                })
                .await;

            let entry = DirEntry { inode, file_type: FileType::Symlink, name: name.to_os_string() };
            entries.push(entry);
        }

        let cue_files = files
            .clone()
            .filter(|path| path.extension() == Some(OsStr::new("cue")))
            .cloned()
            .collect::<HashSet<_>>();
        let flac_files = files
            .clone()
            .filter(|path| path.extension() == Some(OsStr::new("flac")))
            .cloned()
            .collect::<HashSet<_>>();

        let mut cue_infos: Vec<_> = futures::stream::iter(cue_files.iter())
            .filter_map(|cue| {
                let path = real_path.as_ref().to_path_buf();
                async move {
                    let mut cue_path = path.clone();
                    cue_path.push(cue);

                    match self.get_cue_info(&cue_path).await {
                        Ok(cue_arc) => Some(cue_arc),
                        Err(e) => {
                            tracing::error!("Error parsing cue {}: {e:?}", cue_path.display());
                            None
                        }
                    }
                }
            })
            .collect()
            .await;

        let used_whole_files = cue_infos
            .iter()
            .flat_map(|info| {
                info.passthrough_files
                    .iter()
                    .map(|file_info| &file_info.file_path)
                    .chain(info.files_info.iter().map(|file_info| &file_info.file_path))
            })
            .filter(|path| {
                path.extension() == Some(OsStr::new("flac"))
                    || path.extension() == Some(OsStr::new("wav"))
            })
            .cloned()
            .collect::<HashSet<_>>();
        let left_flacs = &flac_files - &used_whole_files;
        let left_cue_infos: Vec<_> = futures::stream::iter(left_flacs.iter())
            .filter_map(|flac| {
                let path = real_path.as_ref().to_path_buf();
                async move {
                    let mut flac_path = path.clone();
                    flac_path.push(flac);

                    match self.get_cue_info(flac).await {
                        Ok(embedded_cue_arc) => Some(embedded_cue_arc),
                        Err(GetCUEError::NoEmbeddedCUEInFlac) => None,
                        Err(e) => {
                            tracing::error!(
                                "Error parsing embedded cue in {}: {e:?}",
                                flac_path.display()
                            );
                            None
                        }
                    }
                }
            })
            .collect()
            .await;
        let flacs_with_cue = left_cue_infos
            .iter()
            .filter_map(|cue_info_arc| {
                let cue_info = cue_info_arc.as_ref();
                flac_files
                    .iter()
                    .find(|entry| entry.file_name() == Some(cue_info.cue_name.as_os_str()))
            })
            .cloned()
            .collect::<HashSet<_>>();
        cue_infos.extend(left_cue_infos);
        for cue_info_arc in cue_infos {
            let cue_info = cue_info_arc.as_ref();
            for passthrough_info in cue_info.passthrough_files.iter() {
                let passthrough_file = &passthrough_info.file_path;
                let ext = passthrough_file.extension().unwrap_or_default();
                if tokio::fs::metadata(passthrough_file).await.is_err() {
                    continue; // 跳过不存在的文件
                }
                let mut vfs_name = cue_info.cue_name.file_name().unwrap().to_os_string();
                // Passthrough files in `CUEInfo` refers to the files with only one track in
                // it
                let track_id = passthrough_info.tracks_info[0].track_id;
                let safe_title = passthrough_info.cue.tracks[track_id].1.title.replace('/', "\\");
                vfs_name.push(format!(
                    "_{}tr{}_{}.{}",
                    separator,
                    track_id + 1,
                    safe_title,
                    ext.to_string_lossy()
                ));

                let mut virtual_path = path.clone();
                virtual_path.push(&vfs_name);

                let inode = self
                    .add_or_update_entry(VirtualFSEntry {
                        parent_inode,
                        virtual_path,
                        origin: VirtualFSEntryOrigin::Passthrough(passthrough_file.clone()),
                    })
                    .await;

                let entry = DirEntry { inode, file_type: FileType::RegularFile, name: vfs_name };
                entries.push(entry);
            }

            for file_info in cue_info.files_info.iter() {
                let vfs_name = cue_info.cue_name.file_name().unwrap().to_os_string();
                let ext = file_info.file_path.extension().unwrap_or_default();
                if ![OsStr::new("flac"), OsStr::new("wav")].contains(&ext) {
                    continue;
                }
                if tokio::fs::metadata(&file_info.file_path).await.is_err() {
                    continue; // 跳过不存在的文件
                }
                for track_info in &file_info.tracks_info {
                    let mut vfs_name = vfs_name.clone();
                    let track_id = track_info.track_id;
                    let safe_title = file_info.cue.tracks[track_id].1.title.replace('/', "\\");
                    vfs_name.push(format!(
                        "_{}tr{}_{}.{}",
                        separator,
                        track_id + 1,
                        safe_title,
                        ext.to_string_lossy()
                    ));

                    let mut virtual_path = path.clone();
                    virtual_path.push(&vfs_name);

                    let inode = self
                        .add_or_update_entry(VirtualFSEntry {
                            parent_inode,
                            virtual_path,
                            origin: VirtualFSEntryOrigin::CUEVirtualFile(
                                cue_info.cue_name.clone(),
                                track_id,
                            ),
                        })
                        .await;

                    let entry =
                        DirEntry { inode, file_type: FileType::RegularFile, name: vfs_name };
                    entries.push(entry);
                }
            }
        }

        let files = files.cloned().collect::<HashSet<_>>();
        let left_files = &files - &used_whole_files;
        let left_files = &left_files - &flacs_with_cue;
        let left_files = &left_files - &cue_files;
        for left_file in left_files {
            if let Some(ext) = left_file.extension().and_then(|e| e.to_str()) {
                let ext_lc = ext.to_ascii_lowercase();
                if self.banned_exts.contains(&ext_lc) {
                    continue; // 跳过黑名单中的文件
                }
            }
            let name = left_file.file_name().unwrap();
            let mut virtual_path = path.clone();
            virtual_path.push(name);

            let inode = self
                .add_or_update_entry(VirtualFSEntry {
                    parent_inode,
                    virtual_path,
                    origin: VirtualFSEntryOrigin::Passthrough(left_file.clone()),
                })
                .await;

            let entry =
                DirEntry { inode, file_type: FileType::RegularFile, name: name.to_os_string() };
            entries.push(entry);
        }

        self.childs_cache.insert(
            parent_inode,
            DirEntryCache { entries: entries.clone(), mtime: mtime.unwrap_or(0) },
        );

        Ok(entries)
    }

    pub async fn get_file_attr(self: Arc<Self>, ino: u64) -> Result<FileAttr, libc::c_int> {
        let guard = self.inode_table.read().await;
        let Some(entry) = guard.get(ino as usize) else {
            return Err(ENOENT);
        };

        let (origin_path, estimated_size) = match entry.origin {
            VirtualFSEntryOrigin::Directory(ref path)
            | VirtualFSEntryOrigin::Symlink(ref path)
            | VirtualFSEntryOrigin::Passthrough(ref path) => (path.clone(), None),

            VirtualFSEntryOrigin::CUEVirtualFile(ref cue_path, track_id) => {
                // 优先查数据库缓存
                let file_hash = match calculate_file_fingerprint(&cue_path).await {
                    Ok(fp) => fp,
                    Err(_) => return Err(EIO),
                };

                match self.db_cache.get_cached_track_size(file_hash, track_id).await {
                    Ok(Some(cached)) => (cue_path.clone(), Some(cached)),
                    Ok(None) | Err(_) => {
                        // 未命中则计算并回写缓存
                        let info = match self.get_cue_info(cue_path).await {
                            Ok(i) => i,
                            Err(e) => {
                                tracing::error!("Error processing {}: {e:?}", cue_path.display());
                                return Err(ENOENT);
                            }
                        };

                        let Some(file_info) = info
                            .files_info
                            .iter()
                            .chain(info.passthrough_files.iter())
                            .find(|fi| fi.tracks_info.iter().any(|ti| ti.track_id == track_id))
                        else {
                            tracing::error!(
                                "Error processing {}: Invalid track id {track_id}",
                                cue_path.display()
                            );
                            return Err(ENOENT);
                        };

                        let size = match flac_process::process_file_for_size(
                            &file_info.file_path,
                            track_id,
                            &file_info.tracks_info,
                            &file_info.cue,
                        )
                        .await
                        {
                            Ok(sz) => sz,
                            Err(e) => {
                                // 保留日志输出，但不返回 EIO。按需返回大小 0。
                                tracing::error!(
                                    "Error calculating size for {} track {}: {:?}",
                                    file_info.file_path.display(),
                                    track_id,
                                    e
                                );
                                0
                            }
                        };
                        if size > 0 {
                            if let Err(e) = self.db_cache.update_track_cache(file_hash, track_id, size).await {
                                tracing::error!(
                                    "Failed to update cache for {} track {}: {}",
                                    cue_path.display(),
                                    track_id,
                                    e
                                );
                            }
                        } else {
                            tracing::debug!(
                                "Skip updating cache for {} track {} because size is 0",
                                cue_path.display(),
                                track_id
                            );
                        }
                        (file_info.file_path.clone(), Some(size))
                    }
                }
            }
        };

        // lstat is a small blocking syscall wrapper; run it in spawn_blocking to avoid blocking
        let lstat_res = tokio::task::spawn_blocking({
            let origin = origin_path.clone();
            move || libc_wrappers::lstat(origin.as_os_str())
        })
        .await
        .map_err(|_| EIO)?;

        match lstat_res {
            Ok(stat) => {
                let mut attr = TrackFS::stat_to_fuse(stat, ino);
                if let Some(estimated_size) = estimated_size {
                    TrackFS::set_attr_size(&mut attr, estimated_size);
                }
                Ok(attr)
            }
            Err(code) => Err(code),
        }
    }
    fn lookup_inode(&self, path: impl AsRef<Path>) -> Option<u64> { Some(*self.inode_lookup.get(path.as_ref())?) }

    async fn add_or_update_entry(&self, entry: VirtualFSEntry) -> u64 {
        let path = entry.virtual_path.clone();
        let inode_found = self.inode_lookup.get(&entry.virtual_path).map(|inode| *inode);

        match inode_found {
            Some(inode) => {
                let mut table = self.inode_table.write().await;
                let idx = inode as usize;
                if idx < table.len() {
                    table.get_mut(idx).map(|e| *e = entry);
                    self.inode_lookup.insert(path, inode);
                    inode
                } else {
                    // out-of-range inode: push as new entry
                    table.push(entry);
                    let new_inode = (table.len() - 1) as u64;
                    self.inode_lookup.insert(path, new_inode);
                    new_inode
                }
            }
            None => {
                let mut table = self.inode_table.write().await;
                table.push(entry);
                let inode = (table.len() - 1) as u64;
                self.inode_lookup.insert(path, inode);
                inode
            }
        }
    }
}

#[allow(dead_code)]
struct LibFlacDecEnc {
    decoder: FlacDecoder,
    encoder: FlacEncoder,
}

#[allow(dead_code)]
impl LibFlacDecEnc {
    fn new() -> Self {
        Self { decoder: FlacDecoder::new(), encoder: FlacEncoder::new() }
    }

    #[allow(dead_code)]
    fn split(&mut self) -> (FlacDecoderGuard<'_>, FlacEncoderGuard<'_>) {
        (FlacDecoderGuard(&mut self.decoder), FlacEncoderGuard(&mut self.encoder))
    }
}

#[allow(dead_code)]
struct FlacDecoderGuard<'a>(&'a mut FlacDecoder);
impl Deref for FlacDecoderGuard<'_> {
    type Target = FlacDecoder;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}
impl DerefMut for FlacDecoderGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}
impl Drop for FlacDecoderGuard<'_> {
    fn drop(&mut self) {
        self.0.cleanup()
    }
}

#[allow(dead_code)]
struct FlacEncoderGuard<'a>(&'a mut FlacEncoder);
impl Deref for FlacEncoderGuard<'_> {
    type Target = FlacEncoder;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}
impl DerefMut for FlacEncoderGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}
impl Drop for FlacEncoderGuard<'_> {
    fn drop(&mut self) {
        self.0.finish();
    }
}

const TTL: Duration = Duration::from_secs(30);

impl TrackFS {
    pub async fn new(
        cap: usize,
        root_dir: impl AsRef<Path>,
        separator: char,
        handle: tokio::runtime::Handle,
        db_url: &str,
        db_max_connections: Option<usize>,
        banned_exts: std::collections::HashSet<String>,
    ) -> anyhow::Result<Self> {
        let libflac_resources =
            (0..num_cpus::get()).map(|_| LibFlacDecEnc::new()).collect::<Vec<_>>();

        let db_cache = db::CacheManager::new(db_url, db_max_connections).await?;

        let inner = TrackFSInner {
            // We need a dummy one, as the fuse inode start with 1
            inode_table: RwLock::new(vec![VirtualFSEntry::default()]),
            inode_lookup: DashMap::new(),
            cue_info_cache: DashMap::new(),
            childs_cache: DashMap::new(),
            frames_cache: concurrent_lru::sharded::LruCache::new(cap as u64),
            libflac_pool: deadpool::unmanaged::Pool::from(libflac_resources),
            db_cache,
            banned_exts,
        };

        let new_self = Self { handle, inner: Arc::new(inner), separator };

        // use tokio async canonicalize to avoid blocking async executor
        let root_dir = tokio::fs::canonicalize(root_dir.as_ref()).await?;
        let root_entry = VirtualFSEntry {
            parent_inode: 0,
            virtual_path: PathBuf::from("/"),
            origin: VirtualFSEntryOrigin::Directory(root_dir),
        };

        new_self.inner.add_or_update_entry(root_entry).await;
        Ok(new_self)
    }

    fn mode_to_filetype(mode: libc::mode_t) -> FileType {
        match mode & libc::S_IFMT {
            libc::S_IFDIR => FileType::Directory,
            libc::S_IFREG => FileType::RegularFile,
            libc::S_IFLNK => FileType::Symlink,
            libc::S_IFBLK => FileType::BlockDevice,
            libc::S_IFCHR => FileType::CharDevice,
            libc::S_IFIFO => FileType::NamedPipe,
            libc::S_IFSOCK => FileType::Socket,
            _ => {
                panic!("unknown file type");
            }
        }
    }

    fn stat_to_fuse(stat: libc::stat64, ino: u64) -> FileAttr {
        // st_mode encodes both the kind and the permissions
        let kind = Self::mode_to_filetype(stat.st_mode);
        let perm = (stat.st_mode & 0o7777) as u16;

        let time = |secs: i64, nanos: i64| {
            SystemTime::UNIX_EPOCH + Duration::new(secs as u64, nanos as u32)
        };

        // libc::nlink_t is wildly different sizes on different platforms:
        // linux amd64: u64
        // linux x86:   u32
        // macOS amd64: u16
        #[allow(clippy::cast_lossless)]
        let nlink = stat.st_nlink as u32;

        FileAttr {
            ino,
            size: stat.st_size as u64,
            blocks: stat.st_blocks as u64,
            atime: time(stat.st_atime, stat.st_atime_nsec),
            mtime: time(stat.st_mtime, stat.st_mtime_nsec),
            ctime: time(stat.st_ctime, stat.st_ctime_nsec),
            crtime: SystemTime::UNIX_EPOCH,
            kind,
            perm,
            nlink,
            uid: stat.st_uid,
            gid: stat.st_gid,
            rdev: stat.st_rdev as u32,
            blksize: stat.st_blksize as u32,
            flags: 0,
        }
    }

    fn set_attr_size(attr: &mut FileAttr, new_size: u64) {
        attr.size = new_size;
        attr.blocks = new_size / attr.blksize as u64;
    }
}

enum TrackFSFileHandle {
    Passthrough(Arc<tokio::sync::Mutex<tokio::io::BufReader<tokio::fs::File>>>),
    InMemory(Arc<Vec<u8>>),
}

// We assume that getting with inode will always in cache
impl Filesystem for TrackFS {
    fn init(
        &mut self,
        _req: &Request<'_>,
        _config: &mut KernelConfig,
    ) -> Result<(), std::ffi::c_int> {
        Ok(())
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        tracing::debug!("lookup file with parent {},name: {:?}", parent.clone(), name);
        let name = name.to_owned();
        let inner = self.inner.clone();
        let separator = self.separator;
        self.handle.spawn(async move {
            let guard = inner.inode_table.read().await;
            let Some(parent_entry) = guard.get(parent as usize) else {
                reply.error(ENOENT);
                return;
            };
            let VirtualFSEntryOrigin::Directory(ref origin_path) = parent_entry.origin else {
                reply.error(ENOTDIR);
                return;
            };

            let mut path = parent_entry.virtual_path.clone();
            let origin_path = origin_path.clone();
            drop(guard);
            if !inner.childs_cache.contains_key(&parent) {
                let _ =
                    inner.update_dir_entries(separator, parent, path.as_path(), origin_path).await;
            }

            path.push(name);
            let Some(inode) = inner.lookup_inode(path) else {
                reply.error(ENOENT);
                return;
            };

            match inner.get_file_attr(inode).await {
                Ok(attr) => reply.entry(&TTL, &attr, 0),
                Err(code) => reply.error(code),
            }
        });
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let inner = self.inner.clone();
        self.handle.spawn(async move {
            match inner.get_file_attr(ino).await {
                Ok(attr) => reply.attr(&TTL, &attr),
                Err(code) => reply.error(code),
            }
        });
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        let inner = self.inner.clone();
        self.handle.spawn(async move {
            // It could be much more complex for symlinks handling, but we only use a simple
            // one now
            let guard = inner.inode_table.read().await; //inner缓存
            let Some(entry) = guard.get(ino as usize) else {
                reply.error(ENOENT);
                return;
            };
            if let VirtualFSEntryOrigin::Symlink(ref origin) = entry.origin {
                match tokio::fs::read_link(origin).await {
                    Ok(target) => reply.data(target.into_os_string().as_bytes()),
                    Err(_) => reply.error(EIO),
                }
            
            } else {
                reply.error(ENOENT);
            }
        });
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        let inner = self.inner.clone();

        self.handle.spawn(async move {
            let guard = inner.inode_table.read().await;
            let Some(entry) = guard.get(ino as usize) else {
                reply.error(ENOENT);
                return;
            };
            let entry = entry.clone();
            drop(guard);

            let handle = match entry.origin {
                VirtualFSEntryOrigin::Passthrough(origin) => {
                    let Ok(file) = tokio::fs::File::open(origin).await else {
                        reply.error(EIO);
                        return;
                    };
                    let reader = tokio::io::BufReader::new(file);
                    // `AsyncRead` in tokio will clear buffer on seek, when accessed from multiple
                    // read requests, this causes race condition inside `tokio::fs::File` and
                    // panicking, so we lock it
                    let reader = tokio::sync::Mutex::new(reader);
                    TrackFSFileHandle::Passthrough(Arc::new(reader))
                }
                VirtualFSEntryOrigin::CUEVirtualFile(cue_path, track_id) => {
                    let inner = inner.clone();
                    let cue_path_async = cue_path.clone();
                    let cue_info_arc = match inner.get_cue_info(&cue_path_async).await {
                        Ok(cue_info) => cue_info,
                        Err(e) => {
                            tracing::error!(
                                "Error while processing {} track {track_id}: {e:?}",
                                cue_path.display()
                            );
                            reply.error(EINVAL);
                            return;
                        }
                    };

                    // Use a reference to avoid cloning large CUEInfo
                    let cue_info = cue_info_arc.as_ref();

                    let Some(file_info) = cue_info.files_info.iter().find(|file_info| {
                        file_info.tracks_info.iter().any(|info| info.track_id == track_id)
                    }) else {
                        {
                            tracing::error!(
                                "Error while processing {} track {track_id}: invalid track id",
                                cue_path.display()
                            );
                            reply.error(EINVAL);
                            return;
                        }
                    };

                    let ext = file_info.file_path.extension();
                    if ext == Some(OsStr::new("flac")) {
                        // 直接复用 get_cue_info 返回的数据，避免不必要的 clone
                        // run process_file in current async context; process_file will
                        // offload heavy blocking work to the blocking threadpool.
                        let result: anyhow::Result<Vec<u8>> = try {
                            let cache_data = inner.frames_cache.get(cue_path_async.clone());
                            let cache_data = cache_data.as_ref().map(|handle| handle.value());

                            let mut out_bytes = std::io::Cursor::new(Vec::<u8>::new());
                            if let Some(cache_data) = process_file(
                                &mut out_bytes,
                                &file_info.file_path,
                                track_id,
                                &file_info.cue,
                                &file_info.tracks_info,
                                cache_data,
                            )
                            .await?
                            {
                                // 复用已有条目，避免无意义的插入——若已存在则仅更新权重/计数
                                inner.frames_cache.advice_evict(cue_path_async.clone());
                                inner.frames_cache.get_or_init(cue_path_async, 1, |_| cache_data);
                            }

                            out_bytes.into_inner()
                        };

                        match result {
                            Ok(bytes) => TrackFSFileHandle::InMemory(Arc::new(bytes)),
                            Err(e) => {
                                tracing::error!(
                                    "Error while processing {} track {track_id}: {e:?}",
                                    cue_path.display()
                                );
                                reply.error(EINVAL);
                                return;
                            }
                        }
                    } else if ext == Some(OsStr::new("wav")) {
                        let Ok(file) = tokio::fs::File::open(&file_info.file_path).await else {
                            reply.error(EIO);
                            return;
                        };
                        let mut reader = tokio::io::BufReader::new(file);
                        let Some(track_info) =
                            file_info.tracks_info.iter().find(|ti| ti.track_id == track_id)
                        else {
                            tracing::error!(
                                "Error while processing {} track {track_id}: invalid track id",
                                cue_path.display()
                            );
                            reply.error(EINVAL);
                            return;
                        };
                        let next_sample_pos = file_info
                            .tracks_info
                            .iter()
                            .find(|ti| ti.track_id == track_id + 1)
                            .map(|ti| ti.sample_pos)
                            .unwrap_or(file_info.total_samples);

                        let wav_info = file_info.wav_info.as_ref().unwrap();
                        let sample_data_size = (next_sample_pos as u32
                            - track_info.sample_pos as u32)
                            * wav_info.format.channels as u32
                            * wav_info.format.bits_per_sample as u32
                            / 8;
                        let sample_offset = track_info.sample_pos as u32
                            * wav_info.format.channels as u32
                            * wav_info.format.bits_per_sample as u32
                            / 8;

                        let mut mem_file = vec![];
                        mem_file.extend(wav_info.to_riff_chunk(sample_data_size));
                        mem_file.extend(wav_info.format.into_bytes());
                        for (chunk_offset, chunk_size) in
                            wav_info.others.iter().filter(|(offset, _)| *offset < wav_info.data.0)
                        {
                            let mut buf = vec![0; *chunk_size as usize + 8];
                            if reader.seek(SeekFrom::Start(*chunk_offset as u64)).await.is_err() {
                                reply.error(EIO);
                                return;
                            }
                            if reader.read_exact(&mut buf).await.is_err() {
                                reply.error(EIO);
                                return;
                            }
                            mem_file.extend(buf);
                        }

                        let mut buf = vec![0; sample_data_size as usize];
                        if reader
                            .seek(SeekFrom::Start((wav_info.data.0 + 8 + sample_offset) as u64))
                            .await
                            .is_err()
                        {
                            reply.error(EIO);
                            return;
                        }
                        if reader.read_exact(&mut buf).await.is_err() {
                            reply.error(EIO);
                            return;
                        }
                        mem_file.extend(wav_info.to_data_header(sample_data_size));
                        mem_file.extend(buf);

                        for (chunk_offset, chunk_size) in
                            wav_info.others.iter().filter(|(offset, _)| *offset > wav_info.data.0)
                        {
                            let mut buf = vec![0; *chunk_size as usize + 8];
                            if reader.seek(SeekFrom::Start(*chunk_offset as u64)).await.is_err() {
                                reply.error(EIO);
                                return;
                            }
                            if reader.read_exact(&mut buf).await.is_err() {
                                reply.error(EIO);
                                return;
                            }
                            mem_file.extend(buf);
                        }

                        TrackFSFileHandle::InMemory(Arc::new(mem_file))
                    } else {
                        reply.error(EINVAL);
                        return;
                    }
                }
                _ => {
                    reply.error(EINVAL);
                    return;
                }
            };
            let handle = Arc::new(handle);
            let handle_ptr = Arc::into_raw(handle) as *mut TrackFSFileHandle;
            tracing::debug!("opening file with ino {}", ino.clone());
            reply.opened(handle_ptr as u64, 0);
        });
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if offset < 0 {
            reply.error(EINVAL);
            return;
        }
        self.handle.spawn(async move {
            // SAFETY: `fh` was created by `Arc::into_raw` in `open`. Reconstruct a temporary
            // Arc from the raw pointer, clone it for this request, then forget the reconstructed
            // Arc to keep the original owner alive. The cloned Arc will be dropped at the end of
            // this async task, ensuring the object remains alive while used.
            let arc_handle: Arc<TrackFSFileHandle> = unsafe {
                let arc = Arc::from_raw(fh as *const TrackFSFileHandle);
                let arc_clone = arc.clone();
                std::mem::forget(arc);
                arc_clone
            };

            match &*arc_handle {
                TrackFSFileHandle::Passthrough(reader) => {
                    let mut buf = vec![0; size as usize];
                    let mut reader = reader.lock().await;

                    if reader.seek(SeekFrom::Start(offset as u64)).await.is_err() {
                        reply.error(EIO);
                        return;
                    }
                    let Ok(read_bytes) = reader.read(&mut buf).await else {
                        reply.error(EIO);
                        return;
                    };
                    tracing::debug!("reading Passthrough file with fh {}, size: {}", fh, size);
                    reply.data(&buf[..read_bytes]);
                }
                TrackFSFileHandle::InMemory(bytes) => {
                    if offset as usize > bytes.len() {
                        reply.data(&[]);
                    } else {
                        let end = offset as usize + size as usize;
                        let end = std::cmp::min(end, bytes.len());
                        tracing::debug!("reading InMemory file with fh {}, size: {}", fh, size);
                        reply.data(&bytes[offset as usize..end]);
                    }
                }
            }
            // arc_handle dropped here
        });
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let handle_ptr = fh as *const TrackFSFileHandle;
        tracing::debug!("releasing file with fh {}", fh);

        // Reconstruct the Arc from raw and drop it here. This will decrement the strong
        // reference count and free the inner object when the last reference is gone.
        unsafe {
            let arc = Arc::from_raw(handle_ptr);
            drop(arc);
        }
        tracing::debug!("release done file with fh {}", fh);
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        tracing::debug!("opening dir with ino {}", ino.clone());
        let separator = self.separator;
        let inner = self.inner.clone();
        self.handle.spawn(async move {
            let guard = inner.inode_table.read().await;
            let Some(entry) = guard.get(ino as usize) else {
                reply.error(ENOENT);
                return;
            };
            let path = entry.virtual_path.clone();
            let origin = entry.origin.clone();
            drop(guard);

            match origin {
                VirtualFSEntryOrigin::Directory(real_path) => {
                    match inner.update_dir_entries(separator, ino, path, real_path).await {
                        Ok(entries) => {
                            let handle = Arc::new(entries);
                            let handle_ptr = Arc::into_raw(handle) as *mut Vec<DirEntry>;
                            tracing::debug!("open done dir with ino {},", ino.clone());
                            reply.opened(handle_ptr as u64, 0);
                        }
                        Err(code) => {
                            reply.error(code);
                        }
                    }
                }
                _ => reply.error(ENOTDIR),
            }
        });
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        // SAFETY: `fh` was created with `Arc::into_raw` in `opendir`. Reconstruct a temporary
        // Arc, clone it for this call, then forget the reconstructed Arc to keep the original
        // raw pointer valid. Use the cloned Arc for iteration.
        let entries_arc: Arc<Vec<DirEntry>> = unsafe {
            let arc = Arc::from_raw(fh as *const Vec<DirEntry>);
            let arc_clone = arc.clone();
            std::mem::forget(arc);
            arc_clone
        };

        tracing::debug!("readdir with fh {}, entries len: {}", fh, entries_arc.len());
        for (i, dir_entry) in entries_arc.iter().skip(offset as usize).enumerate() {
            let failed = reply.add(
                dir_entry.inode,
                offset + i as i64 + 1,
                dir_entry.file_type,
                &dir_entry.name,
            );
            if failed {
                break;
            }
        }
        // entries_arc dropped here
        reply.ok();
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        tracing::debug!("releasing dir with fh {}, ino: {}", fh, ino);
        let handle_ptr = fh as *const Vec<DirEntry>;
        unsafe {
            let arc = Arc::from_raw(handle_ptr);
            drop(arc);
        }
        tracing::debug!("releasedir done with fh {}, ino: {}", fh, ino);
        reply.ok()
    }
}
