use std::{
    ffi::OsStr,
    io::{Cursor, Read, Seek, SeekFrom, Write},
    iter::zip,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, anyhow};
use cue_rw::CUEFile;
use futures::stream::StreamExt;
use itertools::Itertools;
use num_rational::{Rational32, Rational64};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt},
    task::spawn_blocking,
};

// 模块说明：
// - 命名约定：
//   - 采样位置结尾用 `_pos`（单位：samples），字节偏移用 `_offset`，帧大小用 `frame_size(s)`；
//   - 集合命名使用复数；布尔命名使用谓语，如 `cache_valid`。
// - 本模块职责：读取/解析 FLAC 元数据、计算帧与采样位置、按 CUE 分轨写出、回写 StreamInfo。
// - 关键路径均采用块级中文注释，避免逐行注释带来的臃肿。
use crate::{
    flac::{self, FlacBlockingStrategy, FlacFrame, FlacFrameMetadata, FlacFramePosition, FlacMetadataBlock, FlacMetadataBlockContent, FlacMetadataBlockType, FlacParseError, StreamInfoBlock, VorbisCommentBlock},
    libflac_wrapper::{FlacDecoder, FlacEncoder, SeekableRead},
    wav::WavInfo,
};

#[derive(Clone, Debug)]
pub struct TrackInfo {
    pub track_id: usize,
    /// The start position of samples in this file
    pub sample_pos: u64,
}

#[derive(Clone, Debug)]
pub struct CUEInfo {
    pub cue_name: PathBuf,
    pub passthrough_files: Vec<FileInfo>,
    pub files_info: Vec<FileInfo>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct FileInfo {
    pub file_path: PathBuf,
    pub cue: Arc<CUEFile>,
    pub tracks_info: Vec<TrackInfo>,
    pub sample_rate: u32,
    pub total_samples: u64,
    pub channels: u8,
    pub bits_per_sample: u8,
    pub wav_info: Option<WavInfo>,
}

pub async fn process_flac_embedded_cue(
    flac_path: impl AsRef<Path>,
) -> anyhow::Result<Option<CUEInfo>> {
    let audio_info = audio_preprocess(flac_path).await?;
    if audio_info.embedded_cue.is_some() {
        let cue_name = audio_info.path.clone();
        let files_info = process_cue_helper(None, None::<PathBuf>, vec![(0, audio_info)])?;

        Ok(Some(CUEInfo { cue_name, passthrough_files: vec![], files_info }))
    } else {
        Ok(None)
    }
}

pub async fn process_cue(cue_path: impl AsRef<Path>) -> anyhow::Result<CUEInfo> {
    let cue_file = tokio::fs::File::open(cue_path.as_ref()).await?;
    let mut cue_reader = tokio::io::BufReader::new(cue_file);
    let cue = read_cue(&mut cue_reader).await?;
    //tracing::info!("process_cue the cue is {:?}",cue,);
    let cue = Arc::new(cue);

    let cue_file_tracks = cue
        .tracks
        .iter()
        .enumerate()
        .map(|(tid, (i, _))| (tid, i))
        .chunk_by(|(_, i)| **i)
        .into_iter()
        .map(|(i, chunks)| {
            let chunks = chunks.into_iter().map(|(tid, _)| tid).collect::<Vec<_>>();
            (i, chunks.len(), chunks)
        })
        .collect::<Vec<_>>();
    let passthrough_ids = cue_file_tracks
        .into_iter()
        .filter(|(_, count, _)| *count == 1)
        .map(|(i, _, tracks_id)| (i, tracks_id))
        .collect::<Vec<_>>();
    let passthrough_files = passthrough_ids
        .iter()
        .map(|(id, tracks_id)| {
            let mut file_path = cue_path.as_ref().to_path_buf();
            file_path.pop();
            file_path.push(&cue.files[*id]);

            let tracks_info = tracks_id
                .iter()
                .map(|track_id| TrackInfo { track_id: *track_id, sample_pos: 0 })
                .collect();

            FileInfo {
                file_path,
                cue: cue.clone(),
                tracks_info,
                // The following flac info are only used for virtual file size estimation and thus
                // do not matter for passthrough files
                sample_rate: 0,
                total_samples: 0,
                channels: 0,
                bits_per_sample: 0,
                wav_info: None,
            }
        })
        .collect::<Vec<_>>();

    let cue_path_async = cue_path.as_ref().to_path_buf();
    let audio_infos: Vec<anyhow::Result<_>> = futures::stream::iter(
        cue.files
            .iter()
            .enumerate()
            .filter(|(id, _)| !passthrough_ids.iter().any(|(file_id, _)| id == file_id)),
    )
    .map(|(id, flac_name)| {
        let cue_path_async = cue_path_async.clone();
        async move {
            let mut path = cue_path_async;
            path.pop();
            path.push(flac_name);
            Ok((id, audio_preprocess(path).await?))
        }
    })
    .buffer_unordered(num_cpus::get())
    .boxed()
    .collect()
    .await;
    let audio_infos = audio_infos.into_iter().collect::<anyhow::Result<Vec<_>>>()?;

    let files_info = process_cue_helper(Some(cue), Some(cue_path), audio_infos)?;

    Ok(CUEInfo { cue_name: cue_path_async, passthrough_files, files_info })
}

fn process_cue_helper(
    cue: Option<Arc<CUEFile>>,
    cue_path: Option<impl AsRef<Path>>,
    audio_infos: Vec<(usize, AudioBasicInfo)>,
) -> anyhow::Result<Vec<FileInfo>> {
    audio_infos
        .into_iter()
        .map(|(file_id, audio_info)| {
            let embedded = audio_info.embedded_cue.is_some();
            let cue = match audio_info.embedded_cue {
                Some(cue) => Arc::new(cue),
                None => cue.as_ref().unwrap().clone(),
            };

            let tracks_info = parse_cue_tracks_info(&cue, &file_id, audio_info.sample_rate)
                .ok_or_else(|| anyhow!("CUE 文件内容非法或不一致：无法定位 INDEX 01"))?;

            Ok(FileInfo {
                file_path: if embedded {
                    audio_info.path
                } else {
                    let mut file_path = cue_path.as_ref().unwrap().as_ref().to_path_buf();
                    file_path.pop();
                    file_path.push(&cue.files[file_id]);
                    file_path
                },
                cue,
                tracks_info,
                sample_rate: audio_info.sample_rate,
                total_samples: audio_info.total_samples,
                channels: audio_info.channels,
                bits_per_sample: audio_info.bits_per_sample,
                wav_info: audio_info.wav_info,
            })
        })
        .collect()
}

fn parse_cue_tracks_info( // 从 CUE 中解析出给定文件的所有曲目，并计算每个曲目的 INDEX 01 采样起点
    cue: &CUEFile, // CUE 对象：包含所有文件与曲目描述
    file_id: &cue_rw::FileID, // 目标文件在 CUE 中的文件编号（通常与 cue.files 的下标对应）
    sample_rate: u32, // 采样率：用于把时间（秒）换算为采样数
) -> Option<Vec<TrackInfo>> { // 成功返回该文件所有曲目的 TrackInfo 列表；若数据不完整则返回 None
    cue.tracks // 从全部曲目中
        .iter() // 以只读方式迭代
        .enumerate() // 带上曲目在 tracks 向量中的索引（tid）
        .filter(|(_, (i, _))| i == file_id) // 仅保留属于目标文件的曲目（曲目元组为 (file_id, Track)）
        .map(|(tid, (_, t))| { // 对每个曲目，计算该曲目的采样起点
            let (_, ts) = t.indices.iter().find(|(id, _)| *id == 1)?; // 在 Track.indices 里找到 INDEX 01（必需）
            let sample_pos = { // 将 INDEX 01 的时间换算为采样位置
                let seconds = Rational32::from(*ts); // ts 为 cue_rw::Time（实现了 Into<Rational32>），先提为有理数秒
                let seconds = Rational64::new(*seconds.numer() as i64, *seconds.denom() as i64); // 提升到 64 位有理数，减少溢出风险
                let samples = seconds * Rational64::from(sample_rate as i64); // 采样数 = 秒 × 采样率
                samples.to_integer() as u64 // 取整（向下取整）得到起始采样位置
            };
            Some(TrackInfo { track_id: tid, sample_pos }) // 返回该曲目的 TrackInfo 结构
        })
        .collect() // 若任何一个曲目缺失 INDEX 01 会导致对应 map 返回 None，从而整体返回 None
}

#[derive(Default)]
struct AudioBasicInfo {
    path: PathBuf,
    embedded_cue: Option<CUEFile>,
    sample_rate: u32,
    total_samples: u64,
    channels: u8,
    bits_per_sample: u8,
    wav_info: Option<WavInfo>,
}

/// 读取音频基础信息：
/// - FLAC：读取 StreamInfo、可能的内嵌 CUE（VorbisComment.cuesheet/CUESHEET）；
/// - WAV：读取 WavInfo；
/// - 其他扩展名：报错。
async fn audio_preprocess(file_path: impl AsRef<Path>) -> anyhow::Result<AudioBasicInfo> { // 读取音频基础信息（支持 FLAC/WAV），并探测内嵌 CUE
    let file = tokio::fs::File::open(file_path.as_ref()).await?; // 异步打开文件
    let mut reader = tokio::io::BufReader::new(file); // 包装为带缓冲的异步 Reader

    let extension = file_path.as_ref().extension(); // 取扩展名来判断文件类型
    if extension == Some(OsStr::new("flac")) { // 情况一：FLAC 文件
        let mut flac_header = [0; 4]; // 用于存放文件头 4 字节
        reader.read_exact(&mut flac_header).await?; // 读取头部
        if &flac_header != b"fLaC" { // 校验签名
            anyhow::bail!("invalid flac header"); // 非合法 FLAC，直接返回错误
        }

        // 读取全部 metadata blocks（调用前已越过 fLaC 头）
        let metadata_blocks = get_metadata_blocks(&mut reader).await?; // 可能返回 FlacParseError

        // 尝试从 VorbisComment 中提取内嵌 CUE（兼容键名 cuesheet/CUESHEET）
        let mut embedded_cue = None; // 若找到并可解析为 CUEFile，则写入此处
        let vorbis_comment =
            metadata_blocks.iter().find(|b| b.block_type == FlacMetadataBlockType::VorbisComment); // 查找 VorbisComment 块
        if let Some(FlacMetadataBlockContent::VorbisComment(vorbis_comment)) = // 若存在，提取其内容引用
            vorbis_comment.map(|b| &b.content)
        {
            if let Some(cue_str) = vorbis_comment.get(&String::from("cuesheet")) { // 优先小写键
                if let Ok(cue) = CUEFile::try_from(cue_str.as_str()) { // 可解析则记录
                    embedded_cue = Some(cue);
                }
            } else if let Some(cue_str) = vorbis_comment.get(&String::from("CUESHEET")) { // 兼容大写键
                if let Ok(cue) = CUEFile::try_from(cue_str.as_str()) {
                    embedded_cue = Some(cue);
                }
            }
        }

        // 提取 StreamInfo，缺失则认为是非法 FLAC
        let stream_info =
            get_stream_info(&metadata_blocks).ok_or_else(|| anyhow!("Invalid flac file"))?;
        let sample_rate = stream_info.get_sample_rate(); // 采样率
        let total_samples = stream_info.sample_count; // 总采样数
        let channels = stream_info.get_channels(); // 声道数
        let bits_per_sample = stream_info.get_bits(); // 位深

        Ok(AudioBasicInfo { // 组装并返回 FLAC 的基础信息（含可选 embedded_cue）
            path: file_path.as_ref().to_path_buf(), // 文件路径
            embedded_cue, // 可能存在的内嵌 CUE
            sample_rate, // 采样率
            total_samples, // 总样本数
            channels, // 声道数量
            bits_per_sample, // 位深
            wav_info: None, // FLAC 不附带 WavInfo
        })
    } else if extension == Some(OsStr::new("wav")) { // 情况二：WAV 文件
        let wav_info = WavInfo::read_wav(&mut reader).await?; // 解析 WAV 的 RIFF 结构与 fmt/data 区等

        Ok(AudioBasicInfo { // 组装并返回 WAV 的基础信息
            path: file_path.as_ref().to_path_buf(), // 文件路径
            embedded_cue: None, // WAV 不在此处处理 embedded CUE（如有通常在 LIST/adtl 等，当前实现不解析）
            sample_rate: wav_info.format.sample_rate, // 采样率
            total_samples: wav_info.total_samples(), // 通过数据区长度/对齐计算总样本数
            channels: wav_info.format.channels as _, // 声道数
            bits_per_sample: wav_info.format.bits_per_sample as _, // 位深
            wav_info: Some(wav_info), // 附带完整的 WAV 解析结果
        })
    } else { // 其他扩展名：不支持
        anyhow::bail!(
            "Invalid file for processing (not flac or wav): {}",
            file_path.as_ref().display()
        ) // 返回错误，包含路径
    }
}

/// 从 metadata blocks 中取出 StreamInfo 块的引用。
fn get_stream_info(blocks: &[FlacMetadataBlock]) -> Option<&StreamInfoBlock> {
    blocks.iter().find(|b| b.block_type == FlacMetadataBlockType::StreamInfo).and_then(|b| {
        if let FlacMetadataBlockContent::StreamInfo(ref content) = b.content {
            Some(content)
        } else {
            None
        }
    })
}

/// 过滤掉不参与目标轨输出的 metadata（保留 StreamInfo、Vorbis、Picture 等）
fn filter_nonessential_metadata(src: &[FlacMetadataBlock]) -> Vec<FlacMetadataBlock> {
    src.iter()
        .filter(|block| {
            block.block_type != FlacMetadataBlockType::SeekTable
                && block.block_type != FlacMetadataBlockType::CUESheet
        })
        .cloned()
        .collect::<Vec<_>>()
}

fn process_metadata( // 基于 CUE 信息填充（或更新）目标轨的 Vorbis 评论字段
    vorbis_comment: &mut VorbisCommentBlock, // 需要写入的 VorbisComment 块（可变引用）
    cue: &CUEFile, // 整体 CUE：包含专辑级与曲目级信息
    track_id: usize, // 目标曲目在 cue.tracks 中的索引
    track_total: usize, // 总曲目数（用于 TRACKTOTAL）
) {
    vorbis_comment.user_comments.remove("cuesheet"); // 去除可能存在的内嵌 CUE 文本键（小写）
    vorbis_comment.user_comments.remove("CUESHEET"); // 同理，去除大写键

    let Some((_, cue_track)) = cue.tracks.get(track_id) else { // 获取目标曲目的 CUE 信息
        return; // 越界保护：不做修改直接返回
    };

    // 专辑级信息：
    vorbis_comment.add_vorbis_comment("ALBUMARTIST", cue.performer.clone()); // 专辑艺人（整张专辑的演出者）
    vorbis_comment.add_vorbis_comment("ALBUM", cue.title.clone()); // 专辑标题
    if let Some(ref catalog) = cue.catalog { // 可选的目录号/条目号
        vorbis_comment.add_vorbis_comment("CATALOG", catalog.clone());
    }

    // 其他 REM 字段示例：DATE（常见的发行年份/日期）
    if let Some(date) = cue
        .comments
        .iter()
        .find(|line| line.starts_with("REM DATE"))
        .and_then(|line| line.split_once("REM DATE ").map(|(_, date)| date))
    {
        vorbis_comment.add_vorbis_comment("DATE", date.to_string()); // 写入日期字符串
    }

    // 曲目级信息：
    vorbis_comment.add_vorbis_comment("TITLE", cue_track.title.clone()); // 曲目标题
    let performer = cue_track // 曲目艺人：若曲目未单独声明，回退到专辑级 performer
        .performer
        .as_ref()
        .unwrap_or(&cue.performer)
        .clone();
    vorbis_comment.add_vorbis_comment("ARTIST", performer); // 写入 ARTIST
    if let Some(isrc) = cue_track.isrc.as_ref() { // 曲目标识码（若提供）
        vorbis_comment.add_vorbis_comment("ISRC", isrc.clone());
    }
    vorbis_comment.add_vorbis_comment("TRACKNUMBER", (track_id + 1).to_string()); // 轨号（1 基）
    vorbis_comment.add_vorbis_comment("TRACKTOTAL", track_total.to_string()); // 总轨数
}

pub struct FlacCacheData {
    frame_sizes: FileFrameSizes,
    frames_sample_pos: Vec<u64>,
    tracks_head_tail_frames: Vec<EncodedTrackFrames>,
    mtime: i64,
}

/// 将一个含有 CUE 的整轨 FLAC 切分并写出指定轨道。
/// 输入：目标文件路径、目标 track_id、CUE、该文件内所有轨的采样起点（tracks_info）、可选缓存。
/// 产出：将目标轨以 FLAC 写入提供的 writer，并在必要时返回新的缓存。
/// 步骤：
/// 1) 读取并解析元数据（StreamInfo 必须存在），过滤非必要 metadata；
/// 2) 若缓存失效，则在阻塞线程扫描帧大小与每帧起始采样位置，并生成每轨的首/尾拼接帧；
/// 3) 根据 track_id 计算当前轨的起止采样位置；
/// 4) 写出 fLaC 头与 metadata，按采样位置选择帧，写入首帧+主体帧+尾帧；
/// 5) 回写 StreamInfo（更新 block size、样本总数、清空 md5）。
/// 原理：预扫描帧边界与每帧起始采样位置；为每个轨道构造首/尾拼接帧；
///      过滤与重写元数据后，按采样位置选择并写出属于当前轨道的帧，最后回写 StreamInfo。
// 读取并校验元数据（跳过fLaC头），返回元数据块与StreamInfo
async fn read_and_validate_metadata( // 读取并校验 FLAC 元数据（跳过 fLaC 头），返回所有块和 StreamInfo
    async_reader: &mut (impl AsyncRead + AsyncSeekExt + Unpin), // 异步 reader（必须可 seek），通常是 BufReader<File>
    file_path: &Path, // 仅用于错误上下文：报错时包含文件路径
) -> anyhow::Result<(Vec<FlacMetadataBlock>, StreamInfoBlock)> { // 成功时返回 (metadata_blocks, stream_info)
    // 跳过文件开头 4 字节的魔数 "fLaC"，后面紧跟的就是 metadata blocks
    async_reader.seek(SeekFrom::Start(4)).await?; // 若失败，直接向上传播错误
    // 读取所有 metadata blocks，直到遇到 is_last=true 的块
    let metadata_blocks =
        get_metadata_blocks(async_reader).await.context("读取 FLAC 元数据块失败")?; // 增加上下文便于定位
    // 从 blocks 中获取 StreamInfo（按规范必须存在且为首块）。若缺失则认为文件无效
    let stream_info = get_stream_info(&metadata_blocks)
        .cloned() // 这里克隆出一份独立的 StreamInfoBlock（后续可能跨线程使用）
        .ok_or_else(|| anyhow!("无效的 FLAC：缺少 StreamInfo，路径={}", file_path.display()))?; // 构造带路径信息的错误
    Ok((metadata_blocks, stream_info)) // 返回元数据块集合与 StreamInfo
}

// 在阻塞线程中扫描帧信息与生成每轨首尾帧

// 更新/创建 Vorbis 评论块并返回新的元数据块列表
fn build_or_update_vorbis( // 确保存在 VorbisComment 块并填充目标轨的元信息，返回更新后的元数据块列表
    mut metadata_blocks: Vec<FlacMetadataBlock>, // 输入：当前的 metadata 列表（通常已过滤过无关块）
    cue: &CUEFile, // CUE 信息（用于填充专辑/曲目标签）
    track_id: usize, // 目标曲目的索引
    track_total: usize, // 总曲数
) -> Vec<FlacMetadataBlock> { // 输出：更新后的 metadata 列表
    // 在现有元数据中查找 VorbisComment 块；若不存在则创建一个新的并追加到末尾
    let FlacMetadataBlock {
        content: FlacMetadataBlockContent::VorbisComment(vorbis_comment), ..
    } = (match metadata_blocks
        .iter_mut()
        .find(|b| b.block_type == FlacMetadataBlockType::VorbisComment)
    {
        Some(block) => block, // 已存在：直接使用该块
        None => { // 不存在：创建新的 VorbisComment 块并追加
            let mut new_block = FlacMetadataBlock {
                is_last: false, // 先标 false，后续根据位置调整
                block_type: FlacMetadataBlockType::VorbisComment,
                content: FlacMetadataBlockContent::VorbisComment(VorbisCommentBlock::new()),
            };

            if metadata_blocks.len() == 1 { // 边界：仅有一个块（通常是 StreamInfo）时
                // We need to ensure the stream info block is the first one
                metadata_blocks[0].is_last = false; // 确保第一个块不是最后块
                new_block.is_last = true; // 新增的 Vorbis 作为最后块
            }

            metadata_blocks.push(new_block); // 追加到列表末尾
            metadata_blocks.last_mut().unwrap() // 取到刚追加块的可变引用
        }
    })
    else {
        unreachable!() // 上面两种分支必定返回某个可变引用
    };

    // 基于 CUE 内容填充 VorbisComment 的标签字段
    process_metadata(vorbis_comment, cue, track_id, track_total);

    // 为了满足 is_last 的语义，按 is_last 排序，使 is_last=true 的块排在最后
    metadata_blocks.sort_by_key(|block| block.is_last);
    metadata_blocks // 返回更新列表
}

// 在阻塞线程完成最终写入，返回编码后的字节
async fn write_track_bytes_blocking( // 在线程池中执行最终写入（seek + 写帧），返回编码好的字节
    file_path: &Path, // 源 FLAC 路径：阻塞任务会重新打开该文件
    start_offset: u64, // 源文件中帧区的起始字节偏移（从这里开始读取帧）
    metadata_blocks: Vec<FlacMetadataBlock>, // 已准备好的 metadata 列表（已过滤/更新过）
    track_start_pos: u64, // 当前轨起始采样位置
    next_track_start_pos: Option<u64>, // 下一轨起始采样位置（若无则 None）
    heads: &[Vec<u8>], // 预编码的首帧字节序列
    tails: Option<&Vec<Vec<u8>>>, // 预编码的尾帧字节序列（可选）
    frame_sizes: &[u64], // 源 FLAC 每帧大小
    frames_sample_pos: &[u64], // 源 FLAC 每帧起始采样位置
) -> anyhow::Result<Vec<u8>> { // 返回：完整的目标轨字节序列（含 fLaC 头与 metadata 与帧）
    let file_path_owned = file_path.to_path_buf(); // 克隆路径，move 进阻塞闭包
    let metadata_blocks_clone = metadata_blocks.clone(); // 克隆 metadata，避免所有权移动问题
    let frame_sizes = frame_sizes.to_vec(); // 拷贝帧大小数组供闭包使用
    let frames_sample_pos = frames_sample_pos.to_vec(); // 拷贝帧起始采样位置数组
    let heads = heads.to_vec(); // 拷贝首帧字节
    let tails = tails.cloned(); // 拷贝可选尾帧（Option）

    let handle = spawn_blocking(move || -> anyhow::Result<Vec<u8>> { // 在线程池中执行阻塞任务
        let mut reader_block: Box<dyn SeekableRead> = // 在阻塞线程内重新打开源文件，避免跨线程传递句柄
            Box::new(std::io::BufReader::new(std::fs::File::open(&file_path_owned)?));
        reader_block.seek(SeekFrom::Start(start_offset))?; // 定位到帧区起点，后续读取帧

        let mut out_buf = std::io::Cursor::new(Vec::<u8>::new()); // 输出写到内存缓冲（Cursor<Vec<u8>>）
        write_track( // 复用核心同步写函数，负责写入 fLaC、metadata、首帧/主体帧/尾帧并回写 StreamInfo
            &mut reader_block, // 源 reader（已位于帧区）
            &mut out_buf, // 目标 writer（内存缓冲）
            &metadata_blocks_clone, // 元数据块
            track_start_pos, // 轨起点
            next_track_start_pos, // 下一轨起点
            &heads, // 首帧
            tails.as_ref(), // 尾帧（Option）
            &frame_sizes, // 帧大小
            &frames_sample_pos, // 每帧起始采样位置
        )?; // 写入过程中可能返回错误，向外传播

        Ok(out_buf.into_inner()) // 写完后取出底层 Vec<u8> 并返回
    });

    Ok(handle.await??) // 等待阻塞任务完成；两层 ? 先拆 JoinError，再拆 anyhow::Error
}

pub async fn process_file( // 将单个源 FLAC 按 CUE 切分并把指定轨道写入给定 writer（支持缓存）
    writer: &mut (impl Write + Seek), // 输出目标：实现 Write + Seek 的同步 writer（例如 Cursor<Vec<u8>> 或文件）
    file_path: impl AsRef<Path>, // 源 FLAC 文件路径
    track_id: usize, // 目标轨道在 CUE 中的轨号索引（0 基）
    cue: &CUEFile, // 整体 CUE 信息（用于填充 Vorbis 评论等）
    tracks_info: &[TrackInfo], // 当前源文件中所有属于它的 TrackInfo（包括每轨起始采样位置）
    cache_data: Option<&FlacCacheData>, // 可选缓存（若 mtime 未变可复用帧大小、采样位置、首尾帧等）
) -> anyhow::Result<Option<FlacCacheData>> { // 返回：若构建了新缓存则 Some，否则 None
    // 说明：本函数是异步的，但重 CPU/IO 的工作会用 spawn_blocking 在线程池中执行，避免阻塞 Tokio 运行时。
    // 为了避免跨 .await 借用非 Send 的 libflac 封装，这些对象仅在阻塞闭包内部创建与使用。
    let file = tokio::fs::File::open(file_path.as_ref()).await?; // 异步打开源 FLAC 文件
    let mtime = file.metadata().await?.mtime(); // 读取文件的修改时间（用于缓存有效性判断）
    let mut async_reader = tokio::io::BufReader::new(file); // 异步缓冲 reader

    let (raw_metadata_blocks, stream_info) = // 读取并校验元数据（跳过 fLaC 头、解析 blocks、取 StreamInfo）
        read_and_validate_metadata(
            &mut async_reader, // 异步带缓冲 reader（已打开的源 FLAC 文件）
            file_path.as_ref(), // 仅用于错误上下文（用于在报错信息中展示路径）
        )
        .await?;

    // 说明：后续会有多次阻塞型操作（标准文件 IO、libflac 解码/编码），应放入 spawn_blocking 中执行。
    // 我们收集 tracks 的起点，判断缓存是否可用，若缓存无效则在阻塞线程里扫描帧并构造首/尾帧。

    let tracks_sample_pos = tracks_info.iter().map(|ti| ti.sample_pos).collect::<Vec<_>>(); // 当前文件内每轨起点

    // 缓存是否有效：若传入了缓存，且缓存记录的 mtime 不小于当前文件 mtime，则认为可用
    let cache_valid = cache_data.is_some() && cache_data.unwrap().mtime >= mtime; // 注意：>= 以兼容同一时间戳
    // 若缓存可用则直接取缓存；否则在阻塞线程中重新扫描：帧区偏移、每帧大小、每帧采样起点、每轨首尾帧
    let (start_offset, frame_sizes, frames_sample_pos, tracks_head_tail_frames): (
        u64,
        Vec<u64>,
        Vec<u64>,
        Vec<EncodedTrackFrames>,
    ) = if cache_valid { // 使用缓存路径
        let cache = cache_data.unwrap(); // 解引用缓存引用
        (
            cache.frame_sizes.start_offset, // 帧区起始偏移
            cache.frame_sizes.frame_sizes.clone(), // 每帧大小列表
            cache.frames_sample_pos.clone(), // 每帧起始采样位置
            cache.tracks_head_tail_frames.clone(), // 每轨的首/尾拼接帧
        )
    } else { // 无缓存或缓存失效：阻塞线程扫描与构建
        let file_path_owned = file_path.as_ref().to_path_buf(); // 复制路径到阻塞闭包
        let stream_info = stream_info.clone(); // 克隆 StreamInfo 到闭包
        let tracks_sample_pos_cloned = tracks_sample_pos.clone(); // 克隆起点数组

        let blocking_handle = spawn_blocking( // 在线程池执行阻塞任务：扫描帧、计算采样起点、构造首/尾帧
            move || -> anyhow::Result<(u64, Vec<u64>, Vec<u64>, Vec<EncodedTrackFrames>)> {
                // 仅在阻塞环境中创建本地解码器/编码器，避免跨 await 的非 Send 借用
                let mut local_decoder = FlacDecoder::new();
                let mut local_encoder = FlacEncoder::new();

                // 1) 扫描帧大小与帧区起点
                let file_std = std::fs::File::open(&file_path_owned).with_context(|| {
                    format!("打开 FLAC 文件失败（阻塞线程）：{}", file_path_owned.display())
                })?;
                let buf_reader = std::io::BufReader::new(file_std);
                let frame_index =
                    get_frame_sizes(
                        &mut local_decoder, // 局部解码器（仅在阻塞线程内使用）
                        buf_reader, // 标准同步 BufReader<File>，用于阻塞扫描帧
                        file_path_owned.display(), // 仅用于日志标识（路径显示）
                    )?; // 返回 reader 与帧大小索引

                let start_offset = frame_index.data.start_offset; // 帧区字节起点
                let frame_sizes_scanned = frame_index.data.frame_sizes; // 每帧大小

                // 2) 计算每帧对应的起始采样位置（用于后续按照采样边界判断是否越轨）
                let mut reader2 = std::io::BufReader::new(
                    std::fs::File::open(&file_path_owned).with_context(|| {
                        format!("重新打开 FLAC 以扫描帧失败：{}", file_path_owned.display())
                    })?,
                );
                reader2.seek(SeekFrom::Start(start_offset))?; // 从帧区起点开始逐帧扫描
                let frames_sample_pos_scanned = FlacFrame::scan_frames(
                    &mut reader2, // 重新打开并 seek 到帧区起点的阻塞 reader
                    frame_sizes_scanned.iter().copied(), // 按已知的每帧大小顺序扫描
                    &stream_info, // 提供解析帧 header 所需的 StreamInfo（声道、位深、采样率）
                )
                .map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::Other, format!("flac parse: {}", e))
                })?; // 将 flac::Error 转为 io::Error 以复用 anyhow 上下文

                // 3) 为每个轨生成“首帧/尾帧”的拼接编码帧（保持无缝边界）
                let encoded_heads_tails = encode_track_head_tail_frames(
                    &mut local_decoder, // 本地解码器（阻塞线程内使用）
                    &mut local_encoder, // 本地编码器（阻塞线程内使用）
                    &stream_info, // 源文件 StreamInfo（编码参数）
                    frame_index.reader, // get_frame_sizes 返回的 reader（已读到末尾，重新用于解码）
                    &tracks_sample_pos_cloned, // 每个轨的起始采样位置（用于生成首/尾片段）
                    &frames_sample_pos_scanned, // 每个帧的采样起点（用于定位切分帧）
                    file_path_owned.display(), // 日志标识
                )?;

                Ok((
                    start_offset, // 帧区起始偏移（字节）
                    frame_sizes_scanned, // 每帧大小（字节）
                    frames_sample_pos_scanned, // 每帧起始采样位置
                    encoded_heads_tails.data, // 每轨的首/尾拼接帧（已编码为字节）
                )) // 将扫描与构造的结果一并返回
            },
        );

        let (
            start_offset_scanned, // 扫描得到的帧区起始偏移
            frame_sizes_scanned, // 扫描得到的每帧大小
            frames_sample_pos_scanned, // 扫描得到的每帧起始采样位置
            tracks_head_tail_frames_scanned, // 生成的每轨首/尾拼接帧（编码字节）
        ) = blocking_handle.await??; // 等待阻塞任务结束并传播内部错误 // 等待阻塞任务结束并传播内部错误

        (
            start_offset_scanned, // 帧区起点（字节）
            frame_sizes_scanned, // 每帧大小（字节）
            frames_sample_pos_scanned, // 每帧起始采样位置
            tracks_head_tail_frames_scanned, // 每轨首/尾拼接帧（编码字节）
        ) // 将扫描结果带回到异步上下文
    };

    // 读取到的原始 metadata 进行精简：去掉 SeekTable、CUESheet 等对目标轨无必要的块
    let mut metadata_blocks = filter_nonessential_metadata(&raw_metadata_blocks);

    // 根据 track_id 确定该轨在本文件中的索引，并取到总轨数（用于 Vorbis 评论）
    let track_index_in_file = tracks_info
        .iter()
        .position(|t| t.track_id == track_id)
        .ok_or_else(|| anyhow!("Invalid track id"))?;
    let track_total = cue.tracks.len();

    // 计算当前轨起点与下一轨起点（若存在），并取出预编码好的首/尾帧
    let track_start_pos =
        *tracks_sample_pos.get(track_index_in_file).ok_or_else(|| anyhow!("Invalid track id"))?; // 当前轨在源文件中的起始采样位置
    let next_track_start_pos = tracks_sample_pos.get(track_index_in_file + 1).copied(); // 下一轨起始采样位置（若不存在则为 None，表示写到文件尾）
    let current_track_heads_tails = tracks_head_tail_frames
        .get(track_index_in_file) // 取出当前轨对应的首/尾拼接帧
        .ok_or_else(|| anyhow!("Invalid track id"))? // 索引越界即认为轨 id 非法
        .clone(); // 克隆一份，避免后续借用冲突

    // 基于 CUE 更新 VorbisComment（去除内嵌 CUE 文本键，填充专辑/曲目信息）
    metadata_blocks = build_or_update_vorbis(metadata_blocks, cue, track_id, track_total);

    // 最终写入（seek + 写帧）放入阻塞线程，避免阻塞异步运行时；把结果写入内存字节后再回主线程写给 writer
    let metadata_blocks_clone = metadata_blocks.clone(); // 克隆必须数据到阻塞任务（避免跨线程借用）
    let frame_sizes_clone = frame_sizes.clone(); // 克隆每帧大小列表（阻塞任务内读取写入需要）
    let frames_sample_pos_clone = frames_sample_pos.clone(); // 克隆每帧起始采样位置（用于边界判断）
    let current_track_heads_tails_clone = current_track_heads_tails.clone(); // 克隆本轨首/尾预编码帧
    let track_start_pos_copy = track_start_pos; // 复制数值型起点（实现 Copy）
    let next_track_start_pos_copy = next_track_start_pos; // 复制可选的下一轨起点（Option<u64> 实现 Copy）

    let encoded_bytes = write_track_bytes_blocking( // 在线程池中完成最终读写，返回编码好的字节
        file_path.as_ref(), // 源 FLAC 路径（在阻塞线程中重新打开）
        start_offset, // 帧区起始偏移（从此处开始读取源帧）
        metadata_blocks_clone, // 已过滤并更新过的 metadata（包含新 Vorbis 与 StreamInfo 回写所需信息）
        track_start_pos_copy, // 当前轨在源文件中的起始采样位置
        next_track_start_pos_copy, // 下一轨的起始采样位置（用于判断何时停止并写尾帧）
        &current_track_heads_tails_clone.head_frames, // 预编码好的“首帧”字节序列
        current_track_heads_tails_clone.tail_frames.as_ref(), // 可选的“尾帧”字节序列
        &frame_sizes_clone, // 源文件中每一帧的大小（字节），用于逐帧读取
        &frames_sample_pos_clone, // 源文件中每一帧的起始采样位置，用于边界判断
    )
    .await?;

    // 回到异步上下文，把内存中的结果拷贝到调用方提供的 writer 中
    // 若 writer 是内存 Cursor 则几乎不阻塞；若是文件，请确保是非阻塞或自己处理阻塞
    writer.write_all(&encoded_bytes)?;

    // 返回缓存：若之前使用了缓存（cache_valid=true）则无需返回；否则把新结果作为缓存给调用方
    Ok(if cache_valid {
        None
    } else {
        Some(FlacCacheData {
            frame_sizes: FileFrameSizes { start_offset: start_offset, frame_sizes },
            frames_sample_pos,
            tracks_head_tail_frames,
            mtime,
        })
    })
}
pub async fn process_file_for_size( // 计算单个源 FLAC 按 CUE 切分后指定轨道的大小
    file_path: impl AsRef<Path>, // 源 FLAC 文件路径
    track_id: usize, // 目标轨道在 CUE 中的轨号索引（0 基）
    tracks_info: &[TrackInfo],
    cue: &CUEFile, // 当前源文件中所有属于它的 TrackInfo（包括每轨起始采样位置）
) -> anyhow::Result<u64> { // 返回：轨道的总字节大小
    // 说明：本函数是异步的，但重 CPU/IO 的工作会用 spawn_blocking 在线程池中执行，避免阻塞 Tokio 运行时。
    let file = tokio::fs::File::open(file_path.as_ref()).await?; // 异步打开源 FLAC 文件
    let mut async_reader = tokio::io::BufReader::new(file); // 异步缓冲 reader

    let (raw_metadata_blocks, stream_info) = // 读取并校验元数据（跳过 fLaC 头、解析 blocks、取 StreamInfo）
        read_and_validate_metadata(
            &mut async_reader, // 异步带缓冲 reader（已打开的源 FLAC 文件）
            file_path.as_ref(), // 仅用于错误上下文（用于在报错信息中展示路径）
        )
        .await?;

    // 收集该文件中所有轨的起始采样位置
    let tracks_sample_pos = tracks_info.iter().map(|ti| ti.sample_pos).collect::<Vec<_>>();

    // 在阻塞线程中扫描帧大小、每帧起始采样位置，并生成每轨的首/尾帧编码结果
    let (start_offset, frame_sizes, frames_sample_pos, tracks_head_tail_frames): (
        u64,
        Vec<u64>,
        Vec<u64>,
        Vec<EncodedTrackFrames>,
    ) = {
        let file_path_owned = file_path.as_ref().to_path_buf();
        let stream_info = stream_info.clone();
        let tracks_sample_pos_cloned = tracks_sample_pos.clone();

        let blocking_handle = spawn_blocking(
            move || -> anyhow::Result<(u64, Vec<u64>, Vec<u64>, Vec<EncodedTrackFrames>, )> {
                let mut local_decoder = FlacDecoder::new();
                let mut local_encoder = FlacEncoder::new();

                // 1) 扫描帧大小与帧区起点
                let file_std = std::fs::File::open(&file_path_owned).with_context(|| {
                    format!("打开 FLAC 文件失败（阻塞线程）：{}", file_path_owned.display())
                })?;
                let buf_reader = std::io::BufReader::new(file_std);
                let frame_index = get_frame_sizes(&mut local_decoder, buf_reader, file_path_owned.display())?;
                let start_offset = frame_index.data.start_offset;
                let frame_sizes_scanned = frame_index.data.frame_sizes;

                // 2) 每帧起始采样位置
                let mut reader2 = std::io::BufReader::new(
                    std::fs::File::open(&file_path_owned).with_context(|| {
                        format!("重新打开 FLAC 以扫描帧失败：{}", file_path_owned.display())
                    })?,
                );
                reader2.seek(SeekFrom::Start(start_offset))?;
                let frames_sample_pos_scanned = FlacFrame::scan_frames(
                    &mut reader2,
                    frame_sizes_scanned.iter().copied(),
                    &stream_info,
                )
                .map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::Other, format!("flac parse: {}", e))
                })?;
                // 3) 为每轨生成首/尾拼接帧（仅用于计算大小）
                let encoded_heads_tails = encode_track_head_tail_frames(
                    &mut local_decoder,
                    &mut local_encoder,
                    &stream_info,
                    frame_index.reader,
                    &tracks_sample_pos_cloned,
                    &frames_sample_pos_scanned,
                    file_path_owned.display(),
                )?;

                Ok((
                    start_offset,
                    frame_sizes_scanned,
                    frames_sample_pos_scanned,
                    encoded_heads_tails.data,
                ))
            },
        );

        let (start_offset_scanned, frame_sizes_scanned, frames_sample_pos_scanned, tracks_head_tail_frames_scanned) =
            blocking_handle.await??;
        (
            start_offset_scanned,
            frame_sizes_scanned,
            frames_sample_pos_scanned,
            tracks_head_tail_frames_scanned,
        )
    };

    // 过滤掉不参与输出的 metadata，用于计算 metadata 大小
    let mut metadata_blocks = filter_nonessential_metadata(&raw_metadata_blocks);
    let track_total = cue.tracks.len();
        // 计算目标轨索引、起点与下一轨起点
    let track_index_in_file = tracks_info
        .iter()
        .position(|t| t.track_id == track_id)
        .ok_or_else(|| anyhow!("Invalid track id"))?;
    let track_start_pos = *tracks_sample_pos
        .get(track_index_in_file)
        .ok_or_else(|| anyhow!("Invalid track id"))?;
    let next_track_start_pos = tracks_sample_pos.get(track_index_in_file + 1).copied();

    // 取到该轨的首/尾帧编码
    let current_track_heads_tails = tracks_head_tail_frames
        .get(track_index_in_file)
        .ok_or_else(|| anyhow!("Invalid track id"))?
        .clone();
    metadata_blocks = build_or_update_vorbis(metadata_blocks, cue, track_id, track_total);

    let metadata_blocks_clone = metadata_blocks.clone(); // 克隆必须数据到阻塞任务（避免跨线程借用）
    let frame_sizes_clone = frame_sizes.clone(); // 克隆每帧大小列表（阻塞任务内读取写入需要）
    let frames_sample_pos_clone = frames_sample_pos.clone(); // 克隆每帧起始采样位置（用于边界判断）
    let current_track_heads_tails_clone = current_track_heads_tails.clone(); // 克隆本轨首/尾预编码帧
    let track_start_pos_copy = track_start_pos; // 复制数值型起点（实现 Copy）
    let next_track_start_pos_copy = next_track_start_pos; // 复制可选的下一轨起点（Option<u64> 实现 Copy）

    let le123 = write_track_bytes_blocking_for_size(
        file_path.as_ref(),
        start_offset,
        std::sync::Arc::new(metadata_blocks_clone),
        track_start_pos_copy,
        next_track_start_pos_copy,
        std::sync::Arc::new(current_track_heads_tails_clone.head_frames.clone()),
        current_track_heads_tails_clone
            .tail_frames
            .as_ref()
            .map(|v| std::sync::Arc::new(v.clone())),
        std::sync::Arc::new(frame_sizes_clone),
        std::sync::Arc::new(frames_sample_pos_clone),
    )
    .await?;



    let (metadata_size_bytes, head_size_bytes, tail_size_bytes,allblocksize,allmeteblocksize,) = le123;

    //let middle_frames_size_bytes = allblocksize as u64+ metadata_size_bytes as u64;

    //tracing::info!("大小：{}", metadata_size_bytes);
    // 打印统计信息
    //tracing::info!(
    //    "[尺寸统计] 首帧: {} B, 尾帧: {} B, Metadata: {} B, 中间帧: {} B,所有头帧的meta:{}",
    //    head_size_bytes, tail_size_bytes, metadata_size_bytes, allblocksize,allmeteblocksize
    //);

    // 计算总大小（包含 fLaC 头 4 字节 + metadata + 首帧 + 中间帧 + 尾帧）
    let total_size = 4u64
        + head_size_bytes as u64
        + allblocksize as u64
        + tail_size_bytes as u64
        +allmeteblocksize as u64
        +metadata_size_bytes as u64;

    Ok(total_size)
}

/// 读取所有 FLAC Metadata Blocks（调用前需已跳过 fLaC 头），直到遇到 is_last=true。
pub async fn get_metadata_blocks( // 读取所有 FLAC Metadata Blocks（调用前需已跳过 fLaC 头），直到遇到 is_last=true
    mut reader: impl AsyncRead + Unpin, // 异步 reader；不要求 Seek，这里按顺序读取 block
) -> Result<Vec<FlacMetadataBlock>, FlacParseError> { // 返回一个按顺序排列的块列表
    let mut metadata_blocks = vec![]; // 用于累积所有读到的块
    loop { // 连续读取，直到遇到最后一个块（is_last = true）
        let metadata_block = FlacMetadataBlock::read_block(&mut reader).await?; // 读取单个块（内部解析头与内容）
        let is_last = metadata_block.is_last; // 记录该块是否为最后一个

        metadata_blocks.push(metadata_block); // 累积到列表
        if is_last { // 若是最后一个块
            break; // 结束循环
        }
    }

    Ok(metadata_blocks) // 返回块列表
}

async fn read_cue(mut cue_reader: impl AsyncRead + Unpin) -> anyhow::Result<CUEFile> { // 读取并解析 CUE 文本（自动探测常见编码）
    let mut cue_bytes = Vec::new(); // 缓冲读取到的原始字节
    cue_reader.read_to_end(&mut cue_bytes).await?; // 读完整个输入流
    let mut cue_str = None; // 用于保存成功解码得到的字符串
    for encoding in [ // 逐个尝试以下常见编码（按优先级排列）
        encoding_rs::UTF_8,
        encoding_rs::GBK,
        encoding_rs::GB18030,
        encoding_rs::SHIFT_JIS,
        encoding_rs::BIG5,
        encoding_rs::UTF_16LE,
    ] {
        let (str, _, failed) = encoding.decode(&cue_bytes); // 用该编码尝试解码；failed 表示是否出现替换/失败
        if !failed { // 成功无替换
            cue_str = Some(str); // 记录解码得到的 Cow<str>
            break; // 不再尝试其他编码
        }
    }

    let cue_str = cue_str.ok_or_else(|| anyhow!("Invalid cue encoding"))?; // 若所有编码均失败，则报错
    Ok(CUEFile::try_from(&*cue_str)?) // 将字符串解析为 CUEFile 结构（可能返回 anyhow 错误）
}

pub struct RetWithReader<T> {
    pub reader: Box<dyn SeekableRead>,
    pub data: T,
}

pub struct FileFrameSizes {
    pub start_offset: u64,
    pub frame_sizes: Vec<u64>,
}

/// The start byte offset of flac frames, and frame sizes
pub fn get_frame_sizes( // 扫描并返回 FLAC 帧区起点与每帧大小（阻塞，供线程池调用）
    decoder: &mut FlacDecoder, // 解码器实例（阻塞环境内局部使用，不跨线程）
    mut reader: impl Read + Seek + 'static, // 源 reader（阻塞 IO），需支持 Read + Seek
    path_for_logging: impl ToString, // 仅用于解码器内部日志记录（源文件标识）
) -> std::io::Result<RetWithReader<FileFrameSizes>> { // 返回：带回原 reader（装箱）与帧索引数据
    reader.seek(SeekFrom::Start(0))?; // 确保从文件开头开始扫描
    let reader = Box::new(reader); // 装箱成 trait 对象，交给解码器持有
    decoder.init(reader, path_for_logging.to_string()); // 用该 reader 初始化解码器（阻塞）

    let frame_byte_offsets = decoder.scan_frames(); // 扫描得到每个帧的起始字节偏移（含首帧）
    // 通过相邻偏移差分得到每一帧的大小（单位：字节）
    // 注意：
    // - `windows(2)` 会生成形如 [offset[i], offset[i+1]] 的滑动窗口，窗口个数为 N-1；
    // - 对每个窗口做相减（后一个偏移减前一个偏移），即可得到第 i 帧的字节大小；
    // - 由于这里没有“帧区末尾”的终止偏移，因此不会计算最后一帧的大小；
    //   若需要包含最后一帧，可在 offsets 末尾追加帧区终止偏移后再做差分。
    let frame_sizes = frame_byte_offsets
        .windows(2)
        .map(|w| w[1] - w[0])
        .collect::<Vec<_>>();

    let reader = decoder.finish(); // 结束解码器，取回内部的 reader（供后续继续使用）
    Ok(RetWithReader { // 返回包含 reader 与数据的包装结构
        reader,
        data: FileFrameSizes { start_offset: frame_byte_offsets[0], frame_sizes }, // 帧区起点 = 第一帧偏移
    })
}

#[derive(Clone)]
struct EncodedTrackFrames {
    head_frames: Vec<Vec<u8>>,
    tail_frames: Option<Vec<Vec<u8>>>,
}

/// 针对每个轨道，生成首帧与尾帧的“拼接”编码结果：
/// - 首帧：从该轨起始采样位置开始的第一个解码帧；
/// - 尾帧：紧邻下一轨起点之前的解码帧（并切分出当前轨末尾部分）。
/// 这样可以保证相邻轨道之间无缝衔接（避免半帧丢失）。
/// 生成每个轨道的“首帧/尾帧”拼接编码字节：
/// - 确保连续轨道间边界无缝；
/// - 返回每轨的 head_frames 与可选 tail_frames。
fn encode_track_head_tail_frames( // 为每个轨道生成“首帧/尾帧”的拼接编码结果
    decoder: &mut FlacDecoder, // 解码器：用于从源 FLAC 中按采样位置解码出原始 PCM 帧
    encoder: &mut FlacEncoder, // 编码器：用于将拼接出的 PCM 片段重新编码为 FLAC 帧
    stream_info: &StreamInfoBlock, // 源文件的 StreamInfo（提供声道数、位深、采样率等编码参数）
    mut reader: impl Read + Seek + 'static, // 源文件 reader：可读可定位，供解码器初始化使用
    tracks_sample_pos: &[u64], // 每个轨道在源文件中的起始采样位置列表（长度为轨数）
    frames_sample_pos: &[u64], // 源文件中每个 FLAC 帧的起始采样位置（与 frame_sizes 对应）
    path_for_logging: impl ToString, // 仅用于日志（解码器内部记录来源）
) -> anyhow::Result<RetWithReader<Vec<EncodedTrackFrames>>> { // 返回 reader（供后续复用）与每轨的编码帧集合
    reader.seek(SeekFrom::Start(0))?; // 将 reader 定位到文件开头，便于解码器从头初始化
    decoder.init(Box::new(reader), path_for_logging.to_string()); // 用该 reader 初始化解码器

    let mut tracks_head_tail_data = vec![]; // 用于暂存每个轨的(head_pcm, tail_pcm)原始通道数据
    let mut frame_data = None; // 用于跨轨缓存“下一轨的首段 PCM”（即上一轨尾帧切割后的右半部分）
    for track_pos_chunk in tracks_sample_pos.windows(2) { // 逐个处理相邻的两个轨起点（当前轨与下一轨）
        let &[track_pos, next_track_pos] = track_pos_chunk else { unreachable!() }; // 解构出当前起点与下一起点

        let Some(head) = frame_data.take().or_else(|| { // 计算当前轨的首段 PCM：
            decoder.seek(track_pos); // 先 seek 到当前轨的起始采样位置
            decoder.decode_frame() // 解码出含有该起点的第一帧（返回按声道分开的 PCM 向量）
        }) else { // 若解码失败
            anyhow::bail!("error while decoding frames"); // 报错：无法继续计算首尾帧
        };

        let tail_frame_start = // 找到“覆盖下一轨起点”的那个源帧的起始采样位置（用于切尾）
            *frames_sample_pos.iter().rfind(|&&pos| pos <= next_track_pos).unwrap(); // 从右往左找最后一个不超过下一轨起点的帧起点
        decoder.seek(tail_frame_start); // 定位到该帧的起点

        let Some(tail_frame) = decoder.decode_frame() else { // 解码出该帧（包含尾段与下一轨开头）
            anyhow::bail!("error while decoding frames"); // 失败则报错
        };

        let mut tail = vec![]; // 当前轨用于“收尾”的 PCM 片段集合（每个声道一段）
        let mut next_head = vec![]; // 下一轨用于“开头”的 PCM 片段集合（每个声道一段）
        for ch_data in tail_frame { // 遍历当前帧的每个声道的 PCM 数据
            let Some((tail_ch, next_head_ch)) = // 按“下一轨起点在该帧内的偏移”切分本帧的 PCM
                ch_data.split_at_checked((next_track_pos - tail_frame_start) as usize)
            else { // 若切分越界（不应发生），视为解码错误
                anyhow::bail!("error while decoding frames");
            };
            tail.push(tail_ch.to_vec()); // 左半段属于“当前轨尾段”
            next_head.push(next_head_ch.to_vec()); // 右半段属于“下一轨首段”
        }

        tracks_head_tail_data.push((head, Some(tail))); // 记录当前轨的首段与尾段（尾段存在）
        frame_data = Some(next_head); // 把“下一轨首段”缓存起来，供下一轮作为 head 使用
    }
    tracks_head_tail_data.push((frame_data.unwrap(), None)); // 最后一轨只有首段，没有下一轨的尾段
    let reader = decoder.finish(); // 结束解码器，取回内部的 reader 以便后续继续使用

    let tracks_head_tail_frames = tracks_head_tail_data // 对每个轨的 head/tail PCM，分别进行独立编码，得到纯帧字节
        .into_iter()
        .map(|(head, tail)| { // 逐轨处理
            encoder.set_params( // 设置编码参数（与源一致），并指定 block size 为该段长度
                stream_info.get_channels(),
                stream_info.get_bits(),
                stream_info.get_sample_rate(),
                Some(head[0].len() as _),
            );
            encoder.init_stream(); // 初始化编码器状态
            encoder.queue_encode(&head); // 将“首段”PCM 压入编码队列
            let encoded_bytes = // 完成编码，取回编码得到的字节序列
                encoder.finish().ok_or_else(|| anyhow!("error while encoding frames"))?;
            let head_frames = extract_frames(decoder, &encoded_bytes); // 从字节流中剥离出纯音频帧（去掉 metadata）

            let tail_frames = match tail { // 若有“尾段”，重复上述编码流程
                Some(tail) => {
                    encoder.set_params(
                        stream_info.get_channels(),
                        stream_info.get_bits(),
                        stream_info.get_sample_rate(),
                        Some(tail[0].len() as _),
                    );
                    encoder.init_stream();
                    encoder.queue_encode(&tail);
                    let encoded_bytes =
                        encoder.finish().ok_or_else(|| anyhow!("error while encoding frames"))?;
                    Some(extract_frames(decoder, &encoded_bytes)) // 剥离出尾段帧列表
                }
                None => None, // 无尾段（最后一轨）
            };

            Ok(EncodedTrackFrames { head_frames, tail_frames }) // 返回该轨的首帧列表与可选尾帧列表
        })
        .collect::<anyhow::Result<Vec<_>>>()?; // 收集所有轨的结果，或在任一失败时返回错误

    Ok(RetWithReader { reader, data: tracks_head_tail_frames }) // 带回 reader 和结果集合
}

/// Unpacks frames from encoded flac bytes, omitting all metadata blocks
/// 从一次独立编码得到的字节流中，剥离并返回所有音频帧（忽略其中的 metadata）。
fn extract_frames(decoder: &mut FlacDecoder, bytes: &[u8]) -> Vec<Vec<u8>> { // 从一次独立编码得到的字节流中提取纯音频帧（去掉 metadata）
    let vec = bytes.to_vec(); // 拷贝输入字节，避免在 Cursor 中持有对调用方切片的借用，便于解码器独立使用
    let cursor = Cursor::new(vec); // 基于内存字节构造一个 Read + Seek 的游标
    decoder.init(Box::new(cursor), String::new()); // 初始化解码器，仅用于扫描帧边界；日志来源留空
    let frame_offsets = decoder.scan_frames(); // 扫描得到每个帧的起始字节偏移（含首个帧起点）
    decoder.finish(); // 结束解码器并释放内部 reader；接下来按偏移手动切片

    let mut out = vec![]; // 输出：每个元素是一个帧的原始字节向量
    let mut last_offset = frame_offsets[0] as usize; // 上一个帧起点：初始化为第一帧的起点（跳过 header/metadata）
    let (_header, mut left) = bytes.split_at(last_offset); // 丢弃 header/metadata，left 指向帧数据起点
    let mut frame; // 临时变量：承接每次 split_at 拆出的当前帧切片

    for offset in frame_offsets.into_iter().skip(1) { // 遍历后续每个帧的起点偏移
        (frame, left) = left.split_at(offset as usize - last_offset); // 当前帧长度 = 新起点 - 上次起点
        last_offset = offset as usize; // 更新上次起点
        out.push(frame.to_vec()); // 拷贝保存该帧的原始字节
    }
    out // 返回所有帧字节
}

#[allow(clippy::too_many_arguments)]
/// 将指定轨道写出为独立 FLAC：
/// - 写 fLaC 头与元数据（过滤后的 blocks，并在最后回写 StreamInfo）；
/// - 写入轨道的首帧、主体帧与尾帧；
/// - 依据采样位置判断是否跨越到下一轨的起点。
#[allow(clippy::too_many_arguments)]
/// 写出单轨 FLAC：
/// - 会写入 fLaC 头与 metadata；
/// - 首先写入拼接好的首帧，随后写入主体帧；若越过下一轨起点，则写入拼接好的尾帧并停止；
/// - 最后回写 StreamInfo，更新 block size、sample_count、清空 md5。
fn write_track( // 将指定轨道写出为独立 FLAC 文件的核心函数（同步读写）
    mut reader: impl Read + Seek, // 输入流：指向源 FLAC 帧区的可读可定位 reader（已定位到 frames 起始偏移前由调用方控制）
    mut writer: impl Write + Seek, // 输出流：写入目标轨道 FLAC 的 writer（需要支持写与 seek 以便回写 StreamInfo）
    metadata_blocks: &[FlacMetadataBlock], // 需要写入目标文件的元数据块列表（已过滤、并包含 StreamInfo 与 Vorbis 等）
    track_pos: u64, // 当前轨道的起始采样位置（samples）
    next_track_pos: Option<u64>, // 下一轨道的起始采样位置（若无则 None，表示当前轨到末尾）
    head_frames: &[Vec<u8>], // 预先编码好的“首帧”若干块（用于从轨起点精确开始）
    tail_frames: Option<&Vec<Vec<u8>>>, // 预先编码好的“尾帧”（用于在下一轨起点前精确结束）
    frame_sizes: &[u64], // 源 FLAC 每个帧的大小（字节）
    frames_sample_pos: &[u64], // 源 FLAC 每个帧对应的起始采样位置
) -> anyhow::Result<()> { // 返回可能的错误
    writer.write_all(b"fLaC")?; // 写入 FLAC 文件魔数头部

    let stream_info = get_stream_info(metadata_blocks) // 从传入的元数据中取得 StreamInfo 的引用
        .ok_or_else(|| anyhow!("写出轨道失败：元数据块不完整（缺少 StreamInfo）"))?; // 若缺少则报错（无法正确解读帧）
    for block in metadata_blocks.iter() { // 将所有 metadata blocks 逐个写出
        block.write_block(&mut writer)?; // 写出单个 block（StreamInfo 必须第一个，调用处已确保顺序）
    }

    let head_frames = head_frames // 将首帧字节解析为可编辑的 FlacFrame 结构
        .iter()
        .map(|bytes| FlacFrame::read_frame(&**bytes, stream_info, bytes.len())) // 读取每个首帧
        .collect::<Result<Vec<_>, _>>()?; // 若解析失败则报错
    let mut head_frames = Some(head_frames); // 使用 Option 包装，便于只在第一次循环时 take 出来
    let tail_frames = match tail_frames { // 同理，将可选的尾帧字节解析为 FlacFrame 列表
        Some(frames) => Some(
            frames
                .iter()
                .map(|bytes| FlacFrame::read_frame(&**bytes, stream_info, bytes.len()))
                .collect::<Result<Vec<_>, _>>()?, // 解析失败则报错
        ),
        None => None, // 没有尾帧（说明当前轨到文件末尾）
    };

    let mut min_block_size = u16::MAX; // 统计写出帧中的最小 block size，用于回写 StreamInfo
    let mut max_block_size = u16::MIN; // 统计写出帧中的最大 block size

    let mut track_sample_pos = 0; // 当前写出的轨内采样计数，用于填充帧 header 的 sample number
    let mut first_frame = true; // 标记是否为第一次写主体帧之前（需要先写首帧集合）
    let mut fixed_block_size_applied = false; // 标记是否遇到过 Fixed 策略帧（用于正确统计 block size）

    for (size, frame_pos) in zip(frame_sizes, frames_sample_pos) { // 遍历源 FLAC 的每个帧（大小与起始采样位置成对）
        if *frame_pos < track_pos { // 如果该源帧开始位置早于目标轨起点
            reader.seek(SeekFrom::Current(*size as i64))?; // 跳过该帧（向前 seek）
            continue; // 继续看下一帧
        }
        //tracing::info!("process_file: size {:}",size);
        //tracing::info!("process_file: frame_pos {:}",frame_pos);
        //let current_pos = reader.seek(SeekFrom::Current(0))?;
        //tracing::info!("process_file: start pos {:}",current_pos);
        let mut frame = FlacFrame::read_frame(&mut reader, stream_info, *size as _)?; // 读取当前帧并解析为可编辑结构
        let block_size = frame.metadata.block_size.get_size(); // 取出该帧的样本数（block size）

        if first_frame { // 首次进入主体帧写出前，需要先把“首帧集合”写出去
            // This will only be executed once // 仅执行一次
            let head_frames = head_frames.take().unwrap(); // 取出首帧列表（之后变为 None）
            for mut frame in head_frames { // 遍历每个首帧
                frame.metadata.blocking_strategy = FlacBlockingStrategy::Variable; // 统一改为 Variable 策略（自包含的拼接帧）
                frame.metadata.position = FlacFramePosition::SampleCount(track_sample_pos); // 设置该帧在目标轨中的起始采样位置
                let frame_samples = frame.metadata.block_size.get_size(); // 获取该首帧的样本数
                track_sample_pos += frame_samples as u64; // 轨内采样位置前移

                min_block_size = std::cmp::min(min_block_size, frame_samples); // 更新最小 block size
                max_block_size = std::cmp::max(max_block_size, frame_samples); // 更新最大 block size
                writer.write_all(&frame.into_bytes())?; // 将首帧序列化后写入输出
            }
            first_frame = false; // 标记已处理完首帧
            let _current_pos = reader.seek(SeekFrom::Current(0))?;
            //tracing::info!("process_file: first_frame pos {:}",current_pos);
        }

        if !fixed_block_size_applied { // 若尚未遇到 Fixed 策略帧
            min_block_size = std::cmp::min(min_block_size, block_size); // 统计最小 block size
            max_block_size = std::cmp::max(max_block_size, block_size); // 统计最大 block size
            if frame.metadata.blocking_strategy == flac::FlacBlockingStrategy::Fixed { // 如果该帧是 Fixed 策略
                fixed_block_size_applied = true; // 标记后续无需重复考虑 Fixed 的约束
            }
        }

        if next_track_pos.is_some() && *frame_pos > next_track_pos.unwrap() { // 若存在下一轨起点且当前帧已越过该起点
            let tail_frames = tail_frames.unwrap(); // 取出预先编码的尾帧列表
            for mut frame in tail_frames { // 依次写出尾帧，完成精准收尾
                frame.metadata.blocking_strategy = FlacBlockingStrategy::Variable; // 尾帧也使用 Variable 策略
                frame.metadata.position = FlacFramePosition::SampleCount(track_sample_pos); // 设置该尾帧的起始采样位置
                let frame_samples = frame.metadata.block_size.get_size(); // 尾帧样本数
                track_sample_pos += frame_samples as u64; // 推进轨内采样位置

                min_block_size = std::cmp::min(min_block_size, frame_samples); // 更新最小 block size
                max_block_size = std::cmp::max(max_block_size, frame_samples); // 更新最大 block size
                writer.write_all(&frame.into_bytes())?; // 写出尾帧
            }
            let _current_pos = reader.seek(SeekFrom::Current(0))?;
            //tracing::info!("process_file: tail_frames pos {:}",current_pos);
            break; // 完成当前轨写出，结束循环
        } else { // 否则：当前帧仍属于本轨的主体范围
            frame.metadata.blocking_strategy = FlacBlockingStrategy::Variable; // 将源帧的阻塞策略标记为 Variable（连续样本数）//更改为可变

            frame.metadata.position = FlacFramePosition::SampleCount(track_sample_pos); // 设置该帧在目标轨中的起始采样位置//更改了位置
            //let process_file_metadatasize = frame.metadata.to_bytes().len();
            //let temp = frame.clone().into_bytes().len();
            writer.write_all(&frame.into_bytes())?; // 写出该主体帧
            //let _process_file_framedatasize = temp - process_file_metadatasize;
            //tracing::info!("process_file: process_file_metadatasize:{} process_file_framedatasize {}",process_file_metadatasize,process_file_framedatasize);
            track_sample_pos += block_size as u64; // 推进轨内采样位置计数
        }
    }

    let mut stream_info = stream_info.clone(); // 克隆一份 StreamInfo 以便修改回写
    stream_info.min_block_size = min_block_size; // 覆盖最小样本块大小
    stream_info.max_block_size = max_block_size; // 覆盖最大样本块大小
    stream_info.min_frame_size = 0; // 清零 frame size（不再依赖原文件中的范围）
    stream_info.max_frame_size = 0; // 同上
    stream_info.sample_count = track_sample_pos; // 更新本轨总样本数（非常关键）
    stream_info.md5 = [0; 16]; // 清空 MD5（因内容变化，留给后处理或保持 0）
    let block = FlacMetadataBlock { // 用更新后的 StreamInfo 构造新的 metadata block
        is_last: metadata_blocks.len() == 1, // 如果只有 StreamInfo 一个块，则标记为 is_last
        block_type: FlacMetadataBlockType::StreamInfo, // 类型为 StreamInfo
        content: FlacMetadataBlockContent::StreamInfo(stream_info), // 填充内容
    };

    writer.seek(SeekFrom::Start(4))?; // Seek 回文件头后的第一个 block 位置（跳过 "fLaC" 4 字节）
    block.write_block(writer)?; // 回写更新后的 StreamInfo 块

    Ok(()) // 函数成功结束
}

fn write_track_for_size_counting(
    mut reader: impl Read + Seek,
    metadata_blocks: &[FlacMetadataBlock],
    track_pos: u64,
    next_track_pos: Option<u64>,
    head_frames: &[Vec<u8>],
    tail_frames: Option<&Vec<Vec<u8>>>,
    frame_sizes: &[u64],
    frames_sample_pos: &[u64],
) -> anyhow::Result<(usize, usize, usize, u64, usize)> {
    let mut metadata_bytes_total = 0usize;
    let mut all_frames_header_bytes_total = 0usize;
    let mut total_frame_data_bytes: u64 = 0;
    let mut head_frames_bytes_total = 0usize;
    let mut tail_frames_bytes_total = 0usize;

    use crate::flac::{FlacBlockingStrategy, FlacFramePosition};
    //writer.write_all(b"fLaC")?; // 写入 FLAC 文件魔数头部

    let stream_info = get_stream_info(metadata_blocks) // 从传入的元数据中取得 StreamInfo 的引用
        .ok_or_else(|| anyhow!("写出轨道失败：元数据块不完整（缺少 StreamInfo）"))?; // 若缺少则报错（无法正确解读帧）

    for block in metadata_blocks.iter() {
        let content_len = block.content.to_bytes().len();
        metadata_bytes_total += content_len + 4; // 每个 block 的头 4 字节 + 内容长度
    }

    let head_frames = head_frames // 将首帧字节解析为可编辑的 FlacFrame 结构
        .iter()
        .map(|bytes| FlacFrame::read_frame(&**bytes, stream_info, bytes.len())) // 读取每个首帧
        .collect::<Result<Vec<_>, _>>()?; // 若解析失败则报错
    let mut head_frames = Some(head_frames); // 使用 Option 包装，便于只在第一次循环时 take 出来
    let tail_frames = match tail_frames { // 同理，将可选的尾帧字节解析为 FlacFrame 列表
        Some(frames) => Some(
            frames
                .iter()
                .map(|bytes| FlacFrame::read_frame(&**bytes, stream_info, bytes.len()))
                .collect::<Result<Vec<_>, _>>()?, // 解析失败则报错
        ),
        None => None, // 没有尾帧（说明当前轨到文件末尾）
    };

    let mut min_block_size = u16::MAX; // 统计写出帧中的最小 block size，用于回写 StreamInfo
    let mut max_block_size = u16::MIN; // 统计写出帧中的最大 block size

    let mut track_sample_pos = 0; // 当前写出的轨内采样计数，用于填充帧 header 的 sample number
    let mut first_frame = true; // 标记是否为第一次写主体帧之前（需要先写首帧集合）
    let mut fixed_block_size_applied = false; // 标记是否遇到过 Fixed 策略帧（用于正确统计 block size）

    for (size, frame_pos) in zip(frame_sizes, frames_sample_pos) { // 遍历源 FLAC 的每个帧（大小与起始采样位置成对）
        if *frame_pos < track_pos { // 如果该源帧开始位置早于目标轨起点
            let _pos = reader.seek(SeekFrom::Current(*size as i64))?; // 跳过该帧（向前 seek）
            continue; // 继续看下一帧
        }
        //let current_pos = reader.seek(SeekFrom::Current(0))?;
        //tracing::info!("start pos {:}",current_pos);
        //tracing::info!("size {:}",size);
        //tracing::info!("frame_pos {:}",frame_pos);
        let (mut metadata, bytes) = FlacFrameMetadata::read(&mut reader, stream_info)?;
        let _pos = reader.seek(SeekFrom::Current(*size as i64 - bytes as i64))?;
        let block_size = metadata.block_size.get_size(); // 取出该帧的样本数（block size）

        if first_frame { // 首次进入主体帧写出前，需要先把“首帧集合”写出去
            // This will only be executed once // 仅执行一次
            let head_frames = head_frames.take().unwrap(); // 取出首帧列表（之后变为 None）
            for mut frame in head_frames { // 遍历每个首帧
                frame.metadata.blocking_strategy = FlacBlockingStrategy::Variable; // 统一改为 Variable 策略（自包含的拼接帧）
                frame.metadata.position = FlacFramePosition::SampleCount(track_sample_pos); // 设置该帧在目标轨中的起始采样位置
                let frame_samples = frame.metadata.block_size.get_size(); // 获取该首帧的样本数
                track_sample_pos += frame_samples as u64; // 轨内采样位置前移

                min_block_size = std::cmp::min(min_block_size, frame_samples); // 更新最小 block size
                max_block_size = std::cmp::max(max_block_size, frame_samples); // 更新最大 block size
                let header_len = frame.metadata.to_bytes().len();
                head_frames_bytes_total += header_len + frame.frame_data.len() + 2;
            }
            first_frame = false; // 标记已处理完首帧
            let _current_pos = reader.seek(SeekFrom::Current(0))?;
            //tracing::info!("first_frame pos {:}",current_pos);
        }

        if !fixed_block_size_applied { // 若尚未遇到 Fixed 策略帧
            min_block_size = std::cmp::min(min_block_size, block_size); // 统计最小 block size
            max_block_size = std::cmp::max(max_block_size, block_size); // 统计最大 block size
            if metadata.blocking_strategy == flac::FlacBlockingStrategy::Fixed { // 如果该帧是 Fixed 策略
                fixed_block_size_applied = true; // 标记后续无需重复考虑 Fixed 的约束
            }
        }

        if next_track_pos.is_some() && *frame_pos > next_track_pos.unwrap() { // 若存在下一轨起点且当前帧已越过该起点
            let tail_frames = tail_frames.unwrap(); // 取出预先编码的尾帧列表
            for mut frame in tail_frames { // 依次写出尾帧，完成精准收尾
                frame.metadata.blocking_strategy = FlacBlockingStrategy::Variable; // 尾帧也使用 Variable 策略
                frame.metadata.position = FlacFramePosition::SampleCount(track_sample_pos); // 设置该尾帧的起始采样位置
                let frame_samples = frame.metadata.block_size.get_size(); // 尾帧样本数
                track_sample_pos += frame_samples as u64; // 推进轨内采样位置

                min_block_size = std::cmp::min(min_block_size, frame_samples); // 更新最小 block size
                max_block_size = std::cmp::max(max_block_size, frame_samples); // 更新最大 block size
                let header_len = frame.metadata.to_bytes().len();
                tail_frames_bytes_total += header_len + frame.frame_data.len() + 2;
            }
            let _current_pos = reader.seek(SeekFrom::Current(0))?;
            //tracing::info!("tail_frames pos {:}",current_pos);
            break; // 完成当前轨写出，结束循环
        } else { // 否则：当前帧仍属于本轨的主体范围
            metadata.blocking_strategy = FlacBlockingStrategy::Variable; // 将源帧的阻塞策略标记为 Variable（连续样本数）//更改为可变
            metadata.position = FlacFramePosition::SampleCount(track_sample_pos); // 设置该帧在目标轨中的起始采样位置//更改了位置
            //let _metadatasize = metadata.to_bytes().len();
            all_frames_header_bytes_total += metadata.to_bytes().len(); // 累计主体帧头长度
            track_sample_pos += block_size as u64; // 推进轨内采样位置计数
            let mut framedatasize = *size as u64 - bytes as u64;
            framedatasize = framedatasize - 1;//为什么会多一  浪费我一晚上时间  
            total_frame_data_bytes += framedatasize;
            //tracing::info!(" metadatasize:{} framedatasize {}",metadatasize,framedatasize);

        //let mut frame = FlacFrame{metadata: metadata,frame_data: Vec::new(),};
            
        }
    }

    let mut stream_info = stream_info.clone(); // 克隆一份 StreamInfo 以便修改回写
    stream_info.min_block_size = min_block_size; // 覆盖最小样本块大小
    stream_info.max_block_size = max_block_size; // 覆盖最大样本块大小
    stream_info.min_frame_size = 0; // 清零 frame size（不再依赖原文件中的范围）
    stream_info.max_frame_size = 0; // 同上
    stream_info.sample_count = track_sample_pos; // 更新本轨总样本数（非常关键）
    stream_info.md5 = [0; 16]; // 清空 MD5（因内容变化，留给后处理或保持 0）
    let _block = FlacMetadataBlock { // 用更新后的 StreamInfo 构造新的 metadata block
        is_last: metadata_blocks.len() == 1, // 如果只有 StreamInfo 一个块，则标记为 is_last
        block_type: FlacMetadataBlockType::StreamInfo, // 类型为 StreamInfo
        content: FlacMetadataBlockContent::StreamInfo(stream_info), // 填充内容
    };

    //writer.seek(SeekFrom::Start(4))?; // Seek 回文件头后的第一个 block 位置（跳过 "fLaC" 4 字节）
    //block.write_block(metablock_size_writer)?; // 回写更新后的 StreamInfo 块

    Ok((metadata_bytes_total, head_frames_bytes_total, tail_frames_bytes_total, total_frame_data_bytes, all_frames_header_bytes_total)) // 仅返回累计大小，避免中间分配
}




async fn write_track_bytes_blocking_for_size( // 在线程池中执行最终写入（seek + 写帧），返回编码好的字节
    file_path: &Path,
    start_offset: u64,
    metadata_blocks: std::sync::Arc<Vec<FlacMetadataBlock>>,
    track_start_pos: u64,
    next_track_start_pos: Option<u64>,
    heads: std::sync::Arc<Vec<Vec<u8>>>,
    tails: Option<std::sync::Arc<Vec<Vec<u8>>>>,
    frame_sizes: std::sync::Arc<Vec<u64>>,
    frames_sample_pos: std::sync::Arc<Vec<u64>>,
) -> anyhow::Result<(usize, usize, usize, u64, usize)> {
    let file_path_owned = file_path.to_path_buf(); // 克隆路径，move 进阻塞闭包

    // 将阻塞的文件 I/O 和计算放到 tokio 的阻塞线程池中执行
    let handle = tokio::task::spawn_blocking(move || -> anyhow::Result<(usize, usize, usize,u64,usize)> {
        let mut reader_block: Box<dyn SeekableRead> = // 在阻塞线程内重新打开源文件，避免跨线程传递句柄
            Box::new(std::io::BufReader::new(std::fs::File::open(&file_path_owned)?));
        reader_block.seek(SeekFrom::Start(start_offset))?; // 定位到帧区起点，后续读取帧

        let le123 = write_track_for_size_counting(
            &mut reader_block,
            metadata_blocks.as_ref(),
            track_start_pos,
            next_track_start_pos,
            heads.as_ref(),
            tails.as_deref(),
            frame_sizes.as_ref(),
            frames_sample_pos.as_ref(),
        )?;

        Ok(le123) // 返回 le123（三元组）
    });

    let result = handle
        .await
        .map_err(|e| anyhow::anyhow!("spawn_blocking join error: {e}"))??;

    Ok(result)
}