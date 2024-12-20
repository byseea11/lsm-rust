use std::fs::File;
use std::path::Path;
use std::sync::Arc;
pub(crate) mod bloom;
use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use anyhow::{anyhow, bail, Result};
use bytes::{Buf, BufMut};
mod builder;
mod iterator;
use self::bloom::Bloom;
pub use builder::SsTableBuilder;
pub use iterator::SsTableIterator;
#[derive(Clone, Debug, PartialEq, Eq)]
/// 记录key的范围方便快速查询
pub struct BlockMeta {
    /// block的offset方便从file中把block恢复过来
    pub offset: usize,
    /// block的第一个key
    pub first_key: KeyBytes,
    /// block的最后一个key
    pub last_key: KeyBytes,
}

impl BlockMeta {
    ///BlockMeta是方便从file中解码的时候用的
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        // blockmeta len
        let mut estimated_size = std::mem::size_of::<u32>();
        for meta in block_meta {
            // offset size
            estimated_size += std::mem::size_of::<u32>();
            // first_key length
            estimated_size += std::mem::size_of::<u16>();
            // first_key
            estimated_size += meta.first_key.len();
            // last_key length
            estimated_size += std::mem::size_of::<u16>();
            // last_key
            estimated_size += meta.last_key.len();
        }

        // buf预留空间
        buf.reserve(estimated_size);
        let original_len = buf.len();
        buf.put_u32(block_meta.len() as u32);
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(meta.first_key.raw_ref());
            buf.put_u16(meta.last_key.len() as u16);
            buf.put_slice(meta.last_key.raw_ref());
        }
        assert_eq!(estimated_size, buf.len() - original_len);
    }

    /// 解码
    pub fn decode_block_meta(mut buf: &[u8]) -> Result<Vec<BlockMeta>> {
        let mut block_meta = Vec::new();
        let num = buf.get_u32() as usize;
        for _ in 0..num {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_len));
            let last_key_len: usize = buf.get_u16() as usize;
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_len));
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }

        Ok(block_meta)
    }
}

/// 用来刷盘的
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// 创建文件把文件写入磁盘
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// SSTable，磁盘核心数据结构
pub struct SsTable {
    /// SsTable的实际存储单元
    pub(crate) file: FileObject,
    /// 保存Sstable的数据块元数据
    pub(crate) block_meta: Vec<BlockMeta>,
    /// 指示file中元块起点的偏移量
    pub(crate) block_meta_offset: usize,
    id: usize,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, file)
    }

    /// 从文件中读取sstable，一个file是一个sstable
    pub fn open(id: usize, file: FileObject) -> Result<Self> {
        // 获取文件的总大小
        let len = file.size();
        // 获取布隆过滤器
        let raw_bloom_offset = file.read(len - 4, 4)?;
        let bloom_offset = (&raw_bloom_offset[..]).get_u32() as u64;
        let raw_bloom = file.read(bloom_offset, len - 4 - bloom_offset)?;
        let bloom_filter = Bloom::decode(&raw_bloom)?;
        let raw_meta_offset = file.read(bloom_offset - 4, 4)?;
        // 获取块元数据
        let block_meta_offset = (&raw_meta_offset[..]).get_u32() as u64;
        let raw_meta = file.read(block_meta_offset, bloom_offset - 4 - block_meta_offset)?;
        let block_meta = BlockMeta::decode_block_meta(&raw_meta[..])?;
        Ok(Self {
            file,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            bloom: Some(bloom_filter),
        })
    }

    /// 创建SST first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            first_key,
            last_key,
            bloom: None,
        }
    }

    /// 从block meta中读出block的offset然后把block读出来
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.block_meta[block_idx].offset;
        let offset_end = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset);
        let block_data: Vec<u8> = self
            .file
            .read(offset as u64, (offset_end - offset) as u64)?;
        Ok(Arc::new(Block::decode(&block_data[..])))
    }

    /// 可能包含key的block
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() <= key)
            .saturating_sub(1)
    }

    /// block的数量
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }
}
