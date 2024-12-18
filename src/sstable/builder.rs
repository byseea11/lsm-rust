use std::path::Path;
use std::sync::Arc;

use super::{BlockMeta, FileObject, SsTable};
use crate::block::BlockBuilder;
use crate::key::{KeySlice, KeyVec};
use anyhow::Result;
use bytes::BufMut;
/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    // 存block的encoder
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    pub fn new(block_size: usize) -> Self {
        Self {
            data: Vec::new(),
            meta: Vec::new(),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            block_size,
            builder: BlockBuilder::new(block_size),
        }
    }

    /// Adds a key-value pair to SSTable.
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        if self.builder.add(key, value) {
            self.last_key.set_from_slice(key);
            return;
        }

        // 如果添加失败了就创建新块
        self.finish_block();

        // 把kv对添加到新块
        assert!(self.builder.add(key, value));
        self.first_key.set_from_slice(key);
        self.last_key.set_from_slice(key);
    }

    fn finish_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        // 把块encode
        let encoded_block = builder.build().encode();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key).into_key_bytes(),
            last_key: std::mem::take(&mut self.last_key).into_key_bytes(),
        });
        self.data.extend(encoded_block);
    }

    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// 构建SSTable并将其写入给定路径, 使用FileObject结构来操作磁盘对象
    pub fn build(mut self, id: usize, path: impl AsRef<Path>) -> Result<SsTable> {
        self.finish_block();
        let mut buf = self.data;
        let meta_offset = buf.len();
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32(meta_offset as u32);
        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            id,
            file,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            block_meta_offset: meta_offset,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, path)
    }
}
