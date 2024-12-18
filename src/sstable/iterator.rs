#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// sst的iterator
pub struct SsTableIterator {
    sst: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    fn seek_to_first_inner(sst: &Arc<SsTable>) -> Result<(usize, BlockIterator)> {
        let _a = sst.read_block(0);
        Ok((
            0,
            BlockIterator::create_and_seek_to_first(sst.read_block(0)?),
        ))
    }

    /// 创建iter，获取第一个block的第一个kv
    pub fn create_and_seek_to_first(sst: Arc<SsTable>) -> Result<Self> {
        let (blk_idx, blk_iter) = Self::seek_to_first_inner(&sst)?;
        let iter = Self {
            blk_iter,
            sst,
            blk_idx,
        };
        Ok(iter)
    }

    /// 获取第一个block的第一个kv
    pub fn seek_to_first(&mut self) -> Result<()> {
        let (blk_idx, blk_iter) = Self::seek_to_first_inner(&self.sst)?;
        self.blk_idx = blk_idx;
        self.blk_iter = blk_iter;
        Ok(())
    }

    fn seek_to_key_inner(sst: &Arc<SsTable>, key: KeySlice) -> Result<(usize, BlockIterator)> {
        let mut blk_idx = sst.find_block_idx(key);
        let mut blk_iter = BlockIterator::create_and_seek_to_key(sst.read_block(blk_idx)?, key);
        if !blk_iter.is_valid() {
            blk_idx += 1;
            if blk_idx < sst.num_of_blocks() {
                blk_iter = BlockIterator::create_and_seek_to_first(sst.read_block(blk_idx)?);
            }
        }
        Ok((blk_idx, blk_iter))
    }

    /// 创建iter，找到第一个>key的kv对
    pub fn create_and_seek_to_key(sst: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let (blk_idx, blk_iter) = Self::seek_to_key_inner(&sst, key)?;
        let iter = Self {
            blk_iter,
            sst,
            blk_idx,
        };
        Ok(iter)
    }

    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let (blk_idx, blk_iter) = Self::seek_to_key_inner(&self.sst, key)?;
        self.blk_iter = blk_iter;
        self.blk_idx = blk_idx;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// 通过block iter获取key
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// 通过block iter获取value
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// 通过block iter判断当前sst iter是否有效
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// 获取下一个block iter
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx < self.sst.num_of_blocks() {
                self.blk_iter =
                    BlockIterator::create_and_seek_to_first(self.sst.read_block(self.blk_idx)?);
            }
        }
        Ok(())
    }
}
