use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    sstable::{SsTable, SsTableIterator},
};

/// Merge multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstMergeIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstMergeIterator {
    // 创建一个迭代器并移动到第一个有效位置
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        // 校验所有 SST 文件是否合法
        Self::check_sst_valid(&sstables);
        // 如果 SST 文件列表为空，返回一个没有有效元素的迭代器
        if sstables.is_empty() {
            return Ok(Self {
                current: None,   // 当前没有有效元素
                next_sst_idx: 0, // 下一个 SST 文件的索引为 0
                sstables,        // 保存传入的 SST 文件
            });
        }
        // 初始化一个迭代器，首先对第一个 SST 文件进行迭代
        let mut iter = Self {
            current: Some(SsTableIterator::create_and_seek_to_first(
                sstables[0].clone(),
            )?), // 对第一个 SST 文件创建迭代器并移动到第一个元素
            next_sst_idx: 1, // 下一个 SST 文件的索引为 1
            sstables,        // 保存传入的 SST 文件
        };
        // 调用 `move_until_valid` 确保迭代器有效
        iter.move_until_valid()?;
        // 返回创建并初始化后的迭代器
        Ok(iter)
    }

    // 根据指定的键创建迭代器并定位到该键
    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        // 校验所有 SST 文件是否合法
        Self::check_sst_valid(&sstables);
        // 找到与给定键最接近的 SST 文件的索引
        let idx: usize = sstables
            .partition_point(|table| table.first_key().as_key_slice() <= key) // 找到第一个 `first_key` 小于等于指定键的表
            .saturating_sub(1); // 保证索引不会小于 0
                                // 如果没有找到合适的索引，表示所有 SST 文件都比给定的键大，返回一个没有有效元素的迭代器
        if idx >= sstables.len() {
            return Ok(Self {
                current: None,                // 当前没有有效元素
                next_sst_idx: sstables.len(), // 下一个 SST 文件的索引为 SST 列表的长度
                sstables,                     // 保存传入的 SST 文件
            });
        }
        // 初始化一个迭代器，并对找到的 SST 文件进行迭代并定位到指定键
        let mut iter = Self {
            current: Some(SsTableIterator::create_and_seek_to_key(
                sstables[idx].clone(),
                key,
            )?), // 对找到的 SST 文件创建迭代器并定位到指定键
            next_sst_idx: idx + 1, // 下一个 SST 文件的索引
            sstables,              // 保存传入的 SST 文件
        };
        // 确保迭代器有效
        iter.move_until_valid()?;
        // 返回创建并初始化后的迭代器
        Ok(iter)
    }

    // 校验传入的 SST 文件是否有效
    fn check_sst_valid(sstables: &[Arc<SsTable>]) {
        // 检查每个 SST 文件的 first_key 是否小于等于 last_key
        for sst in sstables {
            assert!(sst.first_key() <= sst.last_key());
        }
        // 如果 SST 列表不为空，检查相邻的 SST 文件之间的顺序是否正确
        if !sstables.is_empty() {
            for i in 0..(sstables.len() - 1) {
                // 检查前一个 SST 文件的 last_key 是否小于下一个 SST 文件的 first_key
                assert!(sstables[i].last_key() < sstables[i + 1].first_key());
            }
        }
    }

    // 确保迭代器指向有效元素
    fn move_until_valid(&mut self) -> Result<()> {
        // 循环直到找到有效的元素
        while let Some(iter) = self.current.as_mut() {
            // 如果当前迭代器有效，退出循环
            if iter.is_valid() {
                break;
            }
            // 如果迭代器已经遍历到最后一个 SST 文件，当前迭代器设置为无效
            if self.next_sst_idx >= self.sstables.len() {
                self.current = None;
            } else {
                // 如果当前迭代器无效，则从下一个 SST 文件中创建新的迭代器
                self.current = Some(SsTableIterator::create_and_seek_to_first(
                    self.sstables[self.next_sst_idx].clone(),
                )?);
                // 增加下一个 SST 文件的索引
                self.next_sst_idx += 1;
            }
        }
        // 如果找到了有效元素，则返回
        Ok(())
    }
}

impl StorageIterator for SstMergeIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        if let Some(current) = &self.current {
            assert!(current.is_valid());
            true
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;
        self.move_until_valid()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
