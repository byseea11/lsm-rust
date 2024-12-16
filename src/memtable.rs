#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

/// map：跳表，用于存储数据
/// id：每个memtable都有一个id方便后面压缩
/// approximate_size：用于判断是否到达阈值
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// 判断key是否存在与memtable中
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// 定义memtable接口
impl MemTable {
    /// 创建memtable
    pub fn create(_id: usize) -> Self {
        unimplemented!()
    }

    /// 通过key获取value
    pub fn get(&self, _key: &[u8]) -> Option<Bytes> {
        unimplemented!()
    }

    /// 插入值
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        unimplemented!()
    }

    /// 将memtable合并到sstable
    pub fn flush(&self, _builder: &mut SsTableBuilder) -> Result<()> {
        unimplemented!()
    }

    /// 获取当前memtable的id
    pub fn id(&self) -> usize {
        self.id
    }

    /// 获取当前memtable的值
    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}
