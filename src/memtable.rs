use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::sstable::SsTableBuilder;
use crate::wal::Wal;
use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
/// map：跳表，用于存储数据
/// wal: 日志文件
/// id：每个memtable都有一个id方便后面压缩
/// mem_size：用于判断是否到达阈值
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    mem_size: Arc<AtomicUsize>,
}

/// 判断key是否存在与memtable中，用一个函数复用match
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
    pub fn create(id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            mem_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            map: Arc::new(SkipMap::new()),
            wal: Some(Wal::create(path.as_ref())?),
            id,
            mem_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let map = Arc::new(SkipMap::new());
        Ok(Self {
            id,
            wal: Some(Wal::recover(path.as_ref(), &map)?),
            map,
            mem_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// 通过key获取value
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map.get(key).map(|e| e.value().clone())
    }

    /// 插入值
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let total_size = key.len() + value.len();
        self.map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        self.mem_size
            .fetch_add(total_size, std::sync::atomic::Ordering::Relaxed);
        if let Some(ref wal) = self.wal {
            wal.put(key, value)?;
        }
        Ok(())
    }

    /// 获取当前memtable的id
    pub fn id(&self) -> usize {
        self.id
    }

    /// 获取当前memtable的值
    pub fn mem_size(&self) -> usize {
        self.mem_size.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// 强制wal刷到磁盘
    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// scan范围扫描
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        let (lower, upper) = (map_bound(lower), map_bound(upper));
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((lower, upper)),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        iter.next().unwrap();
        iter
    }

    /// immemtable flush到sst
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            builder.add(KeySlice::from_slice(&entry.key()[..]), &entry.value()[..]);
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// 解决skipmaprangeIter的生命周期问题
#[self_referencing]
pub struct MemTableIterator {
    /// skipmap
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// skipmap iterator
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// 存储当前的kv
    item: (Bytes, Bytes),
}

impl MemTableIterator {
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> KeySlice {
        KeySlice::from_slice(&self.borrow_item().0[..])
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}
