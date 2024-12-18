use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::memtable::{map_bound, MemTable};
use crate::sstable::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

#[derive(Clone)]

/// 目前只实现到imm_memtables(不可变的memtable)这部分，所以把lsmstorageArch先定为这个两个部分
pub struct LsmStorageArch {
    /// 当前的memtable
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, 不可变memtable，当memtable达到阈值之后会merge到imm_memtable
    pub imm_memtables: Vec<Arc<MemTable>>,
    // memtable和sstable合并层
    pub l0_sstables: Vec<usize>,
    // sstable
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

impl LsmStorageArch {
    fn create(options: &LsmStorageOptions) -> Self {
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
/// 定义lsm tree的阈值
pub struct LsmStorageOptions {
    // block size（Byte）
    pub block_size: usize,
    // SST size，也是memtable的max size（Byte）
    pub target_sst_size: usize,
    // 内存中memtable的数量限制（memtable+imm_memtable）
    pub num_memtable_limit: usize,
    // 是否启动wal
    pub enable_wal: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            // b(8)->B(10)->K(10)->M(10)->G 4096B=1*1024*4B=4K 2<<20=2^21B=2^11K=2^1M
            block_size: 4096,         //4K
            target_sst_size: 2 << 20, //2M
            enable_wal: false,
            num_memtable_limit: 50,
        }
    }
}

/// put和delete的逻辑类似，同时处理新增数据时memtable的阈值问题
pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) arch: Arc<RwLock<Arc<LsmStorageArch>>>,
    // memtable的wal需要path
    path: PathBuf,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
}

fn range_overlap(
    user_begin: Bound<&[u8]>,
    user_end: Bound<&[u8]>,
    table_begin: KeySlice,
    table_end: KeySlice,
) -> bool {
    match user_end {
        Bound::Excluded(key) if key <= table_begin.raw_ref() => {
            return false;
        }
        Bound::Included(key) if key < table_begin.raw_ref() => {
            return false;
        }
        _ => {}
    }
    match user_begin {
        Bound::Excluded(key) if key >= table_end.raw_ref() => {
            return false;
        }
        Bound::Included(key) if key > table_end.raw_ref() => {
            return false;
        }
        _ => {}
    }
    true
}

fn key_within(user_key: &[u8], table_begin: KeySlice, table_end: KeySlice) -> bool {
    table_begin.raw_ref() <= user_key && user_key <= table_end.raw_ref()
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let arch = LsmStorageArch::create(&options);
        let next_sst_id = 1;
        if !path.exists() {
            std::fs::create_dir_all(path).context("failed to create DB dir")?;
        }
        let storage = Self {
            arch: Arc::new(RwLock::new(Arc::new(arch))),
            path: path.to_path_buf(),
            next_sst_id: AtomicUsize::new(next_sst_id),
            options: options.into(),
        };

        Ok(storage)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        // 加锁
        let snapshot = {
            let guard = self.arch.read();
            Arc::clone(&guard)
        };

        // 在current memtable中查找数据
        if let Some(value) = snapshot.memtable.get(key) {
            if value.is_empty() {
                return Ok(None);
            }
            return Ok(Some(value));
        }

        // 在immutable memtables中查找数据
        for memtable in snapshot.imm_memtables.iter() {
            if let Some(value) = memtable.get(key) {
                if value.is_empty() {
                    // found tomestone, return key not exists
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());

        let keep_table = |key: &[u8], table: &SsTable| {
            if key_within(
                key,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                return true;
            }
            false
        };

        for table in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table].clone();
            if keep_table(key, &table) {
                l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                    table,
                    KeySlice::from_slice(key),
                )?));
            }
        }
        let iter = MergeIterator::create(l0_iters);

        if iter.is_valid() && iter.key().raw_ref() == key && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }

        Ok(None)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }
    /// 删除也是插入一条记录，记录的值为“”，插入的时候必须插入不为“”的值
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// 强制把memtable冻结为im_memtable
    pub fn force_freeze_memtable(&self) -> Result<()> {
        // 获取最新memtable的id
        let memtable_id = self.next_sst_id();
        // 创建新的memtable
        let memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(
                memtable_id,
                self.path_of_wal(memtable_id),
            )?)
        } else {
            Arc::new(MemTable::create(memtable_id))
        };

        self.freeze_memtable_with_memtable(memtable)?;
        // 标志着一个数据写入周期的结束
        self.sync_dir()?;

        Ok(())
    }

    fn freeze_memtable_with_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        // 获取锁
        let mut guard = self.arch.write();
        // Swap the current memtable with a new one.
        let mut snapshot = guard.as_ref().clone();
        // 当前的memtable替换为新的memtable
        let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
        // 把旧表插入到索引为0的位置
        snapshot.imm_memtables.insert(0, old_memtable.clone());
        // 更新snapshot
        *guard = Arc::new(snapshot);
        drop(guard);
        // 保证数据完全写入
        old_memtable.sync_wal()?;

        Ok(())
    }

    /// 处理新增数据时，memtable的阈值问题
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        for record in batch {
            match record {
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    assert!(!key.is_empty(), "key cannot be empty");
                    let size;
                    {
                        let guard = self.arch.read();
                        guard.memtable.put(key, b"")?;
                        size = guard.memtable.mem_size();
                    }
                    self.try_freeze(size)?;
                }
                WriteBatchRecord::Put(key, value) => {
                    let key = key.as_ref();
                    let value = value.as_ref();
                    assert!(!key.is_empty(), "key cannot be empty");
                    assert!(!value.is_empty(), "value cannot be empty");
                    let size;
                    {
                        let guard = self.arch.read();
                        guard.memtable.put(key, value)?;
                        size = guard.memtable.mem_size();
                    }
                    self.try_freeze(size)?;
                }
            }
        }
        Ok(())
    }

    fn try_freeze(&self, estimated_size: usize) -> Result<()> {
        if estimated_size >= self.options.target_sst_size {
            // 使用读写锁将mem_size锁住，不让别的线程同时插入数据
            let guard = self.arch.read();
            // the memtable could have already been frozen, check again to ensure we really need to freeze
            if guard.memtable.mem_size() >= self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable()?;
            }
        }
        Ok(())
    }

    /// scan实现
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.arch.read();
            Arc::clone(&guard)
        }; // drop global lock here

        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        for memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(memtable.scan(lower, upper)));
        }
        let memtable_iter = MergeIterator::create(memtable_iters);

        let mut table_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for table_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table_id].clone();
            if range_overlap(
                lower,
                upper,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                let iter = match lower {
                    Bound::Included(key) => {
                        SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key))?
                    }
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            table,
                            KeySlice::from_slice(key),
                        )?;
                        if iter.is_valid() && iter.key().raw_ref() == key {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
                };

                table_iters.push(Box::new(iter));
            }
        }

        let l0_iter = MergeIterator::create(table_iters);

        let iter = TwoMergeIterator::create(memtable_iter, l0_iter)?;
        Ok(FusedIterator::new(LsmIterator::new(
            iter,
            map_bound(upper),
        )?))
    }
}
