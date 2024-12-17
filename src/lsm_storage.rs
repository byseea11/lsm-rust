use crate::memtable::MemTable;
use anyhow::{Context, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

#[derive(Clone)]

/// 目前只实现到imm_memtables(不可变的memtable)这部分，所以把lsmstorageArch先定为这个两个部分
pub struct LsmStorageArch {
    /// 当前的memtable
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, 不可变memtable，当memtable达到阈值之后会merge到imm_memtable
    pub imm_memtables: Vec<Arc<MemTable>>,
}

impl LsmStorageArch {
    fn create(options: &LsmStorageOptions) -> Self {
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
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

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) arch: Arc<RwLock<Arc<LsmStorageArch>>>,
    // memtable的wal需要path
    path: PathBuf,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
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
        Ok(None)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        //现在实现是还没有达到阈值
        assert!(!key.is_empty(), "key cannot be empty");
        assert!(!value.is_empty(), "value cannot be empty");
        let guard = self.arch.read();
        guard.memtable.put(key, value)?;
        Ok(())
    }
    /// 删除也是插入一条记录，记录的值为“”，插入的时候必须插入不为“”的值
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        assert!(!key.is_empty(), "key cannot be empty");
        let guard = self.arch.read();
        guard.memtable.put(key, b"")?;
        Ok(())
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

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self) -> Result<()> {
        let memtable_id = self.next_sst_id();
        let memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(
                memtable_id,
                self.path_of_wal(memtable_id),
            )?)
        } else {
            Arc::new(MemTable::create(memtable_id))
        };

        self.freeze_memtable_with_memtable(memtable)?;

        self.sync_dir()?;

        Ok(())
    }

    fn freeze_memtable_with_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        let mut guard = self.arch.write();
        // Swap the current memtable with a new one.
        let mut snapshot = guard.as_ref().clone();
        let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
        // Add the memtable to the immutable memtables.
        snapshot.imm_memtables.insert(0, old_memtable.clone());
        // Update the snapshot.
        *guard = Arc::new(snapshot);

        drop(guard);
        old_memtable.sync_wal()?;

        Ok(())
    }
}
