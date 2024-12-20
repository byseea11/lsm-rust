use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::sst_merge_iterator::SstMergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageArch, LsmStorageInner};
use crate::sstable::{SsTable, SsTableBuilder, SsTableIterator};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    // 先实现两层
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
        }
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    NoCompaction,
}

impl LsmStorageInner {
    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }

    fn trigger_flush(&self) -> Result<()> {
        let res = {
            let arch = self.arch.read();
            arch.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if res {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    /// 决定哪些sst进行压缩
    pub fn force_full_compaction(&self) -> Result<()> {
        // 如果当前的压缩选项为 "NoCompaction"（即禁用压缩），则会 panic
        let CompactionOptions::NoCompaction = self.options.compaction_options else {
            panic!("full compaction can only be called with compaction is not enabled")
        };
        // 创建一个快照，保存当前状态
        let snapshot = {
            let state = self.arch.read();
            state.clone()
        };
        // 获取 L0 和 L1 层的 SST 文件
        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels[0].1.clone();
        // 创建一个压缩任务，表示强制执行全量压缩
        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };
        // 输出当前的压缩任务信息
        println!("force full compaction: {:?}", compaction_task);
        // 执行压缩操作，获取压缩后的新的 SST 文件
        let sstables = self.compact(&compaction_task)?;
        // 存储新的 SST 文件的 ID
        let mut ids = Vec::with_capacity(sstables.len());
        {
            // 获取当前状态，并修改其内容
            let mut state = self.arch.read().as_ref().clone();
            // 移除 L0 和 L1 层的 SST 文件
            for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
                let result = state.sstables.remove(sst);
                // 确保 SST 存在并已移除
                assert!(result.is_some());
            }
            // 将新的 SST 文件插入sstable
            for new_sst in sstables {
                ids.push(new_sst.sst_id());
                let result = state.sstables.insert(new_sst.sst_id(), new_sst);
                // 确保插入的是新的 SST 文件
                assert!(result.is_none());
            }
            // 确保 L1 层的 SST 文件没有被修改
            assert_eq!(l1_sstables, state.levels[0].1);
            // 更新 L1 层的 SST 文件列表
            state.levels[0].1.clone_from(&ids);
            // 更新 L0 层的 SST 文件
            let mut l0_sstables_map = l0_sstables.iter().copied().collect::<HashSet<_>>();
            state.l0_sstables = state
                .l0_sstables
                .iter()
                .filter(|x| !l0_sstables_map.remove(x)) // 移除已经处理过的 L0 文件
                .copied()
                .collect::<Vec<_>>();
            // 确保所有的 L0 文件都已处理
            assert!(l0_sstables_map.is_empty());
            // 将更新后的状态写回
            *self.arch.write() = Arc::new(state);
            // 同步目录（保存更改）
            self.sync_dir()?;
        }
        // 删除原来的 SST 文件
        for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*sst))?; // 删除对应的 SST 文件
        }
        // 输出压缩完成后的新 SST 文件 ID
        println!("force full compaction done, new SSTs: {:?}", ids);
        Ok(())
    }

    /// 实际的压缩操作，合并一些 SST 文件并返回一组新的 SST 文件
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        // 获取当前状态快照
        let snapshot = {
            let state = self.arch.read();
            state.clone()
        };
        match task {
            // 如果是全量压缩任务
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                // 获取 L0 层的迭代器
                let mut l0_iters = Vec::with_capacity(l0_sstables.len());
                for id in l0_sstables.iter() {
                    l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snapshot.sstables.get(id).unwrap().clone(), // 获取 L0 文件的迭代器
                    )?));
                }
                // 获取 L1 层的迭代器
                let mut l1_iters = Vec::with_capacity(l1_sstables.len());
                for id in l1_sstables.iter() {
                    l1_iters.push(snapshot.sstables.get(id).unwrap().clone()); // 获取 L1 文件
                }
                // 使用 TwoMergeIterator 合并 L0 和 L1 层的 SST 文件
                let iter = TwoMergeIterator::create(
                    MergeIterator::create(l0_iters), // 合并 L0 层的迭代器
                    SstMergeIterator::create_and_seek_to_first(l1_iters)?, // 合并 L1 层的迭代器
                )?;
                // 使用合并后的迭代器生成新的 SST 文件
                self.compact_generate_sst_from_iter(iter, task.compact_to_bottom_level())
            }
        }
    }

    /// 从迭代器生成新的 SST 文件
    fn compact_generate_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = None;
        let mut new_sst = Vec::new();
        // 遍历迭代器，构建新的 SST 文件
        while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size)); // 初始化 builder
            }
            let builder_inner = builder.as_mut().unwrap();
            // 如果当前 kv 不是空的，添加到 builder 中
            if compact_to_bottom_level {
                if !iter.value().is_empty() {
                    builder_inner.add(iter.key(), iter.value());
                }
            } else {
                builder_inner.add(iter.key(), iter.value());
            }
            // two merge中的next是排好序的
            iter.next()?;
            // 如果当前 builder 的大小达到目标大小，则生成一个新的 SST 文件
            if builder_inner.estimated_size() >= self.options.target_sst_size {
                let sst_id = self.next_sst_id(); // 获取下一个 SST 文件的 ID
                let builder = builder.take().unwrap(); // 获取 builder 并生成 SST 文件
                let sst = Arc::new(builder.build(sst_id, self.path_of_sst(sst_id))?);
                new_sst.push(sst); // 添加新的 SST 文件到列表
            }
        }
        // 如果最后还有未生成的 SST 文件，则生成一个
        if let Some(builder) = builder {
            let sst_id = self.next_sst_id(); // 获取下一个 SST 文件的 ID
            let sst = Arc::new(builder.build(sst_id, self.path_of_sst(sst_id))?);
            new_sst.push(sst); // 添加新的 SST 文件到列表
        }
        // 返回新的 SST 文件
        Ok(new_sst)
    }
}
