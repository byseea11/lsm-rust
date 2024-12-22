mod simple_leveled;

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::sst_merge_iterator::SstMergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageArch, LsmStorageInner};
use crate::manifest::ManifestRecord;
use crate::sstable::{SsTable, SsTableBuilder, SsTableIterator};
use anyhow::Result;
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Simple(SimpleLeveledCompactionTask),
    // 先实现两层
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::ForceFullCompaction { .. } => true,
        }
    }
}

pub(crate) enum CompactionController {
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageArch) -> Option<CompactionTask> {
        match self {
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),

            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageArch,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageArch, Vec<usize>) {
        match (self, task) {
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }

    pub fn flush_to_l0(&self) -> bool {
        matches!(self, Self::Simple(_) | Self::NoCompaction)
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    Simple(SimpleLeveledCompactionOptions),
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
            self.manifest
                .as_ref()
                .unwrap()
                .add_record(ManifestRecord::Compaction(compaction_task, ids.clone()))?;
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
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                ..
            }) => match upper_level {
                Some(_) => {
                    let mut upper_ssts = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in upper_level_sst_ids.iter() {
                        upper_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                    }
                    let upper_iter = SstMergeIterator::create_and_seek_to_first(upper_ssts)?;
                    let mut lower_ssts = Vec::with_capacity(lower_level_sst_ids.len());
                    for id in lower_level_sst_ids.iter() {
                        lower_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                    }
                    let lower_iter = SstMergeIterator::create_and_seek_to_first(lower_ssts)?;
                    self.compact_generate_sst_from_iter(
                        TwoMergeIterator::create(upper_iter, lower_iter)?,
                        task.compact_to_bottom_level(),
                    )
                }
                None => {
                    let mut upper_iters = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in upper_level_sst_ids.iter() {
                        upper_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            snapshot.sstables.get(id).unwrap().clone(),
                        )?));
                    }
                    let upper_iter = MergeIterator::create(upper_iters);
                    let mut lower_ssts = Vec::with_capacity(lower_level_sst_ids.len());
                    for id in lower_level_sst_ids.iter() {
                        lower_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                    }
                    let lower_iter = SstMergeIterator::create_and_seek_to_first(lower_ssts)?;
                    self.compact_generate_sst_from_iter(
                        TwoMergeIterator::create(upper_iter, lower_iter)?,
                        task.compact_to_bottom_level(),
                    )
                }
            },
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

    /// 触发压缩任务，并应用压缩结果更新 LSM 存储结构
    fn trigger_compaction(&self) -> Result<()> {
        // 创建一个快照，读取当前的存储状态
        let snapshot = {
            let state = self.arch.read(); // 获取存储架构的只读状态
            state.clone() // 克隆状态，避免修改原始状态
        };
        // 生成压缩任务
        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot); // 通过压缩控制器生成压缩任务

        // 如果没有生成任务（即没有需要压缩的任务），直接返回成功
        let Some(task) = task else {
            return Ok(()); // 没有任务要执行，直接返回
        };
        // 打印当前的压缩任务并保存当前的存储结构
        self.dump_structure(); // 打印当前存储结构（用于调试）
        println!("running compaction task: {:?}", task); // 打印当前的压缩任务

        // 执行压缩任务，生成压缩结果（SST 文件）
        let sstables = self.compact(&task)?; // 调用 compact 方法进行实际的压缩

        // 获取压缩后生成的所有 SST 文件的 ID
        let output = sstables.iter().map(|x| x.sst_id()).collect::<Vec<_>>(); // 生成 SST 文件 ID 列表

        // 处理压缩结果，将新生成的 SST 文件添加到快照中，移除旧的文件
        let ssts_to_remove = {
            let mut snapshot = self.arch.read().as_ref().clone(); // 读取并克隆存储架构的当前快照
            let mut new_sst_ids = Vec::new(); // 创建一个新的 SST 文件 ID 列表

            // 将压缩后的 SST 文件插入快照中的 SST 文件集合
            for file_to_add in sstables {
                new_sst_ids.push(file_to_add.sst_id()); // 将新的 SST 文件 ID 添加到列表
                let result = snapshot.sstables.insert(file_to_add.sst_id(), file_to_add); // 插入新的 SST 文件
                assert!(result.is_none()); // 确保没有重复的文件被插入
            }

            // 应用压缩结果，更新快照并获得需要移除的文件
            let (mut snapshot, files_to_remove) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output); // 应用压缩结果，更新快照

            let mut ssts_to_remove = Vec::with_capacity(files_to_remove.len()); // 创建一个新的列表用于存储被移除的文件

            // 将移除的文件从快照中删除
            for file_to_remove in &files_to_remove {
                let result = snapshot.sstables.remove(file_to_remove); // 移除 SST 文件
                assert!(result.is_some(), "cannot remove {}.sst", file_to_remove); // 确保文件确实被移除
                ssts_to_remove.push(result.unwrap()); // 将被移除的文件添加到列表
            }

            // 写回更新后的快照状态
            let mut state = self.arch.write(); // 获取存储架构的可写状态
            *state = Arc::new(snapshot); // 更新存储架构的状态为新的快照
            drop(state); // 释放可写状态的锁
            self.sync_dir()?; // 同步目录，确保文件系统更新
            self.manifest
                .as_ref()
                .unwrap()
                .add_record(ManifestRecord::Compaction(task, new_sst_ids))?;
            ssts_to_remove // 返回被移除的 SST 文件列表
        };

        // 打印压缩完成的信息，显示移除的文件数量和添加的文件数量
        println!(
            "compaction finished: {} files removed, {} files added, output={:?}",
            ssts_to_remove.len(), // 打印移除文件的数量
            output.len(),         // 打印添加文件的数量
            output                // 打印压缩后输出的文件 ID 列表
        );

        // 删除被移除的 SST 文件
        for sst in ssts_to_remove {
            std::fs::remove_file(self.path_of_sst(sst.sst_id()))?; // 删除文件系统中的 SST 文件
        }

        self.sync_dir()?; // 再次同步目录，确保删除操作生效

        Ok(()) // 返回成功
    }

    /// 启动一个新线程执行压缩任务
    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>, // 当前对象的引用，通常是 `Arc<Self>` 以便在线程间共享
        rx: crossbeam_channel::Receiver<()>, // 用于接收信号的通道
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        // 返回一个可选的线程句柄
        // 如果使用的是简单的压缩选项
        if let CompactionOptions::Simple(_) = self.options.compaction_options {
            let this = self.clone(); // 克隆当前对象，以便在线程中使用
            let handle = std::thread::spawn(move || {
                // 启动一个新的线程
                let ticker = crossbeam_channel::tick(Duration::from_millis(50)); // 创建一个定时器，每 50 毫秒触发一次
                loop {
                    crossbeam_channel::select! { // 选择接收信号
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() { // 如果定时器触发，执行压缩任务
                            eprintln!("compaction failed: {}", e); // 打印压缩失败的错误信息
                        },
                        recv(rx) -> _ => return // 如果接收到退出信号，退出循环
                    }
                }
            });
            return Ok(Some(handle)); // 返回线程句柄
        }

        Ok(None) // 如果不使用简单压缩选项，返回 None
    }
}
