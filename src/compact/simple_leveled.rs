use crate::lsm_storage::LsmStorageArch;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    // 下级文件数 / 上级文件数，当 ratio 太低（上层文件太多）时，触发 compaction
    pub size_ratio_percent: usize,
    // 当 L0 中的 SST 个数大于等于该数量时，触发 L0 和 L1 的 Compaction
    pub level0_file_num_compaction_trigger: usize,
    // LSM 树中的级别数（不包括 L0）
    pub max_levels: usize,
}

/// 分层压缩任务
#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // 上级层的索引，如果为 None，则表示是 L0 层的压缩任务
    pub upper_level: Option<usize>,
    // 上级层的 SST 文件 ID 列表
    pub upper_level_sst_ids: Vec<usize>,
    // 下级层的索引
    pub lower_level: usize,
    // 下级层的 SST 文件 ID 列表
    pub lower_level_sst_ids: Vec<usize>,
    // 表示下级层是否是底层（最后一层）
    pub is_lower_level_bottom_level: bool,
}

/// 使用 SimpleLeveledCompactionOptions 来决定何时触发压缩任务
pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// 根据当前存储快照生成一个压缩任务 如果不需要触发压缩，则返回 None，否则返回一个压缩任务
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageArch,
    ) -> Option<SimpleLeveledCompactionTask> {
        let mut level_sizes = Vec::new();
        // L0 层的 SST 文件数量
        level_sizes.push(snapshot.l0_sstables.len());
        // 遍历其它层并记录每层的文件数量
        for (_, files) in &snapshot.levels {
            level_sizes.push(files.len());
        }

        // 遍历每一层判断是否要进行压缩
        for i in 0..self.options.max_levels {
            // 如果 L0 层文件数小于触发阈值，跳过 L0 层的压缩判断
            if i == 0
                && snapshot.l0_sstables.len() < self.options.level0_file_num_compaction_trigger
            {
                continue;
            }
            // 下级层的索引
            let lower_level = i + 1;
            let size_ratio = level_sizes[lower_level] as f64 / level_sizes[i] as f64;
            if size_ratio < self.options.size_ratio_percent as f64 / 100.0 {
                println!(
                    "compaction triggered at level {} and {} with size ratio {}",
                    i, lower_level, size_ratio
                );
                // 进行压缩操作
                return Some(SimpleLeveledCompactionTask {
                    upper_level: if i == 0 { None } else { Some(i) }, // 如果是 L0 层，upper_level 为 None
                    upper_level_sst_ids: if i == 0 {
                        snapshot.l0_sstables.clone() // L0 层的 SST 文件
                    } else {
                        snapshot.levels[i - 1].1.clone() // 其它层的 SST 文件
                    },
                    lower_level,
                    lower_level_sst_ids: snapshot.levels[lower_level - 1].1.clone(),
                    is_lower_level_bottom_level: lower_level == self.options.max_levels,
                });
            }
        }
        None
    }

    /// 应用压缩结果，修改 LSM 存储快照，移除压缩文件并更新相应的 SST 文件列表
    pub fn apply_compaction_result(
        &self,                              // 当前对象的引用，压缩控制器
        snapshot: &LsmStorageArch,          // 当前的 LSM 存储快照
        task: &SimpleLeveledCompactionTask, // 压缩任务，包含上层和下层的 SST 文件信息
        output: &[usize],                   // 压缩结果的输出，表示压缩后生成的新 SST 文件 ID 列表
    ) -> (LsmStorageArch, Vec<usize>) {
        // 返回新的 LSM 存储快照和被移除的文件 ID 列表
        let mut snapshot = snapshot.clone(); // 克隆快照，避免修改原始快照
        let mut files_to_remove = Vec::new(); // 创建一个空的向量，用于存储移除的文件 ID

        // 如果压缩任务包含上层的文件（即不是 L0 层的压缩），处理上层的压缩结果
        if let Some(upper_level) = task.upper_level {
            assert_eq!(
                // 验证上层 SST 文件列表是否一致
                task.upper_level_sst_ids, // 压缩任务中记录的上层 SST 文件列表
                snapshot.levels[upper_level - 1].1, // 当前快照中的上层 SST 文件列表
                "sst mismatched"          // 如果不一致，触发断言错误
            );
            files_to_remove.extend(&snapshot.levels[upper_level - 1].1); // 将上层文件添加到移除列表
            snapshot.levels[upper_level - 1].1.clear(); // 清空当前层的 SST 文件列表，表示移除
        } else {
            // 如果是 L0 层的压缩任务，处理 L0 层的文件移除
            files_to_remove.extend(&task.upper_level_sst_ids); // 将 L0 层的文件添加到移除列表
            let mut l0_ssts_compacted = task
                .upper_level_sst_ids // 获取压缩任务中移除的文件 ID 列表
                .iter() // 遍历所有文件 ID
                .copied() // 拷贝文件 ID
                .collect::<HashSet<_>>(); // 使用 HashSet 来去除重复的文件 ID
            let new_l0_sstables = snapshot
                .l0_sstables // 获取当前 L0 层的文件列表
                .iter() // 遍历文件
                .copied() // 拷贝文件 ID
                .filter(|x| !l0_ssts_compacted.remove(x)) // 过滤掉已被压缩的文件 ID
                .collect::<Vec<_>>(); // 收集剩下的文件为新的 L0 层文件列表
            assert!(l0_ssts_compacted.is_empty()); // 确保所有要压缩的文件都已从 L0 层移除
            snapshot.l0_sstables = new_l0_sstables; // 更新 L0 层的 SST 文件列表
        }

        // 验证下层 SST 文件 ID 列表是否一致
        assert_eq!(
            task.lower_level_sst_ids, // 压缩任务中记录的下层 SST 文件 ID 列表
            snapshot.levels[task.lower_level - 1].1, // 当前快照中的下层 SST 文件列表
            "sst mismatched"          // 如果不一致，触发断言错误
        );
        files_to_remove.extend(&snapshot.levels[task.lower_level - 1].1); // 将下层文件添加到移除列表
        snapshot.levels[task.lower_level - 1].1 = output.to_vec(); // 更新下层的 SST 文件列表为压缩后的结果

        // 返回新的快照和移除的文件 ID 列表
        (snapshot, files_to_remove)
    }
}
