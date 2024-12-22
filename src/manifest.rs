use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

// Manifest 结构体，表示清单文件管理器
pub struct Manifest {
    file: Arc<Mutex<File>>, // 使用 Arc 和 Mutex 封装文件，以实现线程安全的操作
}

#[derive(Serialize, Deserialize)] // 让 ManifestRecord 可以被序列化和反序列化为 JSON 格式
pub enum ManifestRecord {
    // 清单记录的枚举类型，表示不同的操作
    Flush(usize),                           // 刷新操作，usize 是 SST 文件的 ID
    NewMemtable(usize),                     // 新的 Memtable 操作，usize 是 Memtable 的 ID
    Compaction(CompactionTask, Vec<usize>), // 压缩操作，包含压缩任务和涉及的 SST 文件 ID
}

impl Manifest {
    // 创建一个新的 Manifest 文件
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        // 打开文件进行写入，如果文件不存在则创建它
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true) // 允许读取
                    .create_new(true) // 如果文件不存在，则创建新文件
                    .write(true) // 允许写入
                    .open(path) // 打开指定路径的文件
                    .context("failed to create manifest")?, // 错误处理，如果创建失败，则返回错误
            )),
        })
    }

    // 恢复 Manifest 文件并读取其中的记录
    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        // 打开已存在的 Manifest 文件以进行读取
        let mut file = OpenOptions::new()
            .read(true) // 允许读取
            .append(true) // 允许追加写入
            .open(path) // 打开指定路径的文件
            .context("failed to recover manifest")?; // 错误处理，如果打开失败，则返回错误

        let mut buf = Vec::new(); // 用于存储文件的内容
        file.read_to_end(&mut buf)?; // 读取文件内容到 buf 中

        let mut buf_ptr = buf.as_slice(); // 将 buf 转换为 slice，用于按顺序读取数据
        let mut records = Vec::new(); // 用于存储 Manifest 记录的向量

        // 按顺序读取所有的记录
        while buf_ptr.has_remaining() {
            let len = buf_ptr.get_u64(); // 读取记录的长度
            let slice = &buf_ptr[..len as usize]; // 根据长度获取记录的字节数据
            let json = serde_json::from_slice::<ManifestRecord>(slice)?; // 反序列化为 ManifestRecord 类型
            buf_ptr.advance(len as usize); // 移动指针，跳过已经读取的部分
            records.push(json); // 将反序列化后的记录添加到 records 中
        }

        // 返回 Manifest 实例以及读取到的所有记录
        Ok((
            Self {
                file: Arc::new(Mutex::new(file)), // 返回 Manifest 实例
            },
            records, // 返回记录列表
        ))
    }

    // 向 Manifest 文件中添加新的记录
    pub fn add_record(
        &self,
        record: ManifestRecord, // 需要添加的记录
    ) -> Result<()> {
        // 调用内部方法，实际写入记录
        self.add_record_when_init(record)
    }

    // 内部方法，用于初始化时向 Manifest 文件中添加记录
    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        // 获取文件锁，确保文件操作线程安全
        let mut file = self.file.lock();

        // 将记录序列化为 JSON 格式的字节
        let mut buf = serde_json::to_vec(&record)?;

        // 写入记录的长度（8字节，使用大端序）
        file.write_all(&(buf.len() as u64).to_be_bytes())?;
        file.write_all(&buf)?; // 将记录和校验和写入文件
        file.sync_all()?; // 确保数据被写入磁盘

        Ok(())
    }
}
