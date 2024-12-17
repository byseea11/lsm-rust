use anyhow::Context;
use anyhow::Result;
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

/// 日志文件
pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    // 创建日志文件
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        println!("path:{}", path.as_ref().display());
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("failed to create WAL")?,
            ))),
        })
    }

    // 从现有的日志文件中恢复memtable
    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover from WAL")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut rbuf: &[u8] = buf.as_slice();
        while rbuf.has_remaining() {
            let key_len = rbuf.get_u16() as usize;
            let key = Bytes::copy_from_slice(&rbuf[..key_len]);
            rbuf.advance(key_len);
            let value_len = rbuf.get_u16() as usize;
            let value = Bytes::copy_from_slice(&rbuf[..value_len]);
            rbuf.advance(value_len);
            println!("key:{:?}, value:{:?} ", key, value);
            skiplist.insert(key, value);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    // 添加数据到日志文件中,key_len key value_len value
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf: Vec<u8> =
            Vec::with_capacity(key.len() + value.len() + std::mem::size_of::<u16>() * 2);
        buf.put_u16(key.len() as u16);
        buf.put_slice(key);
        buf.put_u16(value.len() as u16);
        buf.put_slice(value);
        file.write_all(&buf)?;
        file.flush()?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
