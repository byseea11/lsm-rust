use bytes::BufMut;

use super::{Block, SIZEOF_U16};
use crate::key::{KeySlice, KeyVec};

/// 在block的基础上再添加一些加速生成block的东西
pub struct BlockBuilder {
    /// 每一个kv的offset
    offsets: Vec<u16>,
    /// 序列化的（entry<key_len key value_len value>）kv对
    data: Vec<u8>,
    /// block的限定大小
    block_size: usize,
    /// block中的第一个key
    first_key: KeyVec,
}

// 寻找公共前缀
fn compute_overlap(first_key: KeySlice, key: KeySlice) -> usize {
    let mut i = 0;
    loop {
        if i >= first_key.len() || i >= key.len() {
            break;
        }
        if first_key.raw_ref()[i] != key.raw_ref()[i] {
            break;
        }
        i += 1;
    }
    i
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    fn estimated_size(&self) -> usize {
        // kv总数量+offset+data_len
        SIZEOF_U16 + self.offsets.len() * SIZEOF_U16 + self.data.len()
    }

    /// 往block中添加一个kv，如果满了就返回false
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        if self.estimated_size() + key.len() + value.len() + SIZEOF_U16 * 3 /* key_len, value_len and offset */ > self.block_size
            && !self.is_empty()
        {
            return false;
        }
        // 添加offset
        self.offsets.push(self.data.len() as u16);
        // 寻找当前key与first key的公共前缀
        let overlap = compute_overlap(self.first_key.as_key_slice(), key);
        // 编码公共前缀
        self.data.put_u16(overlap as u16);
        // 编码剩余部分长度
        self.data.put_u16((key.len() - overlap) as u16);
        // 编码key
        self.data.put(&key.raw_ref()[overlap..]);
        // 编码value长度
        self.data.put_u16(value.len() as u16);
        // 编码value
        self.data.put(value);

        //如果 Block 是空的（即 first_key 是空的），则将当前键设置为 Block 的第一个键
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        true
    }

    /// 判断当前block是否为null
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// 构建block
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
