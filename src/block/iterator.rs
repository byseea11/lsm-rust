use std::sync::Arc;

use crate::{
    block::SIZEOF_U16,
    key::{KeySlice, KeyVec},
};
use bytes::Buf;

use super::Block;

/// 块的迭代器
pub struct BlockIterator {
    /// block
    block: Arc<Block>,
    /// 当前的key，如果为null则代表当前blockiterator无效
    key: KeyVec,
    /// block.data中的当前值范围，对应当前键
    value_range: (usize, usize),
    ///键值对的当前索引，在[0,num_of_elements)的范围内，方便next处理
    idx: usize,
    /// block中的第一个key
    first_key: KeyVec,
}

impl Block {
    fn get_first_key(&self) -> KeyVec {
        let mut buf = &self.data[..];
        buf.get_u16();
        let key_len = buf.get_u16();
        let key = &buf[..key_len as usize];
        KeyVec::from_vec(key.to_vec())
    }
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            first_key: block.get_first_key(),
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
        }
    }

    /// 创建一个块迭代器并查找第一个entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// 创建一个块迭代器，并查找>= key 的第一个键
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// 获取key
    pub fn key(&self) -> KeySlice {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        self.key.as_key_slice()
    }

    /// 获取value
    pub fn value(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// 判断iter是否有效
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// 寻找第一个key
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    /// next
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
    }

    ///查找>= key 的第一个键，块中的键值对在添加时已排序（block是从memtable来的，所以block的data是有序的）
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = low + (high - low) / 2;
            self.seek_to(mid);
            assert!(self.is_valid());
            match self.key().cmp(&key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return,
            }
        }
        self.seek_to(low);
    }

    /// 通过index获取key
    fn seek_to(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }
        let offset = self.block.offsets[idx] as usize;
        self.seek_to_offset(offset);
        self.idx = idx;
    }

    // 通过offset获取data，offset记录的就是data的位置
    fn seek_to_offset(&mut self, offset: usize) {
        let mut entry = &self.block.data[offset..];
        // 公共key
        let overlap_len = entry.get_u16() as usize;
        let key_len = entry.get_u16() as usize;
        let key = &entry[..key_len];
        self.key.clear();
        let _test = self.first_key.raw_ref();
        self.key.append(&self.first_key.raw_ref()[..overlap_len]);
        self.key.append(key);
        entry.advance(key_len);
        // 获取value
        let value_len = entry.get_u16() as usize;
        let value_offset_begin = offset + SIZEOF_U16 + SIZEOF_U16 + key_len + SIZEOF_U16;
        let value_offset_end = value_offset_begin + value_len;
        self.value_range = (value_offset_begin, value_offset_end);
        entry.advance(value_len);
    }
}
