mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// 块，memtable的数据就放在这里面，data是以entry<key_len key value_len value>存储的，offset是entry_len，方便快速读出数据
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        let offsets_len = self.offsets.len();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        // 把数量存在最后
        buf.put_u16(offsets_len as u16);
        buf.into()
    }

    /// 把byte变为block
    pub fn decode(data: &[u8]) -> Self {
        // 获取数量，u16是数量的大小，从后取这个大小的字节就可以获取数量
        let entry_offsets_len = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        // 获取数据的结束位置，因为offset也是u16
        let data_end = data.len() - SIZEOF_U16 - entry_offsets_len * SIZEOF_U16;
        // 从 data_end 到数据末尾的部分-u16是偏移量数组
        let offsets_raw = &data[data_end..data.len() - SIZEOF_U16];
        // 转换
        let offsets = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        // 直接把数据转换就可以了
        let data = data[0..data_end].to_vec();
        Self { data, offsets }
    }
}
