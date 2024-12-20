use anyhow::{bail, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// 实现一个布隆过滤器
pub struct Bloom {
    /// 过滤器的数据，以位为单位存储
    pub(crate) filter: Bytes,
    /// 哈希函数的数量
    pub(crate) k: u8,
}

/// BitSlice trait，用于从字节切片中读取特定位
pub trait BitSlice {
    /// 获取指定位置的位
    fn get_bit(&self, idx: usize) -> bool;
    /// 获取切片的总位数
    fn bit_len(&self) -> usize;
}

/// 可变的BitSlice trait，用于设置指定位置的位
pub trait BitSliceMut {
    /// 设置指定位置的位
    fn set_bit(&mut self, idx: usize, val: bool);
}

/// 布隆过滤器是位数组，本次实现的是byte，输入的index是bit，所以需要先判断在哪个byte然后再便宜
impl<T: AsRef<[u8]>> BitSlice for T {
    /// 获取指定位置的位
    fn get_bit(&self, idx: usize) -> bool {
        // 计算字节位置
        let pos = idx / 8;
        // 计算在字节中的位偏移
        let offset = idx % 8;
        // 通过位与操作判断该位是否为1，offset相当于是掩码，把offset的位设置为1，如果该结果最后是0，代表该位没东西
        (self.as_ref()[pos] & (1 << offset)) != 0
    }

    /// 获取字节切片的位数
    fn bit_len(&self) -> usize {
        // 字节切片的长度乘以8，得到位数
        self.as_ref().len() * 8
    }
}

impl<T: AsMut<[u8]>> BitSliceMut for T {
    /// 设置指定位置的位
    fn set_bit(&mut self, idx: usize, val: bool) {
        // 计算字节位置
        let pos = idx / 8;
        // 计算在字节中的位偏移
        let offset = idx % 8;
        if val {
            // 如果设置为1，通过或操作设置该位
            self.as_mut()[pos] |= 1 << offset;
        } else {
            // 如果设置为0，通过与操作清除该位
            self.as_mut()[pos] &= !(1 << offset);
        }
    }
}

impl Bloom {
    /// 解码布隆过滤器
    pub fn decode(buf: &[u8]) -> Result<Self> {
        // 分离过滤器数据和哈希函数数量
        let filter = &buf[..buf.len() - 1]; // 过滤器数据
        let k = buf[buf.len() - 1]; // 哈希函数的数量
                                    // 返回一个布隆过滤器对象
        Ok(Self {
            filter: filter.to_vec().into(), // 过滤器数据转为Bytes类型
            k,
        })
    }

    /// 编码布隆过滤器
    pub fn encode(&self, buf: &mut Vec<u8>) {
        // 将过滤器数据追加到缓冲区
        buf.extend(&self.filter);
        // 将哈希函数数量追加到缓冲区
        buf.put_u8(self.k);
    }

    /// 根据kv个数和假阳性率计算每个键的位数
    pub fn bloom_bits_per_key(entries: usize, false_positive_rate: f64) -> usize {
        // 根据公式计算所需的位数
        let size =
            -1.0 * (entries as f64) * false_positive_rate.ln() / std::f64::consts::LN_2.powi(2);
        // 计算每个键对应的位数
        let locs = (size / (entries as f64)).ceil();
        // 返回每个键的位数
        locs as usize
    }

    /// 根据键的哈希值构建布隆过滤器
    pub fn build_from_key_hashes(keys: &[u32], bits_per_key: usize) -> Self {
        // 根据公式计算哈希函数的数量
        let k = (bits_per_key as f64 * 0.69) as u32;
        // 限制哈希函数的数量在1到30之间
        let k = k.clamp(1, 30);
        // 计算所需的位数
        let nbits = (keys.len() * bits_per_key).max(64);
        // 计算所需的字节数
        let nbytes = (nbits + 7) / 8;
        let nbits = nbytes * 8;
        // 初始化一个字节缓冲区来存储过滤器
        let mut filter = BytesMut::with_capacity(nbytes);
        filter.resize(nbytes, 0);

        // 通过旋转和加法模拟hash函数
        for h in keys {
            let mut h = *h;
            // 左旋15位，因为是bit，所以左旋可以很好的把hash值打乱
            let delta = h.rotate_left(15);
            for _ in 0..k {
                // 置为1的位置
                let bit_pos = (h as usize) % nbits;
                filter.set_bit(bit_pos, true);
                // 改变hash值
                h = h.wrapping_add(delta);
            }
        }
        // 返回构建好的布隆过滤器
        Self {
            filter: filter.freeze(),
            k: k as u8,
        }
    }

    /// 检查布隆过滤器是否可能包含某个数据
    pub fn may_contain(&self, mut h: u32) -> bool {
        // 如果哈希函数数量大于30，可能使用了新的编码方式
        if self.k > 30 {
            // 对于短布隆过滤器，直接返回true
            true
        } else {
            // 获取过滤器的总位数
            let nbits = self.filter.bit_len();
            // 对哈希值进行旋转操作
            let delta = h.rotate_left(15);

            // 使用旋转后的哈希值来探测布隆过滤器
            for _ in 0..self.k {
                let bit_pos = h % (nbits as u32);
                if !self.filter.get_bit(bit_pos as usize) {
                    return false;
                }
                h = h.wrapping_add(delta);
            }
            // 默认返回true，表示可能包含该数据
            true
        }
    }
}
