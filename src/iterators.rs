pub mod merge_iterator;
pub mod sst_merge_iterator;
pub mod two_merge_iterator;
/// 迭代器是用来遍历容器的，需要有一个position来维护迭代器的位置
pub trait StorageIterator {
    type KeyType<'a>: PartialEq + Eq + PartialOrd + Ord
    where
        Self: 'a;

    /// 获取当前的值
    fn value(&self) -> &[u8];

    /// 获取当前的key
    fn key(&self) -> Self::KeyType<'_>;

    /// 检查当前iterator是否有效
    fn is_valid(&self) -> bool;

    /// 移动到下一个位置
    fn next(&mut self) -> anyhow::Result<()>;

    /// Number of underlying active iterators for this iterator.
    fn num_active_iterators(&self) -> usize {
        1
    }
}
