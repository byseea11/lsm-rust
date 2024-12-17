#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;
use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// 二叉堆构建，最大键值、最大index会在最前面
impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// iter1: b->del, c->4, d->5
/// iter2: a->1, b->2, c->3
/// iter3: e->4
/// merge之后：a->1, b->del, c->4, d->5, e->4
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        // 判断是否传入iters，若为null则直接返回一个null的就行
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        let mut heap = BinaryHeap::new();

        // 如果所有传入的迭代器都是无效的（通过 is_valid() 判断），则从 iters 中取出最后一个迭代器作为当前的有效迭代器，并创建 Self 对象返回。
        if iters.iter().all(|x| !x.is_valid()) {
            // All invalid, select the last one as the current.
            let mut iters = iters;
            return Self {
                iters: heap,
                current: Some(HeapWrapper(0, iters.pop().unwrap())),
            };
        }

        // 遍历所有迭代器并将有效的迭代器加入堆
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }

        // 选择index最大的迭代器作为当前的迭代器
        let current = heap.pop().unwrap();
        Self {
            iters: heap,
            current: Some(current),
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|x| x.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();
        // 获取堆顶的迭代器，并去重
        while let Some(mut inner_iter) = self.iters.peek_mut() {
            // 如果出现下面的情况说明堆被破坏了
            debug_assert!(
                inner_iter.1.key() >= current.1.key(),
                "heap invariant violated"
            );

            // 去重
            if inner_iter.1.key() == current.1.key() {
                // 跳过当前元素
                if let e @ Err(_) = inner_iter.1.next() {
                    // 如果没法调用next则弹出当前iter
                    PeekMut::pop(inner_iter);
                    return e;
                }

                // 判断是否为null
                if !inner_iter.1.is_valid() {
                    PeekMut::pop(inner_iter);
                }
            } else {
                break;
            }
        }

        current.1.next()?;

        // 如果current无效，就让iters最大的iter变为current
        if !current.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *current = iter;
            }
            return Ok(());
        }

        // 判断curent和堆顶元素是否满足条件
        if let Some(mut inner_iter) = self.iters.peek_mut() {
            if *current < *inner_iter {
                std::mem::swap(&mut *inner_iter, current);
            }
        }

        Ok(())
    }
}
