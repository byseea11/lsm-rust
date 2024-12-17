use std::{ops::Bound, sync::Arc};

use bytes::Bytes;
use tempfile::tempdir;

use super::harness::{check_iter_result_by_key, expect_iter_error, MockIterator};
use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    lsm_storage::{LsmStorageInner, LsmStorageOptions},
    memtable::MemTable,
};

#[test]
fn test_task1_memtable_iter() {
    use std::ops::Bound;
    let memtable = MemTable::create(0);
    memtable.put(b"key1", b"value1").unwrap();
    memtable.put(b"key2", b"value2").unwrap();
    memtable.put(b"key3", b"value3").unwrap();

    {
        let mut iter = memtable.scan(Bound::Unbounded, Bound::Unbounded);
        assert_eq!(iter.key().for_testing_key_ref(), b"key1");
        assert_eq!(iter.value(), b"value1");
        assert!(iter.is_valid());
        iter.next().unwrap();
        assert_eq!(iter.key().for_testing_key_ref(), b"key2");
        assert_eq!(iter.value(), b"value2");
        assert!(iter.is_valid());
        iter.next().unwrap();
        assert_eq!(iter.key().for_testing_key_ref(), b"key3");
        assert_eq!(iter.value(), b"value3");
        assert!(iter.is_valid());
        iter.next().unwrap();
        assert!(!iter.is_valid());
    }

    {
        let mut iter = memtable.scan(Bound::Included(b"key1"), Bound::Included(b"key2"));
        assert_eq!(iter.key().for_testing_key_ref(), b"key1");
        assert_eq!(iter.value(), b"value1");
        assert!(iter.is_valid());
        iter.next().unwrap();
        assert_eq!(iter.key().for_testing_key_ref(), b"key2");
        assert_eq!(iter.value(), b"value2");
        assert!(iter.is_valid());
        iter.next().unwrap();
        assert!(!iter.is_valid());
    }

    {
        let mut iter = memtable.scan(Bound::Excluded(b"key1"), Bound::Excluded(b"key3"));
        assert_eq!(iter.key().for_testing_key_ref(), b"key2");
        assert_eq!(iter.value(), b"value2");
        assert!(iter.is_valid());
        iter.next().unwrap();
        assert!(!iter.is_valid());
    }
}

#[test]
fn test_task1_empty_memtable_iter() {
    use std::ops::Bound;
    let memtable = MemTable::create(0);
    {
        let iter = memtable.scan(Bound::Excluded(b"key1"), Bound::Excluded(b"key3"));
        assert!(!iter.is_valid());
    }
    {
        let iter = memtable.scan(Bound::Included(b"key1"), Bound::Included(b"key2"));
        assert!(!iter.is_valid());
    }
    {
        let iter = memtable.scan(Bound::Unbounded, Bound::Unbounded);
        assert!(!iter.is_valid());
    }
}

#[test]
fn test_task2_merge_1() {
    let i1 = MockIterator::new(vec![
        (Bytes::from("a"), Bytes::from("1.1")),
        (Bytes::from("b"), Bytes::from("2.1")),
        (Bytes::from("c"), Bytes::from("3.1")),
        (Bytes::from("e"), Bytes::new()),
    ]);
    let i2 = MockIterator::new(vec![
        (Bytes::from("a"), Bytes::from("1.2")),
        (Bytes::from("b"), Bytes::from("2.2")),
        (Bytes::from("c"), Bytes::from("3.2")),
        (Bytes::from("d"), Bytes::from("4.2")),
    ]);
    let i3 = MockIterator::new(vec![
        (Bytes::from("b"), Bytes::from("2.3")),
        (Bytes::from("c"), Bytes::from("3.3")),
        (Bytes::from("d"), Bytes::from("4.3")),
    ]);

    let mut iter = MergeIterator::create(vec![
        Box::new(i1.clone()),
        Box::new(i2.clone()),
        Box::new(i3.clone()),
    ]);

    check_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.1")),
            (Bytes::from("c"), Bytes::from("3.1")),
            (Bytes::from("d"), Bytes::from("4.2")),
            (Bytes::from("e"), Bytes::new()),
        ],
    );

    let mut iter = MergeIterator::create(vec![Box::new(i3), Box::new(i1), Box::new(i2)]);

    check_iter_result_by_key(
        &mut iter,
        vec![
            (Bytes::from("a"), Bytes::from("1.1")),
            (Bytes::from("b"), Bytes::from("2.3")),
            (Bytes::from("c"), Bytes::from("3.3")),
            (Bytes::from("d"), Bytes::from("4.3")),
            (Bytes::from("e"), Bytes::new()),
        ],
    );
}
