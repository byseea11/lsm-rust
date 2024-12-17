use std::{ops::Bound, sync::Arc};

use bytes::Bytes;
use tempfile::tempdir;

use crate::{
    iterators::StorageIterator,
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
