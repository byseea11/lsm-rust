use crate::wal::Wal;
use crate::{
    lsm_storage::{LsmStorageInner, LsmStorageOptions},
    memtable::MemTable,
};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::tempdir;
#[test]
fn test_task1_memtable_get() {
    let memtable = MemTable::create(0);
    memtable.put(b"key1", b"value1").unwrap();
    memtable.put(b"key2", b"value2").unwrap();
    memtable.put(b"key3", b"value3").unwrap();
    assert_eq!(&memtable.get(b"key1").unwrap()[..], b"value1");
    assert_eq!(&memtable.get(b"key2").unwrap()[..], b"value2");
    assert_eq!(&memtable.get(b"key3").unwrap()[..], b"value3")
}

#[test]
fn test_task2_memtable_wal() {
    let file_path = "/home/cmlp/code/ruststd/end/lsm-xzy/testdb/wal/wal.txt";
    let path = Path::new(file_path);

    if path.exists() {
        // 文件存在，尝试删除
        if let Err(e) = fs::remove_file(file_path) {
            // 删除失败，处理错误
            eprintln!("Failed to remove file {}: {}", file_path, e);
        } else {
            println!("File {} has been removed", file_path);
        }
    }
    let memtable =
        MemTable::create_with_wal(0, file_path.clone()).expect("Failed to create MemTable");
    memtable.put(b"key1", b"value1").unwrap();
    memtable.put(b"key2", b"value2").unwrap();
    memtable.put(b"key3", b"value3").unwrap();
    assert_eq!(&memtable.get(b"key1").unwrap()[..], b"value1");
    assert_eq!(&memtable.get(b"key2").unwrap()[..], b"value2");
    assert_eq!(&memtable.get(b"key3").unwrap()[..], b"value3");
    let memtable2 =
        MemTable::recover_from_wal(0, file_path.clone()).expect("Failed to recover MemTable");
    assert_eq!(&memtable2.get(b"key1").unwrap()[..], b"value1");
    assert_eq!(&memtable2.get(b"key2").unwrap()[..], b"value2");
    assert_eq!(&memtable2.get(b"key3").unwrap()[..], b"value3");
}

#[test]
fn test_task3_lsmstorage() {
    let dir = tempdir().unwrap();
    let storage = Arc::new(
        LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_week1_test()).unwrap(),
    );
    assert_eq!(&storage.get(b"0").unwrap(), &None);
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.put(b"3", b"23333").unwrap();
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233");
    assert_eq!(&storage.get(b"2").unwrap().unwrap()[..], b"2333");
    assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"23333");
    storage.delete(b"2").unwrap();
    assert!(storage.get(b"2").unwrap().is_none());
    storage.delete(b"0").unwrap();
}
