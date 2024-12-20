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

#[test]
fn test_task4_storage_integration() {
    let dir = tempdir().unwrap();
    let storage = Arc::new(
        LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_week1_test()).unwrap(),
    );
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage.force_freeze_memtable().unwrap();
    assert_eq!(storage.arch.read().imm_memtables.len(), 1);
    let previous_mem_size = storage.arch.read().imm_memtables[0].mem_size();
    assert!(previous_mem_size >= 15);
    storage.put(b"1", b"2333").unwrap();
    storage.put(b"2", b"23333").unwrap();
    storage.put(b"3", b"233333").unwrap();
    storage.force_freeze_memtable().unwrap();
    assert_eq!(storage.arch.read().imm_memtables.len(), 2);
    assert!(
        storage.arch.read().imm_memtables[1].mem_size() == previous_mem_size,
        "wrong order of memtables?"
    );

    assert!(storage.arch.read().imm_memtables[0].mem_size() > previous_mem_size);
}

#[test]
fn test_task5_freeze_on_capacity() {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_week1_test();
    options.target_sst_size = 1024;
    options.num_memtable_limit = 1000;
    let storage = Arc::new(LsmStorageInner::open(dir.path(), options).unwrap());
    for _ in 0..1000 {
        storage.put(b"1", b"2333").unwrap();
    }
    let num_imm_memtables = storage.arch.read().imm_memtables.len();
    assert!(num_imm_memtables >= 1, "no memtable frozen?");
    for _ in 0..1000 {
        storage.delete(b"1").unwrap();
    }
    assert!(
        storage.arch.read().imm_memtables.len() > num_imm_memtables,
        "no more memtable frozen?"
    );
}

#[test]
fn test_task6_storage_integration() {
    let dir = tempdir().unwrap();
    let storage = Arc::new(
        LsmStorageInner::open(dir.path(), LsmStorageOptions::default_for_week1_test()).unwrap(),
    );
    assert_eq!(&storage.get(b"0").unwrap(), &None);
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage.force_freeze_memtable().unwrap();
    storage.delete(b"1").unwrap();
    storage.delete(b"2").unwrap();
    storage.put(b"3", b"2333").unwrap();
    storage.put(b"4", b"23333").unwrap();
    storage.force_freeze_memtable().unwrap();
    storage.put(b"1", b"233333").unwrap();
    storage.put(b"3", b"233333").unwrap();
    assert_eq!(storage.arch.read().imm_memtables.len(), 2);
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233333");
    assert_eq!(&storage.get(b"2").unwrap(), &None);
    assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"233333");
    assert_eq!(&storage.get(b"4").unwrap().unwrap()[..], b"23333");
}
