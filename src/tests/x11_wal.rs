use tempfile::tempdir;

use crate::{
    compact::{CompactionOptions, SimpleLeveledCompactionOptions},
    lsm_storage::{LsmStorageOptions, MiniLsm},
    tests::harness::dump_files_in_dir,
};

#[test]
fn test_integration_simple() {
    test_integration(CompactionOptions::Simple(SimpleLeveledCompactionOptions {
        size_ratio_percent: 200,
        level0_file_num_compaction_trigger: 2,
        max_levels: 3,
    }));
}

fn test_integration(compaction_options: CompactionOptions) {
    let dir = tempdir().unwrap();
    let mut options = LsmStorageOptions::default_for_week2_test(compaction_options);
    options.enable_wal = true;
    let storage = MiniLsm::open(&dir, options.clone()).unwrap();
    for i in 0..=20 {
        storage.put(b"0", format!("v{}", i).as_bytes()).unwrap();
        if i % 2 == 0 {
            storage.put(b"1", format!("v{}", i).as_bytes()).unwrap();
        } else {
            storage.delete(b"1").unwrap();
        }
        if i % 2 == 1 {
            storage.put(b"2", format!("v{}", i).as_bytes()).unwrap();
        } else {
            storage.delete(b"2").unwrap();
        }
        storage.inner.force_freeze_memtable().unwrap();
    }
    storage.close().unwrap();
    // ensure some SSTs are not flushed
    assert!(
        !storage.inner.arch.read().memtable.is_empty()
            || !storage.inner.arch.read().imm_memtables.is_empty()
    );
    storage.dump_structure();
    drop(storage);
    dump_files_in_dir(&dir);

    let storage = MiniLsm::open(&dir, options).unwrap();
    assert_eq!(&storage.get(b"0").unwrap().unwrap()[..], b"v20".as_slice());
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"v20".as_slice());
    assert_eq!(storage.get(b"2").unwrap(), None);
}
