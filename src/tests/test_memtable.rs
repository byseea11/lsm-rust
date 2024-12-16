use std::sync::Arc;

use crate::memtable::MemTable;

#[test]
fn test_task1_memtable_get() {
    let wal_path: &str = "./example/test1.txt";
    let memtable = MemTable::create(0, wal_path);
    memtable.put(b"key1", b"value1").unwrap();
    memtable.put(b"key2", b"value2").unwrap();
    memtable.put(b"key3", b"value3").unwrap();
    assert_eq!(&memtable.get(b"key1").unwrap()[..], b"value1");
    assert_eq!(&memtable.get(b"key2").unwrap()[..], b"value2");
    assert_eq!(&memtable.get(b"key3").unwrap()[..], b"value3")
}
