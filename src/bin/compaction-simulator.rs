mod wrapper;
use wrapper::mini_lsm_wrapper;

use std::collections::HashMap;
use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use clap::Parser;
use mini_lsm_wrapper::key::KeyBytes;
use mini_lsm_wrapper::lsm_storage::LsmStorageArch;
use mini_lsm_wrapper::memtable::MemTable;
use mini_lsm_wrapper::sstable::SsTable;

pub struct MockStorage {
    snapshot: LsmStorageArch,
    next_sst_id: usize,
    /// Maps SST ID to the original flushed SST ID
    file_list: HashMap<usize, usize>,
    total_flushes: usize,
    total_writes: usize,
}

impl Default for MockStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MockStorage {
    pub fn new() -> Self {
        let snapshot = LsmStorageArch {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            sstables: Default::default(),
        };
        Self {
            snapshot,
            next_sst_id: 1,
            file_list: Default::default(),
            total_flushes: 0,
            total_writes: 0,
        }
    }

    fn generate_sst_id(&mut self) -> usize {
        let id = self.next_sst_id;
        self.next_sst_id += 1;
        id
    }

    pub fn flush_sst_to_l0(&mut self) -> usize {
        let id = self.generate_sst_id();
        self.snapshot.l0_sstables.push(id);
        self.file_list.insert(id, id);
        self.total_flushes += 1;
        self.total_writes += 1;
        id
    }

    pub fn flush_sst_to_new_tier(&mut self) {
        let id = self.generate_sst_id();
        self.file_list.insert(id, id);
        self.total_flushes += 1;
        self.total_writes += 1;
    }

    pub fn remove(&mut self, files_to_remove: &[usize]) {
        for file_id in files_to_remove {
            let ret = self.file_list.remove(file_id);
            assert!(ret.is_some(), "failed to remove file {}", file_id);
        }
    }

    pub fn dump_size_only(&self) {
        print!("Levels: {}", self.snapshot.l0_sstables.len());
        println!();
    }

    pub fn dump_original_id(&self, always_show_l0: bool) {
        if !self.snapshot.l0_sstables.is_empty() || always_show_l0 {
            println!(
                "L0 ({}): {:?}",
                self.snapshot.l0_sstables.len(),
                self.snapshot.l0_sstables,
            );
        }
    }

    pub fn dump_real_id(&self, always_show_l0: bool) {
        if !self.snapshot.l0_sstables.is_empty() || always_show_l0 {
            println!(
                "L0 ({}): {:?}",
                self.snapshot.l0_sstables.len(),
                self.snapshot.l0_sstables,
            );
        }
    }
}

fn generate_random_key_range() -> (KeyBytes, KeyBytes) {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let begin: usize = rng.gen_range(0..(1 << 31));
    let end: usize = begin + rng.gen_range((1 << 10)..(1 << 31));
    let mut begin_bytes = BytesMut::new();
    let mut end_bytes = BytesMut::new();
    begin_bytes.put_u64(begin as u64);
    end_bytes.put_u64(end as u64);
    (
        KeyBytes::for_testing_from_bytes_no_ts(begin_bytes.freeze()),
        KeyBytes::for_testing_from_bytes_no_ts(end_bytes.freeze()),
    )
}

fn generate_random_split(
    begin_bytes: KeyBytes,
    end_bytes: KeyBytes,
    split: usize,
) -> Vec<(KeyBytes, KeyBytes)> {
    let begin = begin_bytes.for_testing_key_ref().get_u64();
    let end = end_bytes.for_testing_key_ref().get_u64();
    let len = end - begin + 1;
    let mut result = Vec::new();
    let split = split as u64;
    assert!(len >= split, "well, this is unfortunate... run again!");
    for i in 0..split {
        let nb = begin + len * i / split;
        let ne = begin + len * (i + 1) / split - 1;
        let mut begin_bytes = BytesMut::new();
        let mut end_bytes = BytesMut::new();
        begin_bytes.put_u64(nb);
        end_bytes.put_u64(ne);
        result.push((
            KeyBytes::for_testing_from_bytes_no_ts(begin_bytes.freeze()),
            KeyBytes::for_testing_from_bytes_no_ts(end_bytes.freeze()),
        ));
    }
    result
}

fn main() {}
