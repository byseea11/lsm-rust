# todo list

     [√] memtable 
        [√] wal  
     [x] sstable  
        [x] block  
        [x] bloom filter  
        [x] 完成报告  
     [x] lsm storage arch  
        [x] interface
        [x] cli


# 功能实现

### task1【memtable】
    1. 完成memtable内存表的读写
    2. 完成从wal恢复memtable

### task2【memtable immemtable】
    1. 构造lsm tree的内存基础架构
    2. 完成将memtable强制冻结为immemtable
    3. 完成将memtable自动转换为immemtable

