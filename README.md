# todo list

     [√] memtable 
        [√] wal  
        [√] iterator    
     [√] sstable  
        [√] block  
        [√] iterator    
        [√] bloom filter  
     [√] lsm storage arch  
        [√] interface
        [√] cli
     [√] Compact
        [√] l0 l1 compact
        [√] simple compact
     [√] mainfest


# 功能实现

### task1【memtable】
    1. 完成memtable内存表的读写
    2. 完成从wal恢复memtable

### task2【memtable immemtable】
    1. 构造lsm tree的内存基础架构
    2. 完成将memtable强制冻结为immemtable
    3. 完成将memtable自动转换为immemtable
    4. 完成memtable的scan
    5. block以及builder，迭代器明天再写吧
    6. 完成block iterator

### task3【sstable】
    1. 完成sstable builder以及sstable decoder encoder（太难了...）
    2. 完成sstable iter
    3. 实现memtable和sst的merge

### task4【read】
    1. 实现memtable和sstable的scan

### task5【write】
    1. 将 Memtable 刷新到 SST

### task6【cli】
    1. 修改cli文件，实现本项目功能

### task7【compact】
    1. 实现l0_compact
    2. 实现ln_compact

### task7【manifest】
    1. 实现manifest  
