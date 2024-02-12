pub mod btree;

use crate::data::log_record::LogRecordPos;

/// Indexer 抽象索引接口，后续如果想要接入其他的数据结构，则直接实现这个接口即可
pub trait Indexer: Sync + Send {
    /// 向索引中存储key对应的数据位置信息,key已存在就更新value并返回旧的value，否则返回nil
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;
    /// 根据key取出对应的索引位置信息
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;
    /// 根据key,删除对应的索引位置信息，已存在就删除并返回旧的value，否则返回nil
    fn delete(&self, key: Vec<u8>) -> bool; // 根据key,删除对应的索引位置信息，已存在就删除并返回旧的value，否则返回nil
}