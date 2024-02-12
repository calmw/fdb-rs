use std::collections::BTreeMap;
use std::sync::Arc;
use parking_lot::RwLock;
use crate::data::log_record::LogRecordPos;
use crate::index::Indexer;

// Btree索引，主要封装了标准库中的btreeMap结构
pub struct Btree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl Btree {
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new()))
        }
    }
}

impl Indexer for Btree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let mut write_guard = self.tree.write();
        write_guard.insert(key, pos);
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let read_guard = self.tree.read();
        read_guard.get(&key).copied()
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let mut write_guard = self.tree.write();
        let remove_res = write_guard.remove(&key);
        remove_res.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_put() {
        let bt = Btree::new();
        let res1 = bt.put("".as_bytes().to_vec(), LogRecordPos { file_id: 1, offset: 10 });
        assert_eq!(res1, true);
        let res2 = bt.put("aa".as_bytes().to_vec(), LogRecordPos { file_id: 11, offset: 22 });
        assert_eq!(res2, true);
    }

    #[test]
    fn test_btree_get() {
        let bt = Btree::new();
        let res1 = bt.put("".as_bytes().to_vec(), LogRecordPos { file_id: 1, offset: 10 });
        assert_eq!(res1, true);
        let res2 = bt.put("aa".as_bytes().to_vec(), LogRecordPos { file_id: 11, offset: 22 });
        assert_eq!(res2, true);


        let pos1 = bt.get("".as_bytes().to_vec());
        println!("pos={:?}",pos1);
        assert!(pos1.is_some());
        assert_eq!(pos1.unwrap().file_id,1);
        assert_eq!(pos1.unwrap().offset,10);

        let pos1 = bt.get("aa".as_bytes().to_vec());
        println!("pos={:?}",pos1);
        assert!(pos1.is_some());
        assert_eq!(pos1.unwrap().file_id,11);
        assert_eq!(pos1.unwrap().offset,22);
    }

    #[test]
    fn test_btree_del() {
        let bt = Btree::new();
        let res1 = bt.put("".as_bytes().to_vec(), LogRecordPos { file_id: 1, offset: 10 });
        assert_eq!(res1, true);
        let res2 = bt.put("aa".as_bytes().to_vec(), LogRecordPos { file_id: 11, offset: 22 });
        assert_eq!(res2, true);


        let del1 = bt.delete("".as_bytes().to_vec());
        assert!(del1);

        let del2 = bt.delete("not exist key".as_bytes().to_vec());
        println!("del2={:?}",del2);

        let pos1 = bt.get("".as_bytes().to_vec());
        println!("pos={:?}",pos1);

        let pos1 = bt.get("aa".as_bytes().to_vec());
        println!("pos={:?}",pos1);
        assert!(pos1.is_some());
        assert_eq!(pos1.unwrap().file_id,11);
        assert_eq!(pos1.unwrap().offset,22);
    }
}