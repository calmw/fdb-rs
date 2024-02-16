use crate::data::data_file::DataFile;
use crate::data::log_record::LogRecordType::NORMAL;
use crate::data::log_record::{LogRecord, LogRecordPos, LogRecordType};
use crate::errors::Errors::{DataFileNotFound, IndexUpdateFailed, KeyIsEmpty, KeyNotFound};
use crate::errors::Result;
use crate::index;
use crate::options::Options;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// 存储引擎实例结构体
pub struct Engine {
    options: Arc<Options>,
    active_file: Arc<RwLock<DataFile>>,
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    index: Box<dyn index::Indexer>,
}

impl Engine {
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        // 判断key的有效性
        if key.is_empty() {
            return Err(KeyIsEmpty);
        }
        // 构造logRecord
        let mut record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: NORMAL,
        };
        // 追加写到活跃数据文件中
        let log_record_pos = self.append_log_record(&mut record)?;
        // 更新内存索引
        let ok = self.index.put(key.to_vec(), log_record_pos);
        if !ok {
            Err(IndexUpdateFailed)
        }

        Ok(())
    }

    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        // 判断key的有效性
        if key.is_empty() {
            return Err(KeyIsEmpty);
        }
        // 从内存索引中拿到位置信息
        let pos = self.index.get(key.to_vec());
        // 如果key不存在，则直接返回
        if pos.is_none() {
            Err(KeyNotFound)
        }

        // 从对应数据文件中拿到log record
        let log_record_pos = pos.unwrap();
        let active_file = self.active_file.read();
        let older_files = self.older_files.read();
        let log_record = match active_file.get_file_id() == log_record_pos.file_id {
            true => active_file.read_log_record(log_record_pos.offset)?,
            false => {
                let data_file = older_files.get(&log_record_pos.file_id);
                if data_file.is_none() {
                    // 找不到数据文件
                    Err(DataFileNotFound)
                }
                data_file.unwrap().read_log_record(log_record_pos.offset)?;
            }
        };

        // 判断类型
        if log_record.rec_type == LogRecordType::DELETE {
            Err(KeyNotFound)
        }

        // 返回对应的value
        Ok(log_record.value.into())
    }

    fn append_log_record(&self, record: &mut LogRecord) -> Result<LogRecordPos> {
        let dir_path = self.options.dir_path.clone();

        // 输入数据进行编码
        let enc_record = record.encode();
        let record_len = enc_record.len() as u64;

        // 获取并写入到当前活跃文件
        let mut active_file = self.active_file.write();
        // 判断活跃文件大小
        if active_file.get_write_off() + record_len > self.options.data_file_size {
            // 持久化当前活跃文件
            active_file.sync()?;
            // 将活跃文件转换为旧的数据文件，存储到map中
            let current_fid = active_file.get_file_id();
            let mut older_files = self.older_files.write();
            let old_file = DataFile::new(dir_path.clone(), current_fid)?;
            older_files.insert(current_fid, old_file);
            // 打开新的活跃数据文件
            let new_file = DataFile::new(dir_path.clone(), current_fid + 1)?;
            *active_file = new_file;
        }
        // 追加写数据到当前活跃文件中
        let write_off = active_file.write(&enc_record)?;
        // 根据配置项决定是否sync活跃文件
        if self.options.sync_writes {
            active_file.sync()?;
        }

        // 构造数据索引信息
        Ok(LogRecordPos {
            file_id: active_file.get_file_id(),
            offset: write_off,
        })
    }
}
