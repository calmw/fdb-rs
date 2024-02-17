use crate::data::data_file::{DataFile, DATA_FILE_NAME_SUFFIX};
use crate::data::log_record::LogRecordType::{DELETE, NORMAL};
use crate::data::log_record::{LogRecord, LogRecordPos};
use crate::errors::Errors::{
    DataDirectoryCorrupted, DataFileEOF, DataFileNotFound, DataFileSizeTooSmall, DirPathIsEmpty,
    FailedToCreateDatabaseDir, FailedToReadDatabaseDir, IndexUpdateFailed, KeyIsEmpty, KeyNotFound,
};
use crate::errors::{Errors, Result};
use crate::index;
use crate::options::Options;
use bytes::Bytes;
use log::warn;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

pub const INITIAL_FILE_ID: u32 = 0;

/// 存储引擎实例结构体
pub struct Engine {
    options: Arc<Options>,
    active_file: Arc<RwLock<DataFile>>,
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    index: Box<dyn index::Indexer>,
    file_ids: Vec<u32>,
}

impl Engine {
    pub fn open(opts: Options) -> Result<Self> {
        if let Some(e) = check_options(opts.clone()) {
            return Err(e);
        }
        let options = opts.clone();
        // 判断数据目录是否存在，如果不存在的话，则创建这个目录
        let dir_path = options.dir_path.clone();
        if !dir_path.is_dir() {
            if let Err(e) = fs::create_dir_all(dir_path.clone()) {
                warn!("create database directory err:{}", e);
                return Err(FailedToCreateDatabaseDir);
            }
        }
        // 加载数据文件
        let mut data_files = load_data_files(dir_path.clone())?;
        // 设置file ID信息
        let mut file_ids = Vec::new();
        for v in data_files.iter() {
            file_ids.push(v.get_file_id());
        }

        // 将旧数据文件保存到older_files中
        let mut older_files = HashMap::new();
        if data_files.len() > 1 {
            for _ in 0..=data_files.len() - 2 {
                let file = data_files.pop().unwrap();
                older_files.insert(file.get_file_id(), file);
            }
        }

        // 拿到当前活跃文件，即列表中最后一个文件
        let active_file = match data_files.pop() {
            Some(v) => v,
            None => DataFile::new(dir_path.clone(), INITIAL_FILE_ID)?,
        };

        // 构造存储引擎实例
        let engine = Self {
            options: Arc::new(opts.clone()),
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            index: Box::new(index::new_indexer(opts.index_type.clone())),
            file_ids,
        };

        // 加载内存索引
        engine.load_index_from_data_files()?;

        Ok(engine)
    }

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
            return Err(IndexUpdateFailed);
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
            return Err(KeyNotFound);
        }

        // 从对应数据文件中拿到log record
        let log_record_pos = pos.unwrap();
        let active_file = self.active_file.read();
        let older_files = self.older_files.read();
        let log_record = match active_file.get_file_id() == log_record_pos.file_id {
            true => active_file.read_log_record(log_record_pos.offset)?.record,
            false => {
                let data_file = older_files.get(&log_record_pos.file_id);
                if data_file.is_none() {
                    // 找不到数据文件
                    return Err(DataFileNotFound);
                }
                data_file
                    .unwrap()
                    .read_log_record(log_record_pos.offset)?
                    .record
            }
        };

        // 判断类型
        if log_record.rec_type == DELETE {
            return Err(KeyNotFound);
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

    // 从数据文件中加载索引
    // 遍历数据文件中的内容，并依次处理器中的记录
    pub fn load_index_from_data_files(&self) -> Result<()> {
        if self.file_ids.is_empty() {
            return Ok(());
        }
        let active_file = self.active_file.read();
        let older_files = self.older_files.read();
        // 遍历每个文件id,取出对应的数据文件，并加载其中的数据
        for (i, file_id) in self.file_ids.iter().enumerate() {
            let mut offset = 0;
            loop {
                let log_record_read = match *file_id == active_file.get_file_id() {
                    true => active_file.read_log_record(offset),
                    false => {
                        let data_file = older_files.get(file_id).unwrap();
                        data_file.read_log_record(offset)
                    }
                };
                let (log_record, size) = match log_record_read {
                    Ok(result) => (result.record, result.size),
                    Err(e) => {
                        if e == DataFileEOF {
                            break;
                        }
                        return Err(e);
                    }
                };
                // 构建内存索引
                let log_record_pos = LogRecordPos {
                    file_id: *file_id,
                    offset,
                };
                match log_record.rec_type {
                    NORMAL => self.index.put(log_record.key.to_vec(), log_record_pos),
                    DELETE => self.index.delete(log_record.key.to_vec()),
                };
                // 递增offset
                offset += size
            }

            // 设置活跃文件的offset
            if i == self.file_ids.len() - 1 {
                active_file.set_write_off(offset)
            }
        }

        Ok(())
    }
}

fn load_data_files(dir_path: PathBuf) -> Result<Vec<DataFile>> {
    // 读取数据目录
    let dir = fs::read_dir(dir_path.clone());
    if dir.is_err() {
        return Err(FailedToReadDatabaseDir);
    }

    let mut file_ids: Vec<u32> = Vec::new();
    let mut data_files: Vec<DataFile> = Vec::new();
    for file in dir.unwrap() {
        if let Ok(entry) = file {
            // 拿到文件名
            let file_os_str = entry.file_name();
            let file_name = file_os_str.to_str().unwrap();
            // 判断文件名是否以指定后缀结尾
            if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
                let split_names: Vec<&str> = file_name.split(".").collect();
                let file_id = match split_names[0].parse::<u32>() {
                    Ok(fid) => fid,
                    Err(_) => {
                        return Err(DataDirectoryCorrupted);
                    }
                };
                file_ids.push(file_id);
            }
        }
    }
    // 如果没有数据文件，则直接返回
    if file_ids.is_empty() {
        return Ok(data_files);
    }
    // 对文件ID进行排序，从小到大依次加载
    file_ids.sort();
    // 遍历所有的文件ID，依次打开对应的数据文件
    for file_id in file_ids {
        let data_file = DataFile::new(dir_path.clone(), file_id)?;
        data_files.push(data_file);
    }

    Ok(data_files)
}

fn check_options(opts: Options) -> Option<Errors> {
    let dir_path = opts.dir_path.to_str();
    if dir_path.is_none() || dir_path.unwrap().len() == 0 {
        return Some(DirPathIsEmpty);
    }
    if opts.data_file_size <= 0 {
        return Some(DataFileSizeTooSmall);
    }
    None
}
