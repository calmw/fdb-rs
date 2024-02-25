use crate::data::log_record::{
    max_log_record_header_size, LogRecord, LogRecordType, ReadLogRecord,
};
use crate::errors::Errors;
use crate::fio::new_io_manager;
use crate::{errors::Result, fio};
use bytes::{Buf, BytesMut};
use parking_lot::RwLock;
use prost::{decode_length_delimiter, length_delimiter_len};
use std::path::PathBuf;
use std::sync::Arc;

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";

pub struct DataFile {
    file_id: Arc<RwLock<u32>>,
    write_off: Arc<RwLock<u64>>,
    io_manager: Box<dyn fio::IOManager>,
}

impl DataFile {
    // 创建或打开一个新的数据文件
    pub fn new(dir_path: PathBuf, file_id: u32) -> Result<DataFile> {
        // 根据path和ID构造出完整的文件名称
        let file_name = get_data_file_name(dir_path, file_id);
        let io_manager = new_io_manager(file_name)?;

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(file_id)),
            write_off: Arc::new(RwLock::new(0)),
            io_manager: Box::new(io_manager),
        })
    }

    pub fn get_write_off(&self) -> u64 {
        *self.write_off.read()
    }

    pub fn set_write_off(&self, offset: u64) {
        let mut write_guard = self.write_off.write();
        *write_guard = offset
    }

    pub fn get_file_id(&self) -> u32 {
        let read_guard = self.file_id.read();
        *read_guard
    }

    // 根据offset，从数据文件中读取 logRecord
    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        // 先读取出header部分的数据
        let mut header_buf = BytesMut::zeroed(max_log_record_header_size());
        // 取出type,在第一个字节
        let rec_type = header_buf.get_u8();
        // 取出key和value的长度
        let key_size = decode_length_delimiter(&mut header_buf).unwrap();
        let value_size = decode_length_delimiter(&mut header_buf).unwrap();
        // 如果key_size、value_size均为空，则说明读取到了文件末尾，直接返回
        if key_size == 0 && value_size == 0 {
            return Err(Errors::ReadDataFileEOF);
        }

        // 获取实际的header大小
        let actual_header_size =
            length_delimiter_len(key_size) + length_delimiter_len(value_size) + 1;

        // 读取实际的key、value和最后的4字节（CRC校验值）
        let mut kv_buf = BytesMut::zeroed(key_size + value_size + 4);
        self.io_manager
            .read(&mut kv_buf, offset + actual_header_size as u64)?;

        // 构造logRecord
        let mut log_record = LogRecord {
            key: kv_buf.get(..key_size).unwrap().to_vec(),
            value: kv_buf.get(key_size..kv_buf.len() - 4).unwrap().to_vec(),
            rec_type: LogRecordType::from_u8(rec_type),
        };

        // 向前移动到最后的4个字节，就是CRC的值
        kv_buf.advance(key_size + value_size);
        if kv_buf.get_u32() != log_record.get_crc() {
            return Err(Errors::InvalidLogRecordCrc);
        }

        // 构造结果并返回
        Ok(ReadLogRecord {
            record: log_record,
            size: actual_header_size + key_size + value_size + 4,
        })
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let n_bytes = self.io_manager.write(buf)?;
        // 更新write_off字段
        let mut write_off = self.write_off.write();
        *write_off += n_bytes as u64;

        Ok(n_bytes)
    }

    pub fn sync(&self) -> Result<()> {
        self.io_manager.sync()
    }
}

fn get_data_file_name(path: PathBuf, file_id: u32) -> PathBuf {
    let name = std::format!("{:09}", file_id) + DATA_FILE_NAME_SUFFIX;
    path.to_path_buf().join(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_data_file() {
        let dir_path = std::env::temp_dir();
        let data_file_res1 = DataFile::new(dir_path.clone(), 0);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 0);

        let data_file_res2 = DataFile::new(dir_path.clone(), 0);
        assert!(data_file_res2.is_ok());
        let data_file2 = data_file_res2.unwrap();
        assert_eq!(data_file2.get_file_id(), 0);

        let data_file_res3 = DataFile::new(dir_path.clone(), 660);
        assert!(data_file_res3.is_ok());
        let data_file3 = data_file_res3.unwrap();
        assert_eq!(data_file3.get_file_id(), 660);
    }

    #[test]
    fn test_data_file_write() {
        let dir_path = std::env::temp_dir();
        let data_file_res1 = DataFile::new(dir_path.clone(), 100);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 100);

        let write_res1 = data_file1.write("aaa".as_bytes());
        assert!(write_res1.is_ok());
        assert_eq!(write_res1.unwrap(), 3usize);

        let write_res2 = data_file1.write("bbb".as_bytes());
        assert!(write_res2.is_ok());
        assert_eq!(write_res2.unwrap(), 3usize);

        let write_res3 = data_file1.write("ccc".as_bytes());
        assert!(write_res3.is_ok());
        assert_eq!(write_res3.unwrap(), 3usize);
    }

    #[test]
    fn test_data_file_sync() {
        let dir_path = std::env::temp_dir();
        let data_file_res1 = DataFile::new(dir_path.clone(), 200);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 200);

        let sync_res = data_file1.sync();
        assert!(sync_res.is_ok());
    }
}
