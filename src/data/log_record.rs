use prost::length_delimiter_len;
use std::sync::atomic::AtomicU32;

// 数据日志类型
#[derive(PartialEq)]
pub enum LogRecordType {
    // 正常put的数据
    NORMAL = 1,
    // 被删除数据标识，墓碑值
    DELETE = 2,
}

// 数据日志结构体，表示实际写到数据文件中的数据
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

// 数据文件索引信息，描述数据存储到了哪个位置
#[derive(Clone, Copy, Debug)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

// 从数据文件中读取的log record 信息，包含size
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: usize,
}

impl LogRecord {
    pub fn encode(&mut self) -> Vec<u8> {
        todo!()
    }
    pub fn get_crc(&mut self) -> u32 {
        todo!()
    }
}

impl LogRecordType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => LogRecordType::NORMAL,
            2 => LogRecordType::DELETE,
            _ => panic!("unknown log record type"),
        }
    }
}

// Rust 代码把CRC部分放在数据最后部分，为了处理方便不放header里面,获取最大长度，非实际长度
pub fn max_log_record_header_size() -> usize {
    // 类型size + key size + value size
    std::mem::size_of::<u8>() + length_delimiter_len(u32::MAX as usize) * 2
}
