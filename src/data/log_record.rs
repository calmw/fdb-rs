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
    pub(crate) size: u64,
}

impl LogRecord {
    pub fn encode(&mut self) -> Vec<u8> {
        todo!()
    }
}
