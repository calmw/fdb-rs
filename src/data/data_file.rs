use crate::data::log_record::LogRecord;
use crate::{errors::Result, fio};
use parking_lot::RwLock;
use std::path::PathBuf;
use std::sync::Arc;

pub struct DataFile {
    file_id: Arc<RwLock<u32>>,
    write_off: Arc<RwLock<u64>>,
    io_manager: Box<dyn fio::IOManager>,
}

impl DataFile {
    pub fn new(dir_path: PathBuf, file_id: u32) -> Result<DataFile> {
        todo!()
    }
    pub fn get_write_off(&self) -> u64 {
        *self.write_off.read()
    }

    pub fn get_file_id(&self) -> u32 {
        let read_guard = self.file_id.read();
        *read_guard
    }

    pub fn sync(&self) -> Result<()> {
        todo!()
    }

    pub fn read_log_record(&self, offset: u64) -> Result<LogRecord> {
        todo!()
    }

    pub fn write(&self, buf: &[u8]) -> Result<u64> {
        todo!()
    }
}