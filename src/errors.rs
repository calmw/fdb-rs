use log::error;
use std::result;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum Errors {
    #[error("failed to read from data file")]
    FailedReadFromDataFile,

    #[error("failed to write to data file")]
    FailedToWriteToDataFile,

    #[error("failed to sync data file")]
    FailedToSyncDataFile,

    #[error("file to open data file")]
    FailedToOpenDataFile,

    #[error("the key is empty")]
    KeyIsEmpty,

    #[error("memory index failed to updated")]
    IndexUpdateFailed,

    #[error("key is not found in database")]
    KeyNotFound,

    #[error("data file is not found in database")]
    DataFileNotFound,

    #[error("database dir path can not be empty")]
    DirPathIsEmpty,

    #[error("database data file size must be greater than 0")]
    DataFileSizeTooSmall,

    #[error("failed to create database directory")]
    FailedToCreateDatabaseDir,

    #[error("failed to read database directory")]
    FailedToReadDatabaseDir,

    #[error("the database directory maybe corrupted")]
    DataDirectoryCorrupted,

    #[error("read data file eof")]
    ReadDataFileEOF,

    #[error("invalid crc value, log record maybe corrupted")]
    InvalidLogRecordCrc,
}

pub type Result<T> = result::Result<T, Errors>;
