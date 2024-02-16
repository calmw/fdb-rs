use log::error;
use std::result;
use thiserror::Error;

#[derive(Error, Debug)]
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
}

pub type Result<T> = result::Result<T, Errors>;
