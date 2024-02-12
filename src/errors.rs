use std::result;
use log::error;
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
}

pub type Result<T> = result::Result<T, Errors>;