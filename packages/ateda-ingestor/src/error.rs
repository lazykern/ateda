use thiserror::Error;
use std::io;
use apache_avro;
use glob;
use tokio::task::JoinError;
use minio_rsc::error::Error as MinioError;
use std::env::VarError;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Processing error: {0}")]
    Processing(String),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Avro error: {0}")]
    Avro(#[from] apache_avro::Error),

    #[error("Glob pattern error: {0}")]
    Glob(#[from] glob::PatternError),

    #[error("Glob error: {0}")]
    GlobError(#[from] glob::GlobError),

    #[error("Task join error: {0}")]
    Join(#[from] JoinError),

    #[error("MinIO client error: {0}")]
    Minio(#[from] MinioError),

    #[error("Missing environment variable: {0}")]
    MissingEnvVar(#[from] VarError),

    #[error("Invalid endpoint URL: {0}")]
    InvalidUrl(String),
}