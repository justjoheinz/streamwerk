#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]

use anyhow::Result;
use etl::{Extract};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};
use tokio_stream::Stream;

pub mod prelude;

/// Extract step that opens a file and streams its content byte by byte.
pub struct FileExtract;

impl Extract<PathBuf, u8> for FileExtract {
    type StreamType = impl Stream<Item = Result<u8>> + Send;

    fn extract(&self, path: PathBuf) -> Result<Self::StreamType> {
        let stream = async_stream::stream! {
            let file = File::open(&path).await?;
            let mut reader = BufReader::new(file);
            let mut buffer = [0u8; 1];

            loop {
                let n = reader.read(&mut buffer).await?;
                if n == 0 {
                    break;
                }
                yield Ok(buffer[0]);
            }
        };

        Ok(stream)
    }
}

/// Extract step that opens a file and streams its content line by line.
///
/// Uses tokio's BufReader::lines() for efficient line-based reading.
pub struct FileLineExtract;

impl Extract<PathBuf, String> for FileLineExtract {
    type StreamType = impl Stream<Item = Result<String>> + Send;

    fn extract(&self, path: PathBuf) -> Result<Self::StreamType> {
        use tokio::io::AsyncBufReadExt;

        let stream = async_stream::stream! {
            let file = File::open(&path).await?;
            let reader = BufReader::new(file);
            let mut lines = reader.lines();

            while let Some(line) = lines.next_line().await? {
                yield Ok(line);
            }
        };

        Ok(stream)
    }
}
