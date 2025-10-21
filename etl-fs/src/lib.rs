#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]

use anyhow::Result;
use etl::{Extract, Transform};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};
use tokio_stream::Stream;

/// Extract step that opens a file and streams its content as byte chunks.
pub struct FileExtract;

impl Extract<PathBuf, Vec<u8>> for FileExtract {
    type StreamType = impl Stream<Item = Result<Vec<u8>>> + Send;

    fn extract(&self, path: PathBuf) -> Result<Self::StreamType> {
        let stream = async_stream::stream! {
            let file = File::open(&path).await?;
            let mut reader = BufReader::new(file);
            let mut buffer = vec![0u8; 8192];

            loop {
                let n = reader.read(&mut buffer).await?;
                if n == 0 {
                    break;
                }
                yield Ok(buffer[..n].to_vec());
            }
        };

        Ok(stream)
    }
}

/// Transform step that converts byte chunks into lines of strings.
pub struct BytesToLines;

impl Transform<Vec<u8>, String> for BytesToLines {
    type Stream<'a>
        = impl Stream<Item = Result<String>> + Send + 'a
    where
        Self: 'a,
        Vec<u8>: 'a,
        String: 'a;

    fn transform<'a>(&'a self, input: Vec<u8>) -> Result<Self::Stream<'a>> {
        // Convert bytes to string and split by newlines
        let content = String::from_utf8(input)?;
        let lines: Vec<_> = content.lines().map(|s| Ok(s.to_string())).collect();
        Ok(tokio_stream::iter(lines))
    }
}
