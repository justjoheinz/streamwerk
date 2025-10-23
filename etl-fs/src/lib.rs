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


/// Extract step that reads from stdin and streams its content line by line.
///
/// Takes `()` as input (since stdin is always available) and produces a stream of lines.
pub struct StdinLineExtract;


/// Load step that writes items to stdout.
///
/// Accepts any type that implements `Display` and writes each item to stdout
/// followed by a newline.
pub struct StdoutLoad;

/// Write mode for FileLoad - determines whether to create/truncate or append.
#[derive(Debug, Clone, Copy)]
pub enum WriteMode {
    /// Create a new file or truncate existing file
    Create,
    /// Append to existing file or create new file
    Append,
}

/// Load step that writes items to a file.
///
/// Opens the file during initialization, writes each item followed by a newline,
/// and closes/flushes the file during finalization.
pub struct FileLoad<T> {
    path: PathBuf,
    mode: WriteMode,
    file: std::sync::Arc<tokio::sync::Mutex<Option<tokio::io::BufWriter<File>>>>,
    _phantom: std::marker::PhantomData<T>,
}


// Implementation for types that implement Display (like String, formatted output)
impl<T: std::fmt::Display> etl::Load<T> for FileLoad<T> {
    fn initialize(&self) -> impl std::future::Future<Output = Result<()>> + Send {
        let path = self.path.clone();
        let mode = self.mode;
        let file = self.file.clone();
        
        async move {
            use tokio::fs::OpenOptions;
            use tokio::io::BufWriter;
            
            let file_handle = match mode {
                WriteMode::Create => OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&path)
                    .await?,
                WriteMode::Append => OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(&path)
                    .await?,
            };
            
            let writer = BufWriter::new(file_handle);
            *file.lock().await = Some(writer);
            Ok(())
        }
    }

    fn load(&self, item: T) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        
        let file = self.file.clone();
        let item_str = item.to_string();
        
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut guard = file.lock().await;
                let writer = guard.as_mut().ok_or_else(|| anyhow::anyhow!("File not initialized"))?;
                writer.write_all(item_str.as_bytes()).await?;
                writer.write_all(b"\n").await?;
                Ok(())
            })
        })
    }

    fn finalize(&self, _result: &Result<()>) -> impl std::future::Future<Output = Result<()>> + Send {
        let file = self.file.clone();
        
        async move {
            use tokio::io::AsyncWriteExt;
            
            let mut guard = file.lock().await;
            if let Some(mut writer) = guard.take() {
                writer.flush().await?;
            }
            Ok(())
        }
    }
}

impl<T> FileLoad<T> {
    /// Create a new FileLoad that will write to the specified path.
    ///
    /// The file will be opened during pipeline initialization according to the write mode:
    /// - `WriteMode::Create`: Creates new file or truncates existing file
    /// - `WriteMode::Append`: Appends to existing file or creates new file
    pub fn new(path: impl Into<PathBuf>, mode: WriteMode) -> Self {
        Self {
            path: path.into(),
            mode,
            file: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Create a new FileLoad that creates/truncates the file.
    pub fn create(path: impl Into<PathBuf>) -> Self {
        Self::new(path, WriteMode::Create)
    }

    /// Create a new FileLoad that appends to the file.
    pub fn append(path: impl Into<PathBuf>) -> Self {
        Self::new(path, WriteMode::Append)
    }
}

impl<T: std::fmt::Display> etl::Load<T> for StdoutLoad {
    fn load(&self, item: T) -> Result<()> {
        println!("{}", item);
        Ok(())
    }
}

impl Extract<(), String> for StdinLineExtract {
    type StreamType = impl Stream<Item = Result<String>> + Send;

    fn extract(&self, _input: ()) -> Result<Self::StreamType> {
        use tokio::io::AsyncBufReadExt;

        let stream = async_stream::stream! {
            let stdin = tokio::io::stdin();
            let reader = BufReader::new(stdin);
            let mut lines = reader.lines();

            while let Some(line) = lines.next_line().await? {
                yield Ok(line);
            }
        };

        Ok(stream)
    }
}
