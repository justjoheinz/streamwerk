#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]

//! # Streamwerk Filesystem I/O
//!
//! This crate provides extractors and loaders for filesystem operations in streamwerk pipelines.
//!
//! **Note:** This crate re-exports the base [`streamwerk`] crate, so you only need to
//! declare `streamwerk-fs` as a dependency in your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! streamwerk-fs = "0.0.1"
//! ```
//!
//! Access base types via the re-export:
//! - `streamwerk_fs::streamwerk::EtlPipeline`
//! - Or use the prelude: `use streamwerk_fs::prelude::*;`
//!
//! # Extractors
//!
//! - [`ReadExtract`] - Read from any `AsyncRead` source byte-by-byte
//! - [`LinesExtract`] - Decorator to convert byte streams to line streams
//! - [`StdinExtract`] - Read from stdin byte-by-byte
//! - [`FileExtract`] - Read files byte-by-byte as a stream of u8
//! - [`FileLineExtract`] - Read files line-by-line as a stream of String (efficient for text processing)
//! - [`StdinLineExtract`] - Read from stdin line-by-line
//!
//! # Loaders
//!
//! - [`StdoutLoad`] - Write items to stdout (works with any `Display` type)
//! - [`FileLoad`] - Write items to files with configurable write modes:
//!   - `FileLoad::create(path)` - Create new file or truncate existing
//!   - `FileLoad::append(path)` - Append to existing file or create new
//!
//! # Features
//!
//! - Async file I/O using `tokio::fs`
//! - Buffered reading/writing with `BufReader` and `BufWriter` for efficient I/O
//! - Lifecycle-managed file handles (opened in `initialize()`, flushed/closed in `finalize()`)
//! - Supports the `WithHeader` decorator for prepending headers to file output
//!
//! All extractors implement the `Extract` trait and support combinators like `skip()` and `take()`.
//! All loaders implement the `Load` trait with full lifecycle support.
//!
//! For complete usage examples, see the `streamwerk-debug` crate.

use anyhow::Result;
use streamwerk::Extract;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncRead, BufReader};
use tokio_stream::Stream;

// Re-export the base streamwerk crate so users only need to depend on streamwerk-fs
pub use streamwerk;

// Re-export tokio_stream so users don't need to add it as a dependency
pub use tokio_stream;

pub mod prelude;

// ============================================================================
// Extractors
// ============================================================================

/// Extractor that reads bytes from any `AsyncRead` source.
///
/// Streams data byte-by-byte from any type implementing `tokio::io::AsyncRead`.
/// This is a low-level extractor that can work with files, network streams,
/// in-memory buffers, or any other async reader.
///
/// # Example
///
/// ```rust
/// use streamwerk_fs::ReadExtract;
/// use streamwerk::Extract;
/// use anyhow::Result;
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// // Create a reader from a byte slice
/// let data: &[u8] = b"Hello, World!";
/// let reader = std::io::Cursor::new(data);
///
/// let extractor = ReadExtract;
/// let stream = extractor.extract(reader)?;
///
/// // Stream will yield each byte: b'H', b'e', b'l', b'l', b'o', ...
/// # Ok(())
/// # }
/// ```
pub struct ReadExtract;

impl<R> Extract<R, u8> for ReadExtract
where
    R: AsyncRead + Send + Unpin + 'static,
{
    type StreamType = impl Stream<Item = Result<u8>> + Send;

    fn extract(&self, reader: R) -> Result<Self::StreamType> {
        let stream = async_stream::stream! {
            use tokio::io::AsyncReadExt;
            let mut reader = reader;
            let mut buffer = [0u8; 1];

            loop {
                match reader.read(&mut buffer).await {
                    core::result::Result::Ok(0) => break, // EOF
                    core::result::Result::Ok(_) => yield core::result::Result::Ok(buffer[0]),
                    core::result::Result::Err(e) => {
                        yield core::result::Result::Err(anyhow::anyhow!("Read error: {}", e));
                        break;
                    }
                }
            }
        };

        Ok(stream)
    }
}

/// Decorator that converts a byte-stream extractor into a line-stream extractor.
///
/// Wraps any extractor that produces `u8` bytes and groups them into `String` lines
/// separated by newlines. Uses buffered reading for efficiency.
///
/// # Example
///
/// ```rust
/// use streamwerk_fs::{ReadExtract, LinesExtract};
/// use streamwerk::Extract;
/// use anyhow::Result;
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// // Create a byte extractor from a string
/// let data = b"line1\nline2\nline3\n";
/// let reader = std::io::Cursor::new(data);
/// let byte_extractor = ReadExtract;
///
/// // Wrap it with LinesExtract to get lines instead of bytes
/// let line_extractor = LinesExtract::new(byte_extractor);
/// let stream = line_extractor.extract(reader)?;
///
/// // Stream will yield: "line1", "line2", "line3"
/// # Ok(())
/// # }
/// ```
pub struct LinesExtract<E> {
    inner: E,
}

impl<E> LinesExtract<E> {
    /// Create a new LinesExtract that wraps a byte-stream extractor.
    ///
    /// # Arguments
    ///
    /// * `inner` - The byte-stream extractor to wrap
    pub fn new(inner: E) -> Self {
        Self { inner }
    }
}

impl<E, Input> Extract<Input, String> for LinesExtract<E>
where
    E: Extract<Input, u8>,
    Input: 'static,
{
    type StreamType = impl Stream<Item = Result<String>> + Send;

    fn extract(&self, input: Input) -> Result<Self::StreamType> {
        use tokio_stream::StreamExt;

        let byte_stream = self.inner.extract(input)?;

        let line_stream = async_stream::stream! {
            let mut buffer = Vec::new();
            let mut stream = std::pin::pin!(byte_stream);

            while let Some(result) = stream.next().await {
                match result {
                    core::result::Result::Ok(byte) => {
                        if byte == b'\n' {
                            // Found newline - emit the line
                            if let core::result::Result::Ok(line) = String::from_utf8(buffer.clone()) {
                                yield core::result::Result::Ok(line);
                            } else {
                                yield core::result::Result::Err(anyhow::anyhow!("Invalid UTF-8 in line"));
                            }
                            buffer.clear();
                        } else if byte != b'\r' {
                            // Skip \r characters, accumulate others
                            buffer.push(byte);
                        }
                    }
                    core::result::Result::Err(e) => {
                        yield core::result::Result::Err(e);
                        break;
                    }
                }
            }

            // Emit any remaining content as the last line (file without trailing newline)
            if !buffer.is_empty() {
                if let core::result::Result::Ok(line) = String::from_utf8(buffer) {
                    yield core::result::Result::Ok(line);
                } else {
                    yield core::result::Result::Err(anyhow::anyhow!("Invalid UTF-8 in final line"));
                }
            }
        };

        Ok(line_stream)
    }
}

/// Extractor that reads from stdin byte-by-byte.
///
/// Wraps `ReadExtract` with `tokio::io::stdin()` as the input.
/// Takes `()` as input since stdin is always available.
///
/// # Example
///
/// ```rust,no_run
/// use streamwerk_fs::StdinExtract;
/// use streamwerk::Extract;
/// use anyhow::Result;
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let extractor = StdinExtract;
/// let stream = extractor.extract(())?;
///
/// // Stream will yield each byte from stdin
/// # Ok(())
/// # }
/// ```
pub struct StdinExtract;

impl Extract<(), u8> for StdinExtract {
    type StreamType = impl Stream<Item = Result<u8>> + Send;

    fn extract(&self, _input: ()) -> Result<Self::StreamType> {
        ReadExtract.extract(tokio::io::stdin())
    }
}

/// Extract step that opens a file and streams its content byte by byte.
///
/// This is implemented by opening the file and delegating to `ReadExtract`.
pub struct FileExtract;

impl<'a> Extract<&'a Path, u8> for FileExtract {
    type StreamType = impl Stream<Item = Result<u8>> + Send + 'a;

    fn extract(&self, path: &'a Path) -> Result<Self::StreamType> {
        let stream = async_stream::stream! {
            match File::open(path).await {
                Ok(file) => {
                    let read_stream = ReadExtract.extract(file)?;
                    let mut pinned = std::pin::pin!(read_stream);

                    use tokio_stream::StreamExt;
                    while let Some(result) = pinned.next().await {
                        yield result;
                    }
                }
                Err(e) => {
                    yield Err(anyhow::anyhow!("Failed to open file: {}", e));
                }
            }
        };

        Ok(stream)
    }
}

/// Extract step that opens a file and streams its content line by line.
///
/// Uses tokio's BufReader::lines() for efficient line-based reading.
pub struct FileLineExtract;

impl<'a> Extract<&'a Path, String> for FileLineExtract {
    type StreamType = impl Stream<Item = Result<String>> + Send + 'a;

    fn extract(&self, path: &'a Path) -> Result<Self::StreamType> {
        use tokio::io::AsyncBufReadExt;

        let stream = async_stream::stream! {
            let file = File::open(path).await?;
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
/// This is implemented by wrapping `StdinExtract` with `LinesExtract`.
///
/// # Example
///
/// ```rust,no_run
/// use streamwerk_fs::StdinLineExtract;
/// use streamwerk::Extract;
///
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// let extractor = StdinLineExtract::new();
/// let stream = extractor.extract(())?;
/// // Stream will yield lines from stdin
/// # Ok(())
/// # }
/// ```
pub struct StdinLineExtract;

impl StdinLineExtract {
    /// Create a new StdinLineExtract.
    pub fn new() -> LinesExtract<StdinExtract> {
        LinesExtract::new(StdinExtract)
    }
}

impl Default for StdinLineExtract {
    fn default() -> Self {
        Self
    }
}

impl Extract<(), String> for StdinLineExtract {
    type StreamType = <LinesExtract<StdinExtract> as Extract<(), String>>::StreamType;

    fn extract(&self, input: ()) -> Result<Self::StreamType> {
        Self::new().extract(input)
    }
}

// ============================================================================
// Loaders
// ============================================================================

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
impl<T: std::fmt::Display> streamwerk::Load<T> for FileLoad<T> {
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

impl<T: std::fmt::Display> streamwerk::Load<T> for StdoutLoad {
    fn load(&self, item: T) -> Result<()> {
        println!("{}", item);
        Ok(())
    }
}
