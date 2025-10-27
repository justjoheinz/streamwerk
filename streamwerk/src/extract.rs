//! Extract phase of ETL - produces streams of items from input sources.
//!
//! # Core Trait
//!
//! - [`Extract`] - Main trait for extracting data sources into streams
//!
//! # Base Extractors
//!
//! - [`ReadExtract`] - Extract bytes from any `AsyncRead` source
//! - [`StdinExtract`] - Extract bytes from stdin
//! - [`FnExtract`] - Wrap functions as extractors
//!
//! # Extract Decorators
//!
//! Decorators that transform or enhance extractors:
//!
//! - [`MapExtract`] - Transform output items
//! - [`FilterExtract`] - Filter output items
//! - [`FlatMapExtract`] - Map items to streams and flatten
//! - [`LinesExtract`] - Convert byte streams to line streams
//! - [`SkipExtractor`] - Skip first n items
//! - [`TakeExtractor`] - Take at most n items
//!
//! # Extension Trait
//!
//! - [`ExtractExt`] - Provides combinator methods like `skip()` and `take()`

use anyhow::*;
use tokio_stream::{Stream, adapters::{Skip, Take}};
use tokio::io::AsyncRead;

// ============================================================================
// Core Trait
// ============================================================================

/// The Extract phase of ETL - produces a stream of items from an input source.
///
/// This trait represents data sources that convert an input into a stream of outputs.
/// Each item in the stream is wrapped in a `Result` to handle extraction errors.
///
/// # Type Parameters
///
/// * `Input` - The input type used to initialize the extraction
/// * `Output` - The type of items produced by the stream
///
/// # Example
///
/// ```rust
/// use streamwerk::{Extract, FnExtract};
/// use tokio_stream::iter;
/// use anyhow::Result;
///
/// fn read_file(_path: String) -> Result<impl tokio_stream::Stream<Item = Result<String>> + Send> {
///     Ok(iter(vec![Ok("line1".to_string()), Ok("line2".to_string())]))
/// }
///
/// let extractor = FnExtract(read_file);
/// ```
pub trait Extract<Input, Output> {
    /// The stream type produced by this extractor
    type StreamType: Stream<Item = Result<Output>> + Send;

    /// Extract data from the input and produce a stream of results
    fn extract(&self, input: Input) -> Result<Self::StreamType>;
}

// ============================================================================
// Extract Decorators - Stream Combinators
// ============================================================================

/// Skip combinator for extractors.
///
/// Created by calling `ExtractExt::skip()`. This type wraps an extractor
/// and skips the first n items from its output stream.
pub struct SkipExtractor<E> {
    inner: E,
    n: usize,
}

impl<E> SkipExtractor<E> {
    pub fn new(inner: E, n: usize) -> Self {
        Self { inner, n }
    }
}

impl<Input, Output, E> Extract<Input, Output> for SkipExtractor<E>
where
    E: Extract<Input, Output>,
    E::StreamType: Stream<Item = Result<Output>> + Send,
{
    type StreamType = Skip<E::StreamType>;

    fn extract(&self, input: Input) -> Result<Self::StreamType> {
        let stream = self.inner.extract(input)?;
        Ok(tokio_stream::StreamExt::skip(stream, self.n))
    }
}

/// Take combinator for extractors.
///
/// Created by calling `ExtractExt::take()`. This type wraps an extractor
/// and limits its output stream to at most n items.
pub struct TakeExtractor<E> {
    inner: E,
    n: usize,
}

impl<E> TakeExtractor<E> {
    pub fn new(inner: E, n: usize) -> Self {
        Self { inner, n }
    }
}

impl<Input, Output, E> Extract<Input, Output> for TakeExtractor<E>
where
    E: Extract<Input, Output>,
    E::StreamType: Stream<Item = Result<Output>> + Send,
{
    type StreamType = Take<E::StreamType>;

    fn extract(&self, input: Input) -> Result<Self::StreamType> {
        let stream = self.inner.extract(input)?;
        Ok(tokio_stream::StreamExt::take(stream, self.n))
    }
}

/// Extension trait for Extract that provides additional combinators.
pub trait ExtractExt<Input, Output>: Extract<Input, Output> + Sized {
    /// Skip the first n items from the extracted stream.
    fn skip(self, n: usize) -> SkipExtractor<Self> {
        SkipExtractor::new(self, n)
    }

    /// Limit the extracted stream to at most n items.
    fn take(self, n: usize) -> TakeExtractor<Self> {
        TakeExtractor::new(self, n)
    }
}

impl<T, Input, Output> ExtractExt<Input, Output> for T where T: Extract<Input, Output> {}

// ============================================================================
// Extension Trait
// ============================================================================

// (ExtractExt trait is defined above with the combinators)

// ============================================================================
// Base Extractors
// ============================================================================

/// Wrapper that implements [`Extract`] for functions.
///
/// Allows ordinary functions with the signature `Fn(Input) -> Result<Stream>`
/// to be used as extractors in ETL pipelines.
///
/// # Type Requirements
///
/// - Function must return `Result<impl Stream<Item = Result<Output>> + Send>`
/// - All types must be `'static` (owned, not borrowed)
///
/// # Example
///
/// ```rust
/// use streamwerk::FnExtract;
/// use tokio_stream::iter;
/// use anyhow::Result;
///
/// fn read_numbers(_: ()) -> Result<impl tokio_stream::Stream<Item = Result<i32>> + Send> {
///     Ok(iter(vec![Ok(1), Ok(2), Ok(3)]))
/// }
///
/// let extractor = FnExtract(read_numbers);
/// ```
pub struct FnExtract<F>(pub F);

impl<F, Input, Output, S> Extract<Input, Output> for FnExtract<F>
where
    F: Fn(Input) -> Result<S>,
    S: Stream<Item = Result<Output>> + Send + 'static,
    Input: 'static,
    Output: 'static,
{
    type StreamType = S;

    fn extract(&self, input: Input) -> Result<Self::StreamType> {
        (self.0)(input)
    }
}

// ============================================================================
// Extract Decorators - Transformations
// ============================================================================

/// Decorator that transforms output items from an extractor.
///
/// Applies a mapping function to each item in the extracted stream.
/// This allows you to adapt any extractor to produce a different output type.
///
/// # Example
///
/// ```rust
/// use streamwerk::{Extract, FnExtract, MapExtract};
/// use tokio_stream::iter;
/// use anyhow::Result;
///
/// fn extract_numbers(_: ()) -> Result<impl tokio_stream::Stream<Item = Result<i32>> + Send> {
///     Ok(iter(vec![Ok(1), Ok(2), Ok(3)]))
/// }
///
/// let number_extractor = FnExtract(extract_numbers);
/// let string_extractor: MapExtract<_, _, i32> = MapExtract::new(number_extractor, |n: i32| n.to_string());
/// ```
pub struct MapExtract<E, F, OriginalOutput> {
    inner: E,
    mapper: F,
    _phantom: std::marker::PhantomData<OriginalOutput>,
}

impl<E, F, OriginalOutput> MapExtract<E, F, OriginalOutput> {
    /// Create a new MapExtract that transforms output items.
    ///
    /// # Arguments
    ///
    /// * `inner` - The extractor to wrap
    /// * `mapper` - Function to transform output items
    pub fn new(inner: E, mapper: F) -> Self {
        Self {
            inner,
            mapper,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<E, F, Input, OriginalOutput, NewOutput> Extract<Input, NewOutput> for MapExtract<E, F, OriginalOutput>
where
    E: Extract<Input, OriginalOutput>,
    F: Fn(OriginalOutput) -> NewOutput + Sync + Clone + Send,
    OriginalOutput: Send,
    NewOutput: Send,
{
    type StreamType = impl Stream<Item = Result<NewOutput>> + Send;

    fn extract(&self, input: Input) -> Result<Self::StreamType> {
        let stream = self.inner.extract(input)?;
        let mapper = self.mapper.clone();

        Ok(tokio_stream::StreamExt::map(stream, move |item| item.map(&mapper)))
    }
}

/// Decorator that filters output items from an extractor.
///
/// Only items that satisfy the predicate will be included in the output stream.
/// Items that fail the predicate are silently skipped.
///
/// # Example
///
/// ```rust
/// use streamwerk::{Extract, FnExtract, FilterExtract};
/// use tokio_stream::iter;
/// use anyhow::Result;
///
/// fn extract_numbers(_: ()) -> Result<impl tokio_stream::Stream<Item = Result<i32>> + Send> {
///     Ok(iter(vec![Ok(1), Ok(2), Ok(3), Ok(4), Ok(5)]))
/// }
///
/// let number_extractor = FnExtract(extract_numbers);
/// let even_extractor = FilterExtract::new(number_extractor, |n: &i32| n % 2 == 0);
/// ```
pub struct FilterExtract<E, P> {
    inner: E,
    predicate: P,
}

impl<E, P> FilterExtract<E, P> {
    /// Create a new FilterExtract that filters output items.
    ///
    /// # Arguments
    ///
    /// * `inner` - The extractor to wrap
    /// * `predicate` - Function to determine which items to include
    pub fn new(inner: E, predicate: P) -> Self {
        Self { inner, predicate }
    }
}

impl<E, P, Input, Output> Extract<Input, Output> for FilterExtract<E, P>
where
    E: Extract<Input, Output>,
    P: Fn(&Output) -> bool + Sync + Clone + Send,
    Output: Send,
{
    type StreamType = impl Stream<Item = Result<Output>> + Send;

    fn extract(&self, input: Input) -> Result<Self::StreamType> {
        let stream = self.inner.extract(input)?;
        let predicate = self.predicate.clone();

        Ok(tokio_stream::StreamExt::filter_map(stream, move |item| match item {
            core::result::Result::Ok(value) if predicate(&value) => Some(core::result::Result::Ok(value)),
            core::result::Result::Ok(_) => None,
            core::result::Result::Err(e) => Some(core::result::Result::Err(e)),
        }))
    }
}

/// Decorator that maps items to streams and flattens the result.
///
/// Applies a mapping function to each item that produces a stream, then
/// flattens all the resulting streams into a single output stream.
///
/// # Example
///
/// ```rust
/// use streamwerk::{Extract, FnExtract, FlatMapExtract};
/// use tokio_stream::{iter, Stream};
/// use anyhow::Result;
///
/// fn extract_numbers(_: ()) -> Result<impl Stream<Item = Result<i32>> + Send> {
///     Ok(iter(vec![Ok(1), Ok(2), Ok(3)]))
/// }
///
/// fn duplicate(n: i32) -> Result<impl Stream<Item = Result<i32>> + Send> {
///     Ok(iter(vec![Ok(n), Ok(n)]))
/// }
///
/// let number_extractor = FnExtract(extract_numbers);
/// let duplicate_extractor: FlatMapExtract<_, _, i32> = FlatMapExtract::new(number_extractor, duplicate);
/// // Will produce: 1, 1, 2, 2, 3, 3
/// ```
pub struct FlatMapExtract<E, F, OriginalOutput> {
    inner: E,
    mapper: F,
    _phantom: std::marker::PhantomData<OriginalOutput>,
}

impl<E, F, OriginalOutput> FlatMapExtract<E, F, OriginalOutput> {
    /// Create a new FlatMapExtract that maps items to streams and flattens.
    ///
    /// # Arguments
    ///
    /// * `inner` - The extractor to wrap
    /// * `mapper` - Function that maps each item to a stream
    pub fn new(inner: E, mapper: F) -> Self {
        Self {
            inner,
            mapper,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<E, F, Input, OriginalOutput, NewOutput, S> Extract<Input, NewOutput> for FlatMapExtract<E, F, OriginalOutput>
where
    E: Extract<Input, OriginalOutput>,
    F: Fn(OriginalOutput) -> Result<S> + Sync + Clone + Send,
    S: Stream<Item = Result<NewOutput>> + Send + 'static,
    OriginalOutput: Send,
    NewOutput: Send + 'static,
{
    type StreamType = impl Stream<Item = Result<NewOutput>> + Send;

    fn extract(&self, input: Input) -> Result<Self::StreamType> {
        let stream = self.inner.extract(input)?;
        let mapper = self.mapper.clone();

        let flat_stream = async_stream::stream! {
            use tokio_stream::StreamExt;
            let mut outer = std::pin::pin!(stream);

            while let Some(result) = outer.next().await {
                match result {
                    core::result::Result::Ok(item) => {
                        match mapper(item) {
                            core::result::Result::Ok(inner_stream) => {
                                let mut inner = std::pin::pin!(inner_stream);
                                while let Some(inner_result) = inner.next().await {
                                    yield inner_result;
                                }
                            }
                            core::result::Result::Err(e) => {
                                yield core::result::Result::Err(e);
                            }
                        }
                    }
                    core::result::Result::Err(e) => {
                        yield core::result::Result::Err(e);
                    }
                }
            }
        };

        Ok(flat_stream)
    }
}

/// Extractor that reads bytes from any `AsyncRead` source.
///
/// Streams data byte-by-byte from any type implementing `tokio::io::AsyncRead`.
/// This is a low-level extractor that can work with files, network streams,
/// in-memory buffers, or any other async reader.
///
/// # Example
///
/// ```rust
/// use streamwerk::{Extract, ReadExtract};
/// use tokio::io::AsyncRead;
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
/// use streamwerk::{Extract, ReadExtract, LinesExtract};
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
/// use streamwerk::{Extract, StdinExtract};
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
