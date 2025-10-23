//! Extract phase of ETL - produces streams of items from input sources.

use anyhow::*;
use tokio_stream::{Stream, adapters::{Skip, Take}};

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
