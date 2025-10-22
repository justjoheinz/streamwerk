//! # ETL Framework
//!
//! A stream-based ETL (Extract, Transform, Load) framework built on async streams.
//!
//! ## Core Concepts
//!
//! - **Extract**: Data sources that produce streams of items
//! - **Transform**: Stream transformations that map inputs to outputs
//! - **Load**: Data sinks that consume items and perform side effects
//!
//! ## Example
//!
//! ```rust
//! use etl::{FnExtract, FnTransform, FnLoad, EtlPipeline};
//! use tokio_stream::iter;
//! use anyhow::Result;
//!
//! fn extract_data(_: ()) -> Result<impl tokio_stream::Stream<Item = Result<i32>> + Send> {
//!     Ok(iter(vec![Ok(1), Ok(2), Ok(3)]))
//! }
//!
//! fn transform_data(n: i32) -> Result<impl tokio_stream::Stream<Item = Result<i32>> + Send> {
//!     Ok(iter(vec![Ok(n * 2)]))
//! }
//!
//! fn load_data(n: i32) -> Result<()> {
//!     println!("Loaded: {}", n);
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let pipeline = EtlPipeline::new(
//!         FnExtract(extract_data),
//!         FnTransform(transform_data),
//!         FnLoad(load_data)
//!     );
//!     pipeline.run(()).await.unwrap();
//! }
//! ```

#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]

use anyhow::*;
use futures::stream::TryStreamExt;
use std::{marker::PhantomData, pin::pin};
use tokio_stream::{Stream, StreamExt, adapters::Skip};

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
/// use etl::{Extract, FnExtract};
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

pub trait ExtractExt<Input, Output>: Extract<Input, Output> + Sized {
    fn skip(self, n: usize) -> SkipExtractor<Self> {
        SkipExtractor::new(self, n)
    }
}

impl<T, Input, Output> ExtractExt<Input, Output> for T where T: Extract<Input, Output> {}


/// The Load phase of ETL - consumes items and performs side effects.
///
/// This trait represents data sinks that write or store individual items.
/// Each load operation is async and returns a `Result` to handle write errors.
///
/// # Type Parameters
///
/// * `Input` - The type of items to be loaded
///
/// # Example
///
/// ```rust
/// use etl::{Load, FnLoad};
/// use anyhow::Result;
///
/// fn write_to_db(item: i32) -> Result<()> {
///     println!("Writing {} to database", item);
///     Ok(())
/// }
///
/// let loader = FnLoad(write_to_db);
/// ```
pub trait Load<Input> {
    /// Load a single item, performing side effects like writing to storage
    fn load(&self, item: Input) -> Result<()>;
}

/// Stateful transformation phase - maps inputs to outputs while maintaining state.
///
/// This trait extends the ETL framework to support transformations that need to
/// accumulate information across multiple inputs. Unlike `Transform`, which is
/// stateless, `StatefulTransform` maintains mutable state between calls.
///
/// # Type Parameters
///
/// * `Input` - The input type consumed by the transformation
/// * `Output` - The type of items produced in the output stream
///
/// # Lifecycle
///
/// 1. `init_state()` - Called once to create initial state
/// 2. `transform()` - Called for each input, can mutate state and emit outputs
/// 3. `finalize()` - Called after all inputs processed, consumes state to emit final outputs
///
/// # Example
///
/// ```rust
/// #![feature(impl_trait_in_assoc_type)]
/// use etl::StatefulTransform;
/// use tokio_stream::{iter, Stream};
/// use anyhow::Result;
///
/// // Accumulate bytes into complete lines
/// struct BytesToLines;
///
/// impl StatefulTransform<u8, String> for BytesToLines {
///     type State = Vec<u8>;
///
///     type Stream<'a> = impl Stream<Item = Result<String>> + Send + 'a
///     where
///         Self: 'a,
///         u8: 'a,
///         String: 'a,
///         Self::State: 'a;
///
///     fn init_state(&self) -> Self::State {
///         Vec::new()
///     }
///
///     fn transform<'a>(&'a self, state: &'a mut Self::State, input: u8)
///         -> Result<Self::Stream<'a>>
///     {
///         state.push(input);
///         if input == b'\n' {
///             let line = String::from_utf8(state.clone())?;
///             state.clear();
///             Ok(iter(vec![Ok(line)]))
///         } else {
///             Ok(iter(vec![]))
///         }
///     }
///
///     fn finalize<'a>(&'a self, state: Self::State)
///         -> Result<Self::Stream<'a>>
///     {
///         if state.is_empty() {
///             Ok(iter(vec![]))
///         } else {
///             let line = String::from_utf8(state)?;
///             Ok(iter(vec![Ok(line)]))
///         }
///     }
/// }
/// ```
pub trait StatefulTransform<Input, Output> {
    /// The type of state maintained across transformations
    type State;

    /// The stream type produced by transform operations
    type Stream<'a>: Stream<Item = Result<Output>> + Send + 'a
    where
        Self: 'a,
        Input: 'a,
        Output: 'a,
        Self::State: 'a;

    /// Initialize the state for a new transformation session
    fn init_state(&self) -> Self::State;

    /// Transform an input while maintaining state, potentially emitting outputs
    fn transform<'a>(&'a self, state: &'a mut Self::State, input: Input) -> Result<Self::Stream<'a>>;

    /// Finalize the transformation, consuming state and emitting any remaining outputs
    fn finalize<'a>(&'a self, state: Self::State) -> Result<Self::Stream<'a>>;
}

/// The Transform phase of ETL - maps input items to output streams.
///
/// This trait represents transformations that take a single input and produce
/// a stream of outputs. Transformers can be composed using `and_then()`.
///
/// The trait uses Generic Associated Types (GATs) to allow transformers to
/// borrow from `self` for the lifetime `'a` while producing streams.
///
/// # Type Parameters
///
/// * `Input` - The input type consumed by the transformation
/// * `Output` - The type of items produced in the output stream
///
/// # Example
///
/// ```rust
/// use etl::{Transform, FnTransform};
/// use tokio_stream::iter;
/// use anyhow::Result;
///
/// fn split_words(line: String) -> Result<impl tokio_stream::Stream<Item = Result<String>> + Send> {
///     let words: Vec<_> = line.split_whitespace().map(|s| Ok(s.to_string())).collect();
///     Ok(iter(words))
/// }
///
/// let transformer = FnTransform(split_words);
/// ```
pub trait Transform<Input, Output> {
    /// The stream type produced by this transformer
    type Stream<'a>: Stream<Item = Result<Output>> + Send + 'a
    where
        Self: 'a,
        Input: 'a,
        Output: 'a;

    /// Transform an input into a stream of outputs
    fn transform<'a>(&'a self, input: Input) -> Result<Self::Stream<'a>>;

    /// Compose two transformers sequentially.
    ///
    /// The output of `self` becomes the input to `next`, creating a pipeline
    /// where each output from the first transformer is fed into the second.
    ///
    /// # Example
    ///
    /// ```rust
    /// use etl::{Transform, FnTransform};
    /// use tokio_stream::iter;
    /// use anyhow::Result;
    ///
    /// fn double(n: i32) -> Result<impl tokio_stream::Stream<Item = Result<i32>> + Send> {
    ///     Ok(iter(vec![Ok(n * 2)]))
    /// }
    ///
    /// fn add_ten(n: i32) -> Result<impl tokio_stream::Stream<Item = Result<i32>> + Send> {
    ///     Ok(iter(vec![Ok(n + 10)]))
    /// }
    ///
    /// let composed = FnTransform(double).and_then(FnTransform(add_ten));
    /// // Input: 5 -> double -> 10 -> add_ten -> 20
    /// ```
    fn and_then<T2, Output2>(self, next: T2) -> Compose<Self, T2, Output>
    where
        Self: Sized,
        T2: Transform<Output, Output2>,
    {
        Compose {
            first: self,
            second: next,
            _phantom: PhantomData,
        }
    }

    /// Filter items in the stream based on a predicate.
    ///
    /// Only items that satisfy the predicate will pass through.
    /// Items that fail the predicate are silently dropped.
    ///
    /// # Example
    ///
    /// ```rust
    /// use etl::{Transform, FnTransform};
    /// use tokio_stream::iter;
    /// use anyhow::Result;
    ///
    /// fn emit(n: i32) -> Result<impl tokio_stream::Stream<Item = Result<i32>> + Send> {
    ///     Ok(iter(vec![Ok(n)]))
    /// }
    ///
    /// let filtered = FnTransform(emit).filter(|n| *n > 5);
    /// // Input: 3 -> filtered out
    /// // Input: 10 -> passes through
    /// ```
    fn filter<P>(self, predicate: P) -> Filter<Self, P>
    where
        Self: Sized,
        P: Fn(&Output) -> bool,
    {
        Filter {
            transform: self,
            predicate,
        }
    }

    /// Map items in the stream using a function.
    ///
    /// Applies a transformation function to each successful item in the stream.
    /// This is lighter than composing full Transform implementations for simple 1:1 conversions.
    ///
    /// # Example
    ///
    /// ```rust
    /// use etl::{Transform, FnTransform};
    /// use tokio_stream::iter;
    /// use anyhow::Result;
    ///
    /// fn emit(n: i32) -> Result<impl tokio_stream::Stream<Item = Result<i32>> + Send> {
    ///     Ok(iter(vec![Ok(n)]))
    /// }
    ///
    /// let mapped = FnTransform(emit).map(|n| n * 2);
    /// // Input: 5 -> 10
    /// ```
    fn map<F, Output2>(self, f: F) -> Map<Self, F, Output>
    where
        Self: Sized,
        F: Fn(Output) -> Output2,
    {
        Map {
            transform: self,
            f,
            _phantom: PhantomData,
        }
    }

    /// Skip the first n outputs from the stream.
    ///
    /// This method skips the first `n` items produced by the transformer's output stream.
    ///
    /// # Example
    ///
    /// ```rust
    /// use etl::{Transform, FnTransform};
    /// use tokio_stream::iter;
    /// use anyhow::Result;
    ///
    /// fn emit_multiple(n: i32) -> Result<impl tokio_stream::Stream<Item = Result<i32>> + Send> {
    ///     Ok(iter(vec![Ok(n), Ok(n + 1), Ok(n + 2)]))
    /// }
    ///
    /// let skipped = FnTransform(emit_multiple).skip(1);
    /// // Input: 5 -> outputs: 6, 7 (first output 5 is skipped)
    /// ```
    fn skip(self, n: usize) -> SkipTransform<Self, Output>
    where
        Self: Sized,
    {
        SkipTransform {
            transform: self,
            n,
            _phantom: PhantomData,
        }
    }

    /// Scans the output stream using the given initial state and function.
    ///
    /// This method applies a stateful transformation to each output item.
    /// The function receives a mutable reference to the state and the current item,
    /// and returns an optional output. If `None` is returned, the stream ends.
    ///
    /// # Arguments
    ///
    /// * `init` - The initial state value
    /// * `f` - A function that takes a mutable state reference and an item,
    ///         returning `Some(output)` to continue or `None` to end the stream
    ///
    /// # Example
    ///
    /// ```
    /// # use etl::{Transform, FnTransform};
    /// # use tokio_stream::{Stream, iter};
    /// # use anyhow::Result;
    /// // Create a transformer that emits multiple values
    /// fn emit_multiple(x: i32) -> Result<impl Stream<Item = Result<i32>> + Send> {
    ///     Ok(iter(vec![Ok(x), Ok(x + 1), Ok(x + 2)]))
    /// }
    /// let running_sum = FnTransform(emit_multiple).scan(0, |acc, x| {
    ///     *acc += x;
    ///     Some(*acc)
    /// });
    /// // Input: 5 -> outputs: 5, 11, 18 (5, 5+6, 5+6+7)
    /// ```
    fn scan<St, F>(self, init: St, f: F) -> ScanTransform<Self, St, F, Output>
    where
        Self: Sized,
        St: Send,
        F: FnMut(&mut St, Output) -> Option<Output> + Send,
    {
        ScanTransform {
            transform: self,
            init,
            f,
            _phantom: PhantomData,
        }
    }
}

/// Skip combinator for transformers.
///
/// Created by calling `Transform::skip()`. This type wraps a transformer
/// and skips the first n items from its output stream.
///
/// # Type Parameters
///
/// * `T` - The underlying transformer
/// * `OriginalOutput` - The output type
pub struct SkipTransform<T, OriginalOutput> {
    pub transform: T,
    pub n: usize,
    _phantom: PhantomData<OriginalOutput>,
}

impl<T, Input, Output> Transform<Input, Output> for SkipTransform<T, Output>
where
    T: Transform<Input, Output>,
    Output: Send,
{
    type Stream<'a>
        = Skip<T::Stream<'a>>
    where
        Self: 'a,
        Input: 'a,
        Output: 'a;

    fn transform<'a>(&'a self, input: Input) -> Result<Self::Stream<'a>> {
        let stream = self.transform.transform(input)?;
        Ok(tokio_stream::StreamExt::skip(stream, self.n))
    }
}

/// Scan combinator for transformers.
///
/// Created by calling `Transform::scan()`. This type wraps a transformer
/// and applies a stateful transformation to each output item.
///
/// # Type Parameters
///
/// * `T` - The underlying transformer
/// * `St` - The state type
/// * `F` - The scanning function type
/// * `OriginalOutput` - The output type
pub struct ScanTransform<T, St, F, OriginalOutput> {
    pub transform: T,
    pub init: St,
    pub f: F,
    _phantom: PhantomData<OriginalOutput>,
}

impl<T, Input, Output, St, F> Transform<Input, Output> for ScanTransform<T, St, F, Output>
where
    T: Transform<Input, Output>,
    Output: Send,
    St: Clone + Send,
    F: Clone + FnMut(&mut St, Output) -> Option<Output> + Send,
{
    type Stream<'a>
        = impl Stream<Item = Result<Output>> + Send + 'a
    where
        Self: 'a,
        Input: 'a,
        Output: 'a,
        St: 'a,
        F: 'a;

    fn transform<'a>(&'a self, input: Input) -> Result<Self::Stream<'a>> {
        let stream = self.transform.transform(input)?;
        let mut state = self.init.clone();
        let mut f = self.f.clone();

        let scanned = async_stream::stream! {
            let mut pinned = pin!(stream);
            while let Some(result) = pinned.next().await {
                match result {
                    std::result::Result::Ok(value) => {
                        if let Some(output) = f(&mut state, value) {
                            yield std::result::Result::Ok(output);
                        } else {
                            break;
                        }
                    }
                    std::result::Result::Err(e) => {
                        yield std::result::Result::Err(e);
                        break;
                    }
                }
            }
        };

        Ok(scanned)
    }
}

/// Map combinator for transformers.
///
/// Created by calling `Transform::map()`. This type wraps a transformer
/// and applies a mapping function to each successful item in the output stream.
///
/// # Type Parameters
///
/// * `T` - The underlying transformer
/// * `F` - The mapping function type
/// * `OriginalOutput` - The output type before mapping
pub struct Map<T, F, OriginalOutput> {
    pub transform: T,
    pub f: F,
    _phantom: PhantomData<OriginalOutput>,
}

impl<T, F, Input, Output, Output2> Transform<Input, Output2> for Map<T, F, Output>
where
    T: Transform<Input, Output>,
    F: Fn(Output) -> Output2 + Sync,
    Output: Send,
    Output2: Send,
{
    type Stream<'a>
        = impl Stream<Item = Result<Output2>> + Send + 'a
    where
        Self: 'a,
        Input: 'a,
        Output2: 'a;

    fn transform<'a>(&'a self, input: Input) -> Result<Self::Stream<'a>> {
        let stream = self.transform.transform(input)?;
        let f = &self.f;

        Ok(stream.map(move |item| item.map(f)))
    }
}

/// Filter combinator for transformers.
///
/// Created by calling `Transform::filter()`. This type wraps a transformer
/// and filters its output stream based on a predicate function.
///
/// # Type Parameters
///
/// * `T` - The underlying transformer
/// * `P` - The predicate function type
pub struct Filter<T, P> {
    pub transform: T,
    pub predicate: P,
}

impl<T, P, Input, Output> Transform<Input, Output> for Filter<T, P>
where
    T: Transform<Input, Output>,
    P: Fn(&Output) -> bool + Sync,
    Output: Send,
{
    type Stream<'a>
        = impl Stream<Item = Result<Output>> + Send + 'a
    where
        Self: 'a,
        Input: 'a,
        Output: 'a;

    fn transform<'a>(&'a self, input: Input) -> Result<Self::Stream<'a>> {
        let stream = self.transform.transform(input)?;
        let predicate = &self.predicate;

        Ok(stream.filter_map(move |item| match item {
            core::result::Result::Ok(value) if predicate(&value) => Some(Ok(value)),
            core::result::Result::Ok(_) => None,
            core::result::Result::Err(e) => Some(Err(e)),
        }))
    }
}

/// Composition of two transformers.
///
/// Created by calling `Transform::and_then()`. This type chains two transformers
/// so that each output from the first becomes input to the second.
///
/// # Type Parameters
///
/// * `T1` - The first transformer
/// * `T2` - The second transformer
/// * `Mid` - The intermediate type between the two transformers
pub struct Compose<T1, T2, Mid> {
    pub first: T1,
    pub second: T2,
    _phantom: PhantomData<Mid>,
}

impl<T1, T2, Input, Mid, Output> Transform<Input, Output> for Compose<T1, T2, Mid>
where
    T1: Transform<Input, Mid>,
    T2: Transform<Mid, Output> + Sync,
    Mid: Send,
{
    type Stream<'a>
        = impl Stream<Item = Result<Output>> + Send + 'a
    where
        Self: 'a,
        Input: 'a,
        Output: 'a;

    fn transform<'a>(&'a self, input: Input) -> Result<Self::Stream<'a>> {
        let outer = self.first.transform(input)?;
        let second = &self.second;

        Ok(outer
            .then(move |item| {
                let second = second;
                async move {
                    let mid = item?;
                    second.transform(mid)
                }
            })
            .try_flatten())
    }
}


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
/// use etl::FnExtract;
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

/// Complete ETL pipeline combining Extract, Transform, and Load phases.
///
/// An `EtlPipeline` connects three components:
/// 1. **Extract** - Produces initial stream from input
/// 2. **Transform** - Processes each item from extraction, producing new streams
/// 3. **Load** - Consumes final outputs, performing side effects
///
/// # Type Parameters
///
/// * `E` - The extractor type implementing [`Extract`]
/// * `T` - The transformer type implementing [`Transform`]
/// * `L` - The loader type implementing [`Load`]
///
/// # Example
///
/// ```rust
/// use etl::{FnExtract, FnTransform, FnLoad, EtlPipeline};
/// use tokio_stream::iter;
/// use anyhow::Result;
///
/// fn extract(_: ()) -> Result<impl tokio_stream::Stream<Item = Result<i32>> + Send> {
///     Ok(iter(vec![Ok(1), Ok(2)]))
/// }
///
/// fn transform(n: i32) -> Result<impl tokio_stream::Stream<Item = Result<i32>> + Send> {
///     Ok(iter(vec![Ok(n * 2)]))
/// }
///
/// fn load(n: i32) -> Result<()> {
///     println!("{}", n);
///     Ok(())
/// }
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let pipeline = EtlPipeline::new(
///     FnExtract(extract),
///     FnTransform(transform),
///     FnLoad(load)
/// );
/// pipeline.run(()).await.unwrap();
/// # });
/// ```
pub struct EtlPipeline<E, T, L> {
    pub extract: E,
    pub transform: T,
    pub load: L,
}

impl<E, T, L> EtlPipeline<E, T, L> {
    /// Create a new ETL pipeline from extract, transform, and load components.
    pub fn new(extract: E, transform: T, load: L) -> Self {
        Self {
            extract,
            transform,
            load,
        }
    }

    /// Execute the ETL pipeline.
    ///
    /// This runs the complete pipeline:
    /// 1. Extracts a stream from the input
    /// 2. For each extracted item, applies transformation to produce output stream
    /// 3. For each output item, calls load to perform side effects
    ///
    /// Returns `Ok(())` if all phases complete successfully, or the first error encountered.
    ///
    /// # Errors
    ///
    /// Returns an error if any phase (extract, transform, or load) fails.
    pub async fn run<Input, Mid, Output>(&self, input: Input) -> Result<()>
    where
        E: Extract<Input, Mid>,
        T: Transform<Mid, Output> + Sync,
        L: Load<Output>,
        Mid: Send,
    {
        let stream = self.extract.extract(input)?;
        let mut stream = pin!(stream);

        while let Some(mid_result) = stream.next().await {
            let mid = mid_result?;
            let transformed_stream = self.transform.transform(mid)?;
            let mut transformed_stream = pin!(transformed_stream);

            while let Some(output_result) = transformed_stream.next().await {
                let output = output_result?;
                self.load.load(output)?;
            }
        }

        Ok(())
    }
}

/// Wrapper that implements [`Load`] for functions.
///
/// Allows ordinary functions with the signature `Fn(Input) -> Result<()>`
/// to be used as loaders in ETL pipelines.
///
/// # Example
///
/// ```rust
/// use etl::FnLoad;
/// use anyhow::Result;
///
/// fn save_to_db(item: i32) -> Result<()> {
///     println!("Saving {} to database", item);
///     Ok(())
/// }
///
/// let loader = FnLoad(save_to_db);
/// ```
pub struct FnLoad<F>(pub F);

impl<F, Input> Load<Input> for FnLoad<F>
where
    F: Fn(Input) -> Result<()>,
{
    fn load(&self, item: Input) -> Result<()> {
        (self.0)(item)
    }
}

/// Wrapper that implements [`Transform`] for functions.
///
/// Allows ordinary functions with the signature `Fn(Input) -> Result<Stream>`
/// to be used as transformers in ETL pipelines.
///
/// # Type Requirements
///
/// - Function must return `Result<impl Stream<Item = Result<Output>> + Send>`
/// - All types must be `'static` (owned, not borrowed)
/// - Function must be `Sync` for use in composed transformers
///
/// # Example
///
/// ```rust
/// use etl::FnTransform;
/// use tokio_stream::iter;
/// use anyhow::Result;
///
/// fn split_into_digits(n: i32) -> Result<impl tokio_stream::Stream<Item = Result<i32>> + Send> {
///     let digits: Vec<_> = n.to_string()
///         .chars()
///         .filter_map(|c| c.to_digit(10).map(|d| Ok(d as i32)))
///         .collect();
///     Ok(iter(digits))
/// }
///
/// let transformer = FnTransform(split_into_digits);
/// ```
pub struct FnTransform<F>(pub F);

impl<F, Input, Output, S> Transform<Input, Output> for FnTransform<F>
where
    F: Fn(Input) -> Result<S> + Sync,
    S: Stream<Item = Result<Output>> + Send + 'static,
    Input: 'static,
    Output: 'static,
{
    type Stream<'a>
        = S
    where
        Self: 'a,
        Input: 'a,
        Output: 'a;

    fn transform<'a>(&'a self, input: Input) -> Result<Self::Stream<'a>> {
        (self.0)(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::iter;

    fn extract_numbers(_input: ()) -> Result<impl Stream<Item = Result<i32>> + Send> {
        Ok(iter(vec![Ok(1), Ok(2), Ok(3)]))
    }

    fn double(n: i32) -> Result<impl Stream<Item = Result<i32>> + Send> {
        Ok(iter(vec![Ok(n * 2)]))
    }

    fn add_ten(n: i32) -> Result<impl Stream<Item = Result<i32>> + Send> {
        Ok(iter(vec![Ok(n + 10)]))
    }

    #[tokio::test]
    async fn test_fn_extract() {
        let extractor = FnExtract(extract_numbers);
        let result_stream = extractor.extract(()).unwrap();
        let results: Vec<_> = result_stream.collect().await;

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap(), &1);
        assert_eq!(results[1].as_ref().unwrap(), &2);
        assert_eq!(results[2].as_ref().unwrap(), &3);
    }

    #[tokio::test]
    async fn test_fn_transform_composition() {
        let t1 = FnTransform(double);
        let t2 = FnTransform(add_ten);

        let composed = t1.and_then(t2);

        let result_stream = composed.transform(5).unwrap();
        let results: Vec<_> = result_stream.collect().await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_ref().unwrap(), &20); // 5 * 2 = 10, 10 + 10 = 20
    }

    #[tokio::test]
    async fn test_etl_pipeline() {
        use std::sync::{Arc, Mutex};

        let results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = Arc::clone(&results);

        let load_fn = move |n: i32| -> Result<()> {
            results_clone.lock().unwrap().push(n);
            Ok(())
        };

        let extractor = FnExtract(extract_numbers);
        let transformer = FnTransform(double);
        let loader = FnLoad(load_fn);

        let pipeline = EtlPipeline::new(extractor, transformer, loader);
        pipeline.run(()).await.unwrap();

        let final_results = results.lock().unwrap();
        assert_eq!(final_results.len(), 3);
        assert_eq!(final_results[0], 2);
        assert_eq!(final_results[1], 4);
        assert_eq!(final_results[2], 6);
    }

    #[tokio::test]
    async fn test_etl_pipeline_with_composition() {
        use std::sync::{Arc, Mutex};

        let results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = Arc::clone(&results);

        let load_fn = move |n: i32| -> Result<()> {
            results_clone.lock().unwrap().push(n);
            Ok(())
        };

        let extractor = FnExtract(extract_numbers);
        let t1 = FnTransform(double);
        let t2 = FnTransform(add_ten);
        let transformer = t1.and_then(t2);
        let loader = FnLoad(load_fn);

        let pipeline = EtlPipeline::new(extractor, transformer, loader);
        pipeline.run(()).await.unwrap();

        let final_results = results.lock().unwrap();
        assert_eq!(final_results.len(), 3);
        assert_eq!(final_results[0], 12); // 1 * 2 = 2, 2 + 10 = 12
        assert_eq!(final_results[1], 14); // 2 * 2 = 4, 4 + 10 = 14
        assert_eq!(final_results[2], 16); // 3 * 2 = 6, 6 + 10 = 16
    }

    #[tokio::test]
    async fn test_filter() {
        fn identity(n: i32) -> Result<impl Stream<Item = Result<i32>> + Send> {
            Ok(iter(vec![Ok(n)]))
        }

        let transformer = FnTransform(identity).filter(|n| *n > 2);

        // Test item that passes filter
        let result_stream = transformer.transform(5).unwrap();
        let results: Vec<_> = result_stream.collect().await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_ref().unwrap(), &5);

        // Test item that fails filter
        let result_stream = transformer.transform(1).unwrap();
        let results: Vec<_> = result_stream.collect().await;
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_filter_in_pipeline() {
        use std::sync::{Arc, Mutex};

        let results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = Arc::clone(&results);

        let load_fn = move |n: i32| -> Result<()> {
            results_clone.lock().unwrap().push(n);
            Ok(())
        };

        let extractor = FnExtract(extract_numbers); // Produces 1, 2, 3
        let transformer = FnTransform(double).filter(|n| *n >= 4); // Only >= 4
        let loader = FnLoad(load_fn);

        let pipeline = EtlPipeline::new(extractor, transformer, loader);
        pipeline.run(()).await.unwrap();

        let final_results = results.lock().unwrap();
        assert_eq!(final_results.len(), 2);
        assert_eq!(final_results[0], 4); // 2 * 2 = 4
        assert_eq!(final_results[1], 6); // 3 * 2 = 6
    }

    #[tokio::test]
    async fn test_filter_composition() {
        use std::sync::{Arc, Mutex};

        let results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = Arc::clone(&results);

        let load_fn = move |n: i32| -> Result<()> {
            results_clone.lock().unwrap().push(n);
            Ok(())
        };

        let extractor = FnExtract(extract_numbers); // Produces 1, 2, 3
        let transformer = FnTransform(double)
            .filter(|n| *n >= 4) // Only >= 4 (keeps 4, 6)
            .and_then(FnTransform(add_ten)); // Add 10
        let loader = FnLoad(load_fn);

        let pipeline = EtlPipeline::new(extractor, transformer, loader);
        pipeline.run(()).await.unwrap();

        let final_results = results.lock().unwrap();
        assert_eq!(final_results.len(), 2);
        assert_eq!(final_results[0], 14); // 2 * 2 = 4, 4 + 10 = 14
        assert_eq!(final_results[1], 16); // 3 * 2 = 6, 6 + 10 = 16
    }

    #[tokio::test]
    async fn test_map() {
        fn identity(n: i32) -> Result<impl Stream<Item = Result<i32>> + Send> {
            Ok(iter(vec![Ok(n)]))
        }

        let transformer = FnTransform(identity).map(|n| n * 3);

        let result_stream = transformer.transform(5).unwrap();
        let results: Vec<_> = result_stream.collect().await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_ref().unwrap(), &15);
    }

    #[tokio::test]
    async fn test_map_in_pipeline() {
        use std::sync::{Arc, Mutex};

        let results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = Arc::clone(&results);

        let load_fn = move |n: i32| -> Result<()> {
            results_clone.lock().unwrap().push(n);
            Ok(())
        };

        let extractor = FnExtract(extract_numbers); // Produces 1, 2, 3
        let transformer = FnTransform(double).map(|n| n + 1); // Double then add 1
        let loader = FnLoad(load_fn);

        let pipeline = EtlPipeline::new(extractor, transformer, loader);
        pipeline.run(()).await.unwrap();

        let final_results = results.lock().unwrap();
        assert_eq!(final_results.len(), 3);
        assert_eq!(final_results[0], 3); // 1 * 2 + 1 = 3
        assert_eq!(final_results[1], 5); // 2 * 2 + 1 = 5
        assert_eq!(final_results[2], 7); // 3 * 2 + 1 = 7
    }

    #[tokio::test]
    async fn test_map_filter_composition() {
        use std::sync::{Arc, Mutex};

        let results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = Arc::clone(&results);

        let load_fn = move |n: i32| -> Result<()> {
            results_clone.lock().unwrap().push(n);
            Ok(())
        };

        let extractor = FnExtract(extract_numbers); // Produces 1, 2, 3
        let transformer = FnTransform(double)
            .map(|n| n + 1) // Double then add 1: 3, 5, 7
            .filter(|n| *n > 4) // Only > 4: keeps 5, 7
            .and_then(FnTransform(add_ten)); // Add 10
        let loader = FnLoad(load_fn);

        let pipeline = EtlPipeline::new(extractor, transformer, loader);
        pipeline.run(()).await.unwrap();

        let final_results = results.lock().unwrap();
        assert_eq!(final_results.len(), 2);
        assert_eq!(final_results[0], 15); // 2 * 2 + 1 = 5, 5 + 10 = 15
        assert_eq!(final_results[1], 17); // 3 * 2 + 1 = 7, 7 + 10 = 17
    }

    #[tokio::test]
    async fn test_skip() {
        fn emit_multiple(n: i32) -> Result<impl Stream<Item = Result<i32>> + Send> {
            Ok(iter(vec![Ok(n), Ok(n + 1), Ok(n + 2)]))
        }

        let transformer = FnTransform(emit_multiple).skip(1);

        // Input: 5 produces stream [5, 6, 7], skip 1 gives [6, 7]
        let result_stream = transformer.transform(5).unwrap();
        let results: Vec<_> = result_stream.collect().await;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_ref().unwrap(), &6);
        assert_eq!(results[1].as_ref().unwrap(), &7);
    }

    #[tokio::test]
    async fn test_skip_in_pipeline() {
        use std::sync::{Arc, Mutex};

        let results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = Arc::clone(&results);

        let load_fn = move |n: i32| -> Result<()> {
            results_clone.lock().unwrap().push(n);
            Ok(())
        };

        fn emit_multiple(n: i32) -> Result<impl Stream<Item = Result<i32>> + Send> {
            Ok(iter(vec![Ok(n), Ok(n + 1), Ok(n + 2)]))
        }

        let extractor = FnExtract(extract_numbers); // Produces 1, 2, 3
        let transformer = FnTransform(emit_multiple).skip(1); // Each input produces 3 outputs, skip first
        let loader = FnLoad(load_fn);

        let pipeline = EtlPipeline::new(extractor, transformer, loader);
        pipeline.run(()).await.unwrap();

        let final_results = results.lock().unwrap();
        // Input 1: [1, 2, 3] -> skip 1 -> [2, 3]
        // Input 2: [2, 3, 4] -> skip 1 -> [3, 4]
        // Input 3: [3, 4, 5] -> skip 1 -> [4, 5]
        assert_eq!(final_results.len(), 6);
        assert_eq!(final_results[0], 2);
        assert_eq!(final_results[1], 3);
        assert_eq!(final_results[2], 3);
        assert_eq!(final_results[3], 4);
        assert_eq!(final_results[4], 4);
        assert_eq!(final_results[5], 5);
    }

    #[tokio::test]
    async fn test_scan() {
        fn emit_multiple(n: i32) -> Result<impl Stream<Item = Result<i32>> + Send> {
            Ok(iter(vec![Ok(n), Ok(n + 1), Ok(n + 2)]))
        }

        let transformer = FnTransform(emit_multiple).scan(0, |acc, x| {
            *acc += x;
            Some(*acc)
        });

        // Input: 5 produces stream [5, 6, 7]
        // Scan produces running sum: [5, 11, 18]
        let result_stream = transformer.transform(5).unwrap();
        let results: Vec<_> = result_stream.collect().await;
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap(), &5);  // 0 + 5
        assert_eq!(results[1].as_ref().unwrap(), &11); // 5 + 6
        assert_eq!(results[2].as_ref().unwrap(), &18); // 11 + 7
    }

    #[tokio::test]
    async fn test_scan_in_pipeline() {
        use std::sync::{Arc, Mutex};

        let results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = Arc::clone(&results);

        let load_fn = move |n: i32| -> Result<()> {
            results_clone.lock().unwrap().push(n);
            Ok(())
        };

        fn emit_multiple(n: i32) -> Result<impl Stream<Item = Result<i32>> + Send> {
            Ok(iter(vec![Ok(n), Ok(n + 1), Ok(n + 2)]))
        }

        let extractor = FnExtract(extract_numbers); // Produces 1, 2, 3
        let transformer = FnTransform(emit_multiple).scan(0, |acc, x| {
            *acc += x;
            Some(*acc)
        });
        let loader = FnLoad(load_fn);

        let pipeline = EtlPipeline::new(extractor, transformer, loader);
        pipeline.run(()).await.unwrap();

        let final_results = results.lock().unwrap();
        // Input 1: [1, 2, 3] -> running sum (state starts at 0): [1, 3, 6]
        // Input 2: [2, 3, 4] -> running sum (state resets to 0): [2, 5, 9]
        // Input 3: [3, 4, 5] -> running sum (state resets to 0): [3, 7, 12]
        assert_eq!(final_results.len(), 9);
        assert_eq!(final_results[0], 1);
        assert_eq!(final_results[1], 3);
        assert_eq!(final_results[2], 6);
        assert_eq!(final_results[3], 2);
        assert_eq!(final_results[4], 5);
        assert_eq!(final_results[5], 9);
        assert_eq!(final_results[6], 3);
        assert_eq!(final_results[7], 7);
        assert_eq!(final_results[8], 12);
    }
}
