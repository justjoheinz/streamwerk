//! Transform phase of ETL - maps input items to output streams.

use anyhow::*;
use futures::stream::TryStreamExt;
use std::{marker::PhantomData, pin::pin};
use tokio_stream::{Stream, StreamExt};

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
/// use streamwerk::{Transform, FnTransform};
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
    /// use streamwerk::{iter_ok, Transform, FnTransform};
    /// use anyhow::Result;
    ///
    /// fn double(n: i32) -> Result<impl tokio_stream::Stream<Item = Result<i32>> + Send> {
    ///     Ok(iter_ok(vec![n * 2]))
    /// }
    ///
    /// fn add_ten(n: i32) -> Result<impl tokio_stream::Stream<Item = Result<i32>> + Send> {
    ///     Ok(iter_ok(vec![n + 10]))
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
    /// use streamwerk::{Transform, FnTransform};
    /// use streamwerk::once_ok;
    /// use anyhow::Result;
    ///
    /// fn emit(n: i32) -> Result<impl tokio_stream::Stream<Item = Result<i32>> + Send> {
    ///     Ok(once_ok(n))
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
    /// use streamwerk::{Transform, FnTransform};
    /// use streamwerk::once_ok;
    /// use anyhow::Result;
    ///
    /// fn emit(n: i32) -> Result<impl tokio_stream::Stream<Item = Result<i32>> + Send> {
    ///     Ok(once_ok(n))
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
    ///   returning `Some(output)` to continue or `None` to end the stream
    ///
    /// # Example
    ///
    /// ```
    /// # use streamwerk::{iter_ok, Transform, FnTransform};
    /// # use tokio_stream::Stream;
    /// # use anyhow::Result;
    /// // Create a transformer that emits multiple values
    /// fn emit_multiple(x: i32) -> Result<impl Stream<Item = Result<i32>> + Send> {
    ///     Ok(iter_ok(vec![x, x + 1, x + 2]))
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

/// Identity transformer that passes through its input as a single-item stream.
///
/// This transformer simply wraps the input value in a stream containing one item.
/// It's useful as a starting point for applying combinators like `filter()`, `map()`,
/// or `take()` without needing to write a custom transformation function.
///
/// # Example
///
/// ```rust
/// use streamwerk::{Identity, Transform};
///
/// // Use identity with filter to select items
/// let filter_evens = Identity.filter(|n: &i32| n % 2 == 0);
/// ```
pub struct Identity;

impl<T> Transform<T, T> for Identity
where
    T: Send + 'static,
{
    type Stream<'a>
        = impl Stream<Item = Result<T>> + Send + 'a
    where
        Self: 'a,
        T: 'a;

    fn transform<'a>(&'a self, input: T) -> Result<Self::Stream<'a>> {
        Ok(crate::once_ok(input))
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
/// use streamwerk::FnTransform;
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
