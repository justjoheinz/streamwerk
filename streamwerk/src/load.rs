//! Load phase of ETL - consumes items and performs side effects.
//!
//! # Core Trait
//!
//! - [`Load`] - Main trait for loading data with lifecycle hooks
//!
//! # Base Loaders
//!
//! - [`FnLoad`] - Wrap functions as loaders
//!
//! # Load Decorators
//!
//! Decorators that enhance loaders with additional functionality:
//!
//! - [`MapLoad`] - Transform items before loading
//! - [`FilterLoad`] - Filter items before loading
//! - [`PrefixLoad`] - Add items before main data
//! - [`SuffixLoad`] - Add items after main data
//! - [`BatchLoad`] - Buffer and batch items before loading
//! - [`WithHeader`] - Add header to string-based loaders

use anyhow::*;

// ============================================================================
// Core Trait
// ============================================================================

/// The Load phase of ETL - consumes items and performs side effects.
///
/// This trait represents data sinks that write or store individual items.
/// Each load operation is async and returns a `Result` to handle write errors.
///
/// The trait provides lifecycle hooks for initialization and finalization:
/// - `initialize()` is called once before processing any items
/// - `load()` is called for each item in the stream
/// - `finalize()` is called once after all items are processed
///
/// # Type Parameters
///
/// * `Input` - The type of items to be loaded
///
/// # Example
///
/// ```rust
/// use streamwerk::{Load, FnLoad};
/// use anyhow::Result;
///
/// async fn write_to_db(item: i32) -> Result<()> {
///     println!("Writing {} to database", item);
///     Ok(())
/// }
///
/// let loader = FnLoad(write_to_db);
/// ```
pub trait Load<Input> {
    /// Initialize the loader before processing any items.
    ///
    /// This is called once by the pipeline before any `load()` calls.
    /// Use this to set up resources like file handles, database connections,
    /// or print headers.
    ///
    /// The default implementation does nothing.
    fn initialize(&self) -> impl std::future::Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Load a single item, performing side effects like writing to storage
    fn load(&self, item: Input) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Finalize the loader after all items have been processed.
    ///
    /// This is called once by the pipeline after all `load()` calls complete.
    /// The `result` parameter indicates whether the pipeline succeeded or failed.
    /// Use this to close resources, flush buffers, or perform cleanup.
    ///
    /// The default implementation does nothing and returns `Ok(())`, allowing
    /// the pipeline to proceed with its original result.
    fn finalize(&self, _result: &Result<()>) -> impl std::future::Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
}

// ============================================================================
// Base Loaders
// ============================================================================

/// Wrapper that implements [`Load`] for functions.
///
/// Allows ordinary functions with the signature `Fn(Input) -> Result<()>`
/// to be used as loaders in ETL pipelines.
///
/// # Example
///
/// ```rust
/// use streamwerk::FnLoad;
/// use anyhow::Result;
///
/// async fn save_to_db(item: i32) -> Result<()> {
///     println!("Saving {} to database", item);
///     Ok(())
/// }
///
/// let loader = FnLoad(save_to_db);
/// ```
pub struct FnLoad<F>(pub F);

impl<F, Input, Fut> Load<Input> for FnLoad<F>
where
    F: Fn(Input) -> Fut,
    Fut: std::future::Future<Output = Result<()>> + Send,
{
    fn load(&self, item: Input) -> impl std::future::Future<Output = Result<()>> + Send {
        (self.0)(item)
    }
}

// ============================================================================
// Load Decorators
// ============================================================================

/// Decorator that adds a header to any `Load<String>` implementation.
///
/// Writes the header string during initialization, then delegates all
/// subsequent `load()` calls to the inner loader. This allows any string-based
/// loader to support headers without duplication.
///
/// This is implemented as a thin wrapper around `PrefixLoad` for backward compatibility.
///
/// # Example
///
/// ```rust
/// use streamwerk::prelude::*;
/// use anyhow::Result;
///
/// // Example loader that prints to stdout
/// struct StdoutLoad;
/// impl Load<String> for StdoutLoad {
///     fn load(&self, item: String) -> impl std::future::Future<Output = Result<()>> + Send {
///         async move {
///             println!("{}", item);
///             Ok(())
///         }
///     }
/// }
///
/// // Stdout with header
/// let loader = WithHeader::new(StdoutLoad, "Name,Age");
/// ```
pub struct WithHeader<L> {
    inner: PrefixLoad<L, String>,
}

impl<L> WithHeader<L> {
    /// Create a new header decorator around the given loader.
    ///
    /// The header will be written during `initialize()` before any data items.
    pub fn new(inner: L, header: impl Into<String>) -> Self {
        Self {
            inner: PrefixLoad::new(inner, vec![header.into()]),
        }
    }
}

impl<L> Load<String> for WithHeader<L>
where
    L: Load<String> + Sync,
{
    fn initialize(&self) -> impl std::future::Future<Output = Result<()>> + Send {
        self.inner.initialize()
    }

    fn load(&self, item: String) -> impl std::future::Future<Output = Result<()>> + Send {
        self.inner.load(item)
    }

    fn finalize(&self, result: &Result<()>) -> impl std::future::Future<Output = Result<()>> + Send {
        self.inner.finalize(result)
    }
}

/// Decorator that transforms items before loading.
///
/// Applies a mapping function to each item before passing it to the inner loader.
/// This allows you to adapt any loader to work with a different input type.
///
/// # Example
///
/// ```rust
/// use streamwerk::{Load, FnLoad, MapLoad};
/// use anyhow::Result;
///
/// async fn save_string(s: String) -> Result<()> {
///     println!("Saving: {}", s);
///     Ok(())
/// }
///
/// let string_loader = FnLoad(save_string);
/// let int_loader: MapLoad<_, _, i32> = MapLoad::new(string_loader, |n: i32| n.to_string());
/// ```
pub struct MapLoad<L, F, Input> {
    inner: L,
    mapper: F,
    _phantom: std::marker::PhantomData<Input>,
}

impl<L, F, Input> MapLoad<L, F, Input> {
    /// Create a new MapLoad that transforms items before loading.
    ///
    /// # Arguments
    ///
    /// * `inner` - The loader to wrap
    /// * `mapper` - Function to transform items before loading
    pub fn new(inner: L, mapper: F) -> Self {
        Self {
            inner,
            mapper,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<L, F, Input, Output> Load<Input> for MapLoad<L, F, Input>
where
    L: Load<Output> + Sync,
    F: Fn(Input) -> Output + Sync,
{
    fn initialize(&self) -> impl std::future::Future<Output = Result<()>> + Send {
        self.inner.initialize()
    }

    fn load(&self, item: Input) -> impl std::future::Future<Output = Result<()>> + Send {
        let mapped = (self.mapper)(item);
        self.inner.load(mapped)
    }

    fn finalize(&self, result: &Result<()>) -> impl std::future::Future<Output = Result<()>> + Send {
        self.inner.finalize(result)
    }
}

/// Decorator that filters items before loading.
///
/// Only items that satisfy the predicate will be passed to the inner loader.
/// Items that fail the predicate are silently skipped.
///
/// # Example
///
/// ```rust
/// use streamwerk::{Load, FnLoad, FilterLoad};
/// use anyhow::Result;
///
/// async fn save_number(n: i32) -> Result<()> {
///     println!("Saving: {}", n);
///     Ok(())
/// }
///
/// let loader = FnLoad(save_number);
/// let even_loader = FilterLoad::new(loader, |n: &i32| n % 2 == 0);
/// ```
pub struct FilterLoad<L, P> {
    inner: L,
    predicate: P,
}

impl<L, P> FilterLoad<L, P> {
    /// Create a new FilterLoad that filters items before loading.
    ///
    /// # Arguments
    ///
    /// * `inner` - The loader to wrap
    /// * `predicate` - Function to determine which items to load
    pub fn new(inner: L, predicate: P) -> Self {
        Self { inner, predicate }
    }
}

impl<L, P, Input> Load<Input> for FilterLoad<L, P>
where
    L: Load<Input> + Sync,
    P: Fn(&Input) -> bool + Sync,
    Input: Send,
{
    fn initialize(&self) -> impl std::future::Future<Output = Result<()>> + Send {
        self.inner.initialize()
    }

    fn load(&self, item: Input) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            if (self.predicate)(&item) {
                self.inner.load(item).await
            } else {
                Ok(()) // Skip items that don't match predicate
            }
        }
    }

    fn finalize(&self, result: &Result<()>) -> impl std::future::Future<Output = Result<()>> + Send {
        self.inner.finalize(result)
    }
}

/// Decorator that adds prefix items before the main data.
///
/// Loads one or more prefix items during initialization, then delegates
/// all subsequent `load()` calls to the inner loader. This is a generalization
/// of `WithHeader` that works with any item type and supports multiple prefix items.
///
/// # Example
///
/// ```rust
/// use streamwerk::{Load, FnLoad, PrefixLoad};
/// use anyhow::Result;
///
/// async fn print_number(n: i32) -> Result<()> {
///     println!("{}", n);
///     Ok(())
/// }
///
/// let loader = FnLoad(print_number);
/// // Will print -1, -2 before any data items
/// let with_prefix = PrefixLoad::new(loader, vec![-1, -2]);
/// ```
pub struct PrefixLoad<L, T> {
    inner: L,
    prefix: Vec<T>,
}

impl<L, T> PrefixLoad<L, T> {
    /// Create a new PrefixLoad that adds items before the main data.
    ///
    /// # Arguments
    ///
    /// * `inner` - The loader to wrap
    /// * `prefix` - Items to load during initialization
    pub fn new(inner: L, prefix: Vec<T>) -> Self {
        Self { inner, prefix }
    }
}

impl<L, T> Load<T> for PrefixLoad<L, T>
where
    L: Load<T> + Sync,
    T: Clone + Send,
{
    fn initialize(&self) -> impl std::future::Future<Output = Result<()>> + Send {
        let prefix = self.prefix.clone();
        let inner = &self.inner;

        async move {
            inner.initialize().await?;
            for item in prefix {
                inner.load(item).await?;
            }
            Ok(())
        }
    }

    fn load(&self, item: T) -> impl std::future::Future<Output = Result<()>> + Send {
        self.inner.load(item)
    }

    fn finalize(&self, result: &Result<()>) -> impl std::future::Future<Output = Result<()>> + Send {
        self.inner.finalize(result)
    }
}

/// Decorator that adds suffix items after the main data.
///
/// Delegates all `load()` calls to the inner loader, then loads one or more
/// suffix items during finalization. This is useful for adding footers or
/// closing elements to the output.
///
/// # Example
///
/// ```rust
/// use streamwerk::{Load, FnLoad, SuffixLoad};
/// use anyhow::Result;
///
/// async fn print_string(s: String) -> Result<()> {
///     println!("{}", s);
///     Ok(())
/// }
///
/// let loader = FnLoad(print_string);
/// // Will print "---END---" after all data items
/// let with_suffix = SuffixLoad::new(loader, vec!["---END---".to_string()]);
/// ```
pub struct SuffixLoad<L, T> {
    inner: L,
    suffix: Vec<T>,
}

impl<L, T> SuffixLoad<L, T> {
    /// Create a new SuffixLoad that adds items after the main data.
    ///
    /// # Arguments
    ///
    /// * `inner` - The loader to wrap
    /// * `suffix` - Items to load during finalization
    pub fn new(inner: L, suffix: Vec<T>) -> Self {
        Self { inner, suffix }
    }
}

impl<L, T> Load<T> for SuffixLoad<L, T>
where
    L: Load<T> + Sync,
    T: Clone + Send,
{
    fn initialize(&self) -> impl std::future::Future<Output = Result<()>> + Send {
        self.inner.initialize()
    }

    fn load(&self, item: T) -> impl std::future::Future<Output = Result<()>> + Send {
        self.inner.load(item)
    }

    fn finalize(&self, result: &Result<()>) -> impl std::future::Future<Output = Result<()>> + Send {
        let suffix = self.suffix.clone();
        let inner = &self.inner;

        async move {
            // Only load suffix items if the pipeline succeeded
            if result.is_ok() {
                for item in suffix {
                    inner.load(item).await?;
                }
            }
            inner.finalize(result).await
        }
    }
}

/// Decorator that batches items before loading.
///
/// Collects items into a buffer and loads them as a `Vec<T>` when the buffer
/// reaches the specified batch size. On finalization, any remaining buffered
/// items are flushed even if the batch is incomplete.
///
/// # Example
///
/// ```rust
/// use streamwerk::{Load, FnLoad, BatchLoad};
/// use anyhow::Result;
///
/// async fn save_batch(batch: Vec<i32>) -> Result<()> {
///     println!("Saving batch of {} items", batch.len());
///     Ok(())
/// }
///
/// let batch_loader = FnLoad(save_batch);
/// let loader = BatchLoad::new(batch_loader, 100); // Batch size of 100
/// ```
pub struct BatchLoad<L, T> {
    inner: L,
    batch_size: usize,
    buffer: std::sync::Arc<std::sync::Mutex<Vec<T>>>,
}

impl<L, T> BatchLoad<L, T>
where
    L: Load<Vec<T>>,
{
    /// Create a new BatchLoad that buffers items before loading.
    ///
    /// # Arguments
    ///
    /// * `inner` - The loader to wrap (must accept `Vec<T>`)
    /// * `batch_size` - Number of items to collect before loading
    pub fn new(inner: L, batch_size: usize) -> Self {
        Self {
            inner,
            batch_size,
            buffer: std::sync::Arc::new(std::sync::Mutex::new(Vec::with_capacity(batch_size))),
        }
    }

    async fn flush_buffer(&self) -> Result<()> {
        let batch = {
            let mut buffer = self.buffer.lock().unwrap();
            if buffer.is_empty() {
                return Ok(());
            }
            std::mem::replace(&mut *buffer, Vec::with_capacity(self.batch_size))
        }; // Lock is dropped here

        self.inner.load(batch).await?;
        Ok(())
    }
}

impl<L, T> Load<T> for BatchLoad<L, T>
where
    L: Load<Vec<T>> + Sync,
    T: Send,
{
    fn initialize(&self) -> impl std::future::Future<Output = Result<()>> + Send {
        self.inner.initialize()
    }

    fn load(&self, item: T) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            let should_flush = {
                let mut buffer = self.buffer.lock().unwrap();
                buffer.push(item);
                buffer.len() >= self.batch_size
            };

            if should_flush {
                let batch = {
                    let mut buffer = self.buffer.lock().unwrap();
                    std::mem::replace(&mut *buffer, Vec::with_capacity(self.batch_size))
                };
                self.inner.load(batch).await?;
            }

            Ok(())
        }
    }

    fn finalize(&self, result: &Result<()>) -> impl std::future::Future<Output = Result<()>> + Send {
        let inner = &self.inner;

        async move {
            // Flush any remaining items in the buffer
            if result.is_ok() {
                self.flush_buffer().await?;
            }
            inner.finalize(result).await
        }
    }
}
