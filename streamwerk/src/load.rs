//! Load phase of ETL - consumes items and performs side effects.

use anyhow::*;

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
/// fn write_to_db(item: i32) -> Result<()> {
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
    fn load(&self, item: Input) -> Result<()>;

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
/// fn save_to_db(item: i32) -> Result<()> {
///     println!("Saving {} to database", item);
///     Ok(())
/// }
///
/// let loader = FnLoad(save_to_db);
/// ```
pub struct FnLoad<F>(pub F);


/// Decorator that adds a header to any `Load<String>` implementation.
///
/// Writes the header string during initialization, then delegates all
/// subsequent `load()` calls to the inner loader. This allows any string-based
/// loader to support headers without duplication.
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
///     fn load(&self, item: String) -> Result<()> {
///         println!("{}", item);
///         Ok(())
///     }
/// }
///
/// // Stdout with header
/// let loader = WithHeader::new(StdoutLoad, "Name,Age");
/// ```
pub struct WithHeader<L> {
    inner: L,
    header: String,
}

impl<L> WithHeader<L> {
    /// Create a new header decorator around the given loader.
    ///
    /// The header will be written during `initialize()` before any data items.
    pub fn new(inner: L, header: impl Into<String>) -> Self {
        Self {
            inner,
            header: header.into(),
        }
    }
}

impl<L> Load<String> for WithHeader<L>
where
    L: Load<String> + Sync,
{
    fn initialize(&self) -> impl std::future::Future<Output = Result<()>> + Send {
        let header = self.header.clone();
        let inner = &self.inner;

        async move {
            inner.initialize().await?;
            inner.load(header)?;
            Ok(())
        }
    }

    fn load(&self, item: String) -> Result<()> {
        self.inner.load(item)
    }

    fn finalize(&self, result: &Result<()>) -> impl std::future::Future<Output = Result<()>> + Send {
        self.inner.finalize(result)
    }
}

impl<F, Input> Load<Input> for FnLoad<F>
where
    F: Fn(Input) -> Result<()>,
{
    fn load(&self, item: Input) -> Result<()> {
        (self.0)(item)
    }
}
