//! Load phase of ETL - consumes items and performs side effects.

use anyhow::*;

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
