//! ETL pipeline implementation.

use anyhow::*;
use std::pin::pin;
use tokio_stream::StreamExt;

use crate::{Extract, Load, Transform};

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
    /// 1. Initializes the loader
    /// 2. Extracts a stream from the input
    /// 3. For each extracted item, applies transformation to produce output stream
    /// 4. For each output item, calls load to perform side effects
    /// 5. Finalizes the loader with the pipeline result
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
        // Initialize the loader
        self.load.initialize().await?;

        // Execute the pipeline
        let result: Result<()> = async {
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
        .await;

        // Finalize the loader with the pipeline result
        self.load.finalize(&result).await?;
        result
    }
}
