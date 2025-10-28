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
//! use streamwerk::{iter_ok, FnExtract, FnTransform, FnLoad, EtlPipeline};
//! use anyhow::Result;
//!
//! fn extract_data(_: ()) -> Result<impl tokio_stream::Stream<Item = Result<i32>> + Send> {
//!     Ok(iter_ok(vec![1, 2, 3]))
//! }
//!
//! fn transform_data(n: i32) -> Result<impl tokio_stream::Stream<Item = Result<i32>> + Send> {
//!     Ok(iter_ok(vec![n * 2]))
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

pub mod extract;
pub mod load;
pub mod pipeline;
pub mod transform;

pub mod prelude;

// Re-export main types at the crate root for convenience
pub use extract::{
    Extract, ExtractExt, FilterExtract, FlatMapExtract, FnExtract, MapExtract,
    SkipExtractor, TakeExtractor,
};
pub use load::{BatchLoad, FilterLoad, FnLoad, Load, MapLoad, PrefixLoad, SuffixLoad};
pub use pipeline::EtlPipeline;
pub use transform::{
    Compose, Filter, FnTransform, Identity, Map, ScanTransform, Transform,
};

// Re-export tokio_stream so users don't need to add it as a dependency
pub use tokio_stream;

// Stream helpers
use anyhow::Result;
use tokio_stream::Stream;

/// Creates a stream containing a single `Ok(value)` item.
///
/// This is a convenience function for creating streams that return a single
/// successful result, commonly used in Transform implementations.
///
/// # Example
///
/// ```rust
/// use streamwerk::once_ok;
/// use tokio_stream::StreamExt;
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let stream = once_ok(42);
/// let results: Vec<_> = stream.collect().await;
/// assert_eq!(results.len(), 1);
/// assert_eq!(results[0].as_ref().unwrap(), &42);
/// # });
/// ```
pub fn once_ok<T>(value: T) -> impl Stream<Item = Result<T>> + Send
where
    T: Send,
{
    tokio_stream::once(Ok(value))
}

/// Creates a stream containing a single `Err` item.
///
/// This is a convenience function for creating streams that return a single
/// error result, commonly used in Transform implementations for error handling.
///
/// # Example
///
/// ```rust
/// use streamwerk::once_err;
/// use tokio_stream::StreamExt;
/// use anyhow::anyhow;
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let stream = once_err::<i32>(anyhow!("Something went wrong"));
/// let results: Vec<_> = stream.collect().await;
/// assert_eq!(results.len(), 1);
/// assert!(results[0].is_err());
/// # });
/// ```
pub fn once_err<T>(error: anyhow::Error) -> impl Stream<Item = Result<T>> + Send
where
    T: Send,
{
    tokio_stream::once(Err(error))
}

/// Creates a stream from a vector of values, wrapping each in `Ok`.
///
/// This is a convenience function for creating streams from multiple successful
/// results, commonly used in Transform implementations. Instead of manually
/// wrapping each value in `Ok`, this function does it automatically.
///
/// # Example
///
/// ```rust
/// use streamwerk::iter_ok;
/// use tokio_stream::StreamExt;
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let stream = iter_ok(vec![1, 2, 3]);
/// let results: Vec<_> = stream.collect().await;
/// assert_eq!(results.len(), 3);
/// assert_eq!(results[0].as_ref().unwrap(), &1);
/// assert_eq!(results[1].as_ref().unwrap(), &2);
/// assert_eq!(results[2].as_ref().unwrap(), &3);
/// # });
/// ```
pub fn iter_ok<T>(values: Vec<T>) -> impl Stream<Item = Result<T>> + Send
where
    T: Send,
{
    tokio_stream::iter(values.into_iter().map(Ok))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::Stream;
    use anyhow::Result;

    fn extract_numbers(_input: ()) -> Result<impl Stream<Item = Result<i32>> + Send> {
        Ok(iter_ok(vec![1, 2, 3]))
    }

    fn double(n: i32) -> Result<impl Stream<Item = Result<i32>> + Send> {
        Ok(iter_ok(vec![n * 2]))
    }

    fn add_ten(n: i32) -> Result<impl Stream<Item = Result<i32>> + Send> {
        Ok(iter_ok(vec![n + 10]))
    }

    #[tokio::test]
    async fn test_fn_extract() {
        use tokio_stream::StreamExt;

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
        use tokio_stream::StreamExt;

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
        use tokio_stream::StreamExt;

        fn identity(n: i32) -> Result<impl Stream<Item = Result<i32>> + Send> {
            Ok(once_ok(n))
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
        use tokio_stream::StreamExt;

        fn identity(n: i32) -> Result<impl Stream<Item = Result<i32>> + Send> {
            Ok(once_ok(n))
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
    async fn test_scan() {
        use tokio_stream::StreamExt;

        fn emit_multiple(n: i32) -> Result<impl Stream<Item = Result<i32>> + Send> {
            Ok(iter_ok(vec![n, n + 1, n + 2]))
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
            Ok(iter_ok(vec![n, n + 1, n + 2]))
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
