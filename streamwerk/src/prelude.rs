//! Prelude module for convenient imports.
//!
//! This module re-exports the most commonly used types and traits from the ETL framework,
//! including tokio_stream types.
//! You can import everything you need with a single use statement:
//!
//! ```rust
//! use streamwerk::prelude::*;
//! ```

pub use crate::extract::{
    Extract, ExtractExt, FilterExtract, FlatMapExtract, FnExtract, LinesExtract, MapExtract,
    ReadExtract, SkipExtractor, StdinExtract, TakeExtractor,
};
pub use crate::load::{
    BatchLoad, FilterLoad, FnLoad, Load, MapLoad, PrefixLoad, SuffixLoad, WithHeader,
};
pub use crate::transform::{
    Compose, Filter, FnTransform, Identity, Map, ScanTransform, Transform,
};
pub use crate::pipeline::EtlPipeline;
pub use crate::{iter_ok, once_err, once_ok};
pub use tokio_stream::{Stream, StreamExt};