//! Prelude module for convenient imports.
//!
//! This module re-exports the most commonly used types and traits from the ETL framework.
//! You can import everything you need with a single use statement:
//!
//! ```rust
//! use streamwerk::prelude::*;
//! ```

pub use crate::extract::{Extract, ExtractExt, FnExtract, SkipExtractor, TakeExtractor};
pub use crate::load::{FnLoad, Load, WithHeader};
pub use crate::transform::{
    Compose, Filter, FnTransform, Identity, Map, ScanTransform, Transform,
};
pub use crate::pipeline::EtlPipeline;
pub use crate::{iter_ok, once_err, once_ok};