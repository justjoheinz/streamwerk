//! Prelude module for convenient imports.
//!
//! This module re-exports the most commonly used types from the etl-csv crate.
//! You can import everything you need with a single use statement:
//!
//! ```rust
//! use etl_csv::prelude::*;
//! ```

pub use crate::{CsvDeserializer, CsvSerializer, JsonlDeserializer, JsonlSerializer};
