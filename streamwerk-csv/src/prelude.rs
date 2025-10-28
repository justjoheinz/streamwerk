//! Prelude module for convenient imports.
//!
//! This module re-exports the most commonly used types from the streamwerk-csv crate,
//! the base streamwerk crate, and tokio_stream.
//! You can import everything you need with a single use statement:
//!
//! ```rust
//! use streamwerk_csv::prelude::*;
//! ```

pub use crate::{CsvConfig, CsvDeserializer, CsvSerializer, JsonlDeserializer, JsonlSerializer};
pub use streamwerk::prelude::*;
pub use tokio_stream::{Stream, StreamExt};
