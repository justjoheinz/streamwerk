//! Prelude module for convenient imports.
//!
//! This module re-exports the most commonly used types from the streamwerk-fs crate.
//! You can import everything you need with a single use statement:
//!
//! ```rust
//! use streamwerk_fs::prelude::*;
//! ```

pub use crate::{
    FileExtract, FileLineExtract, FileLoad, StdinLineExtract, StdoutLoad, WriteMode,
};
