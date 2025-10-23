//! Prelude module for convenient imports.
//!
//! This module re-exports the most commonly used types from the streamwerk-sse crate.
//! You can import everything you need with a single use statement:
//!
//! ```rust
//! use streamwerk_sse::prelude::*;
//! ```
//!
//! ## Example
//!
//! ```rust,no_run
//! # #![feature(impl_trait_in_assoc_type)]
//! use streamwerk_sse::prelude::*;
//! use streamwerk::{Extract, EtlPipeline, FnTransform, FnLoad};
//! use serde::Deserialize;
//! use tokio_stream::{iter, Stream};
//! use anyhow::Result;
//!
//! #[derive(Deserialize, Debug)]
//! struct Event {
//!     message: String,
//! }
//!
//! fn transform(event: Event) -> Result<impl Stream<Item = Result<String>> + Send> {
//!     Ok(streamwerk::once_ok(event.message))
//! }
//!
//! fn load(msg: String) -> Result<()> {
//!     println!("{}", msg);
//!     Ok(())
//! }
//!
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! let pipeline = EtlPipeline::new(
//!     SseExtract::<Event>::new(),
//!     FnTransform(transform),
//!     FnLoad(load)
//! );
//! // pipeline.run("https://example.com/events".to_string()).await?;
//! # Ok(())
//! # }
//! ```

pub use crate::{SseConfig, SseEvent, SseExtract, SseExtractRef, SseExtractWithType};
