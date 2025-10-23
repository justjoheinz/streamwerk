#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]

//! # SSE Extract for ETL Framework
//!
//! This crate provides extractors for Server-Sent Events (SSE) streams,
//! allowing SSE endpoints to be used as data sources in ETL pipelines.
//!
//! ## Quick Start
//!
//! Use the prelude for convenient imports:
//!
//! ```rust,no_run
//! # #![feature(impl_trait_in_assoc_type)]
//! use etl_sse::prelude::*;
//! use etl::{EtlPipeline, FnTransform, FnLoad};
//! use serde::Deserialize;
//! use tokio_stream::{iter, Stream};
//! use anyhow::Result;
//!
//! #[derive(Deserialize, Debug)]
//! struct Event {
//!     message: String,
//!     timestamp: u64,
//! }
//!
//! fn transform(event: Event) -> Result<impl Stream<Item = Result<String>> + Send> {
//!     Ok(etl::once_ok(format!("{}: {}", event.timestamp, event.message)))
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

use anyhow::{Context, Result};
use etl::Extract;
use futures::StreamExt as FuturesStreamExt;
use serde::de::DeserializeOwned;
use sse_stream::SseStream;
use std::marker::PhantomData;
use tokio_stream::Stream;

pub mod prelude;

/// Wrapper type that includes both the SSE event type and deserialized payload.
///
/// This is useful when you need to filter or process events based on their type
/// (e.g., Mastodon's "update", "delete", "notification" events).
#[derive(Debug, Clone)]
pub struct SseEvent<T> {
    /// The event type from the SSE `event:` field (defaults to "message" if not specified)
    pub event_type: String,
    /// The deserialized payload from the SSE `data:` field
    pub data: T,
    /// The event ID from the SSE `id:` field, if present
    pub id: Option<String>,
}

/// Configuration for SSE extraction
#[derive(Debug, Clone)]
pub struct SseConfig {
    /// Request timeout in seconds (None for no timeout)
    pub timeout_secs: Option<u64>,
}

impl Default for SseConfig {
    fn default() -> Self {
        Self {
            timeout_secs: Some(300), // 5 minutes default
        }
    }
}

/// Extractor for Server-Sent Events (SSE) streams.
///
/// This extractor connects to an SSE endpoint and deserializes incoming events
/// into typed structures. Each SSE event's data field is parsed as JSON and
/// deserialized into type `T`.
///
/// # Type Parameters
///
/// * `T` - The type to deserialize SSE event data into. Must implement `DeserializeOwned`.
///
/// # Example
///
/// ```rust,no_run
/// # #![feature(impl_trait_in_assoc_type)]
/// use etl_sse::SseExtract;
/// use etl::{EtlPipeline, FnTransform, FnLoad};
/// use serde::Deserialize;
/// use tokio_stream::{iter, Stream};
/// use anyhow::Result;
///
/// #[derive(Deserialize, Debug)]
/// struct Notification {
///     id: u64,
///     message: String,
/// }
///
/// fn transform(notif: Notification) -> Result<impl Stream<Item = Result<String>> + Send> {
///     Ok(etl::once_ok(format!("Notification #{}: {}", notif.id, notif.message)))
/// }
///
/// fn load(msg: String) -> Result<()> {
///     println!("{}", msg);
///     Ok(())
/// }
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let pipeline = EtlPipeline::new(
///     SseExtract::<Notification>::new(),
///     FnTransform(transform),
///     FnLoad(load)
/// );
/// // pipeline.run("https://example.com/notifications".to_string()).await?;
/// # Ok(())
/// # }
/// ```
pub struct SseExtract<T> {
    config: SseConfig,
    _phantom: PhantomData<T>,
}

impl<T> SseExtract<T> {
    /// Creates a new SSE extractor with default configuration.
    pub fn new() -> Self {
        Self {
            config: SseConfig::default(),
            _phantom: PhantomData,
        }
    }

    /// Creates a new SSE extractor with custom configuration.
    pub fn with_config(config: SseConfig) -> Self {
        Self {
            config,
            _phantom: PhantomData,
        }
    }
}

impl<T> Default for SseExtract<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Extract<String, T> for SseExtract<T>
where
    T: DeserializeOwned + Send + 'static,
{
    type StreamType = impl Stream<Item = Result<T>> + Send;

    fn extract(&self, url: String) -> Result<Self::StreamType> {
        let mut client_builder = reqwest::Client::builder();

        if let Some(timeout_secs) = self.config.timeout_secs {
            client_builder = client_builder.timeout(std::time::Duration::from_secs(timeout_secs));
        }

        let client = client_builder
            .build()
            .context("Failed to build HTTP client")?;

        let stream = async_stream::stream! {
            // Make the HTTP request
            let response = match client.get(&url).send().await {
                Ok(resp) => resp,
                Err(e) => {
                    yield Err(anyhow::anyhow!("Failed to connect to SSE endpoint: {}", e));
                    return;
                }
            };

            // Check if the response is successful
            if !response.status().is_success() {
                yield Err(anyhow::anyhow!(
                    "SSE endpoint returned error status: {}",
                    response.status()
                ));
                return;
            }

            // Convert the response body into a byte stream
            let byte_stream = response.bytes_stream();

            // Decode SSE events from the byte stream
            let mut sse_stream = SseStream::from_byte_stream(byte_stream);

            // Process each SSE event
            while let Some(event_result) = sse_stream.next().await {
                match event_result {
                    Ok(sse_event) => {
                        // Extract the data field from the SSE event
                        let data = match sse_event.data {
                            Some(ref d) if !d.is_empty() => d,
                            _ => continue, // Skip events without data
                        };

                        // Parse the event data as JSON
                        match serde_json::from_str::<T>(data) {
                            Ok(value) => yield Ok(value),
                            Err(e) => {
                                yield Err(anyhow::anyhow!(
                                    "Failed to deserialize SSE event data: {}. Data: {}",
                                    e,
                                    data
                                ));
                            }
                        }
                    }
                    Err(e) => {
                        yield Err(anyhow::anyhow!("SSE stream error: {}", e));
                    }
                }
            }
        };

        Ok(stream)
    }
}

/// Convenience extractor that accepts a URL reference instead of owned String.
///
/// This is a thin wrapper around `SseExtract` that allows URLs to be passed
/// as `&str` instead of `String`.
pub struct SseExtractRef<T> {
    inner: SseExtract<T>,
}

impl<T> SseExtractRef<T> {
    /// Creates a new SSE extractor with default configuration.
    pub fn new() -> Self {
        Self {
            inner: SseExtract::new(),
        }
    }

    /// Creates a new SSE extractor with custom configuration.
    pub fn with_config(config: SseConfig) -> Self {
        Self {
            inner: SseExtract::with_config(config),
        }
    }
}

impl<T> Default for SseExtractRef<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, T> Extract<&'a str, T> for SseExtractRef<T>
where
    T: DeserializeOwned + Send + 'static,
{
    type StreamType = impl Stream<Item = Result<T>> + Send;

    fn extract(&self, url: &'a str) -> Result<Self::StreamType> {
        self.inner.extract(url.to_string())
    }
}

/// Extractor for Server-Sent Events that includes event type information.
///
/// Unlike `SseExtract`, this extractor wraps each event in an `SseEvent<T>` that
/// includes the event type, allowing you to filter or process events based on their type.
///
/// This is particularly useful for APIs like Mastodon that use different event types
/// (e.g., "update", "delete", "notification").
///
/// # Example
///
/// ```rust,no_run
/// # #![feature(impl_trait_in_assoc_type)]
/// use etl_sse::{SseExtractWithType, SseEvent};
/// use etl::{iter_ok, EtlPipeline, FnTransform, FnLoad};
/// use serde::Deserialize;
/// use tokio_stream::Stream;
/// use anyhow::Result;
///
/// #[derive(Deserialize, Debug)]
/// struct Status {
///     id: String,
///     content: String,
/// }
///
/// fn filter_updates(event: SseEvent<Status>) -> Result<impl Stream<Item = Result<String>> + Send> {
///     let items: Vec<String> = if event.event_type == "update" {
///         vec![format!("Update: {}", event.data.content)]
///     } else {
///         vec![] // Skip non-update events (empty stream)
///     };
///     Ok(etl::iter_ok(items))
/// }
///
/// fn load(msg: String) -> Result<()> {
///     println!("{}", msg);
///     Ok(())
/// }
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let pipeline = EtlPipeline::new(
///     SseExtractWithType::<Status>::new(),
///     FnTransform(filter_updates),
///     FnLoad(load)
/// );
/// // pipeline.run("https://example.com/stream".to_string()).await?;
/// # Ok(())
/// # }
/// ```
pub struct SseExtractWithType<T> {
    config: SseConfig,
    _phantom: PhantomData<T>,
}

impl<T> SseExtractWithType<T> {
    /// Creates a new SSE extractor with event type information.
    pub fn new() -> Self {
        Self {
            config: SseConfig::default(),
            _phantom: PhantomData,
        }
    }

    /// Creates a new SSE extractor with custom configuration.
    pub fn with_config(config: SseConfig) -> Self {
        Self {
            config,
            _phantom: PhantomData,
        }
    }
}

impl<T> Default for SseExtractWithType<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Extract<String, SseEvent<T>> for SseExtractWithType<T>
where
    T: DeserializeOwned + Send + 'static,
{
    type StreamType = impl Stream<Item = Result<SseEvent<T>>> + Send;

    fn extract(&self, url: String) -> Result<Self::StreamType> {
        let mut client_builder = reqwest::Client::builder();

        if let Some(timeout_secs) = self.config.timeout_secs {
            client_builder = client_builder.timeout(std::time::Duration::from_secs(timeout_secs));
        }

        let client = client_builder
            .build()
            .context("Failed to build HTTP client")?;

        let stream = async_stream::stream! {
            // Make the HTTP request
            let response = match client.get(&url).send().await {
                Ok(resp) => resp,
                Err(e) => {
                    yield Err(anyhow::anyhow!("Failed to connect to SSE endpoint: {}", e));
                    return;
                }
            };

            // Check if the response is successful
            if !response.status().is_success() {
                yield Err(anyhow::anyhow!(
                    "SSE endpoint returned error status: {}",
                    response.status()
                ));
                return;
            }

            // Convert the response body into a byte stream
            let byte_stream = response.bytes_stream();

            // Decode SSE events from the byte stream
            let mut sse_stream = SseStream::from_byte_stream(byte_stream);

            // Process each SSE event
            while let Some(event_result) = sse_stream.next().await {
                match event_result {
                    Ok(sse_event) => {
                        // Extract the data field from the SSE event
                        let data = match sse_event.data {
                            Some(ref d) if !d.is_empty() => d,
                            _ => continue, // Skip events without data
                        };

                        // Parse the event data as JSON
                        match serde_json::from_str::<T>(data) {
                            Ok(value) => {
                                let event = SseEvent {
                                    event_type: sse_event.event.unwrap_or_else(|| "message".to_string()),
                                    data: value,
                                    id: sse_event.id,
                                };
                                yield Ok(event);
                            }
                            Err(e) => {
                                yield Err(anyhow::anyhow!(
                                    "Failed to deserialize SSE event data: {}. Data: {}",
                                    e,
                                    data
                                ));
                            }
                        }
                    }
                    Err(e) => {
                        yield Err(anyhow::anyhow!("SSE stream error: {}", e));
                    }
                }
            }
        };

        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize, Debug, PartialEq)]
    struct TestEvent {
        id: u64,
        message: String,
    }

    #[test]
    fn test_sse_extract_creation() {
        let _extractor = SseExtract::<TestEvent>::new();
        let _extractor_with_config = SseExtract::<TestEvent>::with_config(SseConfig {
            timeout_secs: Some(60),
        });
    }

    #[test]
    fn test_sse_extract_ref_creation() {
        let _extractor = SseExtractRef::<TestEvent>::new();
        let _extractor_with_config = SseExtractRef::<TestEvent>::with_config(SseConfig {
            timeout_secs: None,
        });
    }

    // Note: Integration tests with actual SSE endpoints would require
    // either a mock server or a real SSE endpoint, which is beyond
    // the scope of basic unit tests.
}
