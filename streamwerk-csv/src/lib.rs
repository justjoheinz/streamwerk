#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]

//! # Streamwerk CSV and JSONL Support
//!
//! This crate provides transformers for CSV and JSON Lines serialization/deserialization
//! in streamwerk pipelines.
//!
//! **Note:** This crate re-exports the base [`streamwerk`] crate, so you only need to
//! declare `streamwerk-csv` as a dependency in your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! streamwerk-csv = "0.0.1"
//! ```
//!
//! Access base types via the re-export:
//! - `streamwerk_csv::streamwerk::EtlPipeline`
//! - Or use the prelude: `use streamwerk_csv::prelude::*;`
//!
//! # CSV Transformers
//!
//! - [`CsvSerializer`] - Serialize Rust structs to CSV line strings
//! - [`CsvDeserializer`] - Deserialize CSV line strings to Rust structs
//! - [`CsvConfig`] - Configuration for CSV format (delimiter, quotes, escaping)
//!
//! # JSON Lines (JSONL) Transformers
//!
//! - [`JsonlSerializer`] - Serialize Rust structs to JSON line strings
//! - [`JsonlDeserializer`] - Deserialize JSON line strings to Rust structs
//!
//! # Features
//!
//! - Serde-based serialization/deserialization for any serde-compatible type
//! - Configurable CSV format (comma, tab, or custom delimiters)
//! - No headers in serialized output (use `WithHeader` decorator for headers)
//! - Compact single-line JSON output suitable for JSONL format
//! - Line-by-line processing for memory-efficient streaming
//!
//! All transformers implement the `Transform` trait and can be composed with other
//! transformations using `and_then()`, `filter()`, and `map()`.
//!
//! # CSV Configuration Examples
//!
//! ```rust
//! use streamwerk_csv::{CsvSerializer, CsvConfig};
//!
//! // Default comma-separated values
//! let csv_serializer = CsvSerializer::new();
//!
//! // Tab-separated values (TSV)
//! let tsv_serializer = CsvSerializer::with_config(CsvConfig::tsv());
//!
//! // Custom delimiter
//! let pipe_serializer = CsvSerializer::with_config(
//!     CsvConfig::new().with_delimiter(b'|')
//! );
//! ```
//!
//! For complete usage examples, see the `streamwerk-debug` crate.

use anyhow::Result;
use streamwerk::Transform;
use serde::{Serialize, de::DeserializeOwned};
use tokio_stream::Stream;

// Re-export the base streamwerk crate so users only need to depend on streamwerk-csv
pub use streamwerk;

// Re-export tokio_stream so users don't need to add it as a dependency
pub use tokio_stream;

pub mod prelude;

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for CSV serialization and deserialization.
///
/// Allows customization of the CSV format, including delimiter, quotes, and escaping.
#[derive(Debug, Clone)]
pub struct CsvConfig {
    /// The field delimiter character (default: `,`)
    pub delimiter: u8,
    /// Whether to write/expect quotes around fields (default: true for RFC 4180 compliance)
    pub quote: u8,
    /// The escape character (default: `"`)
    pub escape: Option<u8>,
    /// Whether double quotes should be used for escaping quotes (default: true)
    pub double_quote: bool,
}

impl Default for CsvConfig {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote: b'"',
            escape: None,
            double_quote: true,
        }
    }
}

impl CsvConfig {
    /// Create a new CSV config with comma delimiter (default).
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a config for tab-separated values (TSV).
    pub fn tsv() -> Self {
        Self {
            delimiter: b'\t',
            ..Self::default()
        }
    }

    /// Set the delimiter character.
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Set the quote character.
    pub fn with_quote(mut self, quote: u8) -> Self {
        self.quote = quote;
        self
    }

    /// Set the escape character.
    pub fn with_escape(mut self, escape: Option<u8>) -> Self {
        self.escape = escape;
        self
    }

    /// Set whether to use double quotes for escaping.
    pub fn with_double_quote(mut self, double_quote: bool) -> Self {
        self.double_quote = double_quote;
        self
    }
}

// ============================================================================
// CSV Transformers
// ============================================================================

/// Transform that serializes CSV-serializable types into CSV line strings.
///
/// Takes any type that implements `serde::Serialize` and converts it to
/// a CSV-formatted string. Supports configuration for custom delimiters (e.g., tabs).
///
/// # Example
///
/// ```rust
/// # #![feature(impl_trait_in_assoc_type)]
/// use streamwerk_csv::{CsvSerializer, CsvConfig};
/// use streamwerk::{EtlPipeline, FnExtract, FnLoad};
/// use serde::Serialize;
/// use tokio_stream::{iter, Stream};
/// use anyhow::Result;
///
/// #[derive(Serialize)]
/// struct Person {
///     name: String,
///     age: u32,
/// }
///
/// fn extract(_: ()) -> Result<impl Stream<Item = Result<Person>> + Send> {
///     Ok(streamwerk::once_ok(Person { name: "Alice".to_string(), age: 30 }))
/// }
///
/// fn load(csv_line: String) -> Result<()> {
///     println!("{}", csv_line);
///     Ok(())
/// }
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// // Use default comma delimiter
/// let serializer = CsvSerializer::new();
///
/// // Or use tab delimiter
/// let tsv_serializer = CsvSerializer::with_config(CsvConfig::tsv());
///
/// let pipeline = EtlPipeline::new(
///     FnExtract(extract),
///     serializer,
///     FnLoad(load)
/// );
/// pipeline.run(()).await.unwrap();
/// # });
/// ```
pub struct CsvSerializer {
    config: CsvConfig,
}

impl CsvSerializer {
    /// Create a new CSV serializer with default configuration (comma delimiter).
    pub fn new() -> Self {
        Self {
            config: CsvConfig::default(),
        }
    }

    /// Create a new CSV serializer with custom configuration.
    pub fn with_config(config: CsvConfig) -> Self {
        Self { config }
    }
}

impl Default for CsvSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Transform<T, String> for CsvSerializer
where
    T: Serialize,
{
    type Stream<'a>
        = impl Stream<Item = Result<String>> + Send + 'a
    where
        Self: 'a,
        T: 'a,
        String: 'a;

    fn transform<'a>(&'a self, input: T) -> Result<Self::Stream<'a>> {
        use csv::WriterBuilder;
        use std::io::Cursor;

        let mut builder = WriterBuilder::new();
        builder.has_headers(false);
        builder.delimiter(self.config.delimiter);
        builder.quote(self.config.quote);
        builder.double_quote(self.config.double_quote);

        if let Some(escape) = self.config.escape {
            builder.escape(escape);
        }

        let mut wtr = builder.from_writer(Cursor::new(Vec::new()));

        wtr.serialize(&input)?;
        let data = wtr.into_inner()?.into_inner();
        let csv_line = String::from_utf8(data)?;

        // Remove trailing newline if present
        let csv_line = csv_line.trim_end_matches('\n').to_string();
        Ok(streamwerk::once_ok(csv_line))
    }
}

/// Transform that deserializes CSV line strings into typed structures.
///
/// Takes a CSV-formatted string and converts it to any type that implements
/// `serde::Deserialize`. Supports configuration for custom delimiters (e.g., tabs).
///
/// # Example
///
/// ```rust
/// # #![feature(impl_trait_in_assoc_type)]
/// use streamwerk_csv::{CsvDeserializer, CsvConfig};
/// use streamwerk::{EtlPipeline, FnExtract, FnLoad};
/// use serde::Deserialize;
/// use tokio_stream::{iter, Stream};
/// use anyhow::Result;
///
/// #[derive(Deserialize, Debug)]
/// struct Person {
///     name: String,
///     age: u32,
/// }
///
/// fn extract(_: ()) -> Result<impl Stream<Item = Result<String>> + Send> {
///     Ok(streamwerk::once_ok("Alice,30".to_string()))
/// }
///
/// fn load(person: Person) -> Result<()> {
///     println!("{}: {}", person.name, person.age);
///     Ok(())
/// }
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// // Use default comma delimiter
/// let deserializer = CsvDeserializer::new();
///
/// // Or use tab delimiter for TSV files
/// let tsv_deserializer = CsvDeserializer::with_config(CsvConfig::tsv());
///
/// let pipeline = EtlPipeline::new(
///     FnExtract(extract),
///     deserializer,
///     FnLoad(load)
/// );
/// pipeline.run(()).await.unwrap();
/// # });
/// ```
pub struct CsvDeserializer {
    config: CsvConfig,
}

impl CsvDeserializer {
    /// Create a new CSV deserializer with default configuration (comma delimiter).
    pub fn new() -> Self {
        Self {
            config: CsvConfig::default(),
        }
    }

    /// Create a new CSV deserializer with custom configuration.
    pub fn with_config(config: CsvConfig) -> Self {
        Self { config }
    }
}

impl Default for CsvDeserializer {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Transform<String, T> for CsvDeserializer
where
    T: DeserializeOwned + Send + 'static,
{
    type Stream<'a>
        = impl Stream<Item = Result<T>> + Send + 'a
    where
        Self: 'a,
        String: 'a,
        T: 'a;

    fn transform<'a>(&'a self, input: String) -> Result<Self::Stream<'a>> {
        use csv::ReaderBuilder;
        use std::io::Cursor;

        let mut builder = ReaderBuilder::new();
        builder.has_headers(false);
        builder.delimiter(self.config.delimiter);
        builder.quote(self.config.quote);
        builder.double_quote(self.config.double_quote);

        if let Some(escape) = self.config.escape {
            builder.escape(Some(escape));
        }

        let mut reader = builder.from_reader(Cursor::new(input));

        // Read the single record
        let mut record = csv::StringRecord::new();
        if reader.read_record(&mut record)? {
            let value: T = record.deserialize(None)?;
            Ok(streamwerk::once_ok(value))
        } else {
            Err(anyhow::anyhow!("No CSV record found in input"))
        }
    }
}

// ============================================================================
// JSON Lines (JSONL) Transformers
// ============================================================================

/// Transform that deserializes JSONL (JSON Lines) strings into typed structures.
///
/// Takes a JSON-formatted string (one line from a JSONL file) and converts it to any type
/// that implements `serde::Deserialize` using serde_json.
///
/// # Example
///
/// ```rust
/// # #![feature(impl_trait_in_assoc_type)]
/// use streamwerk_csv::JsonlDeserializer;
/// use streamwerk::{EtlPipeline, FnExtract, FnLoad};
/// use serde::Deserialize;
/// use tokio_stream::{iter, Stream};
/// use anyhow::Result;
///
/// #[derive(Deserialize, Debug)]
/// struct Person {
///     name: String,
///     age: u32,
/// }
///
/// fn extract(_: ()) -> Result<impl Stream<Item = Result<String>> + Send> {
///     Ok(streamwerk::once_ok(r#"{"name":"Alice","age":30}"#.to_string()))
/// }
///
/// fn load(person: Person) -> Result<()> {
///     println!("{}: {}", person.name, person.age);
///     Ok(())
/// }
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let pipeline = EtlPipeline::new(
///     FnExtract(extract),
///     JsonlDeserializer,
///     FnLoad(load)
/// );
/// pipeline.run(()).await.unwrap();
/// # });
/// ```
pub struct JsonlDeserializer;

impl<T> Transform<String, T> for JsonlDeserializer
where
    T: DeserializeOwned + Send + 'static,
{
    type Stream<'a>
        = impl Stream<Item = Result<T>> + Send + 'a
    where
        Self: 'a,
        String: 'a,
        T: 'a;

    fn transform<'a>(&'a self, input: String) -> Result<Self::Stream<'a>> {
        let value: T = serde_json::from_str(&input)?;
        Ok(streamwerk::once_ok(value))
    }
}

/// Transform that serializes types into JSONL (JSON Lines) formatted strings.
///
/// Takes any type that implements `serde::Serialize` and converts it to
/// a JSON-formatted string suitable for JSONL files using serde_json.
///
/// # Example
///
/// ```rust
/// # #![feature(impl_trait_in_assoc_type)]
/// use streamwerk_csv::JsonlSerializer;
/// use streamwerk::{EtlPipeline, FnExtract, FnLoad};
/// use serde::Serialize;
/// use tokio_stream::{iter, Stream};
/// use anyhow::Result;
///
/// #[derive(Serialize)]
/// struct Person {
///     name: String,
///     age: u32,
/// }
///
/// fn extract(_: ()) -> Result<impl Stream<Item = Result<Person>> + Send> {
///     Ok(streamwerk::once_ok(Person { name: "Alice".to_string(), age: 30 }))
/// }
///
/// fn load(json_line: String) -> Result<()> {
///     println!("{}", json_line);
///     Ok(())
/// }
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let pipeline = EtlPipeline::new(
///     FnExtract(extract),
///     JsonlSerializer,
///     FnLoad(load)
/// );
/// pipeline.run(()).await.unwrap();
/// # });
/// ```
pub struct JsonlSerializer;

impl<T> Transform<T, String> for JsonlSerializer
where
    T: Serialize,
{
    type Stream<'a>
        = impl Stream<Item = Result<String>> + Send + 'a
    where
        Self: 'a,
        T: 'a,
        String: 'a;

    fn transform<'a>(&'a self, input: T) -> Result<Self::Stream<'a>> {
        let json_line = serde_json::to_string(&input)?;
        Ok(streamwerk::once_ok(json_line))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use tokio_stream::StreamExt;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Person {
        name: String,
        age: u32,
    }

    #[tokio::test]
    async fn test_csv_serializer() {
        let serializer = CsvSerializer::new();
        let person = Person {
            name: "Alice".to_string(),
            age: 30,
        };

        let stream = serializer.transform(person).unwrap();
        let results: Vec<_> = stream.collect().await;

        assert_eq!(results.len(), 1);
        let csv = results[0].as_ref().unwrap();
        assert_eq!(csv, "Alice,30");
    }

    #[tokio::test]
    async fn test_csv_deserializer() {
        let deserializer = CsvDeserializer::new();
        let csv_line = "Bob,25".to_string();

        let stream: <CsvDeserializer as Transform<String, Person>>::Stream<'_> =
            deserializer.transform(csv_line).unwrap();
        let results: Vec<_> = stream.collect().await;

        assert_eq!(results.len(), 1);
        let person = results[0].as_ref().unwrap();
        assert_eq!(
            person,
            &Person {
                name: "Bob".to_string(),
                age: 25
            }
        );
    }

    #[tokio::test]
    async fn test_roundtrip() {
        let original = Person {
            name: "Charlie".to_string(),
            age: 35,
        };

        // Serialize
        let serializer = CsvSerializer::new();
        let stream = serializer.transform(original).unwrap();
        let mut results: Vec<_> = stream.collect().await;
        let csv_line = results.pop().unwrap().unwrap();

        // Deserialize
        let deserializer = CsvDeserializer::new();
        let stream: <CsvDeserializer as Transform<String, Person>>::Stream<'_> =
            deserializer.transform(csv_line).unwrap();
        let mut results: Vec<_> = stream.collect().await;
        let roundtrip: Person = results.pop().unwrap().unwrap();

        assert_eq!(
            roundtrip,
            Person {
                name: "Charlie".to_string(),
                age: 35
            }
        );
    }

    #[tokio::test]
    async fn test_tsv_serializer() {
        let serializer = CsvSerializer::with_config(CsvConfig::tsv());
        let person = Person {
            name: "Alice".to_string(),
            age: 30,
        };

        let stream = serializer.transform(person).unwrap();
        let results: Vec<_> = stream.collect().await;

        assert_eq!(results.len(), 1);
        let tsv = results[0].as_ref().unwrap();
        assert_eq!(tsv, "Alice\t30");
    }

    #[tokio::test]
    async fn test_tsv_deserializer() {
        let deserializer = CsvDeserializer::with_config(CsvConfig::tsv());
        let tsv_line = "Bob\t25".to_string();

        let stream: <CsvDeserializer as Transform<String, Person>>::Stream<'_> =
            deserializer.transform(tsv_line).unwrap();
        let results: Vec<_> = stream.collect().await;

        assert_eq!(results.len(), 1);
        let person = results[0].as_ref().unwrap();
        assert_eq!(
            person,
            &Person {
                name: "Bob".to_string(),
                age: 25
            }
        );
    }

    #[tokio::test]
    async fn test_jsonl_serializer() {
        let serializer = JsonlSerializer;
        let person = Person {
            name: "Alice".to_string(),
            age: 30,
        };

        let stream = serializer.transform(person).unwrap();
        let results: Vec<_> = stream.collect().await;

        assert_eq!(results.len(), 1);
        let json = results[0].as_ref().unwrap();
        assert_eq!(json, r#"{"name":"Alice","age":30}"#);
    }

    #[tokio::test]
    async fn test_jsonl_deserializer() {
        let deserializer = JsonlDeserializer;
        let json_line = r#"{"name":"Bob","age":25}"#.to_string();

        let stream: <JsonlDeserializer as Transform<String, Person>>::Stream<'_> =
            deserializer.transform(json_line).unwrap();
        let results: Vec<_> = stream.collect().await;

        assert_eq!(results.len(), 1);
        let person = results[0].as_ref().unwrap();
        assert_eq!(
            person,
            &Person {
                name: "Bob".to_string(),
                age: 25
            }
        );
    }

    #[tokio::test]
    async fn test_jsonl_roundtrip() {
        let original = Person {
            name: "Charlie".to_string(),
            age: 35,
        };

        // Serialize
        let serializer = JsonlSerializer;
        let stream = serializer.transform(original).unwrap();
        let mut results: Vec<_> = stream.collect().await;
        let json_line = results.pop().unwrap().unwrap();

        // Deserialize
        let deserializer = JsonlDeserializer;
        let stream: <JsonlDeserializer as Transform<String, Person>>::Stream<'_> =
            deserializer.transform(json_line).unwrap();
        let mut results: Vec<_> = stream.collect().await;
        let roundtrip: Person = results.pop().unwrap().unwrap();

        assert_eq!(
            roundtrip,
            Person {
                name: "Charlie".to_string(),
                age: 35
            }
        );
    }
}
