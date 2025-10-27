#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]

//! # Streamwerk CSV and JSONL Support
//!
//! This crate provides transformers for CSV and JSON Lines serialization/deserialization
//! in streamwerk pipelines.
//!
//! # CSV Transformers
//!
//! - [`CsvSerializer`] - Serialize Rust structs to CSV line strings
//! - [`CsvDeserializer`] - Deserialize CSV line strings to Rust structs
//!
//! # JSON Lines (JSONL) Transformers
//!
//! - [`JsonlSerializer`] - Serialize Rust structs to JSON line strings
//! - [`JsonlDeserializer`] - Deserialize JSON line strings to Rust structs
//!
//! # Features
//!
//! - Serde-based serialization/deserialization for any serde-compatible type
//! - No headers in serialized output (use `WithHeader` decorator for headers)
//! - Compact single-line JSON output suitable for JSONL format
//! - Line-by-line processing for memory-efficient streaming
//!
//! All transformers implement the `Transform` trait and can be composed with other
//! transformations using `and_then()`, `filter()`, and `map()`.
//!
//! For complete usage examples, see the `streamwerk-debug` crate.

use anyhow::Result;
use streamwerk::Transform;
use serde::{Serialize, de::DeserializeOwned};
use tokio_stream::Stream;

pub mod prelude;

// ============================================================================
// CSV Transformers
// ============================================================================

/// Transform that serializes CSV-serializable types into CSV line strings.
///
/// Takes any type that implements `serde::Serialize` and converts it to
/// a CSV-formatted string using csv-line.
///
/// # Example
///
/// ```rust
/// # #![feature(impl_trait_in_assoc_type)]
/// use streamwerk_csv::CsvSerializer;
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
/// let pipeline = EtlPipeline::new(
///     FnExtract(extract),
///     CsvSerializer,
///     FnLoad(load)
/// );
/// pipeline.run(()).await.unwrap();
/// # });
/// ```
pub struct CsvSerializer;

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

        let mut wtr = WriterBuilder::new()
            .has_headers(false)
            .from_writer(Cursor::new(Vec::new()));

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
/// `serde::Deserialize` using csv-line.
///
/// # Example
///
/// ```rust
/// # #![feature(impl_trait_in_assoc_type)]
/// use streamwerk_csv::CsvDeserializer;
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
/// let pipeline = EtlPipeline::new(
///     FnExtract(extract),
///     CsvDeserializer,
///     FnLoad(load)
/// );
/// pipeline.run(()).await.unwrap();
/// # });
/// ```
pub struct CsvDeserializer;

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
        let value: T = csv_line::from_str(&input)?;
        Ok(streamwerk::once_ok(value))
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
        let serializer = CsvSerializer;
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
        let deserializer = CsvDeserializer;
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
        let serializer = CsvSerializer;
        let stream = serializer.transform(original).unwrap();
        let mut results: Vec<_> = stream.collect().await;
        let csv_line = results.pop().unwrap().unwrap();

        // Deserialize
        let deserializer = CsvDeserializer;
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
