# Streamwerk

A composable, async stream-based ETL (Extract, Transform, Load) framework for Rust.

_WORK IN PROGRESS - SUBJECT TO CHANGE_

## Overview

This workspace implements a modular ETL framework built on async streams using `tokio_stream`. The framework provides reusable components for building data processing pipelines with a focus on composition and type safety.

## Architecture

The ETL framework is built around three core phases:

- **Extract** - Produces streams of data from various sources (files, stdin, SSE endpoints)
- **Transform** - Processes and converts data items, with support for filtering, mapping, and composition
- **Load** - Consumes data items and performs side effects (writing to stdout, files, etc.)

All operations produce `Stream<Item = Result<T>>` types to handle errors during stream processing.

## Workspace Crates

### `streamwerk` - Core Framework

The foundational crate defining the ETL traits and pipeline orchestration.

**Key Types:**

- `Extract<Input, Output>` - Trait for data extraction
- `Transform<Input, Output>` - Trait for data transformation with GATs for lifetime management
- `Load<Input>` - Trait for data loading with lifecycle hooks (`initialize`, `load`, `finalize`)
- `EtlPipeline` - Orchestrates Extract → Transform → Load execution
- `WithHeader<L>` - Generic decorator for adding headers to any `Load<String>` implementation

**Features:**

- Composable transforms via `and_then()`, `filter()`, `map()`, `scan()`
- Type-level composition with `Compose<T1, T2>`
- Load lifecycle management for resource initialization and cleanup
- Function wrappers (`FnExtract`, `FnTransform`, `FnLoad`) for ergonomic integration

### `streamwerk-fs` - Filesystem I/O

Extractors and loaders for filesystem operations.

**Extractors:**

- `FileExtract` - Read files byte-by-byte
- `FileLineExtract` - Read files line-by-line
- `StdinLineExtract` - Read from stdin line-by-line

**Loaders:**

- `StdoutLoad` - Write items to stdout
- `FileLoad<T>` - Write items to files with configurable modes:
  - `FileLoad::create(path)` - Create/truncate file
  - `FileLoad::append(path)` - Append to file
  - Supports `WriteMode::Create` and `WriteMode::Append`

**Features:**

- Async file I/O using `tokio::fs`
- Buffered reading/writing with `BufReader`/`BufWriter`
- Lifecycle-managed file handles (opened in `initialize()`, closed in `finalize()`)

### `streamwerk-csv` - CSV and JSONL Support

Transformers for CSV and JSON Lines serialization/deserialization.

**Transformers:**

- `CsvSerializer` - Serialize structs to CSV strings
- `CsvDeserializer` - Deserialize CSV strings to structs
- `JsonlSerializer` - Serialize structs to JSON Lines
- `JsonlDeserializer` - Deserialize JSON Lines to structs

**Features:**

- Serde-based serialization/deserialization
- No header output from serializers (use `WithHeader` wrapper for headers)
- Compact single-line JSON output suitable for JSONL format

### `streamwerk-sse` - Server-Sent Events

Extractor for consuming Server-Sent Events (SSE) streams.

**Extractors:**

- `SseExtractWithType<T>` - Extract and deserialize SSE events

**Types:**

- `SseEvent<T>` - Wrapper containing event type, data, and optional ID

**Features:**

- Async SSE stream consumption
- Type-safe deserialization of event payloads
- Event filtering by event type

### `streamwerk-debug` - Examples

Demonstration programs showcasing the ETL framework capabilities.

**Examples:**

- `--sample csv` - Filter persons over 50 from CSV file to stdout
- `--sample stdin` - Same as csv but reading from stdin
- `--sample file` - Filter persons to CSV file with header
- `--sample sse` - Extract hashtags from Mastodon public timeline

Run examples:

```bash
cargo run -p streamwerk-debug -- --sample csv
cat persons.csv | cargo run -p streamwerk-debug -- --sample stdin
cargo run -p streamwerk-debug -- --sample file
cargo run -p streamwerk-debug -- --sample sse
```

## Example Usage

```rust
use streamwerk::prelude::*;
use streamwerk_fs::prelude::*;
use streamwerk_csv::prelude::*;
use std::path::PathBuf;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Person {
    name: String,
    age: u32,
}

// Create a pipeline that:
// 1. Reads persons.csv line-by-line
// 2. Deserializes CSV to Person structs
// 3. Filters persons over 50
// 4. Serializes back to CSV
// 5. Writes to output.csv with header
let pipeline = EtlPipeline::new(
    FileLineExtract.skip(1),
    CsvDeserializer
        .filter(|p: &Person| p.age > 50)
        .and_then(CsvSerializer),
    WithHeader::new(FileLoad::create("output.csv"), "name,age")
);

pipeline.run(PathBuf::from("persons.csv")).await?;
```

## Design Patterns

### Stream-Based Processing

All operations produce `Stream<Item = Result<T>>` for consistent error handling throughout the pipeline.

### Composable Transformations

Transformers can be chained using `and_then()` for sequential composition:

```rust
CsvDeserializer
    .filter(|person| person.age > 50)
    .map(|person| person.name)
    .and_then(SomeOtherTransform)
```

### Load Lifecycle

The `Load` trait provides initialization and finalization hooks:

```rust
trait Load<Input> {
    fn initialize(&self) -> impl Future<Output = Result<()>> + Send;
    fn load(&self, item: Input) -> Result<()>;
    fn finalize(&self, result: &Result<()>) -> impl Future<Output = Result<()>> + Send;
}
```

This enables:

- Resource setup (file handles, database connections) in `initialize()`
- Efficient per-item processing in `load()` without Option checks
- Guaranteed cleanup in `finalize()` with awareness of pipeline success/failure

### Decorator Pattern

Generic decorators like `WithHeader<L>` extend functionality without duplication:

```rust
WithHeader::new(StdoutLoad, "Header")        // Stdout with header
WithHeader::new(FileLoad::create("f.csv"), "name,age")  // File with CSV header
```

## Requirements

- Rust nightly (uses `type_alias_impl_trait` and `impl_trait_in_assoc_type`)
- Edition 2024
- See `rust-toolchain.toml` for exact version

## Building

```bash
cargo build                    # Build all crates
cargo test                     # Run tests
cargo clippy                   # Run linter
cargo run -p streamwerk-debug -- -s csv  # Run example
```

## License

[Specify your license here]
