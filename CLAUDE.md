# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

- `cargo build` - Build the project
- `cargo test` - Run tests
- `cargo clippy` - Run linter
- `cargo fmt` - Format code

## Rust Toolchain Requirements

This project requires **nightly Rust** with specific unstable features:
- `type_alias_impl_trait`
- `impl_trait_in_assoc_type`

The toolchain configuration is in `rust-toolchain.toml`. The project uses Rust edition 2024.

## Architecture

This is a library crate implementing an ETL (Extract, Transform, Load) framework using async streams.

### Core Design Patterns

**Stream-based pipeline architecture**: The library defines transformation pipelines that operate on async streams using `tokio_stream::Stream`. All operations produce `Stream<Item = Result<T>>` types to handle errors during stream processing.

**Composable transformations**: The `Transform` trait is the central abstraction. It takes an input and produces an async stream of outputs. Transformers can be composed using the `and_then()` method via the `Compose` type, which chains transformations where each output from the first transform becomes input to the second.

**GATs for lifetime management**: The `Transform` trait uses Generic Associated Types (GATs) with the `Stream<'a>` associated type to properly handle borrowing across async stream boundaries. This allows transformers to borrow `&self` while producing streams.

**Type-level composition**: The `Compose<T1, T2>` struct composes two transforms at the type level. Its implementation uses `impl Trait` in the associated type position to hide complex nested stream types from users.

### Key Implementation Details

The `Compose::transform` implementation uses `then()` and `try_flatten()` to handle the nested stream structure - each item from the first stream is transformed into a new stream by the second transformer, then all streams are flattened into a single output stream.

The `Extract` trait (partially defined) follows a similar pattern but appears less developed than `Transform`.
- When you use clippy and you fix one of the clippy errors or warnings, tell me what you want to fix and ask if I want that.