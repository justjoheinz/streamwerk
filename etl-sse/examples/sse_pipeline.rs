#![feature(impl_trait_in_assoc_type)]

//! Example demonstrating SSE extraction in an ETL pipeline
//!
//! This example shows how to use SseExtract as a data source in an ETL pipeline.
//! Note: This is a demonstration of the API - it requires a real SSE endpoint to run.

use anyhow::Result;
use etl::{EtlPipeline, FnLoad, FnTransform};
use etl_sse::prelude::*;
use serde::{Deserialize, Serialize};
use tokio_stream::{iter, Stream};

#[derive(Deserialize, Serialize, Debug)]
struct Event {
    id: u64,
    message: String,
    timestamp: u64,
}

fn transform_event(event: Event) -> Result<impl Stream<Item = Result<String>> + Send> {
    // Transform the event into a formatted string
    let formatted = format!(
        "[{}] Event {}: {}",
        event.timestamp, event.id, event.message
    );
    Ok(iter(vec![Ok(formatted)]))
}

fn print_event(message: String) -> Result<()> {
    println!("{}", message);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // This example demonstrates the API but won't run without a real SSE endpoint
    println!("SSE ETL Pipeline Example");
    println!("========================\n");

    println!("To use this example, replace the URL with a real SSE endpoint that emits");
    println!("JSON events matching the Event structure.\n");

    // Example pipeline structure:
    // 1. Extract events from SSE endpoint
    // 2. Transform events into formatted strings
    // 3. Load (print) the formatted strings

    let extractor = SseExtract::<Event>::new();
    let transformer = FnTransform(transform_event);
    let loader = FnLoad(print_event);

    let _pipeline = EtlPipeline::new(extractor, transformer, loader);

    // Uncomment to run with a real SSE endpoint:
    // pipeline.run("https://your-sse-endpoint.com/events".to_string()).await?;

    println!("Pipeline created successfully!");
    println!("\nPipeline structure:");
    println!("  Extract: SseExtract<Event> - connects to SSE endpoint");
    println!("  Transform: format events as strings");
    println!("  Load: print to console");

    Ok(())
}
