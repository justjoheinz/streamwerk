use anyhow::Result;
use etl::prelude::*;
use etl_csv::prelude::*;
use etl_fs::prelude::*;
use etl_sse::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Person {
    name: String,
    age: u32,
}

// Mastodon status update (we use serde_json::Value to accept any JSON structure)
// The actual Mastodon status has many fields, but we'll just use the raw JSON

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== ETL Pipeline Demo: Filter Persons Over 50 ===\n");

    // Shared state for collecting results
    let results = Arc::new(Mutex::new(Vec::new()));
    let results_clone = Arc::clone(&results);

    // Load function that collects persons
    let load_person = move |person: Person| -> Result<()> {
        results_clone.lock().unwrap().push(person);
        Ok(())
    };

    // Build the pipeline with composition and filtering
    let pipeline = EtlPipeline::new(
        FileLineExtract.skip(1), // Extract: PathBuf → Stream<String>, skip header
        CsvDeserializer // Transform: String → Person
            .filter(|person : &Person| person.age > 50), // Filter: Keep only persons over 50
        FnLoad(load_person),     // Load: Person → collect
    );

    // Run the pipeline
    pipeline.run(PathBuf::from("persons.csv")).await?;

    // Display results
    {
        let persons = results.lock().unwrap();
        println!("Persons over 50 years old:");
        println!("{:<30} Age", "Name");
        println!("{}", "-".repeat(40));

        for person in persons.iter() {
            println!("{:<30} {}", person.name, person.age);
        }

        println!("\nTotal persons over 50: {}", persons.len());
    } // Drop MutexGuard here

    println!("\n=== ETL Pipeline Demo: Mastodon Public Timeline (Update Events) ===\n");
    println!("Connecting to Mastodon SSE stream...");
    println!("Filtering for 'update' events only...");
    println!("Will process first 15 events (some may be filtered out) and then exit.\n");

    // Load function to print to stdout
    fn print_event(message: String) -> Result<()> {
        println!("{}", message);
        println!("{}", "-".repeat(80));
        Ok(())
    }

    // Build the SSE pipeline using Identity with filter and map
    let sse_pipeline = EtlPipeline::new(
        SseExtractWithType::<serde_json::Value>::new().take(15), // Only take first 15 SSE events total
        Identity
            .filter(|event: &SseEvent<serde_json::Value>| event.event_type == "update")
            .map(|event: SseEvent<serde_json::Value>| {
                format!(
                    "[UPDATE - Event ID: {}]\n{}",
                    event.id.as_deref().unwrap_or("none"),
                    serde_json::to_string_pretty(&event.data).unwrap_or_else(|_| "{}".to_string())
                )
            }),
        FnLoad(print_event),
    );

    // Run the SSE pipeline
    sse_pipeline
        .run("https://mastodon.arroyo.dev/api/v1/streaming/public".to_string())
        .await?;

    Ok(())
}
