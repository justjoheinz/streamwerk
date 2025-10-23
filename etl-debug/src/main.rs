use anyhow::Result;
use clap::Parser;
use etl::prelude::*;
use etl_csv::prelude::*;
use etl_fs::prelude::*;
use etl_sse::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Person {
    name: String,
    age: u32,
}

impl std::fmt::Display for Person {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:<30} {}", self.name, self.age)
    }
}

#[derive(Parser, Debug)]
#[command(name = "etl-debug")]
#[command(about = "ETL pipeline examples", long_about = None)]
struct Args {
    /// Which sample to run (csv, stdin, file, sse)
    #[arg(short, long)]
    sample: String,
}

async fn run_csv_sample() -> Result<()> {
    let header = format!(
        "=== ETL Pipeline Demo: Filter Persons Over 50 ===\n\n\
         {:<30} Age\n\
         {}",
        "Name",
        "-".repeat(40)
    );

    // Build the pipeline with composition and filtering
    let pipeline = EtlPipeline::new(
        FileLineExtract.skip(1), // Extract: PathBuf → Stream<String>, skip header
        CsvDeserializer // Transform: String → Person
            .filter(|person: &Person| person.age > 50) // Filter: Keep only persons over 50
            .map(|person: Person| person.to_string()), // Convert Person to String for WithHeader
        WithHeader::new(StdoutLoad, header), // Load: String → stdout with header
    );

    // Run the pipeline
    pipeline.run(PathBuf::from("persons.csv")).await?;

    Ok(())
}

async fn run_stdin_sample() -> Result<()> {
    let header = format!(
        "=== ETL Pipeline Demo: Filter Persons Over 50 (from stdin) ===\n\n\
         {:<30} Age\n\
         {}",
        "Name",
        "-".repeat(40)
    );

    // Build the pipeline with composition and filtering
    let pipeline = EtlPipeline::new(
        StdinLineExtract.skip(1), // Extract: () → Stream<String>, skip header
        CsvDeserializer // Transform: String → Person
            .filter(|person: &Person| person.age > 50) // Filter: Keep only persons over 50
            .map(|person: Person| person.to_string()), // Convert Person to String for WithHeader
        WithHeader::new(StdoutLoad, header), // Load: String → stdout with header
    );

    // Run the pipeline
    pipeline.run(()).await?;

    Ok(())
}


async fn run_file_sample() -> Result<()> {
    // Build the pipeline that writes filtered persons to a CSV file
    let pipeline = EtlPipeline::new(
        FileLineExtract.skip(1), // Extract: PathBuf → Stream<String>, skip header
        CsvDeserializer // Transform: String → Person
            .filter(|person: &Person| person.age > 50) // Filter: Keep only persons over 50
            .and_then(CsvSerializer), // Transform: Person → CSV String
        WithHeader::new(FileLoad::create("output.csv"), "name,age"), // Load: CSV String → file with CSV header
    );

    // Run the pipeline
    pipeline.run(PathBuf::from("persons.csv")).await?;

    println!("Filtered persons written to output.csv");

    Ok(())
}

async fn run_sse_sample() -> Result<()> {
    let header = format!(
        "=== ETL Pipeline Demo: Mastodon Tags from Public Timeline ===\n\n\
         Connecting to Mastodon SSE stream...\n\
         Filtering for 'update' events and extracting tags...\n\
         Will process first 15 events (some may be filtered out) and then exit.\n\n\
         {:<30} Tag URL\n\
         {}",
        "Tag Name",
        "-".repeat(80)
    );

    // Define a Tag struct using serde for deserialization
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Tag {
        name: String,
        url: String,
    }

    impl std::fmt::Display for Tag {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:<30} {}", self.name, self.url)
        }
    }

    // Define an UpdateEvent struct to hold parsed update event data
    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    struct UpdateEvent {
        tags: Vec<Tag>,
    }

    // Map function to parse SSE event JSON into UpdateEvent structure using serde
    fn parse_update_event(event: SseEvent<serde_json::Value>) -> UpdateEvent {
        serde_json::from_value(event.data).unwrap_or(UpdateEvent::default())
    }

    // Transform function to emit individual tags from an UpdateEvent
    fn emit_tags(
        update: UpdateEvent,
    ) -> Result<impl tokio_stream::Stream<Item = Result<Tag>> + Send> {
        Ok(iter_ok(update.tags))
    }

    // Build the SSE pipeline using Identity with filter, map to UpdateEvent, then emit tags
    let sse_pipeline = EtlPipeline::new(
        SseExtractWithType::<serde_json::Value>::new().take(15), // Only take first 15 SSE events total
        Identity
            .filter(|event: &SseEvent<serde_json::Value>| event.event_type == "update")
            .map(parse_update_event)
            .and_then(FnTransform(emit_tags))
            .map(|tag: Tag| tag.to_string()), // Convert Tag to String for WithHeader
        WithHeader::new(StdoutLoad, header),
    );

    // Run the SSE pipeline
    sse_pipeline
        .run("https://mastodon.arroyo.dev/api/v1/streaming/public".to_string())
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    match args.sample.as_str() {
        "csv" => run_csv_sample().await?,
        "stdin" => run_stdin_sample().await?,
        "file" => run_file_sample().await?,
        "sse" => run_sse_sample().await?,
        _ => {
            eprintln!("Unknown sample: {}", args.sample);
            eprintln!("Available samples: csv, stdin, file, sse");
            std::process::exit(1);
        }
    }

    Ok(())
}
