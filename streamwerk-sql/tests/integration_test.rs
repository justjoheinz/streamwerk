#![cfg(test)]
#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]

use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection};
use streamwerk::{Extract, EtlPipeline, ExtractExt, FnLoad, FnTransform};
use streamwerk_sql::PostgresExtract;
use std::sync::{Arc, Mutex};
use tokio_stream::StreamExt;

// Define the schema
diesel::table! {
    persons (id) {
        id -> Int4,
        name -> Text,
        age -> Int4,
    }
}

#[derive(Queryable, Selectable, Debug, Clone, PartialEq)]
#[diesel(table_name = persons)]
struct Person {
    id: i32,
    name: String,
    age: i32,
}

/// Get database URL from environment or return None
fn get_database_url() -> Option<String> {
    std::env::var("DATABASE_URL").ok()
}

#[tokio::test]
#[ignore] // Only run when DATABASE_URL is set
async fn test_postgres_extract_all_persons() {
    let database_url = get_database_url()
        .expect("DATABASE_URL must be set to run this test");

    // Establish connection
    let conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("Failed to connect to database");

    // Build query to fetch all persons
    let query = persons::table.select(Person::as_select());

    // Create extractor
    let extractor = PostgresExtract::<_, Person>::new(query);

    // Collect results
    let results = Arc::new(Mutex::new(Vec::new()));
    let results_clone = Arc::clone(&results);

    // Build pipeline
    let pipeline = EtlPipeline::new(
        extractor,
        FnTransform(|person: Person| Ok(streamwerk::once_ok(person))),
        FnLoad(move |person: Person| {
            results_clone.lock().unwrap().push(person);
            Ok(())
        }),
    );

    // Run pipeline
    pipeline.run(conn).await.expect("Pipeline failed");

    // Verify results
    let persons = results.lock().unwrap();
    assert_eq!(persons.len(), 50, "Should have loaded 50 persons");

    // Verify first person (Alice Johnson, 45)
    assert_eq!(persons[0].name, "Alice Johnson");
    assert_eq!(persons[0].age, 45);
}

#[tokio::test]
#[ignore] // Only run when DATABASE_URL is set
async fn test_postgres_extract_filtered_query() {
    let database_url = get_database_url()
        .expect("DATABASE_URL must be set to run this test");

    let conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("Failed to connect to database");

    // Query persons older than 60
    let query = persons::table
        .filter(persons::age.gt(60))
        .select(Person::as_select())
        .order(persons::age.asc());

    let extractor = PostgresExtract::<_, Person>::new(query);

    let results = Arc::new(Mutex::new(Vec::new()));
    let results_clone = Arc::clone(&results);

    let pipeline = EtlPipeline::new(
        extractor,
        FnTransform(|person: Person| Ok(streamwerk::once_ok(person))),
        FnLoad(move |person: Person| {
            results_clone.lock().unwrap().push(person);
            Ok(())
        }),
    );

    pipeline.run(conn).await.expect("Pipeline failed");

    let persons = results.lock().unwrap();

    // Verify all persons are older than 60
    assert!(!persons.is_empty(), "Should have some persons over 60");
    for person in persons.iter() {
        assert!(person.age > 60, "Person {} should be over 60", person.name);
    }
}

#[tokio::test]
#[ignore] // Only run when DATABASE_URL is set
async fn test_postgres_extract_with_skip_and_take() {
    let database_url = get_database_url()
        .expect("DATABASE_URL must be set to run this test");

    let conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("Failed to connect to database");

    // Query with ordering
    let query = persons::table
        .select(Person::as_select())
        .order(persons::id.asc());

    // Use skip and take combinators
    let extractor = PostgresExtract::<_, Person>::new(query)
        .skip(5)
        .take(10);

    let results = Arc::new(Mutex::new(Vec::new()));
    let results_clone = Arc::clone(&results);

    let pipeline = EtlPipeline::new(
        extractor,
        FnTransform(|person: Person| Ok(streamwerk::once_ok(person))),
        FnLoad(move |person: Person| {
            results_clone.lock().unwrap().push(person);
            Ok(())
        }),
    );

    pipeline.run(conn).await.expect("Pipeline failed");

    let persons = results.lock().unwrap();
    assert_eq!(persons.len(), 10, "Should have exactly 10 persons after skip(5).take(10)");

    // Verify we skipped the first 5 (IDs 1-5) and got IDs 6-15
    assert_eq!(persons[0].id, 6);
    assert_eq!(persons[9].id, 15);
}

#[tokio::test]
#[ignore] // Only run when DATABASE_URL is set
async fn test_postgres_extract_direct_stream_usage() {
    let database_url = get_database_url()
        .expect("DATABASE_URL must be set to run this test");

    let conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("Failed to connect to database");

    let query = persons::table
        .filter(persons::age.lt(30))
        .select(Person::as_select());

    let extractor = PostgresExtract::<_, Person>::new(query);

    // Use extract directly without pipeline
    let stream = extractor.extract(conn).expect("Extract failed");
    tokio::pin!(stream);

    let mut count = 0;
    while let Some(result) = stream.next().await {
        let person = result.expect("Should get valid person");
        assert!(person.age < 30, "Person {} should be under 30", person.name);
        count += 1;
    }

    assert!(count > 0, "Should have persons under 30");
}
