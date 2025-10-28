#![cfg(test)]
#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]

use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection};
use streamwerk::{Extract, EtlPipeline, ExtractExt, FnExtract, FnLoad, FnTransform};
use streamwerk_sql::{PostgresExtract, PostgresLoad};
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
        FnLoad(|person: Person| {
            let results = Arc::clone(&results_clone);
            async move {
                results.lock().unwrap().push(person);
                Ok(())
            }
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
        FnLoad(|person: Person| {
            let results = Arc::clone(&results_clone);
            async move {
                results.lock().unwrap().push(person);
                Ok(())
            }
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
        FnLoad(|person: Person| {
            let results = Arc::clone(&results_clone);
            async move {
                results.lock().unwrap().push(person);
                Ok(())
            }
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

#[derive(Insertable)]
#[diesel(table_name = persons)]
struct NewPerson {
    name: String,
    age: i32,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_postgres_load_insert_persons() {
    let database_url = get_database_url()
        .expect("DATABASE_URL must be set to run this test");

    // Connect and create loader
    let conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("Failed to connect to database");

    let loader = PostgresLoad::new(persons::table, conn);

    // Count persons before insert
    let count_before = {
        let conn = AsyncPgConnection::establish(&database_url).await.unwrap();
        let query = persons::table.select(diesel::dsl::count_star());
        let extractor = PostgresExtract::<_, i64>::new(query);
        let stream = extractor.extract(conn).unwrap();
        tokio::pin!(stream);
        stream.next().await.unwrap().unwrap()
    };

    // Build pipeline that inserts new persons
    let pipeline = EtlPipeline::new(
        FnExtract(|_| Ok(streamwerk::iter_ok(vec![100, 101, 102]))),
        FnTransform(|n: i32| {
            let person = NewPerson {
                name: format!("Test Person {}", n),
                age: n,
            };
            Ok(streamwerk::once_ok(person))
        }),
        loader
    );

    // Run pipeline
    pipeline.run(()).await.expect("Pipeline failed");

    // Verify persons were inserted
    let count_after = {
        let conn = AsyncPgConnection::establish(&database_url).await.unwrap();
        let query = persons::table.select(diesel::dsl::count_star());
        let extractor = PostgresExtract::<_, i64>::new(query);
        let stream = extractor.extract(conn).unwrap();
        tokio::pin!(stream);
        stream.next().await.unwrap().unwrap()
    };

    assert_eq!(count_after, count_before + 3, "Should have inserted 3 persons");

    // Clean up - delete test persons
    {
        let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
        diesel_async::RunQueryDsl::execute(
            diesel::delete(persons::table.filter(persons::name.like("Test Person%"))),
            &mut conn
        )
        .await
        .expect("Failed to clean up test data");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_postgres_extract_and_load_pipeline() {
    let database_url = get_database_url()
        .expect("DATABASE_URL must be set to run this test");

    // Extract persons with age > 65, transform to NewPerson with incremented age, insert back
    let extract_conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    let load_conn = AsyncPgConnection::establish(&database_url).await.unwrap();

    let query = persons::table
        .filter(persons::age.gt(65))
        .select(Person::as_select())
        .limit(2);

    let extractor = PostgresExtract::<_, Person>::new(query);
    let loader = PostgresLoad::new(persons::table, load_conn);

    let pipeline = EtlPipeline::new(
        extractor,
        FnTransform(|person: Person| {
            let new_person = NewPerson {
                name: format!("{} (copy)", person.name),
                age: person.age + 1,
            };
            Ok(streamwerk::once_ok(new_person))
        }),
        loader
    );

    pipeline.run(extract_conn).await.expect("Pipeline failed");

    // Verify copies were created
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    let copies: Vec<Person> = diesel_async::RunQueryDsl::load(
        persons::table
            .filter(persons::name.like("% (copy)"))
            .select(Person::as_select()),
        &mut conn
    )
    .await
    .expect("Failed to load copies");

    assert_eq!(copies.len(), 2, "Should have created 2 copies");

    // Clean up
    diesel_async::RunQueryDsl::execute(
        diesel::delete(persons::table.filter(persons::name.like("% (copy)"))),
        &mut conn
    )
    .await
    .expect("Failed to clean up test data");
}
