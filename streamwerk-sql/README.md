# streamwerk-sql

PostgreSQL extractors and loaders for streamwerk using Diesel and diesel-async.

## Features

- Type-safe query execution using Diesel's QueryDsl
- Async streaming of database rows using diesel-async
- Async insertion of items into database tables
- Owned connection lifecycle - connection released when stream/pipeline completes
- Compatible with all Diesel query builder types

## Examples

### Extracting Data

```rust
use streamwerk::{EtlPipeline, FnTransform, FnLoad};
use streamwerk_sql::PostgresExtract;
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, AsyncConnection};

// Define your table schema
table! {
    users (id) {
        id -> Int4,
        name -> Text,
    }
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = users)]
struct User {
    id: i32,
    name: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Build query using Diesel's QueryDsl
    let query = users::table.select(User::as_select());

    // Create extractor with the query
    let extractor = PostgresExtract::<_, User>::new(query);

    // Build pipeline
    let pipeline = EtlPipeline::new(
        extractor,
        FnTransform(|user: User| Ok(streamwerk::once_ok(user))),
        FnLoad(|user: User| {
            println!("User: {} - {}", user.id, user.name);
            Ok(())
        })
    );

    // Establish connection and run pipeline
    // Connection will be released when stream completes
    let database_url = "postgres://localhost/mydb";
    let conn = AsyncPgConnection::establish(database_url).await?;
    pipeline.run(conn).await?;
    Ok(())
}
```

### Loading Data

```rust
use streamwerk::{EtlPipeline, FnExtract, FnTransform};
use streamwerk_sql::PostgresLoad;
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, AsyncConnection};

// Define your table schema
table! {
    users (id) {
        id -> Int4,
        name -> Text,
    }
}

#[derive(Insertable)]
#[diesel(table_name = users)]
struct NewUser {
    name: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Establish connection
    let database_url = "postgres://localhost/mydb";
    let conn = AsyncPgConnection::establish(database_url).await?;

    // Create loader with connection
    let loader = PostgresLoad::new(users::table, conn);

    // Build pipeline that generates and inserts users
    let pipeline = EtlPipeline::new(
        FnExtract(|_| Ok(streamwerk::iter_ok(vec!["Alice", "Bob", "Carol"]))),
        FnTransform(|name: &str| {
            let user = NewUser {
                name: name.to_string(),
            };
            Ok(streamwerk::once_ok(user))
        }),
        loader
    );

    // Run pipeline - connection will be released when done
    pipeline.run(()).await?;
    Ok(())
}
```

### Extract-Transform-Load Pipeline

Extract data from one table, transform it, and load into another:

```rust
use streamwerk::{EtlPipeline, FnTransform};
use streamwerk_sql::{PostgresExtract, PostgresLoad};
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, AsyncConnection};

// ... table and struct definitions ...

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url = "postgres://localhost/mydb";

    // Separate connections for extract and load
    let extract_conn = AsyncPgConnection::establish(database_url).await?;
    let load_conn = AsyncPgConnection::establish(database_url).await?;

    // Query source data
    let query = source_table::table
        .filter(source_table::active.eq(true))
        .select(SourceRecord::as_select());

    let extractor = PostgresExtract::new(query);
    let loader = PostgresLoad::new(dest_table::table, load_conn);

    let pipeline = EtlPipeline::new(
        extractor,
        FnTransform(|record: SourceRecord| {
            let dest_record = DestRecord {
                name: record.name,
                processed: true,
            };
            Ok(streamwerk::once_ok(dest_record))
        }),
        loader
    );

    pipeline.run(extract_conn).await?;
    Ok(())
}
```

## Testing

Integration tests require a PostgreSQL database. A Docker setup is provided for convenience.

### Start the test database

```bash
cd streamwerk-sql/docker
docker-compose up -d
```

This starts a PostgreSQL 15 database with:
- Database: `streamwerk_test`
- User: `streamwerk`
- Password: `streamwerk`
- Port: `5432`
- Sample data: 50 persons loaded from `persons.csv`

### Run integration tests

```bash
export DATABASE_URL="postgres://streamwerk:streamwerk@localhost/streamwerk_test"
cargo test -p streamwerk-sql -- --ignored
https://www.ulyssesguide.com/https://www.ulyssesguide.com/```

### Stop the database

```bash
cd streamwerk-sql/docker
docker-compose down
```

See `docker/README.md` for more details about the test database setup.

## Connection Lifecycle

The extractor takes ownership of the database connection when passed as input to the pipeline. The connection is automatically dropped and released when the stream completes (either successfully or due to an error). This ensures proper cleanup without requiring explicit connection management.

## License

MIT OR Apache-2.0
