# streamwerk-sql

PostgreSQL extractors for streamwerk using Diesel and diesel-async.

## Features

- Type-safe query execution using Diesel's QueryDsl
- Async streaming of database rows using diesel-async
- Owned connection lifecycle - connection released when stream completes
- Compatible with all Diesel query builder types

## Example

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
