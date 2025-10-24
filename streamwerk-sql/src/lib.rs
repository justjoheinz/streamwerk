#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]

//! # Streamwerk SQL
//!
//! This crate provides database extractors for streamwerk pipelines using Diesel.
//!
//! ## Extractors
//!
//! - [`PostgresExtract`] - Execute Diesel queries against PostgreSQL and stream individual rows
//!
//! ## Features
//!
//! - Type-safe query execution using Diesel's QueryDsl
//! - Async streaming of database rows using diesel-async
//! - Owned connection lifecycle - connection released when stream completes
//! - Compatible with all Diesel query builder types
//!
//! ## Example
//!
//! ```rust,no_run
//! use streamwerk::{EtlPipeline, FnTransform, FnLoad};
//! use streamwerk_sql::PostgresExtract;
//! use diesel::prelude::*;
//! use diesel_async::{AsyncPgConnection, AsyncConnection};
//! use anyhow::Result;
//!
//! // Define your table schema
//! table! {
//!     users (id) {
//!         id -> Int4,
//!         name -> Text,
//!     }
//! }
//!
//! #[derive(Queryable, Selectable)]
//! #[diesel(table_name = users)]
//! struct User {
//!     id: i32,
//!     name: String,
//! }
//!
//! # async fn example() -> Result<()> {
//! // Build query using Diesel's QueryDsl
//! let query = users::table.select(User::as_select());
//!
//! // Create extractor with the query
//! let extractor = PostgresExtract::<_, User>::new(query);
//!
//! // Build pipeline
//! let pipeline = EtlPipeline::new(
//!     extractor,
//!     FnTransform(|user: User| Ok(streamwerk::once_ok(user))),
//!     FnLoad(|user: User| {
//!         println!("User: {} - {}", user.id, user.name);
//!         Ok(())
//!     })
//! );
//!
//! // Establish connection and run pipeline
//! // Connection will be released when stream completes
//! let database_url = "postgres://localhost/mydb";
//! let conn = AsyncPgConnection::establish(database_url).await?;
//! pipeline.run(conn).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Connection Lifecycle
//!
//! The extractor takes ownership of the database connection. When the stream completes
//! (either successfully or due to error), the connection is automatically dropped and
//! released. This ensures proper cleanup without requiring explicit connection management.
//!
//! ## Testing
//!
//! Integration tests require a PostgreSQL database. Use the provided Docker setup:
//!
//! ```bash
//! # Start PostgreSQL with sample data
//! cd streamwerk-sql/docker
//! docker-compose up -d
//!
//! # Run integration tests
//! export DATABASE_URL="postgres://streamwerk:streamwerk@localhost/streamwerk_test"
//! cargo test -p streamwerk-sql -- --ignored
//!
//! # Stop database
//! docker-compose down
//! ```
//!
//! The test database includes a `persons` table with 50 sample records. See
//! `streamwerk-sql/docker/README.md` for details.

use anyhow::Result;
use diesel::query_builder::QueryId;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use streamwerk::Extract;
use tokio_stream::Stream;

pub mod prelude;

/// Extract step that executes a Diesel query against PostgreSQL and streams the results.
///
/// This extractor executes a query and streams rows individually. The database connection
/// is passed as input to the `extract()` method and is consumed during query execution.
/// The connection is automatically released when the stream completes.
///
/// # Type Parameters
///
/// * `Q` - The Diesel query type (must implement `RunQueryDsl<AsyncPgConnection>`)
/// * `R` - The result row type
///
/// # Lifecycle
///
/// - Query is stored in the extractor on construction
/// - Connection is passed as input to `extract()`
/// - Query execution begins when the stream is polled
/// - Rows are streamed individually as they're fetched from the database
/// - Connection is dropped (and released) when the stream completes or is dropped
pub struct PostgresExtract<Q, R> {
    query: Q,
    _phantom: std::marker::PhantomData<R>,
}

impl<Q, R> PostgresExtract<Q, R> {
    /// Create a new PostgreSQL extractor with a query.
    ///
    /// The connection will be passed when calling `extract()` in the pipeline.
    ///
    /// # Arguments
    ///
    /// * `query` - The Diesel query to execute
    pub fn new(query: Q) -> Self {
        Self {
            query,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<Q, R> Extract<AsyncPgConnection, R> for PostgresExtract<Q, R>
where
    Q: RunQueryDsl<AsyncPgConnection> + QueryId + Clone + Send + 'static,
    R: Send + 'static,
    for<'a> Q: diesel_async::methods::LoadQuery<'a, AsyncPgConnection, R>,
{
    type StreamType = impl Stream<Item = Result<R>> + Send;

    fn extract(&self, mut connection: AsyncPgConnection) -> Result<Self::StreamType> {
        let query = self.query.clone();

        let stream = async_stream::stream! {
            use futures::StreamExt;

            // Execute query and get stream of results
            match query.load_stream::<R>(&mut connection).await {
                Ok(result_stream) => {
                    // Pin the stream to poll it
                    tokio::pin!(result_stream);

                    // Stream each row
                    while let Some(row_result) = result_stream.next().await {
                        yield row_result.map_err(|e| anyhow::anyhow!("Database error: {}", e));
                    }
                }
                Err(e) => {
                    yield Err(anyhow::anyhow!("Failed to execute query: {}", e));
                }
            }

            // Connection is dropped here, releasing it
        };

        Ok(stream)
    }
}
