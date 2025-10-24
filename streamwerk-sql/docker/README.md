# PostgreSQL Test Database

This directory contains Docker configuration for running a PostgreSQL database with sample data for testing streamwerk-sql.

## Quick Start

```bash
cd streamwerk-sql/docker
docker-compose up -d
```

The database will be available at:
- Host: `localhost`
- Port: `5432`
- Database: `streamwerk_test`
- User: `streamwerk`
- Password: `streamwerk`

Connection string:
```
postgres://streamwerk:streamwerk@localhost/streamwerk_test
```

## Running Tests

Set the `DATABASE_URL` environment variable and run tests:

```bash
export DATABASE_URL="postgres://streamwerk:streamwerk@localhost/streamwerk_test"
cargo test -p streamwerk-sql -- --ignored
```

## Sample Data

The database contains a `persons` table with 50 rows loaded from `persons.csv`:

| Column | Type    | Description |
|--------|---------|-------------|
| id     | SERIAL  | Primary key |
| name   | TEXT    | Person name |
| age    | INTEGER | Person age  |

## Stopping the Database

```bash
docker-compose down
```

To remove volumes as well:
```bash
docker-compose down -v
```
