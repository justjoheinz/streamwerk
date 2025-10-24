-- Create persons table
CREATE TABLE persons (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    age INTEGER NOT NULL
);

-- Load data from persons.csv
COPY persons(name, age) FROM '/docker-entrypoint-initdb.d/persons.csv' DELIMITER ',' CSV HEADER;

-- Create index on age for query performance
CREATE INDEX idx_persons_age ON persons(age);
