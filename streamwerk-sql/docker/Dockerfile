FROM postgres:15

# Copy initialization scripts
COPY init.sql /docker-entrypoint-initdb.d/
COPY persons.csv /docker-entrypoint-initdb.d/

# Set default database and user
ENV POSTGRES_DB=streamwerk_test
ENV POSTGRES_USER=streamwerk
ENV POSTGRES_PASSWORD=streamwerk

# Expose PostgreSQL port
EXPOSE 5432
