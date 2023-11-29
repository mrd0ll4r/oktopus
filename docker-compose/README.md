# Docker Compose Setup

This contains all things necessary to run Oktopus via docker-compose.

## Volumes

Most of the containers use volumes.
Most of these are mapped to named volumes by default, which is fine in most cases.

The file worker furthermore uses a volume for temporary storage of files while they are being downloaded, and for completed downloads.
These are mapped to corresponding directories in the working directory by default.
This is annoying w.r.t. file permissions, but they are world-readable, at least...

### Useful Scripts

The `spawn_shell.sh` script will launch a shell in the `busybox` container inside the docker-compose network.

The `connect_to_db.sh` script will connect to the database by spawning `psql` in the database container.
This is nice because the host does not need Postgres installed.

The `run-csv-query.sh` script executes a given query file against the database and saves the result as gzipped CSV.

The `stats.sql` script can be used to get a quick overview of data present in the database.

The `storage_usage.sql` script can be used to get an overview of storage requirements of tables and indices in the database.