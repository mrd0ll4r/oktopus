# Docker Compose Setup

This contains all things necessary to run Oktopus via docker-compose.

### Useful Scripts

The `spawn_shell.sh` script will launch a shell in the `busybox` container inside the docker-compose network.

The `connect_to_db.sh` script will connect to the database via the port mapped to localhost of the host.

The `stats.sql` script can be used to get a quick overview of data present in the database.

The `storage_usage.sql` script can be used to get an overview of storage requirements of tables and indices in the database.