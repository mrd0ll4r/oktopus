# IPFS Filesystem Data Analysis

## Import

https://stackoverflow.com/a/16311908

Basic idea:
- Create empty schema in `public` by letting diesel run on the DB once.
- Rename schema `public` to `analysis` or something like that
- For each db dump:
  - Import the dump via `psql`. This will put it in schema `public`.
  - Run diesel on that, which will apply all migrations to get the most recent schema.
  - Rename `public` to something like `backup_<date>`.
- Ferry over the data from each `backup_*` schema to `analysis` sequentially, drop each after it's done (should make this atomic, if possible)

We could also `diesel migration up` or something like that... hmm

We could also point diesel to the different schema: https://yakshav.es/using-diesel-with-a-postgres-schema/

Alternatively, we could import everything as is in different _databases_ and then use FDW to access them: https://www.postgresql.org/docs/current/postgres-fdw.html
This makes importing the dumps easier, but I think working with FDW is much more annoying that working with different schemas in one database.

We should put diesel and whatnot into a container, like so: https://github.com/WillSquire/docker-diesel-cli/blob/master/Dockerfile
Then we can mount the migrations in there and run it from within a script.