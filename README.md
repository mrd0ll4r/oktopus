# Oktopus IPFS Metadata Crawler/Indexer

A system to download data off IPFS in parallel, using multiple daemons, and save metadata about it to Postgres.

## Running

The easiest way to run this is using `docker compose`.
For that, you'll need the worker images in your local registry, which is achieved via `build-in-docker.sh`.
Check out [docker-compose/](docker-compose/README.md).

You can then use the `post-cids` tool to post CIDs from stdin to RabbitMQ, which will trigger the indexing of those CIDs.

### Manually

Read below for system architecture and what components are required.

The database stuff is managed using [diesel](https://diesel.rs).
Check out [migrations/](migrations) for schema migrations.
The database _should_ be set up automatically through running the indexer, and kept up to date the same way.

You'll need the freedesktop and libmagic MIME databases on the system running the file indexer.
On Ubuntu this is provided via the `shared-mime-info` and `libmagic1` packages.

You'll need a `.env` file containing global information, and potentially configuration for the workers, read below.

## Configuration

There is global configuration via a `.env` file, and worker configuration via command line arguments.

### Global Configuration

An example file can be found in [docker-compose/.env](docker-compose/.env).
The parameters are:

#### `DATABASE_URL`

Configures the database connection.
Format: `postgres://<user>[:<password>]@<host>[:<port>]/<database>`

#### `REDIS_URL`

Configures the redis connection.
Format: `redis://<host>[:<port>]/`

#### `RABBITMQ_URL`

Configures the RabbitMQ connection.
Format: `amqp://<host>[:<port]/[database, URL encoded]`

#### `PROMETHEUS_LISTEN_ADDRESS`

Configures the socket address to start a prometheus endpoint on.
You'll need a prometheus (or compatible) server to scrape this endpoint for metrics.

#### `INDEXER_*_WORKER_CONCURRENCY`

Configures the concurrency for each queue __per worker__.
For example, `INDEXER_BLOCK_WORKER_CONCURRENCY` configures *all* block workers to use this amount of concurrency, i.e., request this many tasks at once from RabbitMQ.
If multiple workers are launched, e.g. one per daemon, the total number of tasks processed concurrently from the `blocks` queue will increase, but each worker (and thus each daemon) will process the number specified here.

#### `INDEXER_*_DOWNLOAD_TIMEOUT_SECS`

Configures the timeout in seconds for IPFS API calls for each of the tasks.
This generally only applies to the first API call of a task, since subsequent calls (children of a directory, blocks of a file) are usually cached and fast.

The first API calls for the tasks are:
- for blocks: `block stat`, which downloads one block.
- for directories: `ls` (full), which downloads the directory block (which should be cached) and the following level of the DAG.
- for files: `cat`, which downloads the entire DAG of the file.
- for HAMTShards: `ls --fast`, which downloads the HAMT DAG, excluding the file leaves.

#### File Worker Downloaded Files Permissions

When running in Docker, mounting the directory of downloaded files on the host can lead to annoying permissions.
The variables `INDEXER_FILE_STORAGE_UID` and `INDEXER_FILE_STORAGE_GID` can be used to control the UID/GID of the files,
when running in Docker.
See the docker compose setup for an example of this.

### Worker Configuration

Each worker needs to know which IPFS daemon to use.
This is configured via `--daemon <API URL>`, e.g., `--daemon http://127.0.0.1:5001`.
The file worker needs a gateway URL as well.

The file worker additionally accepts a `--keep` flag, which instructs it to keep downloaded files.
See the docker compose setup for examples of where these are kept.

## Building

Use `build-in-docker.sh` to compile binaries and worker images within Docker.
This outputs binaries to `out/` and saves the worker images to your local registry.

### Manually

You'll need a Postgres library installed, i.e., `libpq-dev` on Debian-like systems.
You'll also need `protoc`, the Protobuf compiler, somewhere in your path.
Having that, `cargo build --release --locked` should compile all the things.
You should find the binaries in `target/release/`.

## Architecture Idea

- We run multiple IPFS daemons
- We run RabbitMQ with a bunch of (persistent) queues to distribute work
- We run workers, usually one per queue and daemon, to process that work
- Workers process multiple tasks concurrently on their daemon
- RabbitMQ keeps track of (un)finished tasks

### RabbitMQ Queues and Workers

Each queue corresponds to one type of task.
The plan is to launch one indexer per daemon and task.
This keeps the indexer binaries simple, and makes it easier to manage stuff via Docker.
For example, if a daemon dies, all associated indexers can gracefully die, everything will be restarted via Docker, no messages are lost due to RabbitMQ, and everything is fantastic.

Whenever a database operation or an interaction with RabbitMQ fails, the worker exits with a panic.
Database operations failing are ruled out at compile-time for the most up-to-date schema.
Failures thus indicate that the database is running a different schema than expected.
Failing to post tasks to or receive tasks from RabbitMQ should never happen either.
No messages are lost when a worker exits -- they will be auto-NACKed after a while and re-queued.

TODO: the message types (i.e., task definitions) are not up-to-date.

#### Queue `cids`:

- Contains CIDs we know nothing about
- Raw input from the outside world
- Does not need a daemon to process -> one global worker
- Infallible, unless the DB is dead
- Algorithm:
  1. Check codec. If non-filesystem -> skip to 5
  2. (optimization) Check Redis `cids` for CID -> skip to 5
  3. Insert/Upsert into DB
  4. Push task to `blocks`
  5. (optimization) Insert into Redis `cids`
  6. ACK to RabbitMQ

#### Queue `blocks`

- Contains CIDs for which the CID is already in the database, but block-level information is probably not
- Can be done on any daemon, should fetch at most one block
- Job: Download block, insert info into DB, decide how to further process
- Algorithm:
  1. (optimization) Check Redis `blocks` for CID -> skip to 9
  2. (optimization) Check Redis for `failed_blocks` counter -> if >= THRESHOLD skip to 9
  3. Check DB for failed downloads counter -> if >= THRESHOLD skip to 9
  4. (optimization) Check DB for block-level stats, especially UnixFS type -> skip to 8
  5. Download and stat block
  6. If failed: Record in DB, record in Redis, requeue to RabbitMQ, return
  7. Insert block-level info+DAG references into DB, in one transaction
  8. Push CID to `files`, `directories`, or `hamtshards`
  9. (optimization) Insert CID into Redis `blocks` and `cids`
  10. ACK to RabbitMQ
- (optimization) Do not push `directories` or `hamtshards` if the block has no references, also add these to Redis

#### Queue `files`:

- Contains CIDs we know are file(-roots, hopefully, but not necessarily)
- Block-level information is already in the DB
- Job: Compute and insert file-level heuristics of a file and block-level info of its DAG into the DB
- Inserts subsequent DAG blocks and their references into the DB (because they are cached on the same daemon)
- -> inserts the entire DAG of that file, plus heuristics for the topmost block, the root of the file DAG
- This also computes a SHA256 hash of the entire file, as well as multiple alternative CIDs
- Algorithm:
  1. (optimization) Check Redis `files` for CID -> skip to 10
  2. (optimization) Check Redis for `failed_files` counter -> if >= THRESHOLD skip to 10
  3. Check DB for failed downloads counter -> if >= THRESHOLD skip to 10
  4. (optimization) Check DB for file heuristics -> skip to 10
  5. Estimate file size using references -> if too large, skip to 11
  6. Download file and run heuristics, hash calculations, alternative CID calculations etc. on it
  7. Calculate block-level info for DAG blocks
  8. If failed: Record in DB, record in Redis, requeue to RabbitMQ, return
  9. In one transaction:
     1. Insert heuristics
     2. Insert alternative CIDs
     3. Insert SHA256 hash of the file
     4. Insert block-level info for DAG blocks
  10. (optimization) Insert CID into Redis `files`, `blocks`, and `cids`
  11. (optimization) Insert DAG CIDs into Redis `cids` and `blocks`
  12. ACK to RabbitMQ
- (optimization) Marginally faster on the daemon that indexed the block, but probably doesn't matter (because the DAG needs to be fetched)

#### Queue `directories`

- Contains (CID, `List<CID>` of DAG-references) tuples we know are UnixFS directories
- Block-level information is already in the DB
- Job: index directory, index immediate sub-blocks, add tasks for entries
- In general this is an optimization: We could always fast-ls and push into `blocks`
- Algorithm:
  1. (optimization) Check Redis `directories` for CID -> skip to 11
  2. (optimization) Check Redis `failed_directories` counter -> if >= THRESHOLD skip to 11
  3. Check DB for failed downloads counter -> if >= THRESHOLD skip to  11
  4. (optimization) Check DB for directory entries -> skip to 9
  5. Full LS (gets immediate sub-blocks) or fast ls if too many references
  6. For each: (only for full ls)
     1. Get block level info (this should be fast because the blocks are prefetched) 
     2. `object data` to get UnixFS type of entries
  7. If anything failed: Record in DB, record in Redis, requeue to RabbitMQ, return
  8. In one transaction:
     1. Insert block-level info for entries (only for full ls)
     2. Insert directory entries
  9. Push entries to `files`, `directories`, and `hamtshards` (only for full ls)
  10. (optimization) Insert entry CIDs into Redis `cids` and `blocks`
  11. (optimization) Insert directory CID into Redis `directories` ,`blocks`, and `cids`
  12. ACK to RabbitMQ
- (optimization) Marginally faster on the daemon that indexed the block, but probably doesn't matter

#### Queue `hamtshards`

- Contains CIDs we know are `HAMTShard`s
- Block-level information is already in the DB
- Job: index directory, add tasks for entries
- Fast LS -> no information about whether the target is a directory or a file
- Algorithm:
  1. (optimization) Check Redis `hamtshards` for CID -> skip to 10
  2. (optimization) Check Redis `failed_hamtshards` counter -> if >= THRESHOLD skip to 10
  3. Check DB for failed downloads counter -> if >= THRESHOLD skip to 10
  4. (optimization) Check DB for directory entries -> skip to 9
  5. Fast LS and get block stats of all inner DAG nodes (because they should be cached at this point)
  6. If anything failed: Record in DB, record in Redis, requeue to RabbitMQ, return
  7. In one transaction:
     1. Insert CIDs for entries
     2. Insert directory entries (and block stat about DAG links?)
  8. (optimization, maybe) Insert CIDs of DAG-references that are `HAMTShard`s into Redis `hamtshards`?
  9. Push entries to `blocks`
  10. (optimization) Insert CID into Redis `hamtshards`, `blocks`, and `cids`
  11. ACK to RabbitMQ
- (optimization) Marginally faster on the daemon that indexed the block, but probably doesn't matter

### Components

#### Postgres

A simple postgres database.
Schema migrations are handled using diesel, which should happen automatically the first time you launch any of the workers.

The workers add information to the database in layers.
This is done in transactions, so we can be certain that a layer, even though it might be composed of multiple records in multiple tables, is inserted atomically.

The layers are:
- CID-level: `blocks` and `cids`. No downloading has happened. Contains entries for `raw` and `dag-pb` CIDs only.
- Block-level: `block_stats` and `block_links`. The block has been downloaded, we know its UnixFS type and one level of DAG links.
  This generates entries in `successful_downloads` or `failed_downloads` with download type `"block"`.
- File-level: `block_file_mime_types`, `block_file_hashes`, and `block_file_alternative_cids`. The DAG of the file rooted in the given block has been downloaded.
  We only perform this task on the root of the file DAG.
  We determine blocks to be file roots if they have UnixFS type `File` or `Raw` or are `raw` CIDs and
    1. appeared as a directory entry or
    2. were entered from the outside as a CID.
  
  This generates entries in `successful_downloads` or `failed_downloads` with download type `"dag"`.
  This also inserts block-level information about all blocks in the file DAG.
- Directory-level: `directory_entries`. The directory rooted in the given block has been downloaded.
  For HAMTShards, this downloads the entire DAG of the directory.
  This generates entries in `successful_downloads` or `failed_downloads` with download type `"dag"`.

For any block, at most one of (file,directory)-level information is present.
Blocks which appear as sub-blocks in file DAGs are not indexed for file-level metadata, because they usually point to somewhere in the middle of a file.
This is overridden if any of the above two triggers for determining file roots are met.

#### RabbitMQ

We use this to store and distribute tasks.

#### Redis

Used to cache processed or failed CIDs, for each of the task types.
This should, hopefully, alleviate some pressure off the database.

#### IPFS Daemons

Kubo daemons to download stuff.
They are configured to not provide local content, which hopefully makes things faster.
They are launched with `--enable-gc` to keep space requirements low.

TODO if the repo GC is slow we can also just call `repo gc` every day via cron.

#### Workers

There are four types of workers, one per task type:

- The CID worker takes CIDs from the outside world, checks their codec, inserts them, and pushes block tasks.
- The block worker takes block tasks and attempts to `block stat` them.
  This inserts block size, UnixFS type, and DAG links.
  This also creates a file, directory, or hamtshard task, depending on the UnixFS type of the block.
- The file worker takes file tasks and attempts to `cat` them.
  This calculates the SHA256 hash of the downloaded data, determines its MIME type, and computes alternative CIDs for the data.
  This also `block stat`s all blocks in the DAG and inserts block-level information for them.
- The directory worker takes directory tasks and attempts to `ls` them.
  This also `block stat`s all immediate child blocks, which should be cached on the daemon after `ls` finishes.
- The HAMTShard worker takes hamtshard tasks and attempts to `ls --fast` them.
  This _does not_ download the blocks of the entries.
  We do this to get proper entry names, which are tedious to get from just the DAG links.

Additionally, there is a `post-cids` binary which takes CIDs via stdin and posts them to RabbitMQ.
RabbitMQ is the interface between the system running in docker-compose and the host.
This tool feeds data into the system via that interface.

The `convert-cids-to-base16` tool converts a list of CIDs to the format usually exported from the database.
This takes line-separated CIDs from stdin and converts them to version 1, base16 formatted variants.
It also filters out CIDs not examined by the indexer, i.e., limits the output to filesystem-related CIDs.

All tools are written in Rust.
They log to stderr on info level by default, which can be overridden via `RUST_LOG=<level>`

## Limitations

- We only deal with `dag-pb` and `raw` codecs, as these encode filesystem things.
- Of those, we only handle UnixFS `File`, `Directory`, and `HAMTShard` blocks. We ignore `Metadata`, `Symlink`, etc.

## License

MIT