use crate::db::AsyncDBError;
use crate::redis::RedisConnection;
use anyhow::{anyhow, Context};
use cid::Cid;
use diesel::{Connection, PgConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use ipfs_api_backend_hyper::{IpfsApi, IpfsClient};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::env;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[macro_use]
extern crate diesel;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate diesel_migrations;

pub mod cache;
pub mod db;
pub mod hash;
pub mod ipfs;
pub mod logging;
pub mod models;
pub mod prom;
pub mod queue;
pub mod redis;
pub mod schema;

pub type IpfsApiClient = IpfsClient<hyper::client::connect::HttpConnector>;

// Include the `unixfs` module, which is generated from unixfs.proto.
pub mod unixfs {
    include!(concat!(env!("OUT_DIR"), "/unixfs.pb.rs"));
}

pub const CODEC_DAG_PB: u64 = 0x70;
pub const CODEC_RAW: u64 = 0x55;

/*
use deadpool_diesel::postgres::{BuildError, Manager, Pool};
use deadpool_diesel::Runtime;

#[derive(Error, Debug)]
pub enum PoolError {
    #[error("unable to load .env file")]
    Env(dotenvy::Error),
    #[error("missing DATABASE_URL")]
    DatabaseURL,
    #[error("unable to build pool")]
    PoolBuilder(BuildError),
}

pub fn set_up_pool() -> Result<Pool, PoolError> {
    dotenvy::dotenv().map_err(PoolError::Env)?;
    let database_url = env::var("DATABASE_URL").map_err(|_| PoolError::DatabaseURL)?;

    let manager = Manager::new(database_url, Runtime::Tokio1);

    let pool = Pool::builder(manager)
        .max_size(8)
        .build()
        .map_err(|e| PoolError::PoolBuilder(e))?;

    Ok(pool)
}

#[derive(Error, Debug)]
pub enum PoolError {
    #[error("unable to load .env file")]
    Env(dotenvy::Error),
    #[error("missing DATABASE_URL")]
    DatabaseURL,
    #[error("unable to build pool")]
    PoolBuilder(BuildError),
    #[error("other")]
    Other(Box<dyn std::error::Error + Send + Sync + 'static>)
}

pub fn set_up_pool() -> Result<Pool, PoolError> {
    dotenvy::dotenv().map_err(PoolError::Env)?;
    let database_url = env::var("DATABASE_URL").map_err(|_| PoolError::DatabaseURL)?;

    let manager = Manager::new(database_url, Runtime::Tokio1);

    let pool = Pool::builder(manager)
        .max_size(8)
        .build()
        .map_err(|e| PoolError::Other(Box::new(e)))?;

    Ok(pool)
}
*/

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("unable to load .env file")]
    Env(dotenvy::Error),
    #[error("missing DATABASE_URL")]
    DatabaseURL,
    #[error("unable to connect")]
    Connection(diesel::ConnectionError),
}

pub fn establish_connection() -> Result<PgConnection, ConnectionError> {
    dotenvy::dotenv().map_err(ConnectionError::Env)?;
    let database_url = env::var("DATABASE_URL").map_err(|_| ConnectionError::DatabaseURL)?;
    let conn = PgConnection::establish(&database_url).map_err(ConnectionError::Connection)?;

    Ok(conn)
}

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!();

pub fn run_pending_migrations(
    conn: &mut PgConnection,
) -> diesel::migration::Result<Vec<diesel::migration::MigrationVersion>> {
    conn.run_pending_migrations(MIGRATIONS)
}

pub enum CacheCheckResult {
    RedisMarkedDone,
    RedisFailuresAboveThreshold,
    DbFailuresAboveThreshold,
    RedeliveredWithoutDbFailures,
    NeedsProcessing,
}

#[derive(Debug, Clone)]
pub enum Task {
    Block,
    File,
    Directory,
    HamtShard,
}

pub async fn check_task_caches(
    task_type: Task,
    cid: &str,
    block_id: i64,
    cache: redis::Cache,
    redis_conn: &mut RedisConnection,
    db_conn: Arc<Mutex<PgConnection>>,
    failure_threshold: u64,
    redelivered: bool,
) -> Result<CacheCheckResult, AsyncDBError> {
    // Check redis
    debug!("{}: checking redis", cid);
    match cache.is_done(cid, redis_conn).await {
        Ok(done) => {
            debug!("{}: redis status is done={}", cid, done);
            if done {
                // Refresh redis
                redis::mark_done_up_to_logging(cid, redis_conn, cache).await;
                return Ok(CacheCheckResult::RedisMarkedDone);
            }
        }
        Err(err) => {
            warn!("unable to check redis CIDs cache: {:?}", err)
        }
    };

    // Check redis for failures
    debug!("{}: checking redis for failures", cid);
    match cache.num_failed(cid, redis_conn).await {
        Ok(res) => {
            debug!("{}: redis failures is {:?}", cid, res);
            if let Some(failures) = res {
                if failures >= failure_threshold {
                    debug!("{}: too many failures, skipping", cid);
                    // Refresh redis
                    redis::mark_done_up_to_logging(&cid, redis_conn, cache).await;
                    return Ok(CacheCheckResult::RedisFailuresAboveThreshold);
                }
            }
        }
        Err(err) => {
            warn!("unable to check redis CIDs cache: {:?}", err)
        }
    }

    // Check database for failures
    debug!("{}: checking database for failures", cid);
    let db_res = match task_type {
        Task::Block => db::async_get_failed_block_downloads(db_conn, block_id).await,
        Task::File | Task::Directory | Task::HamtShard => {
            db::async_get_failed_dag_downloads(db_conn, block_id).await
        }
    };
    debug!("{}: database failures is {:?}", cid, db_res);

    let db_res = db_res?;

    if redelivered && db_res.is_none() {
        // This happens if a worker is stuck and killed by RabbitMQ.
        // All unacknowledged tasks will be redelivered.
        // Unfortunately, we don't know which task exactly caused the worker to get stuck,
        // so we skip all tasks that are redelivered without a failure in the database.
        return Ok(CacheCheckResult::RedeliveredWithoutDbFailures);
    }

    if let Some(failures) = db_res {
        if failures.len() as u64 >= failure_threshold {
            debug!("{}: too many failures, skipping", cid);
            // Mark done in redis
            redis::mark_done_up_to_logging(&cid, redis_conn, cache).await;
            return Ok(CacheCheckResult::DbFailuresAboveThreshold);
        }
    }

    Ok(CacheCheckResult::NeedsProcessing)
}

pub struct WorkerConnections {
    pub db_conn: Arc<Mutex<PgConnection>>,
    pub redis_conn: RedisConnection,
    pub rabbitmq_conn: lapin::Connection,
}

pub async fn worker_setup() -> anyhow::Result<WorkerConnections> {
    debug!("connecting to database...");
    let mut conn = establish_connection().context("unable to connect to DB")?;
    info!("connected to database");

    debug!("running pending migrations...");
    let migrations = run_pending_migrations(&mut conn)
        .map_err(|e| anyhow!("{}", e))
        .context("unable to run migrations")?;
    info!("ran migrations {:?}", migrations);

    let db_conn = Arc::new(Mutex::new(conn));

    debug!("connecting to redis...");
    let redis_conn = redis::connect_env()
        .await
        .context("unable to connect to redis")?;
    info!("connected to redis");

    debug!("connecting to RabbitMQ...");
    let rabbitmq_conn = queue::connect_env()
        .await
        .context("unable to connect to RabbitMQ")?;
    info!("connected to RabbitMQ");

    debug!("setting up RabbitMQ queues...");
    {
        let chan = rabbitmq_conn
            .create_channel()
            .await
            .context("unable to create RabbitMQ channel")?;
        queue::Queues::set_up_queues(&chan)
            .await
            .context("unable to create RabbitMQ queues")?;
    }
    info!("set up RabbitMQ queues");

    // Update the panic handler to always exit the process.
    // This is necessary, because tokio modifies it in a way that catches async tasks panicking.
    // We could retrieve those by joining the task, which is annoying, since we'd exit as a
    // reaction anyway.
    // So instead, we just patch the panic handler to exit, as usual.
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    debug!("starting prometheus endpoint...");
    let prom_addr =
        prometheus_address_from_env().context("unable to get prometheus listen address")?;
    prom::run_prometheus(prom_addr.clone()).context("unable to run prometheus")?;
    info!("started prometheus endpoint on {}", prom_addr);

    Ok(WorkerConnections {
        db_conn,
        redis_conn,
        rabbitmq_conn,
    })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CIDParts {
    pub cid: Cid,
    pub codec: u64,
    pub multihash: Vec<u8>,
}

impl CIDParts {
    pub fn from_parts(hash: &[u8], codec: u64) -> anyhow::Result<CIDParts> {
        Ok(CIDParts {
            cid: Cid::new(
                cid::Version::V1,
                codec,
                multihash::MultihashGeneric::from_bytes(hash)?,
            )?,
            codec,
            multihash: Vec::from(hash),
        })
    }
}

pub fn parse_cid_to_parts(c: &str) -> Result<CIDParts, cid::Error> {
    let parsed = Cid::try_from(c)?;
    let codec = parsed.codec();
    let multihash = parsed.hash().to_bytes();
    Ok(CIDParts {
        cid: parsed,
        codec,
        multihash,
    })
}

pub fn prometheus_address_from_env() -> anyhow::Result<SocketAddr> {
    dotenvy::dotenv().context("unable to read .env file")?;

    env::var("PROMETHEUS_LISTEN_ADDRESS")
        .context(format!("missing PROMETHEUS_LISTEN_ADDRESS"))?
        .parse::<SocketAddr>()
        .map_err(|err| anyhow!("unable to parse PROMETHEUS_LISTEN_ADDRESS: {:?}", err))
}

fn get_u64_from_env(key: &str) -> anyhow::Result<u64> {
    dotenvy::dotenv().context("unable to read .env file")?;

    let res = env::var(key)
        .context(format!("missing {}", key))?
        .parse::<u64>()
        .context(format!("unable to parse {}", key))?;

    Ok(res)
}

pub fn directory_full_ls_size_limit_from_env() -> anyhow::Result<u64> {
    get_u64_from_env("INDEXER_DIRECTORY_FULL_LS_SIZE_LIMIT")
}

pub fn file_size_limit_from_env() -> anyhow::Result<u64> {
    get_u64_from_env("INDEXER_FILE_SIZE_LIMIT")
}

pub fn failed_block_downloads_threshold_from_env() -> anyhow::Result<u64> {
    get_u64_from_env("INDEXER_FAILED_BLOCK_DOWNLOAD_THRESHOLD")
}

pub fn failed_file_downloads_threshold_from_env() -> anyhow::Result<u64> {
    get_u64_from_env("INDEXER_FAILED_FILE_DOWNLOAD_THRESHOLD")
}

pub fn failed_directory_downloads_threshold_from_env() -> anyhow::Result<u64> {
    get_u64_from_env("INDEXER_FAILED_DIRECTORY_DOWNLOAD_THRESHOLD")
}

pub fn failed_hamtshard_downloads_threshold_from_env() -> anyhow::Result<u64> {
    get_u64_from_env("INDEXER_FAILED_HAMTSHARD_DOWNLOAD_THRESHOLD")
}

pub fn block_worker_ipfs_timeout_secs_from_env() -> anyhow::Result<u64> {
    get_u64_from_env("INDEXER_BLOCK_DOWNLOAD_TIMEOUT_SECS")
}

pub fn file_worker_ipfs_timeout_secs_from_env() -> anyhow::Result<u64> {
    get_u64_from_env("INDEXER_FILE_DOWNLOAD_TIMEOUT_SECS")
}

pub fn directory_worker_ipfs_timeout_secs_from_env() -> anyhow::Result<u64> {
    get_u64_from_env("INDEXER_DIRECTORY_DOWNLOAD_TIMEOUT_SECS")
}

pub fn hamtshard_worker_ipfs_timeout_secs_from_env() -> anyhow::Result<u64> {
    get_u64_from_env("INDEXER_HAMTSHARD_DOWNLOAD_TIMEOUT_SECS")
}
