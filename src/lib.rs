use anyhow::Context;
use cid::Cid;
use diesel::{Connection, PgConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use ipfs_api_backend_hyper::{IpfsApi, IpfsClient};
use serde::{Deserialize, Serialize};
use std::env;
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
pub mod queue;
pub mod redis;
pub mod schema;

pub type IpfsApiClient = IpfsClient<hyper::client::connect::HttpConnector>;

// Include the `items` module, which is generated from items.proto.
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

pub fn get_mime_type(bytes: &[u8]) -> &'static str {
    tree_magic_mini::from_u8(bytes)
}

fn get_u64_from_env(key: &str) -> anyhow::Result<u64> {
    dotenvy::dotenv().context("unable to read .env file")?;

    let res = env::var(key)
        .context(format!("missing {}", key))?
        .parse::<u64>()
        .context(format!("unable to parse {}", key))?;

    Ok(res)
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
