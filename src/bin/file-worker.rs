use crate::TaskOutcome::{Done, Failed, Skipped};
use anyhow::Context;
use clap::{arg, Command};
use diesel::PgConnection;
use futures_util::StreamExt;
use ipfs_api_backend_hyper::{IpfsApi, TryFromUri};
use ipfs_indexer::cache::MimeTypeCache;
use ipfs_indexer::hash::NormalizedAlternativeCid;
use ipfs_indexer::ipfs::{BlockLevelMetadata, ParseReferencedCidFailed};
use ipfs_indexer::prom::OutcomeLabel;
use ipfs_indexer::queue::FileMessage;
use ipfs_indexer::redis::RedisConnection;
use ipfs_indexer::{
    db, hash, ipfs, logging, prom, queue, redis, CIDParts, CacheCheckResult, IpfsApiClient,
    WorkerConnections,
};
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions};
use log::{debug, info, warn};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::set_up_logging().unwrap();

    let matches = Command::new("IPFS Indexer Files Worker")
        .version(clap::crate_version!())
        .author("Leo Balduf <leobalduf@gmail.com>")
        .about("IPFS indexer worker to process UnixFS files and raw blocks.
Takes file-like blocks from an AMQP queue, downloads them, computes heuristics and metadata, and inserts this into a database.
Static configuration is taken from a .env file, see the README for more information.")
        .arg(arg!(--daemon <ADDRESS> "specifies the API URL of the IPFS daemon to use")
            .default_value("http://127.0.0.1:5001"))
        .get_matches();

    let failure_threshold = ipfs_indexer::failed_file_downloads_threshold_from_env()
        .context("unable to determine failed DAG downloads threshold")?;
    let download_timeout = ipfs_indexer::file_worker_ipfs_timeout_secs_from_env()
        .context("unable to determine download timeout")?;
    let file_size_limit =
        ipfs_indexer::file_size_limit_from_env().context("unable to determine file size limit")?;

    let daemon_uri = matches
        .get_one::<String>("daemon")
        .expect("missing required daemon arg");

    debug!("connecting to IPFS daemon...");
    let client: IpfsApiClient = ipfs_api_backend_hyper::IpfsClient::from_str(daemon_uri)
        .context("unable to create client")?;
    let client_with_timeout = ipfs_api_backend_hyper::BackendWithGlobalOptions::new(
        client,
        ipfs_api_backend_hyper::GlobalOptions {
            offline: None,
            timeout: Some(Duration::from_secs(download_timeout)),
        },
    );
    let client = Arc::new(client_with_timeout);
    let daemon_id = client.id(None).await.context("unable to query IPFS API")?;
    info!(
        "connected to IPFS daemon version {} at {} with ID {:?}",
        daemon_id.agent_version, daemon_uri, daemon_id.id
    );
    let daemon_uri = Arc::new(daemon_uri.clone());

    debug!("setting up worker connections");
    let WorkerConnections {
        db_conn,
        redis_conn,
        rabbitmq_conn,
    } = ipfs_indexer::worker_setup()
        .await
        .context("unable to set up connections")?;

    // Load MIME type cache
    let mime_cache = {
        let mut db_conn = db_conn.lock().unwrap();
        Arc::new(Mutex::new(
            MimeTypeCache::new(&mut db_conn).context("unable to set up MIME type cache")?,
        ))
    };

    debug!("creating channels and setting prefetch...");
    let files_chan = rabbitmq_conn
        .create_channel()
        .await
        .context("unable to create RabbitMQ channel")?;
    queue::set_prefetch(
        &files_chan,
        queue::Queues::Files
            .qos_from_env()
            .context("unable to load number of workers")?,
    )
    .await
    .context("unable to set queue prefetch")?;
    debug!("subscribing...");
    let mut files_consumer = queue::Queues::Files
        .subscribe(&files_chan, &format!("files_worker_{}", daemon_id.id))
        .await
        .context("unable to subscribe to directories queue")?;
    info!("set up RabbitMQ channels and consumers");

    info!("listening for tasks");
    while let Some(delivery) = files_consumer.next().await {
        let delivery = delivery.expect("error in consumer");
        debug!("got delivery {:?}", delivery);

        let redis_conn = redis_conn.clone();
        let daemon_uri = daemon_uri.clone();
        let db_conn = db_conn.clone();
        let mime_cache = mime_cache.clone();
        let ipfs_client = client.clone();

        tokio::spawn(async move {
            handle_delivery(
                delivery,
                daemon_uri,
                redis_conn,
                db_conn,
                mime_cache,
                ipfs_client,
                failure_threshold,
                file_size_limit,
            )
            .await
        });
    }

    Ok(())
}

async fn handle_delivery<T>(
    delivery: Delivery,
    daemon_uri: Arc<String>,
    mut redis_conn: RedisConnection,
    db_conn: Arc<Mutex<PgConnection>>,
    mime_cache: Arc<Mutex<MimeTypeCache>>,
    ipfs_client: Arc<T>,
    failure_threshold: u64,
    file_size_limit: u64,
) where
    T: IpfsApi + Sync,
{
    let Delivery {
        data,
        acker,
        redelivered,
        ..
    } = delivery;
    let msg = match queue::decode_file(&data) {
        Ok(cid) => cid,
        Err(err) => {
            warn!("unable to parse FileMessage, skipping: {:?}", err);
            acker
                .nack(BasicNackOptions::default())
                .await
                .expect("unable to NACK delivery");
            return;
        }
    };

    let before = Instant::now();
    let outcome = handle_file(
        msg,
        redelivered,
        &mut redis_conn,
        db_conn,
        mime_cache,
        ipfs_client,
        failure_threshold,
        file_size_limit,
    )
    .await;

    // Record in prometheus
    prom::record_task_duration(
        &*prom::FILE_TASK_STATUS,
        outcome,
        before.elapsed(),
        &daemon_uri,
    );

    // Report to RabbitMQ
    match outcome {
        Skipped(_) | Done => {
            acker
                .ack(BasicAckOptions::default())
                .await
                .expect("unable to ACK delivery");
        }
        Failed(_) => {
            acker
                .nack(BasicNackOptions {
                    requeue: true,
                    ..Default::default()
                })
                .await
                .expect("unable to NACK delivery");
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum SkipReason {
    RedisCached,
    RedisFailureCached,
    DbFailureThreshold,
    UnableToParseReferencedCids,
    FileSizeLimitExceeded,
    RedeliveredWithoutDbFailure,
    DbDone,
}

#[derive(Clone, Copy, Debug)]
enum FailureReason {
    DownloadFailed,
    FailedToComputeAlternativeCids,
    FailedToDetectLibmimeMimeType,
}

#[derive(Clone, Copy, Debug)]
enum TaskOutcome {
    Skipped(SkipReason),
    Done,
    Failed(FailureReason),
}

impl OutcomeLabel for TaskOutcome {
    fn status(&self) -> &'static str {
        match self {
            Skipped(_) => "skipped",
            Done => "done",
            Failed(_) => "failed",
        }
    }

    fn reason(&self) -> &'static str {
        match self {
            Skipped(reason) => match reason {
                SkipReason::RedisCached => "redis_cached",
                SkipReason::RedisFailureCached => "redis_failure_cached",
                SkipReason::DbFailureThreshold => "db_failure_threshold",
                SkipReason::UnableToParseReferencedCids => "unable_to_parse_referenced_cids",
                SkipReason::FileSizeLimitExceeded => "size_limit_exceeded",
                SkipReason::RedeliveredWithoutDbFailure => "redelivered_without_db_failure",
                SkipReason::DbDone => "db_done",
            },
            Done => "done",
            Failed(reason) => match reason {
                FailureReason::DownloadFailed => "download_failed",
                FailureReason::FailedToComputeAlternativeCids => {
                    "failed_to_compute_alternative_cids"
                }
                FailureReason::FailedToDetectLibmimeMimeType => {
                    "failed_to_detect_libmime_mime_type"
                }
            },
        }
    }
}

async fn handle_file<T>(
    msg: FileMessage,
    redelivered: bool,
    redis_conn: &mut RedisConnection,
    db_conn: Arc<Mutex<PgConnection>>,
    mime_cache: Arc<Mutex<MimeTypeCache>>,
    ipfs_client: Arc<T>,
    failure_threshold: u64,
    file_size_limit: u64,
) -> TaskOutcome
where
    T: IpfsApi + Sync,
{
    let FileMessage {
        cid: cid_parts,
        db_block,
        db_links,
    } = msg;
    let cid = format!("{}", cid_parts.cid);

    // Check cumulative file size
    let approx_size = db_links.iter().map(|l| l.size).sum::<i64>() as u64;
    // We give it some wiggle room because we can't trust the link sizes exactly.
    if approx_size > (file_size_limit * 1.2_f64 as u64) {
        return Skipped(SkipReason::FileSizeLimitExceeded);
    }

    // Check caches
    debug!("{}: checking caches", cid);
    match ipfs_indexer::check_task_caches(
        ipfs_indexer::Task::File,
        &cid,
        db_block.id,
        redis::Cache::Files,
        redis_conn,
        db_conn.clone(),
        failure_threshold,
        redelivered,
    )
    .await
    .expect("unable to check block failures in database")
    {
        CacheCheckResult::RedisMarkedDone => return Skipped(SkipReason::RedisCached),
        CacheCheckResult::RedisFailuresAboveThreshold => {
            return Skipped(SkipReason::RedisFailureCached)
        }
        CacheCheckResult::DbFailuresAboveThreshold => {
            return Skipped(SkipReason::DbFailureThreshold)
        }
        CacheCheckResult::RedeliveredWithoutDbFailures => {
            // TODO record failure in DB
            return Skipped(SkipReason::RedeliveredWithoutDbFailure);
        }
        CacheCheckResult::NeedsProcessing => {
            // We have to process it (again)
        }
    }

    // Check database for file-level metadata
    debug!("{}: checking database for file heuristics", cid);
    let res = db::async_get_file_heuristics(db_conn.clone(), db_block.id)
        .await
        .expect("unable to check file heuristics in database");
    if res.is_some() {
        debug!("{}: database has file heuristics, skipping", cid);

        redis_mark_done(&cid, redis_conn).await;

        return Skipped(SkipReason::DbDone);
    }

    debug!(
        "{}: no file heuristics in database, attempting to download",
        cid
    );

    let (
        freedesktop_mime_type,
        libmime_mime_type,
        file_size,
        sha256_hash,
        alternative_cids_normalized,
        layers,
        dag_block_cids,
    ) = match download_file(ipfs_client, cid_parts, &cid).await {
        Ok(res) => match res {
            Ok(metadata) => {
                let FileMetadata {
                    freedesktop_mime_type: mime_type,
                    libmime_mime_type,
                    file_size,
                    sha256_hash,
                    alternative_cids_normalized,
                    layers,
                    dag_block_cids,
                } = metadata;
                (
                    mime_type,
                    libmime_mime_type,
                    file_size,
                    sha256_hash,
                    alternative_cids_normalized,
                    layers,
                    dag_block_cids,
                )
            }
            Err(skip_reason) => return Skipped(skip_reason),
        },
        Err(err) => {
            db::async_insert_dag_download_failure(db_conn.clone(), db_block.id, chrono::Utc::now())
                .await
                .expect("unable to insert download failure into database");
            debug!("{}: marked failed in database", cid);

            redis_mark_failed(&cid, redis_conn).await;

            return Failed(err);
        }
    };

    // Insert into database
    debug!("{}: inserting successful file into database", cid);
    db::async_upsert_successful_file(
        db_conn,
        mime_cache,
        db_block.id,
        freedesktop_mime_type,
        libmime_mime_type,
        file_size,
        sha256_hash,
        alternative_cids_normalized,
        layers,
        chrono::Utc::now(),
    )
    .await
    .expect("unable to upsert file metadata into database");

    debug!("{}: upserted successfully", cid);

    // Mark DAG blocks done in redis
    debug!(
        "{}: marking {} dag blocks done in redis",
        cid,
        dag_block_cids.len()
    );
    for c in dag_block_cids.into_iter() {
        redis_mark_child_done(&c, redis_conn).await
    }

    // Update redis
    debug!("{}: marking done in redis...", cid);
    redis_mark_done(&cid, redis_conn).await;

    Done
}

struct FileMetadata {
    freedesktop_mime_type: &'static str,
    libmime_mime_type: String,
    file_size: i64,
    sha256_hash: Vec<u8>,
    alternative_cids_normalized: Vec<NormalizedAlternativeCid>,
    layers: Vec<Vec<(CIDParts, BlockLevelMetadata)>>,
    dag_block_cids: Vec<String>,
}

async fn download_file<T>(
    ipfs_client: Arc<T>,
    cid_parts: CIDParts,
    cid: &String,
) -> Result<Result<FileMetadata, SkipReason>, FailureReason>
where
    T: IpfsApi + Sync,
{
    let file_data = match ipfs::query_ipfs_for_file_data(&cid, ipfs_client.clone()).await {
        Ok(data) => data,
        Err(err) => {
            debug!("{}: failed to download file: {:?}", cid, err);
            return Err(FailureReason::DownloadFailed);
        }
    };
    debug!(
        "{}: got {} bytes of file data, first bytes: {:?}",
        cid,
        file_data.len(),
        file_data.iter().take(10).collect::<Vec<_>>()
    );
    let file_size = file_data.len();

    // Compute heuristics, hash, alternative CIDs
    let freedesktop_mime_type = get_freedesktop_mime_type(&file_data);

    let libmime_mime_type = get_libmime_mime_type_async(file_data.clone())
        .await
        .map_err(|err| {
            warn!(
                "unable to detect libmime MIME type for file {}: {:?}",
                cid, err
            );
            FailureReason::FailedToDetectLibmimeMimeType
        })?;

    debug!(
        "{}: detected MIME types {} (freedesktop), {} (libmime)",
        cid, freedesktop_mime_type, libmime_mime_type
    );
    let sha256_hash = hash::compute_sha256(&file_data);
    let alternative_cids = hash::AlternativeCids::for_bytes(ipfs_client.clone(), file_data)
        .await
        .map_err(|err| {
            warn!(
                "unable to compute alternative CIDs for file {}: {:?}",
                cid, err
            );
            FailureReason::FailedToComputeAlternativeCids
        })?;
    debug!("{}: computed alternative CIDs {:?}", cid, alternative_cids);
    let alternative_cids_normalized = alternative_cids.normalized_cids().map_err(|err| {
        warn!(
            "unable to get normalized CIDs for alternative CIDs {:?} for file {}: {:?}",
            alternative_cids, cid, err
        );
        FailureReason::FailedToComputeAlternativeCids
    })?;

    // Download block data of entire referenced DAG, in layers.
    debug!("{}: downloading DAG metadata", cid);
    let layers =
        match ipfs::download_dag_block_metadata(cid, cid_parts.codec, None, ipfs_client.clone())
            .await
            .map_err(|err| {
                debug!("{}: unable to get metadata referenced DAG: {:?}", cid, err);
                FailureReason::DownloadFailed
            })? {
            Ok(layers) => layers,
            Err(_) => {
                debug!("{}: failed to parse referenced CID of child blocks", cid);
                return Ok(Err(SkipReason::UnableToParseReferencedCids));
            }
        };

    let dag_block_cids = layers
        .iter()
        .map(|l| l.iter())
        .flatten()
        .map(|(child_cidparts, _)| format!("{}", child_cidparts.cid))
        .collect();

    Ok(Ok(FileMetadata {
        freedesktop_mime_type,
        libmime_mime_type,
        file_size: file_size as i64,
        sha256_hash,
        alternative_cids_normalized,
        layers,
        dag_block_cids,
    }))
}

fn set_up_libmime_detector() -> anyhow::Result<magic::Cookie> {
    // Create configuration
    let cookie = magic::Cookie::open(
        // Treat some OS errors as real errors instead of hiding them somewhere.
        magic::CookieFlags::ERROR |
            // Return a MIME type string.
            magic::CookieFlags::MIME_TYPE,
    )
    .context("unable to set up libmime detector")?;

    // Load the default database.
    cookie
        .load::<&str>(&[])
        .context("unable to load libmime database")?;

    Ok(cookie)
}

pub fn get_freedesktop_mime_type(bytes: &[u8]) -> &'static str {
    tree_magic_mini::from_u8(bytes)
}

pub async fn get_libmime_mime_type_async(bytes: Vec<u8>) -> anyhow::Result<String> {
    tokio::task::spawn_blocking(move || get_libmime_mime_type(bytes))
        .await
        .unwrap()
}

pub fn get_libmime_mime_type(bytes: Vec<u8>) -> anyhow::Result<String> {
    // Set up libmime detector
    let libmime_detector =
        set_up_libmime_detector().context("unable to set up libmime detector")?;

    let libmime_mime_type = libmime_detector
        .buffer(&bytes)
        .context("failed to detect mime type")?;

    Ok(libmime_mime_type)
}

async fn redis_mark_failed(cid: &str, redis_conn: &mut RedisConnection) {
    match redis::Cache::Files.record_failure(cid, redis_conn).await {
        Ok(_) => {
            debug!("{}: marked failed in redis files", cid);
        }
        Err(err) => {
            warn!("unable to update redis files cache: {:?}", err);
        }
    }
}

async fn redis_mark_child_done(cid: &str, redis_conn: &mut RedisConnection) {
    redis::mark_done_up_to_logging(cid, redis_conn, redis::Cache::Blocks).await;
}

async fn redis_mark_done(cid: &str, redis_conn: &mut RedisConnection) {
    redis::mark_done_up_to_logging(cid, redis_conn, redis::Cache::Files).await;
}
