use anyhow::Context;
use clap::{arg, Command};
use diesel::PgConnection;
use futures_util::StreamExt;
use ipfs_api_backend_hyper::{IpfsApi, TryFromUri};
use ipfs_indexer::cache::MimeTypeCache;
use ipfs_indexer::prom::OutcomeLabel;
use ipfs_indexer::queue::FileMessage;
use ipfs_indexer::redis::RedisConnection;
use ipfs_indexer::{
    db, hash, ipfs, logging, prom, queue, redis, CacheCheckResult, IpfsApiClient, WorkerConnections,
};
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions};
use log::{debug, error, info, warn};
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
) where
    T: IpfsApi + Sync,
{
    let Delivery { data, acker, .. } = delivery;
    let msg = match queue::decode_file(&data) {
        Ok(cid) => cid,
        Err(err) => {
            warn!("unable to parse FileMessage, skipping: {:?}", err);
            return;
        }
    };

    let before = Instant::now();
    match handle_file(
        msg,
        &mut redis_conn,
        db_conn,
        mime_cache,
        ipfs_client,
        failure_threshold,
    )
    .await
    {
        Ok(outcome) => {
            // Record in prometheus
            prom::record_task_duration(
                &*prom::FILE_TASK_STATUS,
                outcome,
                before.elapsed(),
                &daemon_uri,
            );

            // Report to RabbitMQ
            acker
                .ack(BasicAckOptions::default())
                .await
                .expect("unable to ACK delivery");
        }
        Err(outcome) => {
            // Record in prometheus
            prom::record_task_duration(
                &*prom::FILE_TASK_STATUS,
                outcome,
                before.elapsed(),
                &daemon_uri,
            );

            // Report to RabbitMQ
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

enum Success {
    RedisCached,
    RedisFailureCached,
    DbFailureThreshold,
    UnableToParseReferencedCids,
    DbDone,
    Done,
}

impl OutcomeLabel for Success {
    fn success(&self) -> bool {
        true
    }

    fn reason(&self) -> &'static str {
        match self {
            Success::RedisCached => "redis_cached",
            Success::RedisFailureCached => "redis_failure_cached",
            Success::DbFailureThreshold => "db_failure_threshold",
            Success::UnableToParseReferencedCids => "unable_to_parse_referenced_cids",
            Success::DbDone => "db_done",
            Success::Done => "done",
        }
    }
}

enum Failure {
    DbSelectFailed,
    DownloadFailed,
    DbUpsertFailed,
    DbFailureInsertFailed,
    FailedToComputeAlternativeCids,
}

impl OutcomeLabel for Failure {
    fn success(&self) -> bool {
        false
    }

    fn reason(&self) -> &'static str {
        match self {
            Failure::DbSelectFailed => "db_select_failed",
            Failure::DownloadFailed => "download_failed",
            Failure::DbUpsertFailed => "db_upsert_failed",
            Failure::DbFailureInsertFailed => "db_failure_insert_failed",
            Failure::FailedToComputeAlternativeCids => "failed_to_compute_alternative_cids",
        }
    }
}

async fn handle_file<T>(
    msg: FileMessage,
    redis_conn: &mut RedisConnection,
    db_conn: Arc<Mutex<PgConnection>>,
    mime_cache: Arc<Mutex<MimeTypeCache>>,
    ipfs_client: Arc<T>,
    failure_threshold: u64,
) -> Result<Success, Failure>
where
    T: IpfsApi + Sync,
{
    /*
        1. (optimization) Check Redis `files` for CID -> skip to 9
    2. (optimization) Check Redis for `failed_files` counter -> if >= THRESHOLD skip to 9
    3. Check DB for failed downloads counter -> if >= THRESHOLD skip to 9
    4. (optimization) Check DB for file heuristics -> skip to 9
    5. Download file and run heuristics, hash calculations, alternative CID calculations etc. on it
    6. Calculate block-level info for DAG blocks
    7. If failed: Record in DB, record in Redis, requeue to RabbitMQ, return
    8. In one transaction:
       1. Insert heuristics
       2. Insert alternative CIDs
       3. Insert SHA256 hash of the file
       4. Insert block-level info for DAG blocks
    9. (optimization) Insert CID into Redis `files`, `blocks`, and `cids`
    10. ACK to RabbitMQ
       */

    let FileMessage {
        cid: cid_parts,
        db_block,
    } = msg;
    let cid = format!("{}", cid_parts.cid);

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
    )
    .await
    {
        Ok(res) => {
            match res {
                CacheCheckResult::RedisMarkedDone => return Ok(Success::RedisCached),
                CacheCheckResult::RedisFailuresAboveThreshold => {
                    return Ok(Success::RedisFailureCached)
                }
                CacheCheckResult::DbFailuresAboveThreshold => {
                    return Ok(Success::DbFailureThreshold)
                }
                CacheCheckResult::NeedsProcessing => {
                    // We have to process it (again)
                }
            }
        }
        Err(err) => {
            error!("unable to check block failures in database: {:?}", err);
            return Err(Failure::DbSelectFailed);
        }
    }

    // Check database for file-level metadata
    debug!("{}: checking database for file heuristics", cid);
    match db::async_get_file_heuristics(db_conn.clone(), db_block.id).await {
        Ok(res) => {
            if res.is_some() {
                debug!("{}: database has file heuristics, skipping", cid);

                redis_mark_done(&cid, redis_conn).await;

                return Ok(Success::DbDone);
            }
        }
        Err(err) => {
            error!("unable to check file heuristics in database: {:?}", err);
            return Err(Failure::DbSelectFailed);
        }
    }

    debug!(
        "{}: no file heuristics in database, attempting to download",
        cid
    );

    let file_data = match ipfs::query_ipfs_for_file_data(&cid, ipfs_client.clone()).await {
        Ok(data) => data,
        Err(err) => {
            debug!("{}: failed to download file: {:?}", cid, err);

            match db::async_insert_dag_download_failure(
                db_conn.clone(),
                db_block.id,
                chrono::Utc::now(),
            )
            .await
            {
                Ok(_) => {
                    debug!("{}: marked failed in database", cid);
                }
                Err(err) => {
                    error!("unable to insert download failure into database: {:?}", err);
                    return Err(Failure::DbFailureInsertFailed);
                }
            }

            redis_mark_failed(&cid, redis_conn).await;

            return Err(Failure::DownloadFailed);
        }
    };
    debug!(
        "{}: got {} bytes of file data, first bytes: {:?}",
        cid,
        file_data.len(),
        file_data.iter().take(10).collect::<Vec<_>>()
    );

    // Compute heuristics, hash, alternative CIDs
    let mime_type = ipfs_indexer::get_mime_type(&file_data);
    debug!("{}: detected MIME type {}", cid, mime_type);
    let sha256_hash = hash::compute_sha256(&file_data);
    let alternative_cids = hash::AlternativeCids::for_bytes(ipfs_client.clone(), file_data)
        .await
        .map_err(|err| {
            warn!(
                "unable to compute alternative CIDs for file {}: {:?}",
                cid, err
            );
            Failure::FailedToComputeAlternativeCids
        })?;
    debug!("{}: computed alternative CIDs {:?}", cid, alternative_cids);
    let alternative_cids_binary = alternative_cids.binary_cidv1s().map_err(|err| {
        warn!(
            "unable to get binary CIDv1s for alternative CIDs {:?} for file {}: {:?}",
            alternative_cids, cid, err
        );
        Failure::FailedToComputeAlternativeCids
    })?;

    // Download block data of entire referenced DAG, in layers.
    debug!("{}: downloading DAG metadata", cid);
    let mut layers = Vec::new();
    let mut dag_block_cids = Vec::new();
    let root_metadata =
        ipfs::query_ipfs_for_block_level_data(&cid, cid_parts.codec, ipfs_client.clone())
            .await
            .map_err(|err| {
                debug!("{}: unable to get metadata for root block: {:?}", cid, err);
                Failure::DownloadFailed
            })?
            .expect("unable to parse children CIDs of block already present in database");
    debug!("{}: got root metadata {:?}", cid, root_metadata);
    let mut current_layer = Some(vec![root_metadata.links]);
    while let Some(layer) = current_layer.take() {
        if layer.is_empty() {
            break;
        }
        let mut wip_layer = Vec::new();
        for (_, _, child_cidparts) in layer.into_iter().flatten() {
            let child_cid = format!("{}", child_cidparts.cid);
            dag_block_cids.push(child_cid.clone());
            let child_metadata = match ipfs::query_ipfs_for_block_level_data(
                &child_cid,
                child_cidparts.codec,
                ipfs_client.clone(),
            )
            .await
            .map_err(|err| {
                debug!("{}: unable to get metadata for root block: {:?}", cid, err);
                Failure::DownloadFailed
            })? {
                Ok(metadata) => metadata,
                Err(_) => {
                    debug!("{}: failed to parse CID of child blocks", cid);
                    return Ok(Success::UnableToParseReferencedCids);
                }
            };
            wip_layer.push((child_cidparts, child_metadata));
        }

        current_layer = Some(
            wip_layer
                .iter()
                .map(|(_, metadata)| metadata.links.clone())
                .collect(),
        );
        layers.push(wip_layer);
    }

    // Insert into database
    debug!("{}: inserting successful file into database", cid);
    if let Err(err) = ipfs_indexer::db::async_upsert_successful_file(
        db_conn,
        mime_cache,
        db_block.id,
        mime_type,
        sha256_hash,
        alternative_cids_binary,
        layers,
        chrono::Utc::now(),
    )
    .await
    {
        error!("unable to upsert file metadata into database: {:?}", err);
        return Err(Failure::DbUpsertFailed);
    }
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

    Ok(Success::Done)
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
