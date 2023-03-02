use crate::TaskOutcome::{Done, Failed, Skipped};
use anyhow::{anyhow, ensure, Context};
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
    Timeouts, WorkerConnections,
};
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions};
use log::{debug, error, info, warn};
use reqwest::{Client, Url};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::io::AsyncReadExt;

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
        .arg(arg!(--gateway <ADDRESS> "specifies the gateway URL of the IPFS daemon to use")
            .default_value("http://127.0.0.1:8080"))
        .get_matches();

    let failure_threshold = ipfs_indexer::failed_file_downloads_threshold_from_env()
        .context("unable to determine failed DAG downloads threshold")?;
    let file_size_limit =
        ipfs_indexer::file_size_limit_from_env().context("unable to determine file size limit")?;
    let timeouts =
        Arc::new(Timeouts::from_env().context("unable to load timeouts from environment")?);

    // Create and/or clear temp storage directory
    let temp_storage_path = Path::new("/ipfs-indexer/tmp");
    ensure!(
        temp_storage_path.is_dir(),
        "temporary file storage directory missing"
    );

    // Test if we can write to it
    debug!(
        "checking if we can write to temp storage at {:?}...",
        temp_storage_path
    );
    let mut tmp_file_path = temp_storage_path.to_path_buf();
    tmp_file_path.push("test.foo");
    std::fs::File::create(&tmp_file_path)
        .context("unable to write to temporary file storage directory")?;
    std::fs::remove_file(&tmp_file_path)
        .context("unable to write to temporary file storage directory")?;
    for entry in std::fs::read_dir(temp_storage_path)
        .context("unable to list temporary file storage directory")?
    {
        let entry = entry.context("unable to get directory entry")?;
        let path = entry.path();
        if path.is_dir() {
            warn!(
                "temporary file storage directory contains directory {:?}, ignoring...",
                path
            );
        } else {
            debug!("removing file from temporary storage directory: {:?}", path);
            std::fs::remove_file(path)
                .context("unable to remove file from temporary storage directory")?;
        }
    }
    let temp_storage_path = Arc::new(temp_storage_path.to_path_buf());

    let daemon_uri = matches
        .get_one::<String>("daemon")
        .expect("missing required daemon arg");
    let gateway_uri = matches
        .get_one::<String>("gateway")
        .expect("missing required gateway arg");
    let gateway_url = Url::parse(gateway_uri).context("unable to parse gateway URL")?;

    debug!("connecting to IPFS daemon...");
    let api_client: IpfsApiClient = ipfs_api_backend_hyper::IpfsClient::from_str(daemon_uri)
        .context("unable to create IPFS API client")?;
    let api_client_with_timeout = ipfs_api_backend_hyper::BackendWithGlobalOptions::new(
        api_client,
        ipfs_api_backend_hyper::GlobalOptions {
            offline: None,
            timeout: Some(timeouts.block_download),
        },
    );
    let ipfs_api_download_client = Arc::new(api_client_with_timeout);
    let daemon_id = ipfs_api_download_client
        .id(None)
        .await
        .context("unable to query IPFS API")?;
    let ipfs_api_upload_client: Arc<IpfsApiClient> = Arc::new(
        ipfs_api_backend_hyper::IpfsClient::from_str(daemon_uri)
            .context("unable to create IPFS API client")?,
    );
    let _ = ipfs_api_upload_client
        .id(None)
        .await
        .context("unable to query IPFS API")?;
    info!(
        "connected to IPFS daemon version {} at {} with ID {:?}",
        daemon_id.agent_version, daemon_uri, daemon_id.id
    );
    let daemon_uri = Arc::new(daemon_uri.clone());

    // Connect to gateway, HEAD something to test
    debug!("connecting to IPFS gateway...");
    let gateway_client = reqwest::Client::builder()
        .use_rustls_tls()
        .build()
        .context("unable to build HTTP client")?;
    // This is the README file shipped with every kubo instance.
    let example_content_url = ipfs::build_gateway_url(
        &gateway_url,
        "QmPZ9gcCEpqKTo6aq61g2nXGUhM4iCL3ewB6LDXZCtioEB",
    );
    let readme_content = gateway_client
        .get(example_content_url)
        .send()
        .await
        .context("unable to fetch example content from gateway")?
        .bytes()
        .await
        .context("unable to read example content from gateway")?;
    ensure!(
        readme_content.len() == 1091,
        "size mismatch for example content"
    );
    info!("connected to gateway at {}", gateway_url);

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

    let currently_running_tasks = Arc::new(tokio::sync::Mutex::new(HashSet::new()));

    info!("listening for tasks");
    while let Some(delivery) = files_consumer.next().await {
        let delivery = delivery.expect("error in consumer");
        debug!("got delivery {:?}", delivery);

        let redis_conn = redis_conn.clone();
        let daemon_uri = daemon_uri.clone();
        let db_conn = db_conn.clone();
        let mime_cache = mime_cache.clone();
        let ipfs_api_download_client = ipfs_api_download_client.clone();
        let ipfs_api_upload_client = ipfs_api_upload_client.clone();
        let gateway_client = gateway_client.clone();
        let gateway_base_url = gateway_url.clone();
        let temp_storage_path = temp_storage_path.clone();
        let currently_running_tasks = currently_running_tasks.clone();
        let timeouts = timeouts.clone();

        tokio::spawn(async move {
            handle_delivery(
                delivery,
                currently_running_tasks,
                daemon_uri,
                redis_conn,
                db_conn,
                mime_cache,
                ipfs_api_download_client,
                ipfs_api_upload_client,
                failure_threshold,
                gateway_client,
                gateway_base_url,
                file_size_limit,
                temp_storage_path,
                timeouts,
            )
            .await
        });
    }

    Ok(())
}

async fn handle_delivery<S, T>(
    delivery: Delivery,
    currently_running_tasks: Arc<tokio::sync::Mutex<HashSet<String>>>,
    daemon_uri: Arc<String>,
    mut redis_conn: RedisConnection,
    db_conn: Arc<Mutex<PgConnection>>,
    mime_cache: Arc<Mutex<MimeTypeCache>>,
    ipfs_api_download_client: Arc<T>,
    ipfs_api_upload_client: Arc<S>,
    failure_threshold: u64,
    gateway_client: reqwest::Client,
    gateway_base_url: Url,
    file_size_limit: u64,
    temp_file_dir: Arc<PathBuf>,
    timeouts: Arc<Timeouts>,
) where
    S: IpfsApi + Sync,
    T: IpfsApi + Sync,
{
    let Delivery {
        delivery_tag,
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
    let cid = format!("{}", msg.cid.cid);

    // Check if we're already processing this CID.
    {
        let mut running_tasks = currently_running_tasks.lock().await;
        if running_tasks.contains(&cid) {
            // We're already working on that, skip it.
            acker
                .ack(BasicAckOptions::default())
                .await
                .expect("unable to ACK delivery");
            return;
        }
        running_tasks.insert(cid.clone());
    }

    let before = Instant::now();
    let outcome = handle_file(
        delivery_tag,
        msg,
        redelivered,
        &mut redis_conn,
        db_conn,
        mime_cache,
        ipfs_api_download_client,
        ipfs_api_upload_client,
        failure_threshold,
        gateway_client,
        gateway_base_url,
        file_size_limit,
        temp_file_dir,
        timeouts,
    )
    .await;

    // Record in prometheus
    prom::record_task_duration(
        &*prom::FILE_TASK_STATUS,
        outcome,
        before.elapsed(),
        &daemon_uri,
    );

    // Remove from list of running tasks
    {
        let mut running_tasks = currently_running_tasks.lock().await;
        assert!(running_tasks.remove(&cid), "stolen task");
    }

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
    FailedToComputeHash,
    FailedToComputeAlternativeCids,
    FailedToDetectFreedesktopMimeType,
    FailedToDetectLibmagicMimeType,
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
                FailureReason::FailedToDetectLibmagicMimeType => {
                    "failed_to_detect_libmagic_mime_type"
                }
                FailureReason::FailedToComputeHash => "failed_to_compute_hash",
                FailureReason::FailedToDetectFreedesktopMimeType => {
                    "failed_to_detect_freedesktop_mime_type"
                }
            },
        }
    }
}

async fn handle_file<S, T>(
    delivery_tag: u64,
    msg: FileMessage,
    redelivered: bool,
    redis_conn: &mut RedisConnection,
    db_conn: Arc<Mutex<PgConnection>>,
    mime_cache: Arc<Mutex<MimeTypeCache>>,
    ipfs_api_download_client: Arc<S>,
    ipfs_api_upload_client: Arc<T>,
    failure_threshold: u64,
    gateway_client: reqwest::Client,
    gateway_base_url: Url,
    file_size_limit: u64,
    temp_file_dir: Arc<PathBuf>,
    timeouts: Arc<Timeouts>,
) -> TaskOutcome
where
    S: IpfsApi + Sync,
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
        start_ts,
        head_finished_ts,
        download_finished_ts,
        end_ts,
        freedesktop_mime_type,
        libmime_mime_type,
        file_size,
        sha256_hash,
        alternative_cids_normalized,
        layers,
        dag_block_cids,
    ) = match download_file(
        delivery_tag,
        ipfs_api_download_client,
        ipfs_api_upload_client,
        gateway_client,
        gateway_base_url,
        file_size_limit,
        temp_file_dir,
        cid_parts,
        &cid,
        timeouts,
    )
    .await
    {
        Ok(res) => match res {
            Ok(metadata) => {
                let FileMetadataFull {
                    file_metadata,
                    layers,
                    dag_block_cids,
                } = metadata;
                let FileMetadata {
                    start_ts,
                    head_finished_ts,
                    download_finished_ts,
                    end_ts,
                    freedesktop_mime_type,
                    libmime_mime_type,
                    file_size,
                    sha256_hash,
                    alternative_cids_normalized,
                } = file_metadata;
                (
                    start_ts,
                    head_finished_ts,
                    download_finished_ts,
                    end_ts,
                    freedesktop_mime_type,
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
        end_ts,
        start_ts,
        head_finished_ts,
        download_finished_ts,
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

struct FileMetadataFull {
    file_metadata: FileMetadata,
    layers: Vec<
        Vec<(
            CIDParts,
            BlockLevelMetadata,
            chrono::DateTime<chrono::Utc>,
            chrono::DateTime<chrono::Utc>,
        )>,
    >,
    dag_block_cids: Vec<String>,
}

struct FileMetadata {
    start_ts: chrono::DateTime<chrono::Utc>,
    head_finished_ts: chrono::DateTime<chrono::Utc>,
    download_finished_ts: chrono::DateTime<chrono::Utc>,
    end_ts: chrono::DateTime<chrono::Utc>,
    freedesktop_mime_type: &'static str,
    libmime_mime_type: String,
    file_size: i64,
    sha256_hash: Vec<u8>,
    alternative_cids_normalized: Vec<NormalizedAlternativeCid>,
}

async fn download_file<S, T>(
    delivery_tag: u64,
    ipfs_api_download_client: Arc<S>,
    ipfs_api_upload_client: Arc<T>,
    gateway_client: reqwest::Client,
    gateway_base_url: Url,
    file_size_limit: u64,
    temp_file_dir: Arc<PathBuf>,
    cid_parts: CIDParts,
    cid: &String,
    timeouts: Arc<Timeouts>,
) -> Result<Result<FileMetadataFull, SkipReason>, FailureReason>
where
    S: IpfsApi + Sync,
    T: IpfsApi + Sync,
{
    let file_path = {
        let mut temp_file_dir = temp_file_dir.to_path_buf();
        temp_file_dir.push(format!("{}_{}", cid, delivery_tag));
        temp_file_dir
    };

    let file_metadata_res = download_and_analyze_file(
        ipfs_api_upload_client,
        gateway_client,
        &gateway_base_url,
        file_size_limit,
        &cid,
        &file_path,
        timeouts.clone(),
    )
    .await;
    // Make sure to remove the file again
    let _ = std::fs::remove_file(&file_path).map_err(|err| {
        if let std::io::ErrorKind::NotFound = err.kind() {
            // Probably deleted already, whatever.
        } else {
            warn!("unable to remove temporary file {:?}: {:?}", file_path, err)
        }
    });

    let file_metadata = match file_metadata_res.map_err(|err| {
        debug!("{}: unable to download file: {:?}", cid, err);
        err
    })? {
        Ok(file_metadata) => file_metadata,
        Err(err) => {
            debug!("{}: skipping because of {:?}", cid, err);
            return Ok(Err(err));
        }
    };

    // Download block data of entire referenced DAG, in layers.
    debug!("{}: downloading DAG metadata", cid);
    let layers = match ipfs::download_dag_block_metadata(
        cid,
        cid_parts.codec,
        None,
        ipfs_api_download_client.clone(),
        timeouts,
    )
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
        .map(|(child_cidparts, _, _, _)| format!("{}", child_cidparts.cid))
        .collect();

    Ok(Ok(FileMetadataFull {
        file_metadata,
        layers,
        dag_block_cids,
    }))
}

async fn download_and_analyze_file<T>(
    ipfs_api_upload_client: Arc<T>,
    gateway_client: Client,
    gateway_base_url: &Url,
    file_size_limit: u64,
    cid: &str,
    file_path: &PathBuf,
    timeouts: Arc<Timeouts>,
) -> Result<Result<FileMetadata, SkipReason>, FailureReason>
where
    T: IpfsApi + Sync,
{
    debug!(
        "{}: downloading via gateway to temporary file {:?}",
        cid, file_path
    );
    let start_ts = chrono::Utc::now();
    let (file_size, head_finished_ts) = match ipfs::query_ipfs_for_file_data(
        &cid,
        gateway_client.clone(),
        &gateway_base_url,
        file_size_limit.clone(),
        &file_path,
        timeouts,
    )
    .await
    .map_err(|e| {
        debug!("{}: failed to download file: {:?}", cid, e);
        FailureReason::DownloadFailed
    })? {
        Ok((file_size, head_finished_ts)) => (file_size, head_finished_ts),
        Err(too_large_size) => {
            debug!(
                "{}: skipping because size advertised by gateway is {} (limit is {})",
                cid, too_large_size, file_size_limit
            );
            return Ok(Err(SkipReason::FileSizeLimitExceeded));
        }
    };
    let download_finished_ts = chrono::Utc::now();
    debug!("{}: downloaded {} bytes via gateway", cid, file_size);

    // Sanity check: Does the file have the correct size?
    let fs_metadata = tokio::fs::metadata(file_path).await.map_err(|err| {
        error!("{}: unable to stat downloaded file: {:?}", cid, err);
        FailureReason::DownloadFailed
    })?;
    if fs_metadata.len() != file_size {
        error!(
            "{}: downloaded {} bytes, but file has {} bytes",
            cid,
            file_size,
            fs_metadata.len()
        );

        return Err(FailureReason::DownloadFailed);
    }

    // TODO sanity-check filesize from... sum of child blocks, maybe? with some tolerance?

    // Compute heuristics, hash, alternative CIDs
    let freedesktop_mime_type = get_freedesktop_mime_type_async(&file_path, file_size)
        .await
        .map_err(|err| {
            warn!(
                "unable to detect freedesktop MIME type for file {:?}: {:?}",
                file_path, err
            );
            FailureReason::FailedToDetectFreedesktopMimeType
        })?;

    let libmagic_mime_type = get_libmagic_mime_type_async(&file_path, file_size)
        .await
        .map_err(|err| {
            warn!(
                "unable to detect libmagic MIME type for file {:?}: {:?}",
                file_path, err
            );
            FailureReason::FailedToDetectLibmagicMimeType
        })?;

    debug!(
        "{}: detected MIME types {} (freedesktop), {} (libmagic)",
        cid, freedesktop_mime_type, libmagic_mime_type
    );
    let sha256_hash = hash::compute_sha256(&file_path, file_size)
        .await
        .map_err(|err| {
            warn!(
                "unable to compute SHA256 hash for file {:?}: {:?}",
                file_path, err
            );
            FailureReason::FailedToComputeHash
        })?;
    debug!("{}: computed SHA256 hash {:?}", cid, sha256_hash);
    let alternative_cids =
        hash::AlternativeCids::for_bytes(ipfs_api_upload_client.clone(), &file_path)
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
            "unable to get normalized CIDs for alternative CIDs {:?} for file {:?}: {:?}",
            alternative_cids, file_path, err
        );
        FailureReason::FailedToComputeAlternativeCids
    })?;

    Ok(Ok(FileMetadata {
        start_ts,
        head_finished_ts,
        download_finished_ts,
        end_ts: chrono::Utc::now(),
        freedesktop_mime_type,
        libmime_mime_type: libmagic_mime_type,
        file_size: file_size as i64,
        sha256_hash,
        alternative_cids_normalized,
    }))
}

fn set_up_libmagic_detector() -> anyhow::Result<magic::Cookie> {
    // Create configuration
    let cookie = magic::Cookie::open(
        // Treat some OS errors as real errors instead of hiding them somewhere.
        magic::CookieFlags::ERROR |
            // Return a MIME type string.
            magic::CookieFlags::MIME_TYPE,
    )
    .context("unable to set up libmagic detector")?;

    // Load the default database.
    cookie
        .load::<&str>(&[])
        .context("unable to load libmagic database")?;

    Ok(cookie)
}

async fn read_file_head(
    file_path: &Path,
    file_size: u64,
    num_bytes_to_read: u64,
) -> anyhow::Result<Vec<u8>> {
    let f = tokio::fs::File::open(file_path)
        .await
        .context("unable to open file")?;
    let mut buf = Vec::with_capacity(num_bytes_to_read as usize);
    f.take(num_bytes_to_read)
        .read_to_end(&mut buf)
        .await
        .context("unable to read file")?;

    let expected_buffer_size = file_size.min(num_bytes_to_read) as usize;
    if buf.len() != expected_buffer_size {
        return Err(anyhow!(
            "unable to read file head, wanted {} bytes, got {} bytes",
            expected_buffer_size,
            buf.len()
        ));
    }

    Ok(buf)
}

pub async fn get_freedesktop_mime_type_async(
    file_path: &Path,
    file_size: u64,
) -> anyhow::Result<&'static str> {
    // We read 2K because that's what tree_magic would do if we were to give it a file path.
    let buf = read_file_head(file_path, file_size, 2048)
        .await
        .context("unable to read file")?;

    Ok(tree_magic_mini::from_u8(&buf))
}

pub async fn get_libmagic_mime_type_async(
    file_path: &Path,
    file_size: u64,
) -> anyhow::Result<String> {
    // We read 256K, because that's what libmagic would do if we were to give it a file path.
    let buf = read_file_head(file_path, file_size, 256 * 1024)
        .await
        .context("unable to read file")?;

    tokio::task::block_in_place(|| get_libmagic_mime_type(buf))
}

pub fn get_libmagic_mime_type(buf: Vec<u8>) -> anyhow::Result<String> {
    // Set up detector
    let libmagic_detector =
        set_up_libmagic_detector().context("unable to set up libmagic detector")?;

    let libmime_mime_type = libmagic_detector
        .buffer(&buf)
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
