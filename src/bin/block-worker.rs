use anyhow::Context;
use clap::{arg, Command};
use diesel::PgConnection;
use futures_util::StreamExt;
use ipfs_api_backend_hyper::{IpfsApi, TryFromUri};
use ipfs_indexer::prom::OutcomeLabel;
use ipfs_indexer::queue::{BlockMessage, DirectoryMessage, FileMessage, HamtShardMessage};
use ipfs_indexer::redis::RedisConnection;
use ipfs_indexer::{
    db, ipfs, logging, models, prom, queue, redis, CacheCheckResult, IpfsApiClient,
    WorkerConnections,
};
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions};
use log::{debug, error, info, warn};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::set_up_logging().unwrap();

    let matches = Command::new("IPFS Indexer Blocks Worker")
        .version(clap::crate_version!())
        .author("Leo Balduf <leobalduf@gmail.com>")
        .about("IPFS indexer worker to process single blocks.
Takes blocks from an AMQP queue, downloads them, inserts metadata and links into a database, and posts file, directory, or HAMTShard tasks back to AMQP.
Static configuration is taken from a .env file, see the README for more information.")
        .arg(arg!(--daemon <ADDRESS> "specifies the API URL of the IPFS daemon to use")
            .default_value("http://127.0.0.1:5001"))
        .get_matches();

    let failure_threshold = ipfs_indexer::failed_block_downloads_threshold_from_env()
        .context("unable to determine failed block downloads threshold")?;
    let download_timeout = ipfs_indexer::block_worker_ipfs_timeout_secs_from_env()
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

    debug!("creating channels and setting prefetch...");
    let blocks_chan = rabbitmq_conn
        .create_channel()
        .await
        .context("unable to create RabbitMQ channel")?;
    let files_chan = Arc::new(
        rabbitmq_conn
            .create_channel()
            .await
            .context("unable to create RabbitMQ channel")?,
    );
    let directories_chan = Arc::new(
        rabbitmq_conn
            .create_channel()
            .await
            .context("unable to create RabbitMQ channel")?,
    );
    let hamtshards_chan = Arc::new(
        rabbitmq_conn
            .create_channel()
            .await
            .context("unable to create RabbitMQ channel")?,
    );
    queue::set_prefetch(
        &blocks_chan,
        queue::Queues::Blocks
            .qos_from_env()
            .context("unable to load number of workers")?,
    )
    .await
    .context("unable to set queue prefetch")?;
    debug!("subscribing...");
    let mut blocks_consumer = queue::Queues::Blocks
        .subscribe(&blocks_chan, &format!("blocks_worker_{}", daemon_id.id))
        .await
        .context("unable to subscribe to blocks queue")?;
    info!("set up RabbitMQ consumer and channels");

    info!("listening for tasks");
    while let Some(delivery) = blocks_consumer.next().await {
        let delivery = delivery.expect("error in consumer");
        debug!("got delivery {:?}", delivery);

        let daemon_uri = daemon_uri.clone();
        let redis_conn = redis_conn.clone();
        let files_chan = files_chan.clone();
        let directories_chan = directories_chan.clone();
        let hamtshards_chan = hamtshards_chan.clone();
        let db_conn = db_conn.clone();
        let ipfs_client = client.clone();

        tokio::spawn(async move {
            handle_delivery(
                delivery,
                daemon_uri,
                redis_conn,
                files_chan,
                directories_chan,
                hamtshards_chan,
                db_conn,
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
    files_chan: Arc<lapin::Channel>,
    directories_chan: Arc<lapin::Channel>,
    hamtshards_chan: Arc<lapin::Channel>,
    db_conn: Arc<Mutex<PgConnection>>,
    ipfs_client: Arc<T>,
    failure_threshold: u64,
) where
    T: IpfsApi + Sync,
{
    let Delivery { data, acker, .. } = delivery;
    let msg = match queue::decode_block(&data) {
        Ok(cid) => cid,
        Err(err) => {
            warn!("unable to parse task BlockMessage, skipping: {:?}", err);
            return;
        }
    };

    let before = Instant::now();
    match handle_block(
        msg,
        &mut redis_conn,
        files_chan,
        directories_chan,
        hamtshards_chan,
        db_conn,
        ipfs_client,
        failure_threshold,
    )
    .await
    {
        Ok(outcome) => {
            // Record in prometheus
            prom::record_task_duration(
                &*prom::BLOCK_TASK_STATUS,
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
                &*prom::BLOCK_TASK_STATUS,
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
    SkippedUnixFsSymlinkOrMetadata,
    UnableToParseReferencedCids,
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
            Success::SkippedUnixFsSymlinkOrMetadata => "skipped_unixfs_symlink_or_metadata",
            Success::UnableToParseReferencedCids => "unable_to_parse_referenced_cids",
            Success::Done => "done",
        }
    }
}

enum Failure {
    DbSelectFailed,
    DownloadFailed,
    DbUpsertFailed,
    DbFailureInsertFailed,
    FailedToPostTask,
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
            Failure::DbFailureInsertFailed => "db_insert_failed",
            Failure::FailedToPostTask => "failed_to_post_task",
        }
    }
}

async fn handle_block<T>(
    msg: BlockMessage,
    redis_conn: &mut RedisConnection,
    files_chan: Arc<lapin::Channel>,
    directories_chan: Arc<lapin::Channel>,
    hamtshards_chan: Arc<lapin::Channel>,
    db_conn: Arc<Mutex<PgConnection>>,
    ipfs_client: Arc<T>,
    failure_threshold: u64,
) -> Result<Success, Failure>
where
    T: IpfsApi + Sync,
{
    /*
        1. (optimization) Check Redis `blocks` for CID -> skip to 9
    2. (optimization) Check Redis for `failed_blocks` counter -> if >= THRESHOLD skip to 9
    3. Check DB for failed downloads counter -> if >= THRESHOLD skip to 9
    4. (optimization) Check DB for block-level stats, especially UnixFS type -> skip to 8
    5. Download and stat block
    6. If failed: Record in DB, record in Redis, requeue to RabbitMQ, return
    7. Insert block-level info+DAG references into DB, in one transaction
    8. Push CID to `files`, `directories`, or `hamtshards`
    9. (optimization) Insert CID into Redis `blocks` and `cids`
       */

    let BlockMessage {
        cid: cid_parts,
        db_block,
    } = msg;
    let cid = format!("{}", cid_parts.cid);

    // Check caches
    debug!("{}: checking caches", cid);
    match ipfs_indexer::check_task_caches(
        ipfs_indexer::Task::Block,
        &cid,
        db_block.id,
        redis::Cache::Blocks,
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

    // Check database for block-level stats
    debug!("{}: checking database for block-level stats", cid);
    let block_stats = match db::async_get_block_stats(db_conn.clone(), db_block.id).await {
        Ok(res) => res,
        Err(err) => {
            error!("unable to check block status in database: {:?}", err);
            return Err(Failure::DbSelectFailed);
        }
    };

    let (block_stat, links) = match block_stats {
        Some((stat, links)) => (stat, links),
        None => {
            // If not present, download and insert
            debug!(
                "{}: no block-level data in database, attempting to download",
                cid
            );

            let block_level_metadata =
                match ipfs::query_ipfs_for_block_level_data(&cid, cid_parts.codec, ipfs_client)
                    .await
                {
                    Ok(metadata) => {
                        match metadata {
                            Ok(metadata) => metadata,
                            Err(_) => {
                                // Parsing the referenced CID failed.
                                // TODO record this as a failure? Probably not.
                                return Ok(Success::UnableToParseReferencedCids);
                            }
                        }
                    }
                    Err(err) => {
                        debug!("{}: unable to get block-level metadata: {:?}", cid, err);

                        match db::async_insert_block_download_failure(
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
                                error!(
                                    "unable to insert download failure into database: {:?}",
                                    err
                                );
                                return Err(Failure::DbFailureInsertFailed);
                            }
                        }

                        redis_mark_failed(&cid, redis_conn).await;

                        return Err(Failure::DownloadFailed);
                    }
                };
            debug!("{}: got metadata {:?}", cid, block_level_metadata);

            debug!("{}: inserting metadata to database", cid);
            let (block_stat, links) = match db::async_upsert_successful_block(
                db_conn.clone(),
                db_block.id,
                block_level_metadata.block_size,
                block_level_metadata.unixfs_type_id,
                block_level_metadata.links,
                chrono::Utc::now(),
            )
            .await
            {
                Ok(res) => res,
                Err(err) => {
                    error!("unable to upsert block metadata into database: {:?}", err);
                    return Err(Failure::DbUpsertFailed);
                }
            };
            debug!(
                "{}: upserted, got block stat {:?}, links {:?}",
                cid, block_stat, links
            );

            (block_stat, links)
        }
    };
    debug!(
        "{}: database contains block {:?}, links {:?}",
        cid, block_stat, links
    );

    // Examine UnixFS type to post followup task to RabbitMQ
    let post_job_res = match block_stat.unixfs_type_id {
        models::UNIXFS_TYPE_METADATA_ID | models::UNIXFS_TYPE_SYMLINK_ID => {
            debug!("{}: skipping UnixFS metadata or symlink", cid);
            redis_mark_done(&cid, redis_conn).await;
            return Ok(Success::SkippedUnixFsSymlinkOrMetadata);
        }
        models::UNIXFS_TYPE_FILE_ID | models::UNIXFS_TYPE_RAW_ID => {
            debug!("{}: posting file task", cid);
            queue::post_file(
                &files_chan,
                &FileMessage {
                    cid: cid_parts,
                    db_block,
                },
            )
            .await
        }
        models::UNIXFS_TYPE_DIRECTORY_ID => {
            debug!("{}: posting directory task", cid);
            queue::post_directory(
                &directories_chan,
                &DirectoryMessage {
                    cid: cid_parts,
                    db_block,
                    db_links: links,
                },
            )
            .await
        }
        models::UNIXFS_TYPE_HAMT_SHARD_ID => {
            debug!("{}: posting hamtshard task", cid);
            queue::post_hamtshard(
                &hamtshards_chan,
                &HamtShardMessage {
                    cid: cid_parts,
                    db_block,
                },
            )
            .await
        }
        _ => {
            unreachable!("invalid UnixFS type in database")
        }
    };
    match post_job_res {
        Ok(confirmation) => {
            debug!("{}: posted task, got confirmation {:?}", cid, confirmation)
        }
        Err(err) => {
            error!("unable to post task: {:?}", err);
            return Err(Failure::FailedToPostTask);
        }
    }

    // Update redis
    debug!("{}: marking done in redis...", cid);
    redis_mark_done(&cid, redis_conn).await;

    Ok(Success::Done)
}

async fn redis_mark_failed(cid: &str, redis_conn: &mut RedisConnection) {
    match redis::Cache::Blocks.record_failure(cid, redis_conn).await {
        Ok(_) => {
            debug!("{}: marked failed in redis blocks", cid);
        }
        Err(err) => {
            warn!("unable to update redis blocks cache: {:?}", err);
        }
    }
}

async fn redis_mark_done(cid: &str, redis_conn: &mut RedisConnection) {
    redis::mark_done_up_to_logging(cid, redis_conn, redis::Cache::Blocks).await;
}
