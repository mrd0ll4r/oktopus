use crate::TaskOutcome::{Done, Failed, Skipped};
use anyhow::Context;
use clap::{arg, Command};
use diesel::PgConnection;
use futures_util::StreamExt;
use ipfs_api_backend_hyper::{IpfsApi, TryFromUri};
use ipfs_indexer::db::{Block, BlockLink, BlockStat, Cid, DirectoryEntry};
use ipfs_indexer::prom::OutcomeLabel;
use ipfs_indexer::queue::{BlockMessage, DirectoryMessage, FileMessage, HamtShardMessage};
use ipfs_indexer::redis::RedisConnection;
use ipfs_indexer::{
    db, ipfs, logging, models, prom, queue, redis, CIDParts, CacheCheckResult, IpfsApiClient,
    WorkerConnections,
};
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions};
use log::{debug, info, warn};
use std::iter;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::set_up_logging().unwrap();

    let matches = Command::new("IPFS Indexer Directories Worker")
        .version(clap::crate_version!())
        .author("Leo Balduf <leobalduf@gmail.com>")
        .about("IPFS indexer worker to process UnixFS directories.
Takes UnixFS directory blocks from an AMQP queue, lists their entries, and inserts metadata into a database.
Static configuration is taken from a .env file, see the README for more information.")
        .arg(arg!(--daemon <ADDRESS> "specifies the API URL of the IPFS daemon to use")
            .default_value("http://127.0.0.1:5001"))
        .get_matches();

    let failure_threshold = ipfs_indexer::failed_directory_downloads_threshold_from_env()
        .context("unable to determine failed DAG downloads threshold")?;
    let download_timeout = ipfs_indexer::directory_worker_ipfs_timeout_secs_from_env()
        .context("unable to determine download timeout")?;
    let full_ls_size_limit = ipfs_indexer::directory_full_ls_size_limit_from_env()
        .context("unable to determine size limit for full directory listing")?;

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
    let directories_chan = rabbitmq_conn
        .create_channel()
        .await
        .context("unable to create RabbitMQ channel")?;
    let blocks_chan = Arc::new(
        rabbitmq_conn
            .create_channel()
            .await
            .context("unable to create RabbitMQ channel")?,
    );
    let files_chan = Arc::new(
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
        &directories_chan,
        queue::Queues::Directories
            .qos_from_env()
            .context("unable to load number of workers")?,
    )
    .await
    .context("unable to set queue prefetch")?;
    debug!("subscribing...");
    let mut directories_consumer = queue::Queues::Directories
        .subscribe(
            &directories_chan,
            &format!("directories_worker_{}", daemon_id.id),
        )
        .await
        .context("unable to subscribe to directories queue")?;
    let directories_chan = Arc::new(directories_chan);
    info!("set up RabbitMQ channels and consumer");

    info!("listening for tasks");
    while let Some(delivery) = directories_consumer.next().await {
        let delivery = delivery.expect("error in consumer");
        debug!("got delivery {:?}", delivery);

        let redis_conn = redis_conn.clone();
        let blocks_chan = blocks_chan.clone();
        let files_chan = files_chan.clone();
        let daemon_uri = daemon_uri.clone();
        let directories_chan = directories_chan.clone();
        let hamtshards_chan = hamtshards_chan.clone();
        let db_conn = db_conn.clone();
        let ipfs_client = client.clone();

        tokio::spawn(async move {
            handle_delivery(
                delivery,
                daemon_uri,
                redis_conn,
                blocks_chan,
                files_chan,
                directories_chan,
                hamtshards_chan,
                db_conn,
                ipfs_client,
                failure_threshold,
                full_ls_size_limit,
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
    blocks_chan: Arc<lapin::Channel>,
    files_chan: Arc<lapin::Channel>,
    directories_chan: Arc<lapin::Channel>,
    hamtshards_chan: Arc<lapin::Channel>,
    db_conn: Arc<Mutex<PgConnection>>,
    ipfs_client: Arc<T>,
    failure_threshold: u64,
    full_ls_size_limit: u64,
) where
    T: IpfsApi + Sync,
{
    let Delivery {
        data,
        acker,
        redelivered,
        ..
    } = delivery;
    let msg = match queue::decode_directory(&data) {
        Ok(cid) => cid,
        Err(err) => {
            warn!("unable to parse task DirectoryMessage, skipping: {:?}", err);
            return;
        }
    };

    let before = Instant::now();
    let outcome = handle_directory(
        msg,
        redelivered,
        &mut redis_conn,
        blocks_chan,
        files_chan,
        directories_chan,
        hamtshards_chan,
        db_conn,
        ipfs_client,
        failure_threshold,
        full_ls_size_limit,
    )
    .await;

    prom::record_task_duration(
        &*prom::DIRECTORY_TASK_STATUS,
        outcome,
        before.elapsed(),
        &daemon_uri,
    );

    // Report to RabbitMQ
    match outcome {
        Done(_) | Skipped(_) => {
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
    NoLinks,
    DbFailureThreshold,
    UnableToParseReferencedCids,
    RedeliveredWithoutDbFailure,
}

#[derive(Clone, Copy, Debug)]
enum FailureReason {
    DownloadFailed,
}

#[derive(Clone, Copy, Debug)]
enum DoneReason {
    FullLs,
    FastLs,
}

#[derive(Clone, Copy, Debug)]
enum TaskOutcome {
    Done(DoneReason),
    Skipped(SkipReason),
    Failed(FailureReason),
}

impl OutcomeLabel for TaskOutcome {
    fn status(&self) -> &'static str {
        match self {
            Done(_) => "done",
            Skipped(_) => "skipped",
            Failed(_) => "failed",
        }
    }

    fn reason(&self) -> &'static str {
        match self {
            Done(reason) => match reason {
                DoneReason::FullLs => "ls_full",
                DoneReason::FastLs => "ls_fast",
            },
            Skipped(reason) => match reason {
                SkipReason::RedisCached => "redis_cached",
                SkipReason::RedisFailureCached => "redis_failure_cached",
                SkipReason::NoLinks => "no_links",
                SkipReason::DbFailureThreshold => "db_failure_threshold",
                SkipReason::UnableToParseReferencedCids => "unable_to_parse_referenced_cids",
                SkipReason::RedeliveredWithoutDbFailure => "redelivered_without_db_failure",
            },
            Failed(reason) => match reason {
                FailureReason::DownloadFailed => "download_failed",
            },
        }
    }
}

async fn handle_directory<T>(
    msg: DirectoryMessage,
    redelivered: bool,
    redis_conn: &mut RedisConnection,
    blocks_chan: Arc<lapin::Channel>,
    files_chan: Arc<lapin::Channel>,
    directories_chan: Arc<lapin::Channel>,
    hamtshards_chan: Arc<lapin::Channel>,
    db_conn: Arc<Mutex<PgConnection>>,
    ipfs_client: Arc<T>,
    failure_threshold: u64,
    full_ls_size_limit: u64,
) -> TaskOutcome
where
    T: IpfsApi + Sync,
{
    let DirectoryMessage {
        cid: cid_parts,
        db_block,
        db_links,
    } = msg;
    let cid = format!("{}", cid_parts.cid);

    // Check caches
    debug!("{}: checking caches", cid);
    match ipfs_indexer::check_task_caches(
        ipfs_indexer::Task::Directory,
        &cid,
        db_block.id,
        redis::Cache::Directories,
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

    // Check if block has references
    if db_links.is_empty() {
        debug!("{}: has no links, skipping", cid);
        redis_mark_done(&cid, redis_conn).await;
        return Skipped(SkipReason::NoLinks);
    }

    // Check if block has too many references
    if db_links.len() as u64 > full_ls_size_limit {
        debug!(
            "{}: has {} links, doing fast ls and posting block tasks",
            cid,
            db_links.len()
        );

        let entries =
            match fast_index_and_insert(redis_conn, db_conn, ipfs_client, db_block, &cid).await {
                Ok(entries) => entries,
                Err(res) => return Failed(res),
            };
        debug!(
            "{}: successfully fast-ls'ed and inserted links into database: {:?}",
            cid, entries
        );

        // Parse database structures into CIDParts
        let entries = entries
            .into_iter()
            .map(|(entry, cid, block)| {
                let cidparts = CIDParts::from_parts(&block.multihash, cid.codec as u64)?;
                Ok((entry, cidparts, block))
            })
            .collect::<anyhow::Result<Vec<_>>>()
            .unwrap(); // I really hope this never goes wrong

        // Post block tasks
        for entry in entries.into_iter() {
            let (_, cidparts, block) = entry;
            let child_cid = format!("{}", cidparts.cid);

            debug!("{}/{}: posting block task", cid, child_cid);
            let confirmation = queue::post_block(
                &blocks_chan,
                &BlockMessage {
                    cid: cidparts,
                    db_block: block,
                },
            )
            .await
            .expect("unable to post task");
            debug!(
                "{}/{}: posted task, got confirmation {:?}",
                cid, child_cid, confirmation
            )
        }

        // Update redis
        debug!("{}: marking done in redis...", cid);
        redis_mark_done(&cid, redis_conn).await;

        return Done(DoneReason::FastLs);
    }

    // Check database for directory listing
    debug!("{}: checking database for directory listing", cid);
    let directory_entries = db::async_get_directory_listing(db_conn.clone(), db_block.id)
        .await
        .expect("unable to check directory listing in database");

    let entries = match directory_entries {
        Some(entries) => {
            if entries.iter().any(|e| e.3.is_none()) {
                // If we are missing any of the block stat information: reindex
                debug!(
                    "{}: incomplete block-level metadata for directory, attempting to download",
                    cid
                );
                match index_and_insert(redis_conn, db_conn, ipfs_client, db_block, &cid).await {
                    Ok(entries) => match entries {
                        Ok(entries) => entries,
                        Err(skip_reason) => return Skipped(skip_reason),
                    },
                    Err(res) => return Failed(res),
                }
            } else {
                // Information in the database is complete, we just need to transform it a bit
                entries
                    .into_iter()
                    .map(|(entry, c, block, stat)| {
                        // Cannot fail due to the check above
                        let (stat, links) = stat.unwrap();
                        (entry, c, block, stat, links)
                    })
                    .collect::<Vec<_>>()
            }
        }
        None => {
            // If not present, download and index
            debug!(
                "{}: no directory entries in database, attempting to download",
                cid
            );

            match index_and_insert(redis_conn, db_conn, ipfs_client, db_block, &cid).await {
                Ok(entries) => match entries {
                    Ok(entries) => entries,
                    Err(skip_reason) => return Skipped(skip_reason),
                },
                Err(res) => return Failed(res),
            }
        }
    };
    debug!("{}: database contains entries {:?}", cid, entries);

    // Parse database structures into CIDParts
    let entries = entries
        .into_iter()
        .map(|(entry, cid, block, stat, links)| {
            let cidparts = CIDParts::from_parts(&block.multihash, cid.codec as u64)?;
            Ok((entry, cidparts, block, stat, links))
        })
        .collect::<anyhow::Result<Vec<_>>>()
        .unwrap(); // I really hope this never goes wrong

    // Mark each child as done in redis
    for (_, cidparts, _, _, _) in entries.iter() {
        redis_mark_child_done(&format!("{}", cidparts.cid), redis_conn).await
    }

    // Examine UnixFS type of each child to post followup task to RabbitMQ
    for (_, cidparts, block, block_stat, links) in entries.into_iter() {
        let child_cid = format!("{}", cidparts.cid);
        let post_job_res = match block_stat.unixfs_type_id {
            models::UNIXFS_TYPE_METADATA_ID | models::UNIXFS_TYPE_SYMLINK_ID => {
                debug!("{}/{}: skipping UnixFS metadata or symlink", cid, child_cid);
                continue;
            }
            models::UNIXFS_TYPE_FILE_ID | models::UNIXFS_TYPE_RAW_ID => {
                debug!("{}/{}: posting file task", cid, child_cid);
                queue::post_file(
                    &files_chan,
                    &FileMessage {
                        cid: cidparts,
                        db_block: block,
                        db_links: links,
                    },
                )
                .await
            }
            models::UNIXFS_TYPE_DIRECTORY_ID => {
                debug!("{}/{}: posting directory task", cid, child_cid);
                queue::post_directory(
                    &directories_chan,
                    &DirectoryMessage {
                        cid: cidparts,
                        db_block: block,
                        db_links: links,
                    },
                )
                .await
            }
            models::UNIXFS_TYPE_HAMT_SHARD_ID => {
                debug!("{}/{}: posting hamtshard task", cid, child_cid);
                queue::post_hamtshard(
                    &hamtshards_chan,
                    &HamtShardMessage {
                        cid: cidparts,
                        db_block: block,
                    },
                )
                .await
            }
            _ => {
                unreachable!("invalid UnixFS type in database")
            }
        };
        let confirmation = post_job_res.expect("unable to post task");
        debug!(
            "{}/{}: posted task, got confirmation {:?}",
            cid, child_cid, confirmation
        )
    }

    // Update redis
    debug!("{}: marking done in redis...", cid);
    redis_mark_done(&cid, redis_conn).await;

    Done(DoneReason::FullLs)
}

async fn index_and_insert<T>(
    redis_conn: &mut RedisConnection,
    db_conn: Arc<Mutex<PgConnection>>,
    ipfs_client: Arc<T>,
    db_block: Block,
    cid: &String,
) -> Result<
    Result<Vec<(DirectoryEntry, Cid, Block, BlockStat, Vec<BlockLink>)>, SkipReason>,
    FailureReason,
>
where
    T: IpfsApi + Sync,
{
    let listing = match ipfs::query_ipfs_for_directory_listing(&cid, ipfs_client.clone(), true)
        .await
    {
        Ok(listing) => listing,
        Err(err) => {
            debug!("{}: failed to list directory: {:?}", cid, err);

            db::async_insert_dag_download_failure(db_conn.clone(), db_block.id, chrono::Utc::now())
                .await
                .expect("unable to insert download failure into database");
            debug!("{}: marked failed in database", cid);

            redis_mark_failed(&cid, redis_conn).await;

            return Err(FailureReason::DownloadFailed);
        }
    };
    debug!("{}: got directory listing {:?}", cid, listing);

    // Download block data
    let mut block_metadata = Vec::new();
    for (_, _, cidparts) in listing.iter() {
        let child_cid = format!("{}", cidparts.cid);
        debug!("{}: getting block stats for child {}", cid, child_cid);
        let metadata = match ipfs::query_ipfs_for_block_level_data(
            &child_cid,
            cidparts.codec,
            ipfs_client.clone(),
        )
        .await
        {
            Ok(metadata) => match metadata {
                Ok(metadata) => metadata,
                Err(_) => {
                    debug!("{}: failed to parse CID of child blocks", cid);
                    return Ok(Err(SkipReason::UnableToParseReferencedCids));
                }
            },
            Err(err) => {
                debug!("{}: failed to download children blocks: {:?}", cid, err);

                db::async_insert_dag_download_failure(
                    db_conn.clone(),
                    db_block.id,
                    chrono::Utc::now(),
                )
                .await
                .expect("unable to insert download failure into database");
                debug!("{}: marked failed in database", cid);

                redis_mark_failed(&cid, redis_conn).await;

                return Err(FailureReason::DownloadFailed);
            }
        };

        block_metadata.push(metadata);
    }
    let entries = listing
        .into_iter()
        .zip(block_metadata)
        .map(|((name, size, cidparts), metadata)| (name, size, cidparts, Some(metadata)))
        .collect();
    debug!("{}: augmented listing with stats {:?}", cid, entries);

    debug!("{}: inserting listing and metadata into database", cid);
    let entries = db::async_upsert_successful_directory(
        db_conn.clone(),
        db_block.id,
        entries,
        None,
        chrono::Utc::now(),
    )
    .await
    .expect("unable to upsert block metadata into database");
    debug!("{}: upserted, got entries {:?}", cid, entries);

    let entries = entries
        .into_iter()
        .map(|(direntry, c, block, stats)| {
            // We inserted a full ls, including block stats, so we get those back (that's the contract)
            let (stat, links) = stats.unwrap();
            (direntry, c, block, stat, links)
        })
        .collect();

    Ok(Ok(entries))
}

async fn fast_index_and_insert<T>(
    redis_conn: &mut RedisConnection,
    db_conn: Arc<Mutex<PgConnection>>,
    ipfs_client: Arc<T>,
    db_block: Block,
    cid: &String,
) -> Result<Vec<(DirectoryEntry, Cid, Block)>, FailureReason>
where
    T: IpfsApi + Sync,
{
    let listing = match ipfs::query_ipfs_for_directory_listing(&cid, ipfs_client.clone(), false)
        .await
    {
        Ok(listing) => listing,
        Err(err) => {
            debug!("{}: failed to list directory: {:?}", cid, err);

            db::async_insert_dag_download_failure(db_conn.clone(), db_block.id, chrono::Utc::now())
                .await
                .expect("unable to insert download failure into database");
            debug!("{}: marked failed in database", cid);

            redis_mark_failed(&cid, redis_conn).await;

            return Err(FailureReason::DownloadFailed);
        }
    };
    debug!("{}: got directory listing {:?}", cid, listing);

    // We don't have block-level metadata, so we augment this with None
    let entries = listing
        .into_iter()
        .zip(iter::repeat(None))
        .map(|((name, size, cidparts), metadata)| (name, size, cidparts, metadata))
        .collect();

    debug!("{}: inserting listing and metadata into database", cid);
    let entries = db::async_upsert_successful_directory(
        db_conn.clone(),
        db_block.id,
        entries,
        None,
        chrono::Utc::now(),
    )
    .await
    .expect("unable to upsert block metadata into database");
    debug!("{}: upserted, got entries {:?}", cid, entries);

    let entries = entries
        .into_iter()
        .map(|(direntry, c, block, stats)| {
            assert!(stats.is_none());
            (direntry, c, block)
        })
        .collect();

    Ok(entries)
}

async fn redis_mark_failed(cid: &str, redis_conn: &mut RedisConnection) {
    match redis::Cache::Directories
        .record_failure(cid, redis_conn)
        .await
    {
        Ok(_) => {
            debug!("{}: marked failed in redis directories", cid);
        }
        Err(err) => {
            warn!("unable to update redis directories cache: {:?}", err);
        }
    }
}

async fn redis_mark_child_done(cid: &str, redis_conn: &mut RedisConnection) {
    redis::mark_done_up_to_logging(cid, redis_conn, redis::Cache::Blocks).await
}

async fn redis_mark_done(cid: &str, redis_conn: &mut RedisConnection) {
    redis::mark_done_up_to_logging(cid, redis_conn, redis::Cache::Directories).await
}
