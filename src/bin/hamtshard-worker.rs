use crate::TaskOutcome::{Done, Failed, Skipped};
use anyhow::Context;
use clap::{arg, Command};
use diesel::PgConnection;
use futures_util::StreamExt;
use ipfs_api_backend_hyper::{IpfsApi, TryFromUri};
use ipfs_indexer::db::{Block, Cid, DirectoryEntry};
use ipfs_indexer::prom::OutcomeLabel;
use ipfs_indexer::queue::{BlockMessage, HamtShardMessage};
use ipfs_indexer::redis::RedisConnection;
use ipfs_indexer::{
    db, ipfs, logging, prom, queue, redis, CIDParts, CacheCheckResult, IpfsApiClient,
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

    let matches = Command::new("IPFS Indexer HAMTShards Worker")
        .version(clap::crate_version!())
        .author("Leo Balduf <leobalduf@gmail.com>")
        .about("IPFS indexer worker to process UnixFS HAMTShards.
Takes UnixFS HAMTShard blocks from an AMQP queue, lists their entries, and inserts metadata into a database.
Static configuration is taken from a .env file, see the README for more information.")
        .arg(arg!(--daemon <ADDRESS> "specifies the API URL of the IPFS daemon to use")
            .default_value("http://127.0.0.1:5001"))
        .get_matches();

    let failure_threshold = ipfs_indexer::failed_hamtshard_downloads_threshold_from_env()
        .context("unable to determine failed DAG downloads threshold")?;
    let download_timeout = ipfs_indexer::hamtshard_worker_ipfs_timeout_secs_from_env()
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
    let hamtshards_chan = rabbitmq_conn
        .create_channel()
        .await
        .context("unable to create RabbitMQ channel")?;
    let blocks_chan = Arc::new(
        rabbitmq_conn
            .create_channel()
            .await
            .context("unable to create RabbitMQ channel")?,
    );
    queue::set_prefetch(
        &hamtshards_chan,
        queue::Queues::HAMTShards
            .qos_from_env()
            .context("unable to load number of workers")?,
    )
    .await
    .context("unable to set queue prefetch")?;
    debug!("subscribing...");
    let mut hamtshards_consumer = queue::Queues::HAMTShards
        .subscribe(
            &hamtshards_chan,
            &format!("hamtshards_worker_{}", daemon_id.id),
        )
        .await
        .context("unable to subscribe to hamtshards queue")?;
    info!("set up RabbitMQ channels and consumer");

    info!("listening for tasks");
    while let Some(delivery) = hamtshards_consumer.next().await {
        let delivery = delivery.expect("error in consumer");
        debug!("got delivery {:?}", delivery);

        let redis_conn = redis_conn.clone();
        let blocks_chan = blocks_chan.clone();
        let daemon_uri = daemon_uri.clone();
        let db_conn = db_conn.clone();
        let ipfs_client = client.clone();

        tokio::spawn(async move {
            handle_delivery(
                delivery,
                daemon_uri,
                redis_conn,
                blocks_chan,
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
    blocks_chan: Arc<lapin::Channel>,
    db_conn: Arc<Mutex<PgConnection>>,
    ipfs_client: Arc<T>,
    failure_threshold: u64,
) where
    T: IpfsApi + Sync,
{
    let Delivery { data, acker, .. } = delivery;
    let msg = match queue::decode_hamtshard(&data) {
        Ok(cid) => cid,
        Err(err) => {
            warn!("unable to parse task DirectoryMessage, skipping: {:?}", err);
            return;
        }
    };

    let before = Instant::now();
    let outcome = handle_hamtshard(
        msg,
        &mut redis_conn,
        blocks_chan,
        db_conn,
        ipfs_client,
        failure_threshold,
    )
    .await;

    // Record in prometheus
    prom::record_task_duration(
        &*prom::HAMTSHARD_TASK_STATUS,
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
}

#[derive(Clone, Copy, Debug)]
enum FailureReason {
    DownloadFailed,
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
            },
            Done => "done",
            Failed(reason) => match reason {
                FailureReason::DownloadFailed => "download_failed",
            },
        }
    }
}

async fn handle_hamtshard<T>(
    msg: HamtShardMessage,
    redis_conn: &mut RedisConnection,
    blocks_chan: Arc<lapin::Channel>,
    db_conn: Arc<Mutex<PgConnection>>,
    ipfs_client: Arc<T>,
    failure_threshold: u64,
) -> TaskOutcome
where
    T: IpfsApi + Sync,
{
    let HamtShardMessage {
        cid: cid_parts,
        db_block,
    } = msg;
    let cid = format!("{}", cid_parts.cid);

    // Check caches
    debug!("{}: checking caches", cid);
    match ipfs_indexer::check_task_caches(
        ipfs_indexer::Task::HamtShard,
        &cid,
        db_block.id,
        redis::Cache::HAMTShards,
        redis_conn,
        db_conn.clone(),
        failure_threshold,
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
        CacheCheckResult::NeedsProcessing => {
            // We have to process it (again)
        }
    }

    // Check database for directory listing
    debug!("{}: checking database for directory listing", cid);
    let directory_entries = db::async_get_directory_listing(db_conn.clone(), db_block.id)
        .await
        .expect("unable to check directory listing in database");

    let entries = match directory_entries {
        Some(entries) => {
            // We have indexed this before -- just need to re-post tasks
            entries
                .into_iter()
                .map(|(entry, c, block, _)| (entry, c, block))
                .collect::<Vec<_>>()
        }
        None => {
            // If not present, download and index
            debug!("{}: doing fast ls", cid);

            let entries = match fast_index_and_insert(
                redis_conn,
                db_conn.clone(),
                ipfs_client,
                db_block.clone(),
                &cid,
                &cid_parts,
            )
            .await
            {
                Ok(entries) => match entries {
                    Ok(entries) => entries,
                    Err(res) => return Skipped(res),
                },
                Err(res) => {
                    db::async_insert_dag_download_failure(
                        db_conn.clone(),
                        db_block.id,
                        chrono::Utc::now(),
                    )
                    .await
                    .expect("unable to insert download failure into database");
                    debug!("{}: marked failed in database", cid);

                    redis_mark_failed(&cid, redis_conn).await;

                    return Failed(res);
                }
            };
            debug!(
                "{}: successfully fast-ls'ed and inserted links into database: {:?}",
                cid, entries
            );

            entries
        }
    };
    debug!("{}: database contains entries {:?}", cid, entries);

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

    Done
}

async fn fast_index_and_insert<T>(
    redis_conn: &mut RedisConnection,
    db_conn: Arc<Mutex<PgConnection>>,
    ipfs_client: Arc<T>,
    db_block: Block,
    cid: &String,
    cid_parts: &CIDParts,
) -> Result<Result<Vec<(DirectoryEntry, Cid, Block)>, SkipReason>, FailureReason>
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

    let listing_cids = listing
        .iter()
        .map(|(_, _, cidparts)| cidparts.clone())
        .collect();

    // Download block data of entire referenced DAG, in layers.
    // Skip directory entries. This is important, since we will submit those as block tasks later on anyway.
    // The whole point of fast-ls'ing is to _not_ download the linked files.
    debug!(
        "{}: downloading DAG metadata excluding directory entries",
        cid
    );
    let layers = match ipfs::download_dag_block_metadata(
        cid,
        cid_parts.codec,
        Some(listing_cids),
        ipfs_client.clone(),
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
        Some(layers),
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

    Ok(Ok(entries))
}

async fn redis_mark_failed(cid: &str, redis_conn: &mut RedisConnection) {
    match redis::Cache::HAMTShards
        .record_failure(cid, redis_conn)
        .await
    {
        Ok(_) => {
            debug!("{}: marked failed in redis hamtshards", cid);
        }
        Err(err) => {
            warn!("unable to update redis hamtshards cache: {:?}", err);
        }
    }
}

async fn redis_mark_done(cid: &str, redis_conn: &mut RedisConnection) {
    redis::mark_done_up_to_logging(cid, redis_conn, redis::Cache::HAMTShards).await
}
