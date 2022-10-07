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
use log::{debug, error, info, warn};
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
    match handle_hamtshard(
        msg,
        &mut redis_conn,
        blocks_chan,
        db_conn,
        ipfs_client,
        failure_threshold,
    )
    .await
    {
        Ok(outcome) => {
            // Record in prometheus
            prom::record_task_duration(
                &*prom::HAMTSHARD_TASK_STATUS,
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
                &*prom::HAMTSHARD_TASK_STATUS,
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
            Failure::DbFailureInsertFailed => "db_failure_insert_failed",
            Failure::FailedToPostTask => "failed_to_post_task",
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
) -> Result<Success, Failure>
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

    // Check database for directory listing
    debug!("{}: checking database for directory listing", cid);
    let directory_entries =
        match db::async_get_directory_listing(db_conn.clone(), db_block.id).await {
            Ok(res) => res,
            Err(err) => {
                error!("unable to check directory listing in database: {:?}", err);
                return Err(Failure::DbSelectFailed);
            }
        };

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

            let entries =
                match fast_index_and_insert(redis_conn, db_conn, ipfs_client, db_block, &cid).await
                {
                    Ok(entries) => entries,
                    Err(res) => return res,
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
        match queue::post_block(
            &blocks_chan,
            &BlockMessage {
                cid: cidparts,
                db_block: block,
            },
        )
        .await
        {
            Ok(confirmation) => {
                debug!(
                    "{}/{}: posted task, got confirmation {:?}",
                    cid, child_cid, confirmation
                )
            }
            Err(err) => {
                error!("unable to post task: {:?}", err);
                return Err(Failure::FailedToPostTask);
            }
        }
    }

    // Update redis
    debug!("{}: marking done in redis...", cid);
    redis_mark_done(&cid, redis_conn).await;

    Ok(Success::Done)
}

async fn fast_index_and_insert<T>(
    redis_conn: &mut RedisConnection,
    db_conn: Arc<Mutex<PgConnection>>,
    ipfs_client: Arc<T>,
    db_block: Block,
    cid: &String,
) -> Result<Vec<(DirectoryEntry, Cid, Block)>, Result<Success, Failure>>
where
    T: IpfsApi + Sync,
{
    let listing =
        match ipfs::query_ipfs_for_directory_listing(&cid, ipfs_client.clone(), false).await {
            Ok(listing) => listing,
            Err(err) => {
                debug!("{}: failed to list directory: {:?}", cid, err);

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
                        return Err(Err(Failure::DbFailureInsertFailed));
                    }
                }

                redis_mark_failed(&cid, redis_conn).await;

                return Err(Err(Failure::DownloadFailed));
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
    let entries = match db::async_upsert_successful_directory(
        db_conn.clone(),
        db_block.id,
        entries,
        chrono::Utc::now(),
    )
    .await
    {
        Ok(res) => res,
        Err(err) => {
            error!("unable to upsert block metadata into database: {:?}", err);
            return Err(Err(Failure::DbUpsertFailed));
        }
    };
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
    match redis::Cache::HAMTShards
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

async fn redis_mark_done(cid: &str, redis_conn: &mut RedisConnection) {
    redis::mark_done_up_to_logging(cid, redis_conn, redis::Cache::HAMTShards).await
}
