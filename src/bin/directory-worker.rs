use anyhow::{anyhow, Context};
use clap::{arg, Command};
use diesel::PgConnection;
use futures_util::StreamExt;
use ipfs_api_backend_hyper::{IpfsApi, TryFromUri};
use ipfs_indexer::queue::{DirectoryMessage, FileMessage, HamtShardMessage};
use ipfs_indexer::redis::RedisConnection;
use ipfs_indexer::{db, ipfs, logging, models, queue, redis, CIDParts, IpfsApiClient};
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions};
use log::{debug, error, info, warn};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
            timeout: Some(Duration::from_secs(30)),
        },
    );
    let client = Arc::new(client_with_timeout);
    let daemon_id = client.id(None).await.context("unable to query IPFS API")?;
    info!(
        "connected to IPFS daemon version {} at {} with ID {:?}",
        daemon_id.agent_version, daemon_uri, daemon_id.id
    );

    debug!("connecting to database...");
    let mut conn = ipfs_indexer::establish_connection().context("unable to connect to DB")?;
    info!("connected to database");

    debug!("running pending migrations...");
    let migrations = ipfs_indexer::run_pending_migrations(&mut conn)
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
    debug!("creating channels and setting prefetch...");

    let directories_chan = rabbitmq_conn
        .create_channel()
        .await
        .context("unable to create RabbitMQ channel")?;
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
    info!("set up RabbitMQ queues");

    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    info!("listening for tasks");
    while let Some(delivery) = directories_consumer.next().await {
        let delivery = delivery.expect("error in consumer");
        debug!("got delivery {:?}", delivery);

        let redis_conn = redis_conn.clone();
        let files_chan = files_chan.clone();
        let directories_chan = directories_chan.clone();
        let hamtshards_chan = hamtshards_chan.clone();
        let db_conn = db_conn.clone();
        let ipfs_client = client.clone();

        tokio::spawn(async move {
            handle_delivery(
                delivery,
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
    let msg = match queue::decode_directory(&data) {
        Ok(cid) => cid,
        Err(err) => {
            warn!("unable to parse task DirectoryMessage, skipping: {:?}", err);
            return;
        }
    };

    match handle_directory(
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
            // TODO prometheus
            acker
                .ack(BasicAckOptions::default())
                .await
                .expect("unable to ACK delivery");
        }
        Err(outcome) => {
            // TODO prometheus
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

enum PositiveOutcome {
    RedisCached,
    RedisFailureCached,
    NoLinks,
    DbFailureThreshold,
    UnableToParseReferencedCids,
    Done,
}

enum ErrorOutcome {
    DbSelectFailed,
    DownloadFailed,
    DbUpsertFailed,
    DbFailureInsertFailed,
    FailedToPostTask,
}

async fn handle_directory<T>(
    msg: DirectoryMessage,
    redis_conn: &mut RedisConnection,
    files_chan: Arc<lapin::Channel>,
    directories_chan: Arc<lapin::Channel>,
    hamtshards_chan: Arc<lapin::Channel>,
    db_conn: Arc<Mutex<PgConnection>>,
    ipfs_client: Arc<T>,
    failure_threshold: u64,
) -> Result<PositiveOutcome, ErrorOutcome>
where
    T: IpfsApi + Sync,
{
    /*
        1. (optimization) Check Redis `directories` for CID -> skip to 11
    2. (optimization) Check Redis `failed_directories` counter -> if >= THRESHOLD skip to 11
    3. Check DB for failed downloads counter -> if >= THRESHOLD skip to  11
    4. (optimization) Check DB for directory entries -> skip to 9
    5. Full LS (gets immediate sub-blocks)
    6. For each:
       1. Get block level info (this should be fast because the blocks are prefetched)
       2. `object data` to get UnixFS type of entries
    7. If anything failed: Record in DB, record in Redis, requeue to RabbitMQ, return
    8. In one transaction:
       1. Insert block-level info for entries
       2. Insert directory entries with correct UnixFS type
    9. (optimization) Insert entry CIDs into Redis `cids` and `blocks`
    10. Push entries to `files`, `directories`, and `hamtshards`
    11. (optimization) Insert directory CID into Redis `directories` ,`blocks`, and `cids`
    12. ACK to RabbitMQ
       */
    let DirectoryMessage {
        cid: cid_parts,
        db_block,
        db_links,
    } = msg;
    let cid = format!("{}", cid_parts.cid);

    // Check redis
    debug!("{}: checking redis", cid);
    match redis::Cache::Directories.is_done(&cid, redis_conn).await {
        Ok(done) => {
            debug!("{}: redis status is done={}", cid, done);
            if done {
                // Refresh redis
                redis_mark_done(&cid, redis_conn).await;
                return Ok(PositiveOutcome::RedisCached);
            }
        }
        Err(err) => {
            warn!("unable to check redis CIDs cache: {:?}", err)
        }
    };

    // Check redis for failures
    debug!("{}: checking redis for failures", cid);
    match redis::Cache::Directories.num_failed(&cid, redis_conn).await {
        Ok(res) => {
            debug!("{}: redis failures is {:?}", cid, res);
            if let Some(failures) = res {
                if failures >= failure_threshold {
                    debug!("{}: too many failures, skipping", cid);
                    redis_mark_done(&cid, redis_conn).await;
                    return Ok(PositiveOutcome::RedisFailureCached);
                }
            }
        }
        Err(err) => {
            warn!("unable to check redis CIDs cache: {:?}", err)
        }
    }

    // Check database for failures
    debug!("{}: checking database for failures", cid);
    match db::async_get_failed_dag_downloads(db_conn.clone(), db_block.id).await {
        Ok(failures) => {
            debug!("{}: database failures is {:?}", cid, failures);
            if let Some(failures) = failures {
                if failures.len() as u64 >= failure_threshold {
                    debug!("{}: too many failures, skipping", cid);
                    redis_mark_done(&cid, redis_conn).await;
                    return Ok(PositiveOutcome::DbFailureThreshold);
                }
            }
        }
        Err(err) => {
            error!("unable to check block failures in database: {:?}", err);
            return Err(ErrorOutcome::DbSelectFailed);
        }
    }

    // Check if block has references
    if db_links.is_empty() {
        debug!("{}: has no links, skipping", cid);
        redis_mark_done(&cid, redis_conn).await;
        return Ok(PositiveOutcome::NoLinks);
    }

    // Check database for directory listing
    debug!("{}: checking database for directory listing", cid);
    let directory_entries =
        match db::async_get_directory_listing(db_conn.clone(), db_block.id).await {
            Ok(res) => res,
            Err(err) => {
                error!("unable to check directory listing in database: {:?}", err);
                return Err(ErrorOutcome::DbSelectFailed);
            }
        };

    let entries = match directory_entries {
        Some(entries) => entries,
        None => {
            // If not present, download and index
            debug!(
                "{}: no directory entries in database, attempting to download",
                cid
            );

            let listing =
                match ipfs::query_ipfs_for_directory_listing(&cid, ipfs_client.clone(), true).await
                {
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
                                error!(
                                    "unable to insert download failure into database: {:?}",
                                    err
                                );
                                return Err(ErrorOutcome::DbFailureInsertFailed);
                            }
                        }

                        redis_mark_failed(&cid, redis_conn).await;

                        return Err(ErrorOutcome::DownloadFailed);
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
                            return Ok(PositiveOutcome::UnableToParseReferencedCids);
                        }
                    },
                    Err(err) => {
                        debug!("{}: failed to download children blocks: {:?}", cid, err);

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
                                error!(
                                    "unable to insert download failure into database: {:?}",
                                    err
                                );
                                return Err(ErrorOutcome::DbFailureInsertFailed);
                            }
                        }

                        redis_mark_failed(&cid, redis_conn).await;

                        return Err(ErrorOutcome::DownloadFailed);
                    }
                };

                block_metadata.push(metadata);
            }
            let entries = listing
                .into_iter()
                .zip(block_metadata)
                .map(|((name, size, cidparts), metadata)| (name, size, cidparts, metadata))
                .collect();
            debug!("{}: augmented listing with stats {:?}", cid, entries);

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
                    return Err(ErrorOutcome::DbUpsertFailed);
                }
            };
            debug!("{}: upserted, got entries {:?}", cid, entries);

            entries
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
    for (entry, cidparts, block, block_stat, links) in entries.into_iter() {
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
        match post_job_res {
            Ok(confirmation) => {
                debug!("{}: posted task, got confirmation {:?}", cid, confirmation)
            }
            Err(err) => {
                error!("unable to post task: {:?}", err);
                return Err(ErrorOutcome::FailedToPostTask);
            }
        }
    }

    // Update redis
    debug!("{}: marking done in redis...", cid);
    redis_mark_done(&cid, redis_conn).await;

    Ok(PositiveOutcome::Done)
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
    match redis::Cache::Cids.mark_done(cid, redis_conn).await {
        Ok(_) => {
            debug!("{}: marked done in redis cids", cid);
        }
        Err(err) => {
            warn!("unable to update redis CIDs cache: {:?}", err)
        }
    }

    match redis::Cache::Blocks.mark_done(cid, redis_conn).await {
        Ok(_) => {
            debug!("{}: marked done in redis blocks", cid);
        }
        Err(err) => {
            warn!("unable to update redis blocks cache: {:?}", err)
        }
    }
}

async fn redis_mark_done(cid: &str, redis_conn: &mut RedisConnection) {
    match redis::Cache::Cids.mark_done(cid, redis_conn).await {
        Ok(_) => {
            debug!("{}: marked done in redis cids", cid);
        }
        Err(err) => {
            warn!("unable to update redis CIDs cache: {:?}", err)
        }
    }

    match redis::Cache::Blocks.mark_done(cid, redis_conn).await {
        Ok(_) => {
            debug!("{}: marked done in redis blocks", cid);
        }
        Err(err) => {
            warn!("unable to update redis blocks cache: {:?}", err)
        }
    }

    match redis::Cache::Directories.mark_done(cid, redis_conn).await {
        Ok(_) => {
            debug!("{}: marked done in redis directories", cid);
        }
        Err(err) => {
            warn!("unable to update redis directories cache: {:?}", err)
        }
    }
}
