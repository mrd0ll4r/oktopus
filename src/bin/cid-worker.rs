use anyhow::{anyhow, Context};
use clap::Command;
use diesel::PgConnection;
use futures_util::StreamExt;
use ipfs_indexer::queue::BlockMessage;
use ipfs_indexer::redis::RedisConnection;
use ipfs_indexer::{db, logging, queue, redis};
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions};
use log::{debug, error, info, warn};
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::set_up_logging().unwrap();

    let _matches = Command::new("IPFS Indexer CIDs Worker")
        .version(clap::crate_version!())
        .author("Leo Balduf <leobalduf@gmail.com>")
        .about("IPFS indexer worker to process CIDs coming in from the outside world.
Takes CIDs from an AMQP queue, parses them, inserts block and CID information into a database, and posts block tasks back to AMQP.
Static configuration is taken from a .env file, see the README for more information.")
        .get_matches();

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
    let cids_chan = rabbitmq_conn
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
        &cids_chan,
        queue::Queues::Cids
            .qos_from_env()
            .context("unable to load number of workers")?,
    )
    .await
    .context("unable to set queue prefetch")?;
    debug!("subscribing...");
    let mut cids_consumer = queue::Queues::Cids
        .subscribe(&cids_chan, "cids_worker")
        .await
        .context("unable to subscribe to cids queue")?;
    info!("set up RabbitMQ queues");

    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    info!("listening for tasks");
    while let Some(delivery) = cids_consumer.next().await {
        let delivery = delivery.expect("error in consumer");
        debug!("got delivery {:?}", delivery);

        let redis_conn = redis_conn.clone();
        let blocks_chan = blocks_chan.clone();
        let db_conn = db_conn.clone();

        tokio::spawn(
            async move { handle_delivery(delivery, redis_conn, blocks_chan, db_conn).await },
        );
    }

    Ok(())
}

async fn handle_delivery(
    delivery: Delivery,
    mut redis_conn: RedisConnection,
    blocks_chan: Arc<lapin::Channel>,
    db_conn: Arc<Mutex<PgConnection>>,
) {
    let Delivery { data, acker, .. } = delivery;
    let cid = match String::from_utf8(data) {
        Ok(cid) => cid,
        Err(err) => {
            warn!("unable to parse task as UTF8 string, skipping: {:?}", err);
            return;
        }
    };

    match handle_cid(cid, &mut redis_conn, &blocks_chan, db_conn).await {
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

enum FailureOutcome {
    DbUpsertFailed,
    FailedToPostBlock,
}

enum PositiveOutcome {
    FailedToParse,
    SkippedNonFsRelated,
    RedisCached,
    Done,
}

async fn handle_cid(
    cid: String,
    redis_conn: &mut RedisConnection,
    blocks_chan: &lapin::Channel,
    db_conn: Arc<Mutex<PgConnection>>,
) -> Result<PositiveOutcome, FailureOutcome> {
    // Parse CID
    debug!("{}: parsing...", cid);
    let cid_parts = match ipfs_indexer::parse_cid_to_parts(&cid) {
        Ok(parts) => parts,
        Err(err) => {
            debug!("unable to parse CID {}: {:?}", cid, err);
            return Ok(PositiveOutcome::FailedToParse);
        }
    };
    debug!("{}: parsed as {:?}", cid, cid_parts);

    // Skip anything that's not DAG_PB or RAW
    if cid_parts.codec != ipfs_indexer::CODEC_DAG_PB && cid_parts.codec != ipfs_indexer::CODEC_RAW {
        debug!("{}: skipping non-filesystem CID", cid);
        return Ok(PositiveOutcome::SkippedNonFsRelated);
    }

    // Check redis
    debug!("{}: checking redis", cid);
    match redis::Cache::Cids.is_done(&cid, redis_conn).await {
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

    // Upsert CID into database
    debug!("{}: upserting...", cid);
    let (db_block, db_cid) = match db::async_upsert_block_and_cid(db_conn, cid_parts.clone()).await
    {
        Ok(res) => res,
        Err(err) => {
            error!("unable to upsert CID into database: {:?}", err);
            return Err(FailureOutcome::DbUpsertFailed);
        }
    };
    debug!(
        "{}: upserted, got block {:?}, cid {:?}",
        cid, db_block, db_cid
    );

    // Post block task to RabbitMQ
    debug!("{}: posting block task...", cid);
    match queue::post_block(
        blocks_chan,
        &BlockMessage {
            cid: cid_parts,
            db_block,
        },
    )
    .await
    {
        Ok(confirmation) => {
            debug!(
                "{}: posted block task, got confirmation {:?}",
                cid, confirmation
            )
        }
        Err(err) => {
            error!("unable to post block job: {:?}", err);
            return Err(FailureOutcome::FailedToPostBlock);
        }
    }

    // Update redis
    debug!("{}: marking done in redis...", cid);
    redis_mark_done(&cid, redis_conn).await;

    Ok(PositiveOutcome::Done)
}

async fn redis_mark_done(cid: &str, redis_conn: &mut RedisConnection) {
    match redis::Cache::Cids.mark_done(cid, redis_conn).await {
        Ok(_) => {
            debug!("{}: marked done in redis", cid);
        }
        Err(err) => {
            warn!("unable to update redis CIDs cache: {:?}", err)
        }
    }
}
