use crate::TaskOutcome::{Done, Skipped};
use anyhow::Context;
use clap::Command;
use diesel::PgConnection;
use futures_util::StreamExt;
use ipfs_indexer::prom::OutcomeLabel;
use ipfs_indexer::queue::BlockMessage;
use ipfs_indexer::redis::RedisConnection;
use ipfs_indexer::{db, logging, prom, queue, redis, WorkerConnections};
use lapin::message::Delivery;
use lapin::options::BasicAckOptions;
use log::{debug, info, warn};
use std::sync::{Arc, Mutex};
use std::time::Instant;

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

    debug!("setting up worker connections");
    let WorkerConnections {
        db_conn,
        redis_conn,
        rabbitmq_conn,
    } = ipfs_indexer::worker_setup()
        .await
        .context("unable to set up connections")?;

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
    info!("set up RabbitMQ channels and consumers");

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

    let before = Instant::now();
    let outcome = handle_cid(cid, &mut redis_conn, &blocks_chan, db_conn).await;

    // Record in prometheus
    prom::record_task_duration_no_daemon(&*prom::CID_TASK_STATUS, outcome, before.elapsed());

    // Report to RabbitMQ
    acker
        .ack(BasicAckOptions::default())
        .await
        .expect("unable to ACK delivery");
}

#[derive(Clone, Copy, Debug)]
enum SkipReason {
    FailedToParse,
    SkippedNonFsRelated,
    RedisCached,
}

#[derive(Clone, Copy, Debug)]
enum TaskOutcome {
    Skipped(SkipReason),
    Done,
}

impl OutcomeLabel for TaskOutcome {
    fn status(&self) -> &'static str {
        match self {
            Skipped(_) => "skipped",
            Done => "done",
        }
    }

    fn reason(&self) -> &'static str {
        match self {
            Skipped(reason) => match reason {
                SkipReason::FailedToParse => "failed_to_parse",
                SkipReason::SkippedNonFsRelated => "skipped_non_fs_related",
                SkipReason::RedisCached => "redis_cached",
            },
            Done => "task_posted",
        }
    }
}

async fn handle_cid(
    cid: String,
    redis_conn: &mut RedisConnection,
    blocks_chan: &lapin::Channel,
    db_conn: Arc<Mutex<PgConnection>>,
) -> TaskOutcome {
    // Parse CID
    debug!("{}: parsing...", cid);
    let cid_parts = match ipfs_indexer::parse_cid_to_parts(&cid) {
        Ok(parts) => parts,
        Err(err) => {
            debug!("unable to parse CID {}: {:?}", cid, err);
            return Skipped(SkipReason::FailedToParse);
        }
    };
    debug!("{}: parsed as {:?}", cid, cid_parts);

    // Skip anything that's not DAG_PB or RAW
    if cid_parts.codec != ipfs_indexer::CODEC_DAG_PB && cid_parts.codec != ipfs_indexer::CODEC_RAW {
        debug!("{}: skipping non-filesystem CID", cid);
        return Skipped(SkipReason::SkippedNonFsRelated);
    }

    // Check redis
    debug!("{}: checking redis", cid);
    match redis::Cache::Cids.is_done(&cid, redis_conn).await {
        Ok(done) => {
            debug!("{}: redis status is done={}", cid, done);
            if done {
                // Refresh redis
                redis_mark_done(&cid, redis_conn).await;
                return Skipped(SkipReason::RedisCached);
            }
        }
        Err(err) => {
            warn!("unable to check redis CIDs cache: {:?}", err)
        }
    };

    // Upsert CID into database
    debug!("{}: upserting...", cid);
    let (db_block, db_cid) = db::async_upsert_block_and_cid(db_conn, cid_parts.clone())
        .await
        .expect("unable to upsert CID into database");
    debug!(
        "{}: upserted, got block {:?}, cid {:?}",
        cid, db_block, db_cid
    );

    // Post block task to RabbitMQ
    debug!("{}: posting block task...", cid);
    let confirmation = queue::post_block(
        blocks_chan,
        &BlockMessage {
            root_cid: cid.clone(),
            cid: cid_parts,
            db_block,
        },
    )
    .await
    .expect("unable to post block job");
    debug!(
        "{}: posted block task, got confirmation {:?}",
        cid, confirmation
    );

    // Update redis
    debug!("{}: marking done in redis...", cid);
    redis_mark_done(&cid, redis_conn).await;

    Done
}

async fn redis_mark_done(cid: &str, redis_conn: &mut RedisConnection) {
    redis::mark_done_up_to_logging(cid, redis_conn, redis::Cache::Cids).await;
}
