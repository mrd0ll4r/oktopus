use anyhow::Context;
use clap::Command;
use ipfs_indexer::{logging, queue};
use log::{debug, info};
use tokio::io::{AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::set_up_logging().unwrap();

    let _matches = Command::new("IPFS indexer Tool to Post CIDs")
        .version(clap::crate_version!())
        .author("Leo Balduf <leobalduf@gmail.com>")
        .about(
            "IPFS indexer tool to post CIDs from the outside world.
Reads CIDs from Stdin and posts them to AMQP.
Configuration is taken from a .env file, see the README for details.",
        )
        .get_matches();

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
    info!("set up RabbitMQ queues");

    debug!("opening stdin for reading");
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    while let Some(cid) = lines.next_line().await? {
        debug!("read cid {}", cid);
        let confirmation = queue::post_cid(&cids_chan, &cid)
            .await
            .context("unable to post CID to RabbitMQ")?;
        debug!("got confirmation {:?}", confirmation);
    }

    cids_chan
        .close(0, "")
        .await
        .context("unable to close RabbitMQ channel")?;

    Ok(())
}
