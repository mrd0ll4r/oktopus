use crate::models::BlockLink;
use crate::{models, CIDParts};
use anyhow::Context;
use lapin::options::{
    BasicConsumeOptions, BasicPublishOptions, BasicQosOptions, QueueDeclareOptions,
};
use lapin::publisher_confirm::Confirmation;
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, Consumer};
use serde::{Deserialize, Serialize};
use std::env;
use strum::EnumIter;
use strum::IntoEnumIterator;

pub async fn connect(addr: &str) -> anyhow::Result<Connection> {
    let conn = Connection::connect(addr, ConnectionProperties::default()).await?;
    Ok(conn)
}

pub async fn connect_env() -> anyhow::Result<Connection> {
    dotenvy::dotenv().context("unable to read .env file")?;
    let broker_url = env::var("RABBITMQ_URL").context("missing RABBITMQ_URL")?;
    connect(&broker_url).await
}

pub async fn create_channel(conn: &Connection) -> anyhow::Result<Channel> {
    let chan = conn.create_channel().await?;
    Ok(chan)
}

pub async fn set_prefetch(c: &Channel, prefetch: u16) -> anyhow::Result<()> {
    c.basic_qos(prefetch, BasicQosOptions::default()).await?;
    Ok(())
}

#[derive(Debug, Copy, Clone, EnumIter)]
pub enum Queues {
    Cids,
    Blocks,
    Files,
    Directories,
    HAMTShards,
}

impl Queues {
    const fn queue_name(&self) -> &'static str {
        match self {
            Queues::Cids => "cids",
            Queues::Blocks => "blocks",
            Queues::Files => "files",
            Queues::Directories => "directories",
            Queues::HAMTShards => "hamtshards",
        }
    }

    pub fn qos_from_env(&self) -> anyhow::Result<u16> {
        dotenvy::dotenv().context("unable to read .env file")?;

        let key = match self {
            Queues::Cids => "INDEXER_CID_WORKER_CONCURRENCY",
            Queues::Blocks => "INDEXER_BLOCK_WORKER_CONCURRENCY",
            Queues::Files => "INDEXER_FILE_WORKER_CONCURRENCY",
            Queues::Directories => "INDEXER_DIRECTORY_WORKER_CONCURRENCY",
            Queues::HAMTShards => "INDEXER_HAMTSHARD_WORKER_CONCURRENCY",
        };

        let num_workers = env::var(key)
            .context(format!("missing {}", key))?
            .parse::<u16>()
            .context(format!("unable to parse {}", key))?;

        Ok(num_workers)
    }

    pub async fn set_up_queues(c: &Channel) -> anyhow::Result<()> {
        for name in Self::iter().map(|q| q.queue_name()) {
            c.queue_declare(
                name,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
        }

        Ok(())
    }

    async fn subscribe_exclusive(
        &self,
        c: &Channel,
        consumer_tag: &str,
    ) -> anyhow::Result<Consumer> {
        let consumer = c
            .basic_consume(
                self.queue_name(),
                consumer_tag,
                BasicConsumeOptions {
                    exclusive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
        Ok(consumer)
    }

    async fn subscribe_nonexclusive(
        &self,
        c: &Channel,
        consumer_tag: &str,
    ) -> anyhow::Result<Consumer> {
        let consumer = c
            .basic_consume(
                self.queue_name(),
                consumer_tag,
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;
        Ok(consumer)
    }

    pub async fn subscribe(&self, c: &Channel, consumer_tag: &str) -> anyhow::Result<Consumer> {
        match self {
            Queues::Cids => self.subscribe_exclusive(c, consumer_tag).await,
            _ => self.subscribe_nonexclusive(c, consumer_tag).await,
        }
    }

    async fn post_task(&self, c: &Channel, payload: &[u8]) -> anyhow::Result<Confirmation> {
        let res = c
            .basic_publish(
                "",
                self.queue_name(),
                BasicPublishOptions {
                    mandatory: true,
                    ..Default::default()
                },
                payload,
                BasicProperties::default().with_delivery_mode(2), // persistent
            )
            .await?
            .await?;
        Ok(res)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMessage {
    pub cid: CIDParts,
    pub db_block: models::Block,
}

pub async fn post_block(c: &Channel, msg: &BlockMessage) -> anyhow::Result<Confirmation> {
    let payload = serde_json::to_vec(&msg)?;
    let res = Queues::Blocks.post_task(c, &payload).await?;
    Ok(res)
}

pub fn decode_block(payload: &[u8]) -> anyhow::Result<BlockMessage> {
    let res = serde_json::from_slice(payload)?;
    Ok(res)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMessage {
    pub cid: CIDParts,
    pub db_block: models::Block,
}

pub async fn post_file(c: &Channel, msg: &FileMessage) -> anyhow::Result<Confirmation> {
    let payload = serde_json::to_vec(&msg)?;
    let res = Queues::Files.post_task(c, &payload).await?;
    Ok(res)
}

pub fn decode_file(payload: &[u8]) -> anyhow::Result<FileMessage> {
    let res = serde_json::from_slice(payload)?;
    Ok(res)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryMessage {
    pub cid: CIDParts,
    pub db_block: models::Block,
    pub db_links: Vec<BlockLink>,
}

pub async fn post_directory(c: &Channel, msg: &DirectoryMessage) -> anyhow::Result<Confirmation> {
    let payload = serde_json::to_vec(&msg)?;
    let res = Queues::Directories.post_task(c, &payload).await?;
    Ok(res)
}

pub fn decode_directory(payload: &[u8]) -> anyhow::Result<DirectoryMessage> {
    let res = serde_json::from_slice(payload)?;
    Ok(res)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HamtShardMessage {
    pub cid: CIDParts,
    pub db_block: models::Block,
}

pub async fn post_hamtshard(c: &Channel, msg: &HamtShardMessage) -> anyhow::Result<Confirmation> {
    let payload = serde_json::to_vec(&msg)?;
    let res = Queues::HAMTShards.post_task(c, &payload).await?;
    Ok(res)
}

pub fn decode_hamtshard(payload: &[u8]) -> anyhow::Result<HamtShardMessage> {
    let res = serde_json::from_slice(payload)?;
    Ok(res)
}

pub async fn post_cid(c: &Channel, cid: &str) -> anyhow::Result<Confirmation> {
    let payload = cid.as_bytes();
    let res = Queues::Cids.post_task(c, payload).await?;
    Ok(res)
}
