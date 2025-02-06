use crate::NotificationType::SuccessfulFile;
use crate::TaskOutcome::{Done, Failed};
use anyhow::{ensure, Context};
use clap::{arg, Command};
use futures_util::StreamExt;
use ipfs_indexer::hash::AlternativeCids;
use ipfs_indexer::prom::OutcomeLabel;
use ipfs_indexer::queue::FinishedFileMessage;
use ipfs_indexer::{logging, prom, queue};
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions};
use log::{debug, error, info, warn};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Debug, Deserialize)]
struct Config {
    pub rabbitmq_uri: String,
    pub endpoint: String,
    pub health_endpoint: String,
    pub daemons: Vec<DaemonConfig>,
}

impl Config {
    async fn load_from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let path = path.as_ref();
        info!("Loading configuration from {}", path.display());

        let res = tokio::fs::read(path)
            .await
            .context("unable to read configuration file")?;
        let cfg = serde_yaml_ng::from_slice::<Config>(&res)
            .context("unable to deserialize configuration")?;

        Ok(cfg)
    }
}

#[derive(Clone, Debug, Deserialize)]
struct DaemonConfig {
    pub host: String,
    pub downloads_root_dir: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::set_up_logging().unwrap();

    let matches = Command::new("IPFS Indexer Finished File Notifier")
        .version(clap::crate_version!())
        .author("Leo Balduf <leobalduf@gmail.com>")
        .arg(arg!(--config <PATH> "specifies the path to the config file to read")
            .default_value("config.yaml")
            .required(true))
        .about("IPFS indexer worker to receive messages about finished file downloads and notify the outside world.")
        .get_matches();

    debug!("reading config file...");
    let config_path = matches
        .get_one::<String>("config")
        .expect("missing required config arg");
    let cfg = Config::load_from_file(config_path)
        .await
        .context("unable to load configuration")?;
    info!("read configuration");
    debug!("config: {:?}", cfg);

    // Make sure we have absolute paths for the storage locations
    let daemon_configs = {
        let configs = cfg.daemons.clone();
        let mut tmp = HashMap::new();
        for cfg in configs.iter() {
            let p = std::path::Path::new(&cfg.downloads_root_dir);
            ensure!(p.is_dir(), "{} is not a directory", cfg.downloads_root_dir);

            let abs_path = std::path::absolute(p).context("unable to calculate absolute path")?;

            ensure!(
                tmp.insert(cfg.host.clone(), abs_path).is_none(),
                "duplicate daemon configuration for {}",
                cfg.host
            );
        }
        Arc::new(tmp)
    };

    debug!("connecting to RabbitMQ...");
    let rabbitmq_conn = queue::connect(&cfg.rabbitmq_uri)
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
    let chan = rabbitmq_conn
        .create_channel()
        .await
        .context("unable to create RabbitMQ channel")?;
    queue::set_prefetch(&chan, 10)
        .await
        .context("unable to set queue prefetch")?;
    debug!("subscribing...");
    let mut consumer = queue::Queues::FinishedFiles
        .subscribe(&chan, "notifier")
        .await
        .context("unable to subscribe to finished files queue")?;
    info!("set up RabbitMQ channels and consumers");

    debug!("connecting to notification endpoint...");
    let client = reqwest::Client::new();
    let remote = cfg.endpoint.parse::<Url>().context("invalid endpoint")?;
    let health_remote = cfg
        .health_endpoint
        .parse::<Url>()
        .context("invalid health endpoint")?;
    client
        .get(health_remote)
        .send()
        .await
        .context("unable to query endpoint health check")?;

    info!("listening for notifications");
    while let Some(delivery) = consumer.next().await {
        let delivery = delivery.expect("error in consumer");
        debug!("got delivery {:?}", delivery);

        let daemon_configs = daemon_configs.clone();
        let remote = remote.clone();
        let client = client.clone();

        tokio::spawn(
            async move { handle_delivery(delivery, daemon_configs, client, remote).await },
        );
    }

    Ok(())
}

async fn handle_delivery(
    delivery: Delivery,
    daemon_configs: Arc<HashMap<String, PathBuf>>,
    http_client: reqwest::Client,
    remote: Url,
) {
    let Delivery { data, acker, .. } = delivery;
    let msg = match queue::decode_finished_file(&data) {
        Ok(msg) => msg,
        Err(err) => {
            warn!("unable to parse finished file message, skipping: {:?}", err);
            acker
                .nack(BasicNackOptions::default())
                .await
                .expect("unable to NACK delivery");
            return;
        }
    };

    let before = Instant::now();
    let outcome = handle_finished_file(msg, daemon_configs, http_client, remote).await;

    // Record in prometheus
    prom::record_task_duration_no_daemon(
        &*prom::NOTIFICATION_TASK_STATUS,
        outcome,
        before.elapsed(),
    );

    // Report to RabbitMQ
    match &outcome {
        Done => {
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
enum FailureReason {
    InvalidDaemon,
    FailedToPost,
}

#[derive(Clone, Copy, Debug)]
enum TaskOutcome {
    Failed(FailureReason),
    Done,
}

impl OutcomeLabel for TaskOutcome {
    fn status(&self) -> &'static str {
        match self {
            Failed(_) => "failed",
            Done => "done",
        }
    }

    fn reason(&self) -> &'static str {
        match self {
            Failed(reason) => match reason {
                FailureReason::InvalidDaemon => "invalid_daemon",
                FailureReason::FailedToPost => "failed_to_post",
            },
            Done => "task_posted",
        }
    }
}

async fn handle_finished_file(
    msg: FinishedFileMessage,
    daemon_configs: Arc<HashMap<String, PathBuf>>,
    http_client: reqwest::Client,
    remote: Url,
) -> TaskOutcome {
    debug!("file download finished: {:?}", msg);

    let download_path = if let Some(storage_path) = &msg.storage_path {
        let root_dir = match daemon_configs
            .get(msg.daemon_name.as_str())
            .map(|c| c.clone())
        {
            Some(root_dir) => root_dir,
            None => {
                return TaskOutcome::Failed(FailureReason::InvalidDaemon);
            }
        };
        let downloaded_path = PathBuf::from(root_dir).join(storage_path);
        debug!("calculated download path: {}", downloaded_path.display());
        Some(format!("{}", downloaded_path.display()))
    } else {
        None
    };

    let notification = Notification {
        original_input_cid: msg.root_cid,
        cid: msg.cid.cid.to_string(),
        record: SuccessfulFile(SuccessfulFileNotification {
            download_finished_ts: msg.download_finished_ts,
            file_size: msg.file_size,
            freedesktop_mime_type: msg.freedesktop_mime_type,
            libmime_mime_type: msg.libmime_mime_type,
            sha256_hash: hex::encode(&msg.sha256_hash),
            alternative_cids: msg.alternative_cids,
            storage_path: download_path,
            downloaded_by_daemon: msg.daemon_name,
        }),
    };
    debug!("notification: {:?}", notification);

    match http_client.post(remote).json(&notification).send().await {
        Ok(_) => Done,
        Err(err) => {
            error!("error sending notification: {:?}", err);
            Failed(FailureReason::FailedToPost)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Notification {
    pub original_input_cid: String,
    pub cid: String,
    pub record: NotificationType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum NotificationType {
    SuccessfulFile(SuccessfulFileNotification),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SuccessfulFileNotification {
    pub download_finished_ts: chrono::DateTime<chrono::Utc>,
    pub file_size: i64,
    pub freedesktop_mime_type: String,
    pub libmime_mime_type: String,
    pub sha256_hash: String,
    pub alternative_cids: AlternativeCids,
    pub storage_path: Option<String>,
    pub downloaded_by_daemon: String,
}
