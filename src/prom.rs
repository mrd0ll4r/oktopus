use anyhow::Context;
use prometheus_exporter::prometheus::register_histogram_vec;
use prometheus_exporter::prometheus::HistogramVec;
use std::net::SocketAddr;
use std::ops::Deref;
use std::time::Duration;

lazy_static! {
    pub static ref BLOCK_TASK_STATUS: HistogramVec =
        register_histogram_vec!("block_task_status", "TODO", &["daemon", "status", "reason"])
            .unwrap();
    pub static ref FILE_TASK_STATUS: HistogramVec =
        register_histogram_vec!("file_task_status", "TODO", &["daemon", "status", "reason"])
            .unwrap();
    pub static ref DIRECTORY_TASK_STATUS: HistogramVec = register_histogram_vec!(
        "directory_task_status",
        "TODO",
        &["daemon", "status", "reason"]
    )
    .unwrap();
    pub static ref HAMTSHARD_TASK_STATUS: HistogramVec = register_histogram_vec!(
        "hamtshard_task_status",
        "TODO",
        &["daemon", "status", "reason"]
    )
    .unwrap();
    pub static ref CID_TASK_STATUS: HistogramVec =
        register_histogram_vec!("cid_task_status", "TODO", &["status", "reason"]).unwrap();
    pub static ref NOTIFICATION_TASK_STATUS: HistogramVec =
        register_histogram_vec!("notification_task_status", "TODO", &["status", "reason"]).unwrap();
    pub static ref DB_METHOD_CALL_DURATIONS: HistogramVec =
        register_histogram_vec!("db_method_call_durations", "TODO", &["method"]).unwrap();
    pub static ref IPFS_METHOD_CALL_DURATIONS: HistogramVec =
        register_histogram_vec!("ipfs_method_call_durations", "TODO", &["method"]).unwrap();
}

pub trait OutcomeLabel {
    fn status(&self) -> &'static str;
    fn reason(&self) -> &'static str;
}

pub fn record_task_duration<H, O>(hist: H, outcome: O, duration: Duration, daemon: &str)
where
    H: Deref<Target = HistogramVec>,
    O: OutcomeLabel,
{
    hist.get_metric_with_label_values(&[daemon, outcome.status(), outcome.reason()])
        .unwrap()
        .observe(duration.as_secs_f64())
}

pub fn record_task_duration_no_daemon<H, O>(hist: H, outcome: O, duration: Duration)
where
    H: Deref<Target = HistogramVec>,
    O: OutcomeLabel,
{
    hist.get_metric_with_label_values(&[outcome.status(), outcome.reason()])
        .unwrap()
        .observe(duration.as_secs_f64())
}

/// Starts a thread to serve prometheus metrics.
pub fn run_prometheus(addr: SocketAddr) -> anyhow::Result<()> {
    prometheus_exporter::start(addr).context("can not start exporter")?;

    Ok(())
}
