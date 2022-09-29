use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::IntCounterVec;
use std::collections::HashMap;
use std::net::SocketAddr;

lazy_static! {
    pub static ref BLOCK_DOWNlOADS: IntCounterVec = register_int_counter_vec!(
        "block_downloads",
        "TODO",
        &["daemon","status"]
    )
    .unwrap();

    pub static ref CIDS_PROCESSED: IntCounterVec = register_int_counter_vec!(
        "cids_processed",
        "TODO",
        &["daemon","status"]
    )
    .unwrap();
}


/// Starts a thread to serve prometheus metrics.
pub(crate) fn run_prometheus(addr: SocketAddr) -> Result<()> {
    prometheus_exporter::start(addr).context("can not start exporter")?;

    Ok(())
}