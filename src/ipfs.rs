use crate::{ipfs, models, parse_cid_to_parts, CIDParts};
use anyhow::{anyhow, Context};
use futures_util::TryStreamExt;
use ipfs_api_backend_hyper::response::IpfsHeader;
use ipfs_api_backend_hyper::IpfsApi;
use log::{debug, trace, warn};
use prost::Message;
use reqwest::Url;
use std::borrow::BorrowMut;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;

pub async fn download_dag_block_metadata<T>(
    cid: &str,
    codec: u64,
    cids_to_skip: Option<Vec<CIDParts>>,
    ipfs_client: Arc<T>,
) -> anyhow::Result<Result<Vec<Vec<(CIDParts, BlockLevelMetadata)>>, ParseReferencedCidFailed>>
where
    T: IpfsApi + Sync,
{
    debug!(
        "downloading dag block metadata for CID {} with skip list {:?}",
        cid, cids_to_skip
    );

    let skip_set = cids_to_skip
        .unwrap_or_else(Vec::new)
        .into_iter()
        .map(|c| c.cid)
        .collect::<HashSet<_>>();
    let mut layers = Vec::new();
    let root_metadata = query_ipfs_for_block_level_data(&cid, codec, ipfs_client.clone())
        .await?
        .expect("unable to parse children CIDs of block already present in database");
    debug!("{}: got root metadata {:?}", cid, root_metadata);

    let mut current_layer = Some(vec![root_metadata.links]);
    while let Some(layer) = current_layer.take() {
        if layer.is_empty() {
            break;
        }
        let mut wip_layer = Vec::new();
        for (_, _, child_cidparts) in layer.into_iter().flatten() {
            if skip_set.contains(&child_cidparts.cid) {
                continue;
            }

            let child_cid = format!("{}", child_cidparts.cid);
            let child_metadata = match ipfs::query_ipfs_for_block_level_data(
                &child_cid,
                child_cidparts.codec,
                ipfs_client.clone(),
            )
            .await?
            {
                Ok(metadata) => metadata,
                Err(_) => {
                    debug!("{}: failed to parse referenced CID of child blocks", cid);
                    return Ok(Err(()));
                }
            };
            wip_layer.push((child_cidparts, child_metadata));
        }

        current_layer = Some(
            wip_layer
                .iter()
                .map(|(_, metadata)| metadata.links.clone())
                .collect(),
        );
        layers.push(wip_layer);
    }

    Ok(Ok(layers))
}

pub fn build_gateway_url(base_url: &Url, cid: &str) -> Url {
    let mut url = base_url.clone();
    url.set_path(format!("/ipfs/{}", cid).as_str());
    url
}

// Returns Ok(Ok(size)) if the download succeeded, Ok(Err(size)) if it was skipped due to being too
// large, and Err(err) if it failed.
pub async fn query_ipfs_for_file_data(
    c: &str,
    client: reqwest::Client,
    gateway_base_url: &Url,
    file_size_limit: u64,
    head_timeout: Duration,
    download_timeout: Duration,
    file_path: &Path,
) -> anyhow::Result<std::result::Result<i64, i64>> {
    let url = build_gateway_url(gateway_base_url, c);
    debug!("{}: requesting via IPFS gateway at {}...", c, url);

    let _timer = crate::prom::IPFS_METHOD_CALL_DURATIONS
        .get_metric_with_label_values(&["gateway_get"])
        .unwrap()
        .start_timer();

    // Start request, wait for response headers.
    // This usually means that IPFS was able to find the data (or at least the first block or layer)
    let response = {
        let _head_timer = crate::prom::IPFS_METHOD_CALL_DURATIONS
            .get_metric_with_label_values(&["gateway_head"])
            .unwrap()
            .start_timer();

        let request = client.get(url).send();
        match tokio::time::timeout(head_timeout, request).await {
            Ok(result) => result
                .context("unable to perform request")?
                .error_for_status()
                .context("request failed")?,
            Err(_) => return Err(anyhow!("resolve/HEAD timeout")),
        }
    };
    debug!("{}: got initial response from gateway: {:?}", c, response);
    let advertised_file_size = response.content_length();
    if let Some(size) = advertised_file_size {
        if size > file_size_limit {
            return Ok(Err(size as i64));
        }
    }

    debug!("{}: starting download via gateway", c);
    let file_size = perform_gateway_download(c, response, download_timeout, file_path)
        .await
        .map_err(|e| {
            // Try to remove the file if it exists already, on a best-effort basis.
            if let Err(e) = std::fs::remove_file(file_path) {
                warn!(
                    "{}: unable to remove incomplete temp file {:?}: {:?}",
                    c, file_path, e
                )
            }

            e
        })?;

    Ok(Ok(file_size))
}

async fn perform_gateway_download(
    c: &str,
    mut response: reqwest::Response,
    download_timeout: Duration,
    file_path: &Path,
) -> anyhow::Result<i64> {
    let mut file = tokio::fs::File::create(file_path)
        .await
        .context("unable to create file")?;
    let download_deadline = tokio::time::Instant::now() + download_timeout;
    let mut n = 0;

    loop {
        match tokio::time::timeout_at(download_deadline, response.chunk()).await {
            Ok(result) => {
                if let Some(mut chunk) = result.context("unable to read response body")? {
                    trace!("{}: received chunk of {} bytes", c, chunk.len());
                    n += chunk.len() as i64;
                    file.write_all_buf(chunk.borrow_mut())
                        .await
                        .context("unable to write temp file")?;
                } else {
                    // HTTP response completely consumed
                    break;
                }
            }
            Err(_) => {
                // Timeout
                return Err(anyhow!("download/GET timeout"));
            }
        }
    }

    Ok(n)
}

// TODO correctly propagate failing to parse referenced CIDs
pub async fn query_ipfs_for_directory_listing<T>(
    cid: &str,
    ipfs_client: Arc<T>,
    full: bool,
) -> anyhow::Result<Vec<(String, i64, CIDParts)>>
where
    T: IpfsApi + Sync,
{
    let ts_before = Instant::now();
    let res = if full {
        debug!("querying IPFS for full ls");
        ipfs_client
            .ls(cid)
            .await
            .map(|v| v.objects)
            .map_err(|err| anyhow!("{}", err))
            .context("unable to get directory listing")?
    } else {
        debug!("querying IPFS for fast ls");
        ipfs_client
            .ls_with_options(ipfs_api_backend_hyper::request::Ls {
                path: cid,
                stream: Some(true),
                resolve_type: Some(false),
                size: Some(false),
            })
            .map_err(|err| anyhow!("{}", err))
            .map_ok(|v| v.objects)
            .try_concat()
            .await
            .context("unable to get directory listing")?
    };
    let elapsed = ts_before.elapsed();
    crate::prom::IPFS_METHOD_CALL_DURATIONS
        .get_metric_with_label_values(&[if full { "ls_full" } else { "ls_fast" }])
        .unwrap()
        .observe(elapsed.as_secs_f64());

    res.into_iter()
        .flat_map(|entry| entry.links.into_iter())
        .map(|entry| {
            let cidparts = parse_cid_to_parts(&entry.hash)?;
            Ok((entry.name, entry.size as i64, cidparts))
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct BlockLevelMetadata {
    pub block_size: i32,
    pub unixfs_type_id: i32,
    // Links, (name, size, CID)
    pub links: Vec<(String, i64, CIDParts)>,
}

pub type ParseReferencedCidFailed = ();

pub async fn query_ipfs_for_block_level_data<T>(
    cid: &str,
    codec: u64,
    ipfs_client: Arc<T>,
) -> anyhow::Result<Result<BlockLevelMetadata, ParseReferencedCidFailed>>
where
    T: IpfsApi + Sync,
{
    let metadata = query_ipfs_for_block(cid, codec, ipfs_client).await?;

    // Translate UnixFS type
    let unixfs_type_id = if let Some(unixfs_metadata) = &metadata.dag_data {
        match unixfs_metadata.unixfs_type {
            crate::unixfs::data::DataType::Symlink => models::UNIXFS_TYPE_SYMLINK_ID,
            crate::unixfs::data::DataType::Metadata => models::UNIXFS_TYPE_METADATA_ID,
            crate::unixfs::data::DataType::File => models::UNIXFS_TYPE_FILE_ID,
            crate::unixfs::data::DataType::Directory => models::UNIXFS_TYPE_DIRECTORY_ID,
            crate::unixfs::data::DataType::Raw => models::UNIXFS_TYPE_RAW_ID,
            crate::unixfs::data::DataType::HamtShard => models::UNIXFS_TYPE_HAMT_SHARD_ID,
        }
    } else {
        models::UNIXFS_TYPE_RAW_ID
    };

    // Extract and translate links
    let links = match metadata
        .dag_data
        .map_or_else(Vec::new, |l| l.links.or_else(|| Some(Vec::new())).unwrap())
        .into_iter()
        .map(|h| {
            let cid_parts = crate::parse_cid_to_parts(&h.hash)?;
            Ok((h.name, h.size as i64, cid_parts))
        })
        .collect::<anyhow::Result<Vec<_>>>()
    {
        Ok(links) => links,
        Err(err) => {
            debug!("{}: failed to parse referenced CID: {:?}", cid, err);
            return Ok(Err(()));
        }
    };

    Ok(Ok(BlockLevelMetadata {
        block_size: metadata.block_size,
        unixfs_type_id,
        links,
    }))
}

#[derive(Debug)]
struct InternalBlockLevelMetadata {
    block_size: i32,
    dag_data: Option<DagLevelMetadata>,
}

#[derive(Debug)]
struct DagLevelMetadata {
    links: Option<Vec<IpfsHeader>>,
    unixfs_type: crate::unixfs::data::DataType,
}

async fn query_ipfs_for_block<T>(
    cid: &str,
    codec: u64,
    ipfs_client: Arc<T>,
) -> anyhow::Result<InternalBlockLevelMetadata>
where
    T: IpfsApi + Sync,
{
    debug!("{}: block stat", cid);
    let ts_before = Instant::now();
    let block_stat_resp = ipfs_client
        .block_stat(cid)
        .await
        .map_err(|err| anyhow!("{}", err))
        .context("unable to block stat")?;
    let dur_block_stat = ts_before.elapsed();
    debug!(
        "{}: block stat took {:?}, block size: {}",
        cid, dur_block_stat, block_stat_resp.size
    );
    crate::prom::IPFS_METHOD_CALL_DURATIONS
        .get_metric_with_label_values(&["block_stat"])
        .unwrap()
        .observe(dur_block_stat.as_secs_f64());

    if codec == crate::CODEC_RAW {
        // raw blocks have no links or extra data
        return Ok(InternalBlockLevelMetadata {
            block_size: block_stat_resp.size as i32,
            dag_data: None,
        });
    }

    debug!("{}: object get", cid);
    let ts_before = Instant::now();
    let object_get_resp = ipfs_client
        .object_get(cid)
        .await
        .map_err(|err| anyhow!("{}", err))
        .context("unable to object get")?;
    let dur_object_get = ts_before.elapsed();
    debug!(
        "{}: object get took {:?}, got {} links",
        cid,
        dur_object_get,
        object_get_resp.links.len()
    );
    crate::prom::IPFS_METHOD_CALL_DURATIONS
        .get_metric_with_label_values(&["object_get"])
        .unwrap()
        .observe(dur_object_get.as_secs_f64());

    debug!("{}: object data", cid);
    let ts_before = Instant::now();
    let object_data_resp = ipfs_client
        .object_data(cid)
        .map_ok(|chunk| chunk.to_vec())
        .try_concat()
        .await
        .map_err(|err| anyhow!("{}", err))
        .context("unable to object data")?;
    let dur_object_data = ts_before.elapsed();
    debug!(
        "{}: object data took {:?}, got {} bytes",
        cid,
        dur_object_data,
        object_data_resp.len()
    );
    crate::prom::IPFS_METHOD_CALL_DURATIONS
        .get_metric_with_label_values(&["object_data"])
        .unwrap()
        .observe(dur_object_data.as_secs_f64());

    debug!("{}: decoding protobuf", cid);
    let unixfs_block =
        crate::unixfs::Data::decode(&object_data_resp[..]).context("unable to decode protobuf")?;
    debug!("{}: got type {:?}", cid, unixfs_block.r#type());

    Ok(InternalBlockLevelMetadata {
        block_size: block_stat_resp.size as i32,
        dag_data: Some(DagLevelMetadata {
            links: if object_get_resp.links.is_empty() {
                None
            } else {
                Some(object_get_resp.links)
            },
            unixfs_type: unixfs_block.r#type(),
        }),
    })
}
