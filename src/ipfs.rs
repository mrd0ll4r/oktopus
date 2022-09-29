use crate::{models, parse_cid_to_parts, CIDParts};
use anyhow::{anyhow, Context};
use futures_util::TryStreamExt;
use ipfs_api_backend_hyper::response::IpfsHeader;
use ipfs_api_backend_hyper::IpfsApi;
use log::debug;
use prost::Message;
use std::sync::Arc;
use std::time::Instant;

pub async fn query_ipfs_for_file_data<T>(c: &str, client: Arc<T>) -> anyhow::Result<Vec<u8>>
where
    T: IpfsApi + Sync,
{
    client
        .cat(c)
        .map_ok(|chunk| chunk.to_vec())
        .try_concat()
        .await
        .map_err(|err| anyhow!("{}", err))
}

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
