use crate::cache::MimeTypeCache;
use crate::diesel::BelongingToDsl;
use crate::hash::NormalizedAlternativeCid;
use crate::ipfs::BlockLevelMetadata;
pub use crate::models::*;
use crate::CIDParts;
use diesel::{
    BoolExpressionMethods, GroupedBy, OptionalExtension, PgConnection, QueryDsl, RunQueryDsl,
};
use diesel::{Connection, ExpressionMethods};
use log::debug;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::task::JoinError;

#[derive(Error, Debug)]
pub enum AsyncDBError {
    #[error("database returned error")]
    Database(diesel::result::Error),

    #[error("runtime returned error")]
    Runtime(JoinError),
}

pub async fn async_upsert_successful_block(
    conn: Arc<Mutex<PgConnection>>,
    p_block_id: i64,
    block_size: i32,
    unixfs_type_id: i32,
    links: Vec<(String, i64, CIDParts)>,
    ts: chrono::DateTime<chrono::Utc>,
) -> Result<(BlockStat, Vec<BlockLink>), AsyncDBError> {
    let _timer = crate::prom::DB_METHOD_CALL_DURATIONS
        .get_metric_with_label_values(&["upsert_successful_block"])
        .unwrap()
        .start_timer();

    tokio::task::spawn_blocking(move || {
        let mut conn = conn.lock().unwrap();

        upsert_successful_block(&mut conn, p_block_id, block_size, unixfs_type_id, links, ts)
    })
    .await
    .map_err(AsyncDBError::Runtime)?
    .map_err(AsyncDBError::Database)
}

pub async fn async_upsert_successful_directory(
    conn: Arc<Mutex<PgConnection>>,
    p_block_id: i64,
    entries: Vec<(String, i64, CIDParts, Option<BlockLevelMetadata>)>,
    ts: chrono::DateTime<chrono::Utc>,
) -> Result<
    Vec<(
        DirectoryEntry,
        Cid,
        Block,
        Option<(BlockStat, Vec<BlockLink>)>,
    )>,
    AsyncDBError,
> {
    let _timer = crate::prom::DB_METHOD_CALL_DURATIONS
        .get_metric_with_label_values(&["upsert_successful_directory"])
        .unwrap()
        .start_timer();

    tokio::task::spawn_blocking(move || {
        let mut conn = conn.lock().unwrap();

        upsert_successful_directory(&mut conn, p_block_id, entries, ts)
    })
    .await
    .map_err(AsyncDBError::Runtime)?
    .map_err(AsyncDBError::Database)
}

pub async fn async_upsert_successful_file(
    conn: Arc<Mutex<PgConnection>>,
    mime_cache: Arc<Mutex<MimeTypeCache>>,
    p_block_id: i64,
    mime_type: &'static str,
    file_size: i64,
    sha256_hash: Vec<u8>,
    alternative_cids: Vec<NormalizedAlternativeCid>,
    dag: Vec<Vec<(CIDParts, BlockLevelMetadata)>>,
    ts: chrono::DateTime<chrono::Utc>,
) -> Result<(), AsyncDBError> {
    let _timer = crate::prom::DB_METHOD_CALL_DURATIONS
        .get_metric_with_label_values(&["upsert_successful_file"])
        .unwrap()
        .start_timer();

    tokio::task::spawn_blocking(move || {
        let mut conn = conn.lock().unwrap();
        let mut cache = mime_cache.lock().unwrap();

        upsert_successful_file(
            &mut conn,
            &mut cache,
            p_block_id,
            mime_type,
            file_size,
            sha256_hash,
            alternative_cids,
            dag,
            ts,
        )
    })
    .await
    .map_err(AsyncDBError::Runtime)?
    .map_err(AsyncDBError::Database)
}

pub async fn async_insert_block_download_failure(
    conn: Arc<Mutex<PgConnection>>,
    block_id: i64,
    ts: chrono::DateTime<chrono::Utc>,
) -> Result<(), AsyncDBError> {
    let _timer = crate::prom::DB_METHOD_CALL_DURATIONS
        .get_metric_with_label_values(&["insert_block_download_failure"])
        .unwrap()
        .start_timer();

    async_insert_download_failure_idempotent(conn, block_id, DOWNLOAD_TYPE_BLOCK_ID, ts).await
}

pub async fn async_insert_dag_download_failure(
    conn: Arc<Mutex<PgConnection>>,
    block_id: i64,
    ts: chrono::DateTime<chrono::Utc>,
) -> Result<(), AsyncDBError> {
    let _timer = crate::prom::DB_METHOD_CALL_DURATIONS
        .get_metric_with_label_values(&["insert_dag_download_failure"])
        .unwrap()
        .start_timer();

    async_insert_download_failure_idempotent(conn, block_id, DOWNLOAD_TYPE_DAG_ID, ts).await
}

pub async fn async_upsert_block_and_cid(
    conn: Arc<Mutex<PgConnection>>,
    cid: CIDParts,
) -> Result<(Block, Cid), AsyncDBError> {
    let _timer = crate::prom::DB_METHOD_CALL_DURATIONS
        .get_metric_with_label_values(&["upsert_block_and_cid"])
        .unwrap()
        .start_timer();

    tokio::task::spawn_blocking(move || {
        let mut conn = conn.lock().unwrap();
        upsert_block_and_cid(&mut conn, cid)
    })
    .await
    .map_err(AsyncDBError::Runtime)?
    .map_err(AsyncDBError::Database)
}

pub async fn async_get_directory_listing(
    conn: Arc<Mutex<PgConnection>>,
    p_block_id: i64,
) -> Result<
    Option<
        Vec<(
            DirectoryEntry,
            Cid,
            Block,
            Option<(BlockStat, Vec<BlockLink>)>,
        )>,
    >,
    AsyncDBError,
> {
    let _timer = crate::prom::DB_METHOD_CALL_DURATIONS
        .get_metric_with_label_values(&["get_directory_listing"])
        .unwrap()
        .start_timer();

    tokio::task::spawn_blocking(move || {
        let mut conn = conn.lock().unwrap();
        get_directory_listing(&mut conn, p_block_id)
    })
    .await
    .map_err(AsyncDBError::Runtime)?
    .map_err(AsyncDBError::Database)
}

pub async fn async_get_file_heuristics(
    conn: Arc<Mutex<PgConnection>>,
    block_id: i64,
) -> Result<Option<BlockFileMetadata>, AsyncDBError> {
    let _timer = crate::prom::DB_METHOD_CALL_DURATIONS
        .get_metric_with_label_values(&["get_file_heuristics"])
        .unwrap()
        .start_timer();

    tokio::task::spawn_blocking(move || {
        let mut conn = conn.lock().unwrap();
        get_file_metadata(&mut conn, block_id)
    })
    .await
    .map_err(AsyncDBError::Runtime)?
    .map_err(AsyncDBError::Database)
}

pub async fn async_get_block_stats(
    conn: Arc<Mutex<PgConnection>>,
    block_id: i64,
) -> Result<Option<(BlockStat, Vec<BlockLink>)>, AsyncDBError> {
    let _timer = crate::prom::DB_METHOD_CALL_DURATIONS
        .get_metric_with_label_values(&["get_block_stats"])
        .unwrap()
        .start_timer();

    tokio::task::spawn_blocking(move || {
        let mut conn = conn.lock().unwrap();
        get_block_stats(&mut conn, block_id)
    })
    .await
    .map_err(AsyncDBError::Runtime)?
    .map_err(AsyncDBError::Database)
}

pub async fn async_get_failed_block_downloads(
    conn: Arc<Mutex<PgConnection>>,
    block_id: i64,
) -> Result<Option<Vec<FailedDownload>>, AsyncDBError> {
    let _timer = crate::prom::DB_METHOD_CALL_DURATIONS
        .get_metric_with_label_values(&["get_failed_block_downloads"])
        .unwrap()
        .start_timer();

    async_get_failed_downloads(conn, block_id, DOWNLOAD_TYPE_BLOCK_ID).await
}

pub async fn async_get_failed_dag_downloads(
    conn: Arc<Mutex<PgConnection>>,
    block_id: i64,
) -> Result<Option<Vec<FailedDownload>>, AsyncDBError> {
    let _timer = crate::prom::DB_METHOD_CALL_DURATIONS
        .get_metric_with_label_values(&["get_failed_dag_downloads"])
        .unwrap()
        .start_timer();

    async_get_failed_downloads(conn, block_id, DOWNLOAD_TYPE_DAG_ID).await
}

pub fn upsert_successful_file(
    conn: &mut PgConnection,
    mime_cache: &mut MimeTypeCache,
    p_block_id: i64,
    mime_type: &str,
    file_size: i64,
    sha256_hash: Vec<u8>,
    alternative_cids: Vec<NormalizedAlternativeCid>,
    dag: Vec<Vec<(CIDParts, BlockLevelMetadata)>>,
    ts: chrono::DateTime<chrono::Utc>,
) -> Result<(), diesel::result::Error> {
    debug!("upserting successful file for block {}", p_block_id);

    conn.transaction::<(), diesel::result::Error, _>(|conn| {
        // Determine MIME type ID
        let mime_type_id = mime_cache.lookup_or_insert(conn, mime_type)?;

        // Insert MIME type and file size
        insert_block_file_metadata_idempotent(conn, p_block_id, mime_type_id, file_size)?;

        // Insert SHA256 hash
        insert_block_file_hash_idempotent(conn, p_block_id, HASH_TYPE_SHA2_256_ID, &sha256_hash)?;

        // Insert alternative CIDs
        insert_block_file_alternative_cids_idempotent(conn, p_block_id, alternative_cids)?;

        // For each dag layer:
        for layer in dag.into_iter() {
            // Insert successful blocks
            // TODO there is an optimization to be made here if we iterate in reverse and remember
            // the database block IDs of the previous layer.
            for (cidparts, metadata) in layer {
                let (db_block, _) = upsert_block_and_cid(conn, cidparts)?;

                upsert_successful_block(
                    conn,
                    db_block.id,
                    metadata.block_size,
                    metadata.unixfs_type_id,
                    metadata.links,
                    ts,
                )?;
            }
        }

        // Insert download success timestamp
        insert_download_success_idempotent(conn, p_block_id, DOWNLOAD_TYPE_DAG_ID, ts)?;
        debug!("inserted download success");

        Ok(())
    })
}

fn insert_block_file_metadata_idempotent(
    conn: &mut PgConnection,
    p_block_id: i64,
    p_mime_type_id: i32,
    file_size: i64,
) -> Result<(), diesel::result::Error> {
    use crate::schema::block_file_metadata;

    let new_metadata = NewBlockFileMetadata {
        block_id: &p_block_id,
        mime_type_id: &p_mime_type_id,
        file_size: &file_size,
    };

    debug!("upserting block file metadata type {:?}", new_metadata);
    diesel::insert_into(block_file_metadata::table)
        .values(new_metadata)
        .on_conflict_do_nothing()
        .execute(conn)
        .optional()?;

    Ok(())
}

fn insert_block_file_hash_idempotent(
    conn: &mut PgConnection,
    p_block_id: i64,
    hash_type_id: i32,
    digest: &[u8],
) -> Result<(), diesel::result::Error> {
    use crate::schema::block_file_hashes;

    let new_hash = NewBlockFileHash {
        block_id: &p_block_id,
        hash_type_id: &hash_type_id,
        digest,
    };

    debug!("upserting block file hash {:?}", new_hash);
    diesel::insert_into(block_file_hashes::table)
        .values(new_hash)
        .on_conflict_do_nothing()
        .execute(conn)
        .optional()?;

    Ok(())
}

fn insert_block_file_alternative_cids_idempotent(
    conn: &mut PgConnection,
    p_block_id: i64,
    alternative_cids: Vec<NormalizedAlternativeCid>,
) -> Result<(), diesel::result::Error> {
    use crate::schema::block_file_alternative_cids;

    let new_cids: Vec<_> = alternative_cids
        .iter()
        .map(|c| NewBlockFileAlternativeCid {
            block_id: &p_block_id,
            digest: &c.digest,
            codec: &c.codec,
            hash_type_id: &c.hash_type_id,
        })
        .collect();

    debug!("upserting block file alternative CIDs {:?}", new_cids);
    diesel::insert_into(block_file_alternative_cids::table)
        .values(new_cids)
        .on_conflict_do_nothing()
        .execute(conn)
        .optional()?;

    Ok(())
}

pub fn upsert_successful_directory(
    conn: &mut PgConnection,
    p_block_id: i64,
    entries: Vec<(String, i64, CIDParts, Option<BlockLevelMetadata>)>,
    ts: chrono::DateTime<chrono::Utc>,
) -> Result<
    Vec<(
        DirectoryEntry,
        Cid,
        Block,
        Option<(BlockStat, Vec<BlockLink>)>,
    )>,
    diesel::result::Error,
> {
    debug!(
        "upserting successful directory download for block {}, entries {:?}",
        p_block_id, entries
    );

    conn.transaction::<Vec<(
        DirectoryEntry,
        Cid,
        Block,
        Option<(BlockStat, Vec<BlockLink>)>,
    )>, diesel::result::Error, _>(|conn| {
        // Upsert referenced CIDs and block data for all of them
        let entries_with_stats: Result<Vec<_>, diesel::result::Error> = entries
            .iter()
            .map(|(_, _, cidparts, stats)| (cidparts.clone(), stats.clone()))
            .map(|(cid, stats)| {
                let (db_block, db_cid) = upsert_block_and_cid(conn, cid.clone())?;

                match stats {
                    None => Ok((cid, db_cid, db_block, None)),
                    Some(stats) => {
                        let (stat, links) = upsert_successful_block(
                            conn,
                            db_block.id,
                            stats.block_size,
                            stats.unixfs_type_id,
                            stats.links,
                            ts,
                        )?;

                        Ok((cid, db_cid, db_block, Some((stat, links))))
                    }
                }
            })
            .collect();
        debug!("upserted blocks and stats {:?}", entries_with_stats);
        let entries_with_stats = entries_with_stats?;

        // Upsert directory entries
        let directory_entries: Result<Vec<_>, _> = entries
            .iter()
            .zip(entries_with_stats.iter())
            .map(|((name, size, _, _), (_, cid, _, _))| {
                upsert_directory_entry(conn, p_block_id, name, size, &cid.id)
            })
            .collect();
        debug!("upserted directory entries {:?}", directory_entries);

        let directory_entries = directory_entries?;

        // Insert successful download
        insert_download_success_idempotent(conn, p_block_id, DOWNLOAD_TYPE_DAG_ID, ts)?;
        debug!("inserted download success");

        Ok(directory_entries
            .into_iter()
            .zip(entries_with_stats.into_iter())
            .map(|(entry, (_, cid, block, stat))| (entry, cid, block, stat))
            .collect())
    })
}

pub fn upsert_successful_block(
    conn: &mut PgConnection,
    p_block_id: i64,
    block_size: i32,
    unixfs_type_id: i32,
    links: Vec<(String, i64, CIDParts)>,
    ts: chrono::DateTime<chrono::Utc>,
) -> Result<(BlockStat, Vec<BlockLink>), diesel::result::Error> {
    debug!("upserting successful block stat for block {}, size {}, unixfs type ID {}, links {:?}, ts {:?}",p_block_id,block_size,unixfs_type_id,links,ts);

    conn.transaction::<(BlockStat, Vec<BlockLink>), diesel::result::Error, _>(|conn| {
        // Upsert referenced CIDs
        let referenced_cids: Result<Vec<_>, _> = links
            .iter()
            .map(|(_, _, cid_parts)| cid_parts)
            .cloned()
            .map(|cid| upsert_block_and_cid(conn, cid).map(|(_, cid)| cid))
            .collect();
        debug!("got reference CIDs {:?}", referenced_cids);

        let references: Vec<_> = referenced_cids?
            .into_iter()
            .zip(links.into_iter())
            .map(|(cid, (name, size, _))| (name, size, cid))
            .collect();

        // Upsert links
        debug!("upserting links {:?}", references);
        let mut links = Vec::new();
        for link in references.into_iter() {
            let (name, size, cid) = link;
            links.push(upsert_block_link(conn, p_block_id, &name, &size, &cid.id)?);
        }
        debug!("upserted links {:?}", links);

        // Upsert block-level metadata
        let stat = upsert_block_level_metadata(conn, p_block_id, block_size, unixfs_type_id)?;
        debug!("upserted block stat {:?}", stat);

        // Insert successful download
        insert_download_success_idempotent(conn, p_block_id, DOWNLOAD_TYPE_BLOCK_ID, ts)?;
        debug!("inserted download success");

        Ok((stat, links))
    })
}

pub fn upsert_block_level_metadata(
    conn: &mut PgConnection,
    p_block_id: i64,
    block_size: i32,
    unixfs_type_id: i32,
) -> Result<BlockStat, diesel::result::Error> {
    use crate::schema::block_stats;

    let new_stats = NewBlockStat {
        block_id: &p_block_id,
        block_size: &block_size,
        unixfs_type_id: &unixfs_type_id,
    };

    debug!("upserting block stat {:?}", new_stats);
    let stat: BlockStat = diesel::insert_into(block_stats::table)
        .values(new_stats)
        .on_conflict_do_nothing()
        .get_result(conn)
        .optional()
        .and_then(|res| {
            res.map(Ok).unwrap_or_else(|| {
                use crate::schema::block_stats::dsl::*;

                block_stats.filter(block_id.eq(p_block_id)).get_result(conn)
            })
        })?;
    debug!("upserted block stat {:?}", stat);

    Ok(stat)
}

pub fn upsert_directory_entry(
    conn: &mut PgConnection,
    p_block_id: i64,
    name: &str,
    size: &i64,
    cid_id: &i64,
) -> Result<DirectoryEntry, diesel::result::Error> {
    use crate::schema::directory_entries;

    let new_entry = NewDirectoryEntry {
        block_id: &p_block_id,
        name,
        size,
        referenced_cid_id: cid_id,
    };

    debug!("upserting directory entry {:?}", new_entry);
    let entry: DirectoryEntry = diesel::insert_into(directory_entries::table)
        .values(&new_entry)
        .on_conflict_do_nothing()
        .get_result(conn)
        .optional()
        .and_then(|res| {
            res.map(Ok).unwrap_or_else(|| {
                use crate::schema::directory_entries::dsl::*;

                directory_entries
                    .filter(
                        block_id
                            .eq(p_block_id)
                            .and(name.eq(name))
                            .and(referenced_cid_id.eq(cid_id)),
                    )
                    .get_result(conn)
            })
        })?;
    debug!("upserted entry {:?}", entry);

    Ok(entry)
}

pub fn upsert_mime_type(
    conn: &mut PgConnection,
    mime_type: &str,
) -> Result<MimeType, diesel::result::Error> {
    use crate::schema::mime_types;

    let new_mime_type = NewMimeType { name: mime_type };

    debug!("upserting mime type {:?}", new_mime_type);
    let mtype: MimeType = diesel::insert_into(mime_types::table)
        .values(&new_mime_type)
        .on_conflict_do_nothing()
        .get_result(conn)
        .optional()
        .and_then(|res| {
            res.map(Ok).unwrap_or_else(|| {
                use crate::schema::mime_types::dsl::*;

                mime_types.filter(name.eq(mime_type)).get_result(conn)
            })
        })?;
    debug!("upserted MIME type {:?}", mtype);

    Ok(mtype)
}

pub fn upsert_block_link(
    conn: &mut PgConnection,
    p_block_id: i64,
    name: &str,
    size: &i64,
    cid_id: &i64,
) -> Result<BlockLink, diesel::result::Error> {
    use crate::schema::block_links;

    let new_link = NewBlockLink {
        block_id: &p_block_id,
        name,
        size,
        referenced_cid_id: cid_id,
    };

    debug!("upserting block link {:?}", new_link);
    let link: BlockLink = diesel::insert_into(block_links::table)
        .values(&new_link)
        .on_conflict_do_nothing()
        .get_result(conn)
        .optional()
        .and_then(|res| {
            res.map(Ok).unwrap_or_else(|| {
                use crate::schema::block_links::dsl::*;

                block_links
                    .filter(
                        block_id
                            .eq(p_block_id)
                            .and(name.eq(name))
                            .and(referenced_cid_id.eq(cid_id)),
                    )
                    .get_result(conn)
            })
        })?;
    debug!("upserted link {:?}", link);

    Ok(link)
}

pub fn insert_download_success_idempotent(
    conn: &mut PgConnection,
    p_block_id: i64,
    download_type_id: i32,
    ts: chrono::DateTime<chrono::Utc>,
) -> Result<(), diesel::result::Error> {
    use crate::schema::successful_downloads;

    let new_success = NewSuccessfulDownload {
        block_id: &p_block_id,
        download_type_id: &download_type_id,
        ts: &ts,
    };

    // The ON CONFLICT DO NOTHING is important, see the comment on the failure insertion case.

    debug!("inserting download success {:?}", new_success);
    diesel::insert_into(successful_downloads::table)
        .values(&new_success)
        .on_conflict_do_nothing()
        .execute(conn)?;

    Ok(())
}

async fn async_insert_download_failure_idempotent(
    conn: Arc<Mutex<PgConnection>>,
    block_id: i64,
    download_type_id: i32,
    ts: chrono::DateTime<chrono::Utc>,
) -> Result<(), AsyncDBError> {
    let res = tokio::task::spawn_blocking(move || {
        let mut conn = conn.lock().unwrap();
        insert_download_failure_idempotent(&mut conn, block_id, download_type_id, ts)
    })
    .await
    .map_err(AsyncDBError::Runtime)?;
    res.map_err(AsyncDBError::Database)
}

pub fn insert_download_failure_idempotent(
    conn: &mut PgConnection,
    p_block_id: i64,
    download_type_id: i32,
    ts: chrono::DateTime<chrono::Utc>,
) -> Result<(), diesel::result::Error> {
    use crate::schema::failed_downloads;

    let new_failure = NewFailedDownload {
        block_id: &p_block_id,
        download_type_id: &download_type_id,
        ts: &ts,
    };

    // It is important that this is idempotent, i.e., ON CONFLICT DO NOTHING:
    // Whenever a directory or a file is indexed which has duplicate CIDs in their DAGs or directory
    // listing, those will be saved as one block in the database.
    // We then attempt to insert successful download timestamps for them, which cause a conflict.
    // TODO maybe this is not important for failures? But probably can't hurt...

    debug!("inserting download failure {:?}", new_failure);
    diesel::insert_into(failed_downloads::table)
        .values(&new_failure)
        .on_conflict_do_nothing()
        .execute(conn)?;

    Ok(())
}

pub fn upsert_block_and_cid(
    conn: &mut PgConnection,
    cid: CIDParts,
) -> Result<(Block, Cid), diesel::result::Error> {
    use crate::schema::blocks;
    use crate::schema::cids;
    debug!("upserting block and CID for parts {:?}", cid);

    let new_block = NewBlock {
        multihash: &cid.multihash,
    };

    let upserted_block: Block = diesel::insert_into(blocks::table)
        .values(&new_block)
        .on_conflict_do_nothing()
        .get_result(conn)
        .optional()
        .and_then(|res| {
            res.map(Ok).unwrap_or_else(|| {
                use crate::schema::blocks::dsl::*;

                blocks.filter(multihash.eq(cid.multihash)).get_result(conn)
            })
        })?;
    debug!("upserted block {:?}", upserted_block);

    let new_cid = NewCid {
        codec: &(cid.codec as i64),
        block_id: &upserted_block.id,
    };

    let upserted_cid: Cid = diesel::insert_into(cids::table)
        .values(&new_cid)
        .on_conflict_do_nothing()
        .get_result(conn)
        .optional()
        .and_then(|res| {
            res.map(Ok).unwrap_or_else(|| {
                use crate::schema::cids::dsl::*;

                cids.filter(block_id.eq(upserted_block.id)).get_result(conn)
            })
        })?;
    debug!("upserted CID {:?}", upserted_cid);

    Ok((upserted_block, upserted_cid))
}

pub fn get_directory_listing(
    conn: &mut PgConnection,
    p_block_id: i64,
) -> Result<
    Option<
        Vec<(
            DirectoryEntry,
            Cid,
            Block,
            Option<(BlockStat, Vec<BlockLink>)>,
        )>,
    >,
    diesel::result::Error,
> {
    use crate::schema::directory_entries::dsl::*;
    debug!("getting directory listing for block {}", p_block_id);

    let entries = match directory_entries
        .filter(crate::schema::directory_entries::dsl::block_id.eq(p_block_id))
        .load::<DirectoryEntry>(conn)
        .map(|res| {
            debug!("got directory entries {:?}", res);
            res
        }) {
        Ok(entries) => entries,
        Err(err) => {
            if let diesel::NotFound = err {
                return Ok(None);
            }
            return Err(err);
        }
    };
    if entries.is_empty() {
        // TODO investigate why this does not return NotFound
        return Ok(None);
    }

    let cid_ids = entries
        .iter()
        .map(|entry| entry.referenced_cid_id)
        .collect::<Vec<_>>();

    let cids = {
        use crate::schema::cids::dsl::*;
        cids.filter(crate::schema::cids::dsl::id.eq_any(&cid_ids))
            .load::<Cid>(conn)
            .map(|res| {
                debug!("got cids {:?} for entries {:?}", res, entries);
                res
            })?
    };
    let block_ids = cids.iter().map(|c| c.block_id).collect::<Vec<_>>();

    let blocks = {
        use crate::schema::blocks::dsl::*;
        blocks
            .filter(crate::schema::blocks::dsl::id.eq_any(&block_ids))
            .load::<Block>(conn)
            .map(|res| {
                debug!("got blocks {:?} for cids {:?}", res, cids);
                res
            })?
    };
    // This should always hold because of the foreign key constraint.
    assert_eq!(blocks.len(), cids.len());

    let block_stats: Vec<BlockStat> = BlockStat::belonging_to(&blocks)
        .load::<BlockStat>(conn)
        .map(|res| {
            debug!("got block stats {:?} for blocks {:?}", res, blocks);
            res
        })?;

    let grouped_stats = block_stats
        .grouped_by(&blocks)
        .into_iter()
        .map(|stats| {
            if stats.is_empty() {
                None
            } else {
                assert_eq!(stats.len(), 1);
                Some(stats.get(0).unwrap().clone())
            }
        })
        .collect::<Vec<_>>();

    let block_links: Vec<BlockLink> = BlockLink::belonging_to(&blocks)
        .load::<BlockLink>(conn)
        .map(|res| {
            debug!("got block links {:?} for blocks {:?}", res, blocks);
            res
        })?;
    let grouped_links = block_links.grouped_by(&blocks);

    let blocks_with_stat_and_links = blocks
        .into_iter()
        .zip(grouped_stats)
        .zip(grouped_links)
        .map(|((b, s), l)| match s {
            Some(stats) => (b, Some((stats, l))),
            None => {
                assert_eq!(l.len(), 0);
                (b, None)
            }
        })
        .collect::<Vec<_>>();

    // Pretty slow algorithm, but at least it works.
    let entries = entries
        .into_iter()
        .map(|entry| {
            (
                entry.clone(),
                cids.iter()
                    .find(|c| entry.referenced_cid_id == c.id)
                    .unwrap()
                    .clone(),
            )
        })
        .map(|(entry, cid)| {
            (
                entry,
                cid.clone(),
                blocks_with_stat_and_links
                    .iter()
                    .find(|b| cid.block_id == b.0.id)
                    .unwrap()
                    .clone(),
            )
        })
        .map(|(entry, cid, block_stat)| (entry, cid, block_stat.0, block_stat.1))
        .collect();

    Ok(Some(entries))
}

pub fn get_file_metadata(
    conn: &mut PgConnection,
    p_block_id: i64,
) -> Result<Option<BlockFileMetadata>, diesel::result::Error> {
    use crate::schema::block_file_metadata::dsl::*;
    debug!("getting file metadata for block {}", p_block_id);

    block_file_metadata
        .filter(block_id.eq(p_block_id))
        .first::<BlockFileMetadata>(conn)
        .map(|res| {
            debug!("got file metadata {:?}", res);
            res
        })
        .optional()
}

pub fn get_block_stats(
    conn: &mut PgConnection,
    p_block_id: i64,
) -> Result<Option<(BlockStat, Vec<BlockLink>)>, diesel::result::Error> {
    use crate::schema::block_links::dsl::*;
    use crate::schema::block_stats::dsl::*;
    debug!("getting block stat and links for block {}", p_block_id);

    let stat = match block_stats
        .filter(crate::schema::block_stats::dsl::block_id.eq(p_block_id))
        .first::<BlockStat>(conn)
        .map(|res| {
            debug!("got block stat {:?}", res);
            res
        }) {
        Ok(stat) => stat,
        Err(err) => {
            if let diesel::NotFound = err {
                return Ok(None);
            }
            return Err(err);
        }
    };

    match block_links
        .filter(crate::schema::block_links::dsl::block_id.eq(p_block_id))
        .load::<BlockLink>(conn)
        .map(|res| {
            debug!("got block links {:?}", res);
            res
        }) {
        Ok(links) => Ok(Some((stat, links))),
        Err(err) => {
            if let diesel::NotFound = err {
                Ok(Some((stat, Vec::new())))
            } else {
                Err(err)
            }
        }
    }
}

async fn async_get_failed_downloads(
    conn: Arc<Mutex<PgConnection>>,
    block_id: i64,
    download_type_id: i32,
) -> Result<Option<Vec<FailedDownload>>, AsyncDBError> {
    let res = tokio::task::spawn_blocking(move || {
        let mut conn = conn.lock().unwrap();
        get_failed_downloads(&mut conn, block_id, download_type_id)
    })
    .await
    .map_err(AsyncDBError::Runtime)?;
    res.map_err(AsyncDBError::Database)
}

pub fn get_failed_downloads(
    conn: &mut PgConnection,
    p_block_id: i64,
    p_download_type_id: i32,
) -> Result<Option<Vec<FailedDownload>>, diesel::result::Error> {
    use crate::schema::failed_downloads::dsl::*;
    debug!("getting failed downloads for block {}", p_block_id);

    match failed_downloads
        .filter(
            block_id
                .eq(p_block_id)
                .and(download_type_id.eq(p_download_type_id)),
        )
        .load::<FailedDownload>(conn)
        .map(|res| {
            debug!("got failed downloads {:?}", res);
            res
        }) {
        Ok(failures) => Ok(Some(failures)),
        Err(err) => {
            if let diesel::NotFound = err {
                Ok(None)
            } else {
                Err(err)
            }
        }
    }
}
