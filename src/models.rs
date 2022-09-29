use super::schema::*;
use serde::{Deserialize, Serialize};

#[derive(Identifiable, Queryable, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[diesel(table_name=blocks,primary_key(id))]
pub struct Block {
    pub id: i64,
    pub multihash: Vec<u8>,
}

#[derive(Insertable)]
#[diesel(table_name=blocks)]
pub struct NewBlock<'a> {
    pub multihash: &'a Vec<u8>,
}

#[derive(
    Identifiable, Associations, Queryable, Debug, Clone, PartialEq, Eq, Serialize, Deserialize,
)]
#[diesel(table_name=cids, primary_key(id),belongs_to(Block, foreign_key=block_id))]
pub struct Cid {
    pub id: i64,
    pub codec: i64,
    pub block_id: i64,
}

#[derive(Insertable)]
#[diesel(table_name=cids)]
pub struct NewCid<'a> {
    pub codec: &'a i64,
    pub block_id: &'a i64,
}

#[derive(Identifiable, Queryable, PartialEq, Eq, Debug, Clone)]
#[diesel(table_name=download_types,primary_key(id))]
pub struct DownloadType {
    pub id: i32,
    pub name: String,
}

/// Download type constants, inserted into the database via migrations.
pub const DOWNLOAD_TYPE_BLOCK_ID: i32 = 1;
pub const DOWNLOAD_TYPE_DAG_ID: i32 = 2;

lazy_static! {
    pub static ref DOWNLOAD_TYPE_BLOCK: DownloadType = DownloadType {
        id: DOWNLOAD_TYPE_BLOCK_ID,
        name: "block".to_string()
    };
    pub static ref DOWNLOAD_TYPE_DAG: DownloadType = DownloadType {
        id: DOWNLOAD_TYPE_DAG_ID,
        name: "dag".to_string()
    };
}

#[derive(Identifiable, Associations, Queryable, Debug, Clone, PartialEq, Eq)]
#[diesel(table_name=successful_downloads,primary_key(block_id, ts),belongs_to(Block, foreign_key=block_id),belongs_to(DownloadType, foreign_key=download_type_id))]
pub struct SuccessfulDownload {
    pub block_id: i64,
    pub download_type_id: i32,
    pub ts: chrono::DateTime<chrono::Utc>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name=successful_downloads)]
pub struct NewSuccessfulDownload<'a> {
    pub block_id: &'a i64,
    pub download_type_id: &'a i32,
    pub ts: &'a chrono::DateTime<chrono::Utc>,
}

#[derive(Identifiable, Associations, Queryable, Debug, Clone, PartialEq, Eq)]
#[diesel(table_name=failed_downloads,primary_key(block_id, ts),belongs_to(Block, foreign_key=block_id),belongs_to(DownloadType, foreign_key=download_type_id))]
pub struct FailedDownload {
    pub block_id: i64,
    pub download_type_id: i32,
    pub ts: chrono::DateTime<chrono::Utc>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name=failed_downloads)]
pub struct NewFailedDownload<'a> {
    pub block_id: &'a i64,
    pub download_type_id: &'a i32,
    pub ts: &'a chrono::DateTime<chrono::Utc>,
}

#[derive(Identifiable, Queryable, PartialEq, Eq, Debug, Clone)]
#[diesel(table_name=unixfs_types,primary_key(id))]
pub struct UnixFSType {
    pub id: i32,
    pub name: String,
}

/// UnixFS type constants, inserted into the database via migrations.
pub const UNIXFS_TYPE_RAW_ID: i32 = 1;
pub const UNIXFS_TYPE_DIRECTORY_ID: i32 = 2;
pub const UNIXFS_TYPE_FILE_ID: i32 = 3;
pub const UNIXFS_TYPE_METADATA_ID: i32 = 4;
pub const UNIXFS_TYPE_SYMLINK_ID: i32 = 5;
pub const UNIXFS_TYPE_HAMT_SHARD_ID: i32 = 6;

lazy_static! {
    pub static ref UNIXFS_TYPE_RAW: UnixFSType = UnixFSType {
        id: UNIXFS_TYPE_RAW_ID,
        name: "raw".to_string()
    };
    pub static ref UNIXFS_TYPE_DIRECTORY: UnixFSType = UnixFSType {
        id: UNIXFS_TYPE_DIRECTORY_ID,
        name: "directory".to_string()
    };
    pub static ref UNIXFS_TYPE_FILE: UnixFSType = UnixFSType {
        id: UNIXFS_TYPE_FILE_ID,
        name: "file".to_string()
    };
    pub static ref UNIXFS_TYPE_METADATA: UnixFSType = UnixFSType {
        id: UNIXFS_TYPE_METADATA_ID,
        name: "metadata".to_string()
    };
    pub static ref UNIXFS_TYPE_SYMLINK: UnixFSType = UnixFSType {
        id: UNIXFS_TYPE_SYMLINK_ID,
        name: "symlink".to_string()
    };
    pub static ref UNIXFS_TYPE_HAMT_SHARD: UnixFSType = UnixFSType {
        id: UNIXFS_TYPE_HAMT_SHARD_ID,
        name: "HAMTShard".to_string()
    };
}

#[derive(Identifiable, Associations, Queryable, Debug, Clone, PartialEq, Eq)]
#[diesel(table_name=block_stats,primary_key(block_id),belongs_to(Block, foreign_key=block_id), belongs_to(UnixFSType, foreign_key=unixfs_type_id))]
pub struct BlockStat {
    pub block_id: i64,
    pub block_size: i32,
    pub unixfs_type_id: i32,
}

#[derive(Insertable, Debug)]
#[diesel(table_name=block_stats)]
pub struct NewBlockStat<'a> {
    pub block_id: &'a i64,
    pub block_size: &'a i32,
    pub unixfs_type_id: &'a i32,
}

#[derive(Identifiable, Queryable, Debug, Clone, PartialEq, Eq)]
#[diesel(table_name=mime_types,primary_key(id))]
pub struct MimeType {
    pub id: i32,
    pub name: String,
}

#[derive(Insertable, Debug)]
#[diesel(table_name=mime_types)]
pub struct NewMimeType<'a> {
    pub name: &'a str,
}

#[derive(Identifiable, Associations, Queryable, Debug, Clone, PartialEq, Eq)]
#[diesel(table_name=block_file_mime_types,primary_key(block_id),belongs_to(Block, foreign_key=block_id),belongs_to(MimeType, foreign_key=mime_type_id))]
pub struct BlockFileMimeType {
    pub block_id: i64,
    pub mime_type_id: i32,
}

#[derive(Insertable, Debug)]
#[diesel(table_name=block_file_mime_types)]
pub struct NewBlockFileMimeType<'a> {
    pub block_id: &'a i64,
    pub mime_type_id: &'a i32,
}

#[derive(
    Identifiable, Associations, Queryable, Debug, Clone, PartialEq, Eq, Serialize, Deserialize,
)]
#[diesel(table_name=block_links,primary_key(block_id,name,referenced_cid_id),belongs_to(Block, foreign_key=block_id),belongs_to(Cid, foreign_key=referenced_cid_id))]
pub struct BlockLink {
    pub block_id: i64,
    pub name: String,
    pub size: i64,
    pub referenced_cid_id: i64,
}

#[derive(Insertable, Debug)]
#[diesel(table_name=block_links)]
pub struct NewBlockLink<'a> {
    pub block_id: &'a i64,
    pub name: &'a str,
    pub size: &'a i64,
    pub referenced_cid_id: &'a i64,
}

#[derive(Identifiable, Associations, Queryable, Debug, Clone, PartialEq, Eq)]
#[diesel(table_name=directory_entries,primary_key(block_id,name,referenced_cid_id),belongs_to(Block, foreign_key=block_id),belongs_to(Cid, foreign_key=referenced_cid_id))]
pub struct DirectoryEntry {
    pub block_id: i64,
    pub name: String,
    pub size: i64,
    pub referenced_cid_id: i64,
}

#[derive(Insertable, Debug)]
#[diesel(table_name=directory_entries)]
pub struct NewDirectoryEntry<'a> {
    pub block_id: &'a i64,
    pub name: &'a str,
    pub size: &'a i64,
    pub referenced_cid_id: &'a i64,
}

#[derive(Identifiable, Associations, Queryable, Debug, Clone, PartialEq, Eq)]
#[diesel(table_name=block_file_alternative_cids,primary_key(block_id,cid_v1),belongs_to(Block, foreign_key=block_id))]
pub struct BlockFileAlternativeCid {
    pub block_id: i64,
    pub cid_v1: Vec<u8>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name=block_file_alternative_cids)]
pub struct NewBlockFileAlternativeCid<'a> {
    pub block_id: &'a i64,
    pub cid_v1: &'a [u8],
}

#[derive(Identifiable, Queryable, PartialEq, Eq, Debug, Clone)]
#[diesel(table_name=hash_types,primary_key(id))]
pub struct HashType {
    pub id: i32,
    pub name: String,
}

/// Hash type constants, inserted into the database via migrations.
pub const HASH_TYPE_SHA2_256_ID: i32 = 1;

lazy_static! {
    pub static ref HASH_TYPE_SHA2_256: HashType = HashType {
        id: HASH_TYPE_SHA2_256_ID,
        name: "SHA2_256".to_string()
    };
}

#[derive(Identifiable, Associations, Queryable, Debug, Clone, PartialEq, Eq)]
#[diesel(table_name=block_file_hashes,primary_key(block_id,hash_type_id),belongs_to(Block, foreign_key=block_id),belongs_to(HashType, foreign_key=hash_type_id))]
pub struct BlockFileHash {
    pub block_id: i64,
    pub hash_type_id: i32,
    pub digest: Vec<u8>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name=block_file_hashes)]
pub struct NewBlockFileHash<'a> {
    pub block_id: &'a i64,
    pub hash_type_id: &'a i32,
    pub digest: &'a [u8],
}
