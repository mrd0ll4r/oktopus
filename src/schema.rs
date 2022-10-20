// @generated automatically by Diesel CLI.

diesel::table! {
    block_file_alternative_cids (block_id, codec, hash_type_id) {
        block_id -> Int8,
        digest -> Bytea,
        codec -> Int8,
        hash_type_id -> Int4,
    }
}

diesel::table! {
    block_file_hashes (block_id, hash_type_id) {
        block_id -> Int8,
        hash_type_id -> Int4,
        digest -> Bytea,
    }
}

diesel::table! {
    block_file_metadata (block_id) {
        block_id -> Int8,
        mime_type_id -> Int4,
        file_size -> Nullable<Int8>,
    }
}

diesel::table! {
    block_links (block_id, name, referenced_cid_id) {
        block_id -> Int8,
        name -> Text,
        size -> Int8,
        referenced_cid_id -> Int8,
    }
}

diesel::table! {
    block_stats (block_id) {
        block_id -> Int8,
        block_size -> Int4,
        unixfs_type_id -> Int4,
    }
}

diesel::table! {
    blocks (id) {
        id -> Int8,
        multihash -> Bytea,
    }
}

diesel::table! {
    cids (id) {
        id -> Int8,
        codec -> Int8,
        block_id -> Int8,
    }
}

diesel::table! {
    directory_entries (block_id, name, referenced_cid_id) {
        block_id -> Int8,
        name -> Text,
        size -> Int8,
        referenced_cid_id -> Int8,
    }
}

diesel::table! {
    download_types (id) {
        id -> Int4,
        name -> Text,
    }
}

diesel::table! {
    failed_downloads (block_id, download_type_id, ts) {
        block_id -> Int8,
        download_type_id -> Int4,
        ts -> Timestamptz,
    }
}

diesel::table! {
    hash_types (id) {
        id -> Int4,
        name -> Text,
        multiformat_code -> Int8,
        digest_bytes -> Int4,
    }
}

diesel::table! {
    mime_types (id) {
        id -> Int4,
        name -> Text,
    }
}

diesel::table! {
    successful_downloads (block_id, download_type_id, ts) {
        block_id -> Int8,
        download_type_id -> Int4,
        ts -> Timestamptz,
    }
}

diesel::table! {
    unixfs_types (id) {
        id -> Int4,
        name -> Text,
    }
}

diesel::joinable!(block_file_alternative_cids -> blocks (block_id));
diesel::joinable!(block_file_alternative_cids -> hash_types (hash_type_id));
diesel::joinable!(block_file_hashes -> blocks (block_id));
diesel::joinable!(block_file_hashes -> hash_types (hash_type_id));
diesel::joinable!(block_file_metadata -> blocks (block_id));
diesel::joinable!(block_file_metadata -> mime_types (mime_type_id));
diesel::joinable!(block_links -> blocks (block_id));
diesel::joinable!(block_links -> cids (referenced_cid_id));
diesel::joinable!(block_stats -> blocks (block_id));
diesel::joinable!(block_stats -> unixfs_types (unixfs_type_id));
diesel::joinable!(cids -> blocks (block_id));
diesel::joinable!(directory_entries -> blocks (block_id));
diesel::joinable!(directory_entries -> cids (referenced_cid_id));
diesel::joinable!(failed_downloads -> blocks (block_id));
diesel::joinable!(failed_downloads -> download_types (download_type_id));
diesel::joinable!(successful_downloads -> blocks (block_id));
diesel::joinable!(successful_downloads -> download_types (download_type_id));

diesel::allow_tables_to_appear_in_same_query!(
    block_file_alternative_cids,
    block_file_hashes,
    block_file_metadata,
    block_links,
    block_stats,
    blocks,
    cids,
    directory_entries,
    download_types,
    failed_downloads,
    hash_types,
    mime_types,
    successful_downloads,
    unixfs_types,
);
