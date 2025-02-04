use anyhow::Context;
use cid::multibase::Base::Base16Lower;
use clap::Command;
use ipfs_indexer::logging;
use log::{debug, error};
use tokio::io::{AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::set_up_logging().unwrap();

    let _matches = Command::new(
        "IPFS indexer Tool to Convert Supported CIDs to Base16 as used in the Database",
    )
    .version(clap::crate_version!())
    .author("Leo Balduf <leobalduf@gmail.com>")
    .about(
        "IPFS indexer tool to convert a list of CIDs to the base16 format used by the database.\
            This reads CIDs from stdin and outputs CSV of source and converted CID.\
            Unsupported codecs are filtered out.",
    )
    .get_matches();

    debug!("opening stdin for reading");
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    println!("source_cid,normalized_cid");
    while let Some(cid) = lines.next_line().await? {
        debug!("read cid {}", cid);

        // Parse CID
        debug!("{}: parsing...", cid);
        let cid_parts = match ipfs_indexer::parse_cid_to_parts(&cid) {
            Ok(parts) => parts,
            Err(err) => {
                debug!("unable to parse CID {}: {:?}", cid, err);
                continue;
            }
        };
        debug!("{}: parsed as {:?}", cid, cid_parts);

        // Skip anything that's not DAG_PB or RAW
        if cid_parts.codec != ipfs_indexer::CODEC_DAG_PB
            && cid_parts.codec != ipfs_indexer::CODEC_RAW
        {
            debug!("{}: skipping non-filesystem CID", cid);
            continue;
        }

        // Convert to the database format
        let db_formatted_cid = cid_parts
            .cid
            .into_v1()
            .context("unable to convert CID to v1?")
            .and_then(|c| {
                c.to_string_of_base(Base16Lower)
                    .context("unable to print CID as base16")
            });
        match db_formatted_cid {
            Ok(s) => {
                // Print
                println!("{},{}", cid, s)
            }
            Err(err) => {
                error!("{}: unable to convert: {:?}", cid, err)
            }
        }
    }

    Ok(())
}
