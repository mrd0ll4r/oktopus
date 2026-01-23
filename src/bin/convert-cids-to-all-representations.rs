use std::io::stdout;
use anyhow::{anyhow, Context};
use cid::multibase::Base::{Base16Lower, Base32Lower, Base58Btc};
use cid::Cid;
use clap::Command;
use ipfs_indexer::logging;
use log::{debug};
use multihash::Multihash;
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(serde::Serialize)]
struct OutputRecord {
    v1_cid: Option<String>,
    db_cid: Option<String>,
    v0_cid: Option<String>,
    conversion_errors: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::set_up_logging().unwrap();

    let _matches = Command::new(
        "IPFS indexer Tool to Convert Supported CIDs to all the formats :)",
    )
    .version(clap::crate_version!())
    .author("Leo Balduf <leobalduf@gmail.com>")
    .about(
        "IPFS indexer tool to convert a list of CIDs to CIDv0 (if available), CIDv1 base32, and CIDv1 base16 (database format). \
            This reads CIDs from stdin and outputs CSV of CIDv1_base32, CIDv1_base16, CIDv0. \
            Non-filesystem codecs are filtered out.",
    )
    .get_matches();

    debug!("opening stdin for reading");
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    let mut csv_writer = csv::Writer::from_writer(stdout());
    while let Some(cid) = lines.next_line().await? {
        debug!("read CID {}", cid);

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
        let v1_base32_cid = cid_parts
            .cid
            .into_v1()
            .context("unable to convert CID to v1?")
            .and_then(|c| {
                c.to_string_of_base(Base32Lower)
                    .context("unable to print CID as base32")
            });
        let v0_cid = if cid_parts.codec != ipfs_indexer::CODEC_DAG_PB {
            Err(anyhow!("CIDv0 supports dag-pb only"))
        } else {
            Multihash::from_bytes(&cid_parts.multihash)
                .context("invalid multihash?")
                .and_then(|mh| Cid::new_v0(mh).context("unable to build v0 CID"))
                .and_then(|c| {
                    c.to_string_of_base(Base58Btc)
                        .context("unable to print v0 CID?")
                })
        };

        let mut error_strings = Vec::new();
        let db_formatted_cid_s = match db_formatted_cid {
            Err(err) => {
                error_strings.push(format!("DB CID: {:?}", err));
                None
            }
            Ok(s) => Some(s),
        };
        let v1_base32_cid_s = match v1_base32_cid {
            Err(err) => {
                error_strings.push(format!("v1 CID: {:?}", err));
                None
            }
            Ok(s) => Some(s),
        };
        let v0_cid_s = match v0_cid {
            Err(err) => {
                error_strings.push(format!("v0 CID: {:?}", err));
                None
            }
            Ok(s) => Some(s),
        };

        let record = OutputRecord {
            v1_cid: v1_base32_cid_s,
            db_cid: db_formatted_cid_s,
            v0_cid: v0_cid_s,
            conversion_errors: if error_strings.is_empty() {
                None
            } else {
                Some(error_strings.join("; "))
            },
        };

        csv_writer.serialize(&record)?;
    }

    Ok(())
}
