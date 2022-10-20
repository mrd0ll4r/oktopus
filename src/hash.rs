use crate::{models, IpfsApi};
use anyhow::{anyhow, Context};
use cid::Cid;
use sha2::{Digest, Sha256};
use std::io::Cursor;
use std::str::FromStr;
use std::sync::Arc;

pub fn compute_sha256(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();

    hasher.update(data);

    hasher.finalize().into_iter().collect()
}

#[derive(Debug, Clone)]
pub struct AlternativeCids {
    pub sha1: String,
    pub sha2_256: String,
    pub sha2_512: String,
    pub sha3_224: String,
    pub sha3_256: String,
    pub sha3_384: String,
    pub sha3_512: String,
    pub dbl_sha2_256: String,
    // murmur3_x64_64: String, // not allowed by the daemon
    //pub keccak_224: String, // panics the daemon
    pub keccak_256: String,
    //pub keccak_384: String, // panics the daemon
    pub keccak_512: String,
    pub blake3: String,
    // shake_128: String, // not allowed by the daemon
    pub shake_256: String,
    // sha2_256_trunc254_padded: String, // panics the daemon
    // x11: String, // panics the daemon
    // md5: String, // not allowed by the daemon
    // poseidon_bls12_381_a2_fc1: String, // panics the daemon
}

#[derive(Debug, Clone)]
pub struct NormalizedAlternativeCid {
    pub hash_type_id: i32,
    pub codec: i64,
    pub digest: Vec<u8>,
}

impl AlternativeCids {
    pub fn normalized_cids(&self) -> anyhow::Result<Vec<NormalizedAlternativeCid>> {
        vec![
            &self.sha1,
            &self.sha2_256,
            &self.sha2_512,
            &self.sha3_224,
            &self.sha3_256,
            &self.sha3_384,
            &self.sha3_512,
            &self.dbl_sha2_256,
            //&self.keccak_224,
            &self.keccak_256,
            //&self.keccak_384,
            &self.keccak_512,
            &self.blake3,
            &self.shake_256,
        ]
        .into_iter()
        .zip(
            vec![
                models::HASH_TYPE_SHA1_ID,
                models::HASH_TYPE_SHA2_256_ID,
                models::HASH_TYPE_SHA2_512_ID,
                models::HASH_TYPE_SHA3_224_ID,
                models::HASH_TYPE_SHA3_256_ID,
                models::HASH_TYPE_SHA3_384_ID,
                models::HASH_TYPE_SHA3_512_ID,
                models::HASH_TYPE_DBL_SHA2_256_ID,
                models::HASH_TYPE_KECCAK_256_ID,
                models::HASH_TYPE_KECCAK_512_ID,
                models::HASH_TYPE_BLAKE3_256_ID,
                models::HASH_TYPE_SHAKE_256_ID,
            ]
            .into_iter(),
        )
        .map(|(c, id)| {
            Cid::from_str(c)
                .map(|cid| NormalizedAlternativeCid {
                    hash_type_id: id,
                    codec: cid.codec() as i64,
                    digest: cid.hash().digest().to_vec(),
                })
                .map_err(|err| anyhow!("{}", err))
        })
        .collect::<anyhow::Result<_>>()
    }

    pub async fn for_bytes<T>(client: Arc<T>, bytes: Vec<u8>) -> anyhow::Result<AlternativeCids>
    where
        T: IpfsApi + Sync,
    {
        let sha1_cid =
            Self::for_hash_function_and_bytes(client.clone(), "sha1", bytes.clone()).await?;
        let sha2_256_cid =
            Self::for_hash_function_and_bytes(client.clone(), "sha2-256", bytes.clone()).await?;
        let sha2_512_cid =
            Self::for_hash_function_and_bytes(client.clone(), "sha2-512", bytes.clone()).await?;
        let sha3_224_cid =
            Self::for_hash_function_and_bytes(client.clone(), "sha3-224", bytes.clone()).await?;
        let sha3_256_cid =
            Self::for_hash_function_and_bytes(client.clone(), "sha3-256", bytes.clone()).await?;
        let sha3_384_cid =
            Self::for_hash_function_and_bytes(client.clone(), "sha3-384", bytes.clone()).await?;
        let sha3_512_cid =
            Self::for_hash_function_and_bytes(client.clone(), "sha3-512", bytes.clone()).await?;
        let dbl_sha2_256_cid =
            Self::for_hash_function_and_bytes(client.clone(), "dbl-sha2-256", bytes.clone())
                .await?;
        //let keccak_224_cid =
        //    Self::for_hash_function_and_bytes(client.clone(), "keccak-224", bytes.clone()).await?;
        let keccak_256_cid =
            Self::for_hash_function_and_bytes(client.clone(), "keccak-256", bytes.clone()).await?;
        //let keccak_384_cid =
        //    Self::for_hash_function_and_bytes(client.clone(), "keccak-384", bytes.clone()).await?;
        let keccak_512_cid =
            Self::for_hash_function_and_bytes(client.clone(), "keccak-512", bytes.clone()).await?;
        let blake3_cid =
            Self::for_hash_function_and_bytes(client.clone(), "blake3", bytes.clone()).await?;
        let shake_256_cid =
            Self::for_hash_function_and_bytes(client.clone(), "shake-256", bytes.clone()).await?;

        Ok(AlternativeCids {
            sha1: sha1_cid,
            sha2_256: sha2_256_cid,
            sha2_512: sha2_512_cid,
            sha3_224: sha3_224_cid,
            sha3_256: sha3_256_cid,
            sha3_384: sha3_384_cid,
            sha3_512: sha3_512_cid,
            dbl_sha2_256: dbl_sha2_256_cid,
            //keccak_224: keccak_224_cid,
            keccak_256: keccak_256_cid,
            //keccak_384: keccak_384_cid,
            keccak_512: keccak_512_cid,
            blake3: blake3_cid,
            shake_256: shake_256_cid,
        })
    }

    async fn for_hash_function_and_bytes<T>(
        client: Arc<T>,
        hash: &str,
        bytes: Vec<u8>,
    ) -> anyhow::Result<String>
    where
        T: IpfsApi + Sync,
    {
        let cursor = Cursor::new(bytes);

        let opts = ipfs_api_backend_hyper::request::Add {
            only_hash: Some(true),
            cid_version: Some(1),
            hash: Some(hash),
            ..Default::default()
        };

        let _timer = crate::prom::IPFS_METHOD_CALL_DURATIONS
            .get_metric_with_label_values(&[format!("add_{}", hash).as_str()])
            .unwrap()
            .start_timer();
        let resp = client
            .add_with_options(cursor, opts)
            .await
            .map_err(|err| anyhow!("{}", err))
            .context(anyhow!("unable to compute alternative {} cid", hash))?;
        Ok(resp.hash)
    }
}
