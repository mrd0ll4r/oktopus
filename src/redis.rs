use anyhow::Context;
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use std::env;

pub type RedisConnection = MultiplexedConnection;

pub async fn connect(addr: &str) -> anyhow::Result<RedisConnection> {
    let client = redis::Client::open(addr)?;
    let conn = client.get_multiplexed_tokio_connection().await?;
    Ok(conn)
}

pub async fn connect_env() -> anyhow::Result<RedisConnection> {
    dotenvy::dotenv().context("unable to read .env file")?;
    let database_url = env::var("REDIS_URL").context("missing REDIS_URL")?;
    connect(&database_url).await
}

pub enum Cache {
    Cids,
    Blocks,
    Files,
    Directories,
    HAMTShards,
}

impl Cache {
    const fn redis_done_name(&self) -> &'static str {
        match self {
            Cache::Cids => "cids_done",
            Cache::Blocks => "blocks_done",
            Cache::Files => "files_done",
            Cache::Directories => "directories_done",
            Cache::HAMTShards => "hamtshards_done",
        }
    }

    const fn redis_failure_name(&self) -> &'static str {
        match self {
            Cache::Cids => "cids_failure",
            Cache::Blocks => "blocks_failure",
            Cache::Files => "files_failure",
            Cache::Directories => "directories_failure",
            Cache::HAMTShards => "hamtshards_failure",
        }
    }

    pub async fn mark_done(
        &self,
        cid: &str,
        conn: &mut MultiplexedConnection,
    ) -> anyhow::Result<()> {
        conn.hset(self.redis_done_name(), cid, 1).await?;
        Ok(())
    }

    pub async fn record_failure(
        &self,
        cid: &str,
        conn: &mut MultiplexedConnection,
    ) -> anyhow::Result<()> {
        conn.hincr(self.redis_failure_name(), cid, 1).await?;
        Ok(())
    }

    pub async fn is_done(
        &self,
        cid: &str,
        conn: &mut MultiplexedConnection,
    ) -> anyhow::Result<bool> {
        let res: Option<i64> = conn.hget(self.redis_done_name(), cid).await?;
        Ok(res.is_some())
    }

    pub async fn num_failed(
        &self,
        cid: &str,
        conn: &mut MultiplexedConnection,
    ) -> anyhow::Result<Option<u64>> {
        let res = conn.hget(self.redis_failure_name(), cid).await?;
        Ok(res)
    }
}
