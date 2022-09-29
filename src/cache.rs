use crate::models::MimeType;
use diesel::{PgConnection, RunQueryDsl};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

pub(crate) struct Cache<K, V> {
    store: HashMap<K, V>,
    upsert_fn: fn(&mut PgConnection, K) -> Result<V, diesel::result::Error>,
}

impl<K, V> Cache<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    pub(crate) fn new(
        conn: &mut PgConnection,
        load_fn: fn(&mut PgConnection) -> Result<Vec<(K, V)>, diesel::result::Error>,
        upsert_fn: fn(&mut PgConnection, K) -> Result<V, diesel::result::Error>,
    ) -> Result<Cache<K, V>, diesel::result::Error> {
        let items = load_fn(conn)?;

        Ok(Cache {
            store: items.into_iter().collect(),
            upsert_fn,
        })
    }

    pub(crate) fn lookup<Q: ?Sized>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.store.get(key).cloned()
    }

    pub(crate) fn insert(
        &mut self,
        conn: &mut PgConnection,
        key: K,
    ) -> Result<V, diesel::result::Error> {
        // Lookup in cache.
        if let Some(v) = self.store.get(&key).cloned() {
            return Ok(v);
        }

        // Lookup/insert into DB.
        let v = (self.upsert_fn)(conn, key.clone())?;
        self.store.insert(key, v.clone());

        Ok(v)
    }
}

pub struct MimeTypeCache {
    cache: Cache<String, i32>,
}

impl MimeTypeCache {
    fn upsert_mime_type(
        conn: &mut PgConnection,
        mtype: String,
    ) -> Result<i32, diesel::result::Error> {
        Ok(crate::db::upsert_mime_type(conn, &mtype)?.id)
    }

    fn load_mime_types(
        conn: &mut PgConnection,
    ) -> Result<Vec<(String, i32)>, diesel::result::Error> {
        use crate::schema::mime_types::dsl::*;

        mime_types
            .load::<MimeType>(conn)
            .map(|res| res.into_iter().map(|m| (m.name, m.id)).collect())
    }

    pub fn new(conn: &mut PgConnection) -> Result<MimeTypeCache, diesel::result::Error> {
        let cache = Cache::new(conn, Self::load_mime_types, Self::upsert_mime_type)?;
        Ok(MimeTypeCache { cache })
    }

    pub fn lookup_or_insert(
        &mut self,
        conn: &mut PgConnection,
        mtype: &str,
    ) -> Result<i32, diesel::result::Error> {
        if let Some(id) = self.cache.lookup(mtype) {
            return Ok(id);
        }
        self.cache.insert(conn, mtype.to_string())
    }
}
