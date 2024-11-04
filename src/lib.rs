use async_trait::async_trait;
use futures::{AsyncRead, AsyncWrite};
use memcache_async::ascii::Protocol;
use std::fmt::Debug;
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tower_sessions_core::{
    session::{Id, Record},
    session_store, SessionStore,
};

pub struct MemCacheStore<S> {
    client: Mutex<Protocol<S>>,
}

impl<S> MemCacheStore<S> {
    pub fn new(protocol: Protocol<S>) -> Self {
        Self {
            client: Mutex::new(protocol),
        }
    }
}

impl<S> Debug for MemCacheStore<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemCacheStore")
            .field("client", &"Protocol")
            .finish()
    }
}

fn convert_record(record: &Record) -> session_store::Result<(Id, Vec<u8>, u32)> {
    let key = record.id;
    let val = rmp_serde::to_vec(record).map_err(|e| session_store::Error::Encode(e.to_string()))?;
    let expire = (record.expiry_date - OffsetDateTime::now_utc())
        .whole_seconds()
        .max(0) as u32;

    Ok((key, val, expire))
}

#[async_trait]
impl<S> SessionStore for MemCacheStore<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    async fn create(&self, record: &mut Record) -> session_store::Result<()> {
        let (key, val, expire) = convert_record(record)?;

        self.client
            .lock()
            .await
            .add(key, &val, expire)
            .await
            .map_err(|e| session_store::Error::Backend(e.to_string()))
    }

    async fn save(&self, record: &Record) -> session_store::Result<()> {
        let (key, val, expire) = convert_record(record)?;

        self.client
            .lock()
            .await
            .set(key, &val, expire)
            .await
            .map_err(|e| session_store::Error::Backend(e.to_string()))
    }

    async fn load(&self, session_id: &Id) -> session_store::Result<Option<Record>> {
        let val = self
            .client
            .lock()
            .await
            .get(session_id.to_string())
            .await
            .map_err(|e| session_store::Error::Backend(e.to_string()))?;

        rmp_serde::from_slice(&val).map_err(|e| session_store::Error::Decode(e.to_string()))
    }

    async fn delete(&self, session_id: &Id) -> session_store::Result<()> {
        self.client
            .lock()
            .await
            .delete(session_id.to_string())
            .await
            .map_err(|e| session_store::Error::Backend(e.to_string()))
    }
}
