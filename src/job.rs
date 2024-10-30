use chrono::{DateTime, Utc};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use redis::{cmd, RedisError};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{consumer::Consumer, queue::Queue};
static SCHEMA_VERSION: u64 = 1;

#[derive(Debug)]
pub struct Job<'a, D, R: redis::aio::ConnectionLike> {
    pub inner: JobInner<D>,
    pub(crate) queue: Option<&'a Queue<R>>,
    pub(crate) consumer: Option<&'a Consumer<'a, R>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobInner<D> {
    pub _version: u64,
    pub nonce: String,
    pub data: D,
    metadata: JobMetadata,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct JobMetadata {
    pub created_at: DateTime<Utc>,
    pub last_attempt_at: Option<DateTime<Utc>>,
    pub attempts: u64,
    pub finished: bool,
}

impl<'a, D: Serialize + DeserializeOwned, R: redis::aio::ConnectionLike> Job<'a, D, R> {
    fn gen_nonce() -> String {
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(15)
            .map(char::from)
            .collect()
    }

    pub fn new(data: D, queue: &'a Queue<R>) -> Self {
        let nonce = Self::gen_nonce();
        let inner = JobInner {
            _version: SCHEMA_VERSION,
            data,
            nonce,
            metadata: JobMetadata {
                created_at: Utc::now(),
                ..Default::default()
            },
        };

        Self {
            consumer: None,
            queue: Some(queue),
            inner,
        }
    }

    pub(crate) fn redis_key(&self) -> String {
        let queue = self.queue.unwrap();
        crate::keys::format(
            crate::keys::TORTOISE_QUEUE_JOB,
            &[&queue.namespace, &queue.name, &self.inner.nonce],
        )
    }

    pub fn serialized(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.inner)
    }

    pub fn metadata(&self) -> &JobMetadata {
        &self.inner.metadata
    }

    pub fn set_nonce(&mut self, nonce: &str) {
        self.inner.nonce = nonce.to_string();
    }

    pub(crate) fn tick_attempt(&mut self) {
        self.inner.metadata.attempts += 1;
        self.inner.metadata.last_attempt_at = Some(Utc::now());
    }

    pub async fn sync_metadata(&mut self) -> Result<(), RedisError> {
        let queue = self.queue.unwrap();
        let mut connection = queue._redis.lock().await;

        let job_key = self.redis_key();
        let data = cmd("GET")
            .arg(&job_key)
            .query_async::<_, Option<String>>(&mut *connection)
            .await?;

        if let Some(data) = data {
            let data: JobInner<D> = serde_json::from_str(&data).unwrap();
            self.inner.metadata = data.metadata;
        }

        Ok(())
    }

    pub async fn save(&self) -> Result<(), RedisError> {
        let queue = self.queue.unwrap();
        let mut connection = queue._redis.lock().await;

        let serialized = self.serialized().unwrap();
        let job_key = self.redis_key();

        if self.inner.metadata.finished {
            cmd("DEL")
                .arg(&job_key)
                .query_async::<_, ()>(&mut *connection)
                .await?;
            return Ok(());
        }

        cmd("SET")
            .arg(&job_key)
            .arg(&serialized)
            .query_async::<_, ()>(&mut *connection)
            .await?;
        Ok(())
    }

    pub async fn publish(&mut self) -> Result<(), RedisError> {
        let queue = self.queue.unwrap();
        self.save().await?;
        queue.publish(&self.inner.nonce).await?;
        Ok(())
    }

    pub async fn r#return(&mut self) -> Result<(), RedisError> {
        if self.consumer.is_none() {
            panic!("\"Job::return\" must be called from Consumer job")
        }

        self.publish().await?;
        let consumer = self.consumer.as_ref().unwrap();
        consumer.drop_job(&self.inner.nonce).await
    }

    pub async fn delete(&mut self) -> Result<(), RedisError> {
        self.inner.metadata.finished = true;
        self.save().await?;
        if let Some(consumer) = &self.consumer {
            consumer.drop_job(&self.inner.nonce).await?;
        }

        Ok(())
    }
}
