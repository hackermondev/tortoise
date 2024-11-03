use chrono::{serde::ts_milliseconds, DateTime, Utc};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use redis::{cmd, JsonAsyncCommands, RedisError};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{consumer::Consumer, queue::Queue};
static SCHEMA_VERSION: u64 = 1;

#[derive(Debug)]
pub struct Job<'a, D, R: redis::aio::ConnectionLike + Send> {
    pub inner: JobInner<D>,
    pub(crate) queue: Option<&'a Queue<R>>,
    pub(crate) consumer: Option<&'a Consumer<'a, R>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobInner<D> {
    pub _tortoise_version: u64,
    pub nonce: String,

    metadata: JobMetadata,
    pub data: D,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct JobMetadata {
    pub attempts: u64,
    pub created_at: DateTime<Utc>,
    pub last_attempt_at: Option<DateTime<Utc>>,
    #[serde(with = "ts_milliseconds")]
    pub scheduled_retry_after: DateTime<Utc>,

    pub consumer_id: Option<String>,
    pub assigned: bool,

    pub group_tag: Option<String>,
    pub completed: bool,
}

impl<'a, D: Serialize + DeserializeOwned + Send + Sync, R: redis::aio::ConnectionLike + Send>
    Job<'a, D, R>
{
    fn gen_nonce() -> String {
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(15)
            .map(char::from)
            .collect()
    }

    pub fn new(data: D, queue: &'a Queue<R>, nonce: Option<String>) -> Self {
        let nonce = nonce.unwrap_or(Self::gen_nonce());
        let inner = JobInner {
            _tortoise_version: SCHEMA_VERSION,
            data,
            nonce,
            metadata: JobMetadata {
                created_at: Utc::now(),
                scheduled_retry_after: Utc::now(),
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

    pub fn metadata(&self) -> &JobMetadata {
        &self.inner.metadata
    }

    pub async fn sync_metadata(&mut self) -> Result<(), RedisError> {
        let queue = self.queue.unwrap();
        let mut connection = queue._redis.lock().await;

        let job_key = self.redis_key();
        let metadata = connection
            .json_get::<_, _, Option<String>>(job_key, "$.metadata")
            .await?;

        if let Some(metadata) = metadata {
            let metadata: JobMetadata = serde_json::from_str(&metadata).unwrap();
            self.inner.metadata = metadata;
        }

        Ok(())
    }

    pub async fn set_scheduled_retry_after(&mut self, scheduled_retry_after: DateTime<Utc>) {
        self.inner.metadata.scheduled_retry_after = scheduled_retry_after;
    }

    pub fn set_group_tag(&mut self, group_tag: &str) {
        self.inner.metadata.group_tag = Some(group_tag.to_string());
    }

    pub async fn save(&self) -> Result<(), RedisError> {
        let queue = self.queue.unwrap();
        let mut connection = queue._redis.lock().await;
        let job_key = self.redis_key();

        if self.inner.metadata.completed {
            cmd("DEL")
                .arg(&job_key)
                .query_async::<_, ()>(&mut *connection)
                .await?;
            return Ok(());
        }

        connection.json_set(&job_key, "$", &self.inner).await?;
        Ok(())
    }

    pub async fn unassign(&mut self) -> Result<(), RedisError> {
        self.inner.metadata.attempts += 1;
        self.inner.metadata.last_attempt_at = Some(Utc::now());
        self.inner.metadata.consumer_id = None;
        self.inner.metadata.assigned = false;
        self.save().await
    }

    pub async fn delete(&mut self) -> Result<(), RedisError> {
        self.inner.metadata.completed = true;
        self.save().await?;
        Ok(())
    }
}

// impl<'a, D: Send, R: redis::aio::ConnectionLike + Send> Drop for Job<'a, D, R> {
//     fn drop(&mut self) {
//         let consumer = self.consumer.take().unwrap();
//         tokio::spawn(async move {
//             consumer.drop_job(&self.inner.nonce).await;
//         });

//         todo!()
//     }
// }
