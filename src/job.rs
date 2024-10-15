use std::sync::LazyLock;

use chrono::{DateTime, Utc};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use redis::{cmd, RedisError};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{consumer::Consumer, queue::Queue};
static SCHEMA_VERSION: u64 = 1;

static SERDE_QUEUE_DEFAULT: LazyLock<Queue> = LazyLock::new(Queue::default);
fn serde_queue_default() -> &'static Queue {
    &SERDE_QUEUE_DEFAULT
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Job<'a, D> {
    // Serde serialized data
    pub _version: u64,
    pub nonce: String,
    pub data: D,
    metadata: JobMetadata,

    #[serde(skip)]
    #[serde(default = "serde_queue_default")]
    pub(crate) queue: &'a Queue,

    #[serde(skip)]
    pub(crate) consumer: Option<&'a Consumer<'a>>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct JobMetadata {
    pub created_at: DateTime<Utc>,
    pub last_attempt_at: Option<DateTime<Utc>>,
    pub attempts: u64,
    pub finished: bool,
}

impl<'a, D: Serialize + DeserializeOwned> Job<'a, D> {
    fn gen_nonce() -> String {
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(15)
            .map(char::from)
            .collect()
    }

    pub fn new(data: D, queue: &'a Queue) -> Self {
        let nonce = Self::gen_nonce();
        Self {
            _version: SCHEMA_VERSION,
            consumer: None,
            queue,
            nonce,
            data,
            metadata: JobMetadata {
                created_at: Utc::now(),
                ..Default::default()
            },
        }
    }

    pub(crate) fn redis_key(&self) -> String {
        crate::keys::format(
            crate::keys::TORTOISE_QUEUE_JOB,
            &[&self.queue.namespace, &self.queue.name, &self.nonce],
        )
    }

    pub fn serialized(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self)
    }

    pub fn metadata(&self) -> &JobMetadata {
        &self.metadata
    }

    pub fn set_nonce(&mut self, nonce: &str) {
        self.nonce = nonce.to_string();
    }

    pub(crate) fn tick_attempt(&mut self) {
        self.metadata.attempts += 1;
        self.metadata.last_attempt_at = Some(Utc::now());
    }
     
    pub async fn sync_metadata(&mut self, connection: &mut impl redis::aio::ConnectionLike) -> Result<(), RedisError> {
        let job_key = self.redis_key();
        let data = cmd("GET")
            .arg(&job_key)
            .query_async::<_, Option<String>>(connection)
            .await?;

        if let Some(data) = data {
            let data: Job<'_, D> = serde_json::from_str(&data).unwrap();
            self.metadata = data.metadata;
        }

        Ok(())
    }

    pub(crate) async fn save(
        &self,
        connection: &mut impl redis::aio::ConnectionLike,
    ) -> Result<(), RedisError> {
        let serialized = self.serialized().unwrap();
        let job_key = self.redis_key();

        if self.metadata.finished {
            cmd("DEL")
                .arg(&job_key)
                .query_async::<_, ()>(connection)
                .await?;
            return Ok(());
        }

        cmd("SET")
            .arg(&job_key)
            .arg(&serialized)
            .query_async::<_, ()>(connection)
            .await?;
        Ok(())
    }

    pub async fn publish(
        &mut self,
        connection: &mut impl redis::aio::ConnectionLike,
    ) -> Result<(), RedisError> {
        self.save(connection).await?;
        self.queue.publish(&self.nonce, connection).await?;
        Ok(())
    }

    pub async fn r#return(
        &mut self,
        connection: &mut impl redis::aio::ConnectionLike,
    ) -> Result<(), RedisError> {
        if self.consumer.is_none() {
            panic!("\"Job::return\" must be called from Consumer job")
        }

        self.publish(connection).await?;
        let consumer = self.consumer.as_ref().unwrap();
        consumer.drop_job(&self.nonce, connection).await
    }

    pub async fn delete(
        &mut self,
        connection: &mut impl redis::aio::ConnectionLike,
    ) -> Result<(), RedisError> {
        self.metadata.finished = true;
        self.save(connection).await?;
        if let Some(consumer) = &self.consumer {
            consumer.drop_job(&self.nonce, connection).await?;
        }

        Ok(())
    }
}
