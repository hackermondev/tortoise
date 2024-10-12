use std::sync::LazyLock;

use chrono::{DateTime, Utc};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use redis::{cmd, RedisError};
use serde::{Deserialize, Serialize};

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
    pub id: String,
    pub data: D,
    pub metadata: JobMetadata,

    #[serde(skip)]
    #[serde(default = "serde_queue_default")]
    pub(crate) queue: &'a Queue,

    #[serde(skip)]
    pub(crate) consumer: Option<&'a Consumer<'a>>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct JobMetadata {
    pub created_at: DateTime<Utc>,
    pub last_attempt_at: Option<DateTime<Utc>>,
    pub attempts: u64,
    pub finished: bool,
}

impl<'a, D: Serialize> Job<'a, D> {
    fn random_id() -> String {
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(15)
            .map(char::from)
            .collect()
    }

    pub fn new(data: D, queue: &'a Queue) -> Self {
        let id = Self::random_id();
        Self {
            _version: SCHEMA_VERSION,
            consumer: None,
            queue,
            id,
            data,
            metadata: JobMetadata::default(),
        }
    }

    pub fn serialized(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self)
    }

    async fn save(
        &self,
        connection: &mut impl redis::aio::ConnectionLike,
    ) -> Result<(), RedisError> {
        let serialized = self.serialized().unwrap();
        let job_key = crate::keys::format(
            crate::keys::TORTOISE_QUEUE_JOB,
            &[&self.queue.namespace, &self.queue.name, &self.id],
        );

        if self.metadata.finished {
            cmd("DEL").arg(&job_key).exec_async(connection).await?;
            return Ok(());
        }

        cmd("SET")
            .arg(&job_key)
            .arg(&serialized)
            .exec_async(connection)
            .await?;
        Ok(())
    }

    pub async fn publish(
        &mut self,
        connection: &mut impl redis::aio::ConnectionLike,
    ) -> Result<(), RedisError> {
        self.save(connection).await?;
        self.queue.publish(&self.id, connection).await?;
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
        consumer.drop_job(&self.id, connection).await
    }

    pub async fn delete(
        &mut self,
        connection: &mut impl redis::aio::ConnectionLike,
    ) -> Result<(), RedisError> {
        self.metadata.finished = true;
        self.save(connection).await?;
        if let Some(consumer) = &self.consumer {
            consumer.drop_job(&self.id, connection).await?;
        }

        Ok(())
    }
}
