use chrono::Utc;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use redis::{cmd, RedisError};
use serde::de::DeserializeOwned;

use crate::{job::Job, queue::Queue, scripts};

static CONSUMER_PING_EX_SECONDS: usize = 60 * 5;

#[derive(Debug, Clone)]
pub struct Consumer<'a> {
    pub(crate) queue: &'a Queue,
    pub(crate) initialized: bool,
    pub(crate) identifier: String,
}

impl<'a> Consumer<'a> {
    pub fn identifier() -> String {
        let hostname = gethostname::gethostname();
        let token: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();

        format!("{}-{}", hostname.to_str().unwrap(), token)
    }

    pub fn new(queue: &'a Queue) -> Self {
        Self {
            queue,
            initialized: false,
            identifier: Self::identifier(),
        }
    }

    pub(crate) fn key(&self) -> String {
        crate::keys::format(
            crate::keys::TORTOISE_QUEUE_CONSUMER_PROGRESS_LIST,
            &[&self.queue.namespace, &self.queue.name, &self.identifier],
        )
    }

    pub async fn initalize(
        &mut self,
        connection: &mut impl redis::aio::ConnectionLike,
    ) -> Result<(), RedisError> {
        let consumer_list = crate::keys::format(
            crate::keys::TORTOISE_QUEUE_CONSUMERS,
            &[&self.queue.namespace, &self.queue.name],
        );

        cmd("SADD")
            .arg(&consumer_list)
            .arg(&self.identifier)
            .query_async::<_, ()>(connection)
            .await?;
        self.initialized = true;
        Ok(())
    }

    pub async fn pong(
        &self,
        connection: &mut impl redis::aio::ConnectionLike,
    ) -> Result<(), RedisError> {
        let consumer_ping = crate::keys::format(
            crate::keys::TORTOISE_QUEUE_CONSUMER_PROGRESS_PING,
            &[&self.queue.namespace, &self.queue.name, &self.identifier],
        );

        cmd("SET")
            .arg(&consumer_ping)
            .arg(&self.identifier)
            .arg("EX")
            .arg(CONSUMER_PING_EX_SECONDS)
            .query_async::<_, ()>(connection)
            .await?;
        Ok(())
    }

    pub async fn next_job<D: DeserializeOwned>(
        &self,
        connection: &mut impl redis::aio::ConnectionLike,
    ) -> Result<Option<Job<D>>, RedisError> {
        if !self.initialized {
            panic!("uninitialized")
        }

        let queue = self.queue.key();
        let consumer = self.key();

        let job_id = cmd("LMOVE")
            .arg(queue)
            .arg(consumer)
            .arg("RIGHT")
            .arg("LEFT")
            .query_async::<_, Option<String>>(connection)
            .await?;

        if job_id.is_none() {
            return Ok(None);
        }

        let job_id = job_id.unwrap();
        let mut job = self.queue.get_job(&job_id, connection).await?;
        if let Some(job) = job.as_mut() {
            job.consumer = Some(self);
            job.metadata.attempts += 1;
            job.metadata.last_attempt_at = Some(Utc::now());
        }

        self.pong(connection).await?;
        Ok(job)
    }

    pub(crate) async fn drop_job(
        &self,
        job_id: &str,
        connection: &mut impl redis::aio::ConnectionLike,
    ) -> Result<(), RedisError> {
        let key = self.key();
        cmd("LREM")
            .arg(key)
            .arg(1)
            .arg(job_id)
            .query_async::<_, ()>(connection)
            .await?;

        self.pong(connection).await?;
        Ok(())
    }

    pub async fn drop(
        self,
        connection: &mut impl redis::aio::ConnectionLike,
    ) -> Result<usize, RedisError> {
        let queue_key = self.queue.key();
        let key = self.key();

        scripts::ATOMIC_MIGRATE_LIST
            .arg(key)
            .key(queue_key)
            .invoke_async(connection)
            .await
    }
}

impl Consumer<'_> {
    pub async fn ping(
        queue: &Queue,
        consumer_id: &str,
        connection: &mut impl redis::aio::ConnectionLike,
    ) -> Result<bool, RedisError> {
        let consumer_ping = crate::keys::format(
            crate::keys::TORTOISE_QUEUE_CONSUMER_PROGRESS_PING,
            &[&queue.namespace, &queue.name, consumer_id],
        );

        cmd("EXISTS")
            .arg(consumer_ping)
            .query_async(connection)
            .await
    }
}
