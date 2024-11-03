use chrono::Utc;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use redis::{cmd, JsonAsyncCommands, RedisError};
use serde::de::DeserializeOwned;

use crate::{job::Job, queue::Queue, scripts};

static CONSUMER_PING_EX_SECONDS: usize = 60 * 5;

#[derive(Debug, Clone)]
pub struct Consumer<'a, R: redis::aio::ConnectionLike + Send> {
    pub(crate) queue: &'a Queue<R>,
    pub(crate) initialized: bool,
    pub(crate) identifier: String,
}

impl<'a, R: redis::aio::ConnectionLike + Send> Consumer<'a, R> {
    pub fn identifier() -> String {
        let hostname = gethostname::gethostname();
        let token: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();

        format!("{}-{}", hostname.to_str().unwrap(), token)
    }

    pub fn new(queue: &'a Queue<R>) -> Self {
        Self {
            queue,
            initialized: false,
            identifier: crate::clean_tokenizer_str(&Self::identifier()),
        }
    }

    pub async fn initalize(&mut self) -> Result<(), RedisError> {
        let consumer_list = crate::keys::format(
            crate::keys::TORTOISE_QUEUE_CONSUMERS,
            &[&self.queue.namespace, &self.queue.name],
        );

        let mut connection = self.queue._redis.lock().await;
        cmd("SADD")
            .arg(&consumer_list)
            .arg(&self.identifier)
            .query_async::<_, ()>(&mut *connection)
            .await?;
        self.initialized = true;
        Ok(())
    }

    async fn deinitalize(&mut self) -> Result<(), RedisError> {
        let consumer_list = crate::keys::format(
            crate::keys::TORTOISE_QUEUE_CONSUMERS,
            &[&self.queue.namespace, &self.queue.name],
        );

        let mut connection = self.queue._redis.lock().await;
        cmd("SREM")
            .arg(&consumer_list)
            .arg(&self.identifier)
            .query_async::<_, ()>(&mut *connection)
            .await?;
        self.initialized = false;
        Ok(())
    }

    pub async fn pong(&self) -> Result<(), RedisError> {
        let mut connection = self.queue._redis.lock().await;
        let consumer_ping = crate::keys::format(
            crate::keys::TORTOISE_QUEUE_CONSUMER_PROGRESS_PING,
            &[&self.queue.namespace, &self.queue.name, &self.identifier],
        );

        cmd("SET")
            .arg(&consumer_ping)
            .arg(&self.identifier)
            .arg("EX")
            .arg(CONSUMER_PING_EX_SECONDS)
            .query_async::<_, ()>(&mut *connection)
            .await?;
        Ok(())
    }

    pub(crate) async fn drop_job(&self, job_nonce: &str) -> Result<(), RedisError> {
        let mut connection = self.queue._redis.lock().await;
        let job_key = crate::keys::format(
            crate::keys::TORTOISE_QUEUE_JOB,
            &[&self.queue.namespace, &self.queue.name, job_nonce],
        );

        connection
            .json_set(&job_key, "$.metadata.assigned", &String::from("false"))
            .await?;
        connection
            .json_set(&job_key, "$.metadata.consumer_id", &String::from("null"))
            .await?;
        Ok(())
    }

    pub async fn drop(mut self) -> Result<usize, RedisError> {
        let mut connection = self.queue._redis.lock().await;
        let consumer = self.identifier.clone();
        let queue_key = self.queue.key();
        let dropped = scripts::DROP_CONSUMER_JOBS
            .key(queue_key)
            .arg(consumer)
            .invoke_async(&mut *connection)
            .await?;

        drop(connection);
        self.deinitalize().await?;
        Ok(dropped)
    }
}

impl<R: redis::aio::ConnectionLike + Send> Consumer<'_, R> {
    pub async fn ping(queue: &Queue<R>, consumer_id: &str) -> Result<bool, RedisError> {
        let mut connection = queue._redis.lock().await;
        let consumer_ping = crate::keys::format(
            crate::keys::TORTOISE_QUEUE_CONSUMER_PROGRESS_PING,
            &[&queue.namespace, &queue.name, consumer_id],
        );

        cmd("EXISTS")
            .arg(consumer_ping)
            .query_async(&mut *connection)
            .await
    }
}

impl<'a, R: redis::aio::ConnectionLike + Send> Consumer<'a, R> {
    pub async fn next_jobs<'b, D: DeserializeOwned>(
        &'a self,
        limit: usize,
    ) -> Result<Vec<Job<'a, D, R>>, RedisError> {
        let mut connection = self.queue._redis.lock().await;
        let id = &self.identifier;
        let index = self.queue.key();
        let now = Utc::now().timestamp_millis();

        let data = scripts::JOB_SEARCH
            .key(index)
            .arg(id)
            .arg(limit)
            .arg(now)
            .invoke_async::<_, Vec<(String, (String, String))>>(&mut *connection)
            .await?;

        drop(connection);
        let job_ids: Vec<&String> = data.iter().map(|(_, (_, id))| id).collect();
        let mut jobs = vec![];

        for id in job_ids {
            let job = self.queue.get_job::<D>(id).await?;
            if let Some(mut job) = job {
                job.consumer = Some(self);
                jobs.push(job);
            }
        }

        Ok(jobs)
    }

    pub async fn next_jobs_group<'b, D: DeserializeOwned>(
        &'a self,
        limit: usize,
    ) -> Result<Vec<Job<'a, D, R>>, RedisError> {
        let mut connection = self.queue._redis.lock().await;
        let id = &self.identifier;
        let index = self.queue.key();
        let now = Utc::now().timestamp_millis();

        let job_prefix = crate::keys::format(
            crate::keys::TORTOISE_QUEUE_JOB_PREFIX,
            &[&self.queue.namespace, &self.queue.name],
        );
        let job_ids = scripts::JOB_SEARCH_GROUP
            .key(index)
            .arg(id)
            .arg(job_prefix)
            .arg(limit)
            .arg(now)
            .invoke_async::<_, Vec<String>>(&mut *connection)
            .await?;

        drop(connection);
        let mut jobs = vec![];

        for id in job_ids {
            let job = self.queue.get_job::<D>(&id).await?;
            if let Some(mut job) = job {
                job.consumer = Some(self);
                jobs.push(job);
            }
        }

        Ok(jobs)
    }
}
