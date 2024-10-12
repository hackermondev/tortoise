use redis::{cmd, RedisError};
use serde::de::DeserializeOwned;

use crate::{job::Job, queue::Queue};

pub struct Consumer<'a> {
    queue: &'a Queue,
    initialized: bool,
    identifier: String,
}

impl<'a> Consumer<'a> {
    pub fn identifier() -> String {
        String::new()
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
            .exec_async(connection)
            .await?;
        self.initialized = true;
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
            .query_async::<Option<String>>(connection)
            .await?;

        if job_id.is_none() {
            return Ok(None);
        }

        let job_id = job_id.unwrap();
        let job = self.queue.get_job(&job_id, connection).await?;
        Ok(job)
    }
}
