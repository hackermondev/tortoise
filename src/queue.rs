use redis::{cmd, RedisError};
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use crate::{
    consumer::Consumer,
    job::{Job, JobInner},
};

#[derive(Debug)]
pub struct Queue<R: redis::aio::ConnectionLike> {
    pub name: String,
    pub namespace: String,
    pub flags: Flags,
    pub(crate) _redis: Mutex<R>,
}

#[derive(Default, Debug)]
pub struct Flags {}

impl<R: redis::aio::ConnectionLike> Queue<R> {
    pub fn new(name: String, namespace: String, _redis: R) -> Self {
        Self {
            name,
            namespace,
            _redis: Mutex::new(_redis),
            flags: Flags::default(),
        }
    }

    pub(crate) fn key(&self) -> String {
        crate::keys::format(
            crate::keys::TORTOISE_QUEUE_LIST,
            &[&self.namespace, &self.name],
        )
    }

    pub async fn get_job<'a, D: DeserializeOwned>(
        &self,
        id: &str,
    ) -> Result<Option<Job<D, R>>, RedisError> {
        let mut connection = self._redis.lock().await;
        let job_key = crate::keys::format(
            crate::keys::TORTOISE_QUEUE_JOB,
            &[&self.namespace, &self.name, id],
        );

        let data = cmd("GET")
            .arg(job_key)
            .query_async::<_, Option<String>>(&mut *connection)
            .await?;
        if data.is_none() {
            return Ok(None);
        }

        let data = data.unwrap();
        let job = serde_json::from_str::<JobInner<D>>(&data).unwrap();
        let job = Job {
            inner: job,
            queue: Some(self),
            consumer: None,
        };
        Ok(Some(job))
    }

    pub(crate) async fn publish(&self, job_id: &str) -> Result<(), RedisError> {
        let mut connection = self._redis.lock().await;
        cmd("RPUSH")
            .arg(self.key())
            .arg(job_id)
            .query_async::<_, ()>(&mut *connection)
            .await?;

        Ok(())
    }

    pub(crate) async fn get_consumers(
        &self,
        cursor: Option<String>,
    ) -> Result<(Vec<String>, String), RedisError> {
        let mut connection = self._redis.lock().await;
        let consumer_key = crate::keys::format(
            crate::keys::TORTOISE_QUEUE_CONSUMERS,
            &[&self.namespace, &self.name],
        );

        let mut c = cmd("SSCAN");
        c.arg(consumer_key);
        if let Some(cursor) = cursor {
            c.arg(cursor);
        }

        let (cursor, consumers) = c
            .query_async::<_, (String, Vec<String>)>(&mut *connection)
            .await?;
        Ok((consumers, cursor))
    }
}

pub async fn run_cleanup_job<R: redis::aio::ConnectionLike>(
    queue: &Queue<R>,
    connection: &mut impl redis::aio::ConnectionLike,
) -> Result<(), RedisError> {
    let mut cursor: Option<String> = None;
    while let Ok((consumers, _cursor)) = queue.get_consumers(cursor).await {
        cursor = Some(_cursor);
        if consumers.is_empty() {
            break;
        }

        for consumer_id in consumers {
            let online = Consumer::ping(queue, &consumer_id, connection).await?;
            if online {
                continue;
            }

            let consumer = Consumer {
                queue,
                initialized: true,
                identifier: consumer_id,
            };

            consumer.drop(connection).await?;
        }
    }

    Ok(())
}
