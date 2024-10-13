use redis::{cmd, RedisError};
use serde::de::DeserializeOwned;

use crate::{consumer::Consumer, job::Job};

#[derive(Default, Debug)]
pub struct Queue {
    pub name: String,
    pub namespace: String,
    pub flags: Flags,
}

#[derive(Default, Debug)]
pub struct Flags {}

impl Queue {
    pub fn new(name: String, namespace: String) -> Self {
        Self {
            name,
            namespace,
            ..Default::default()
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
        connection: &mut impl redis::aio::ConnectionLike,
    ) -> Result<Option<Job<D>>, RedisError> {
        let job_key = crate::keys::format(
            crate::keys::TORTOISE_QUEUE_JOB,
            &[&self.namespace, &self.name, id],
        );
        let data = cmd("GET")
            .arg(job_key)
            .query_async::<Option<String>>(connection)
            .await?;
        if data.is_none() {
            return Ok(None);
        }

        let data = data.unwrap();
        let mut job = serde_json::from_str::<Job<D>>(&data).unwrap();
        job.queue = self;
        Ok(Some(job))
    }

    pub async fn publish(
        &self,
        job_id: &str,
        connection: &mut impl redis::aio::ConnectionLike,
    ) -> Result<(), RedisError> {
        cmd("RPUSH")
            .arg(self.key())
            .arg(job_id)
            .exec_async(connection)
            .await?;

        Ok(())
    }

    pub(crate) async fn get_consumers(
        &self,
        cursor: Option<String>,
        connection: &mut impl redis::aio::ConnectionLike,
    ) -> Result<(Vec<String>, String), RedisError> {
        let consumer_key = crate::keys::format(
            crate::keys::TORTOISE_QUEUE_CONSUMERS,
            &[&self.namespace, &self.name],
        );

        let mut c = cmd("SSCAN");
        c.arg(consumer_key);
        if let Some(cursor) = cursor {
            c.arg(cursor);
        }

        let (cursor, consumers) = c.query_async::<(String, Vec<String>)>(connection).await?;
        Ok((consumers, cursor))
    }
}

pub async fn run_cleanup_job(
    queue: &Queue,
    connection: &mut impl redis::aio::ConnectionLike,
) -> Result<(), RedisError> {
    let mut cursor: Option<String> = None;
    while let Ok((consumers, _cursor)) = queue.get_consumers(cursor, connection).await {
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
