use redis::{cmd, JsonAsyncCommands, RedisError};
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use crate::{
    consumer::Consumer,
    job::{Job, JobInner},
};

#[cfg(feature = "garbage_collector")]
use tokio::sync::mpsc::channel;

#[derive(Debug)]
pub struct Queue<R: redis::aio::ConnectionLike + Send> {
    pub name: String,
    pub namespace: String,
    pub flags: Flags,
    pub(crate) _redis: Mutex<R>,
}

#[derive(Default, Debug)]
pub struct Flags {}

impl<R: redis::aio::ConnectionLike + Send> Queue<R> {
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
            crate::keys::TORTOISE_QUEUE_INDEX,
            &[&self.namespace, &self.name],
        )
    }

    pub async fn ensure_index(&self) -> Result<(), RedisError> {
        let index_name = self.key();
        let prefix = crate::keys::format(
            crate::keys::TORTOISE_QUEUE_JOB_PREFIX,
            &[&self.namespace, &self.name],
        );

        let command = crate::keys::format(crate::TORTOISE_JOB_INDEX, &[&index_name, &prefix]);
        let command = command.replace("\n", " ");
        let command = command.trim();

        let args = command
            .split(" ")
            .filter(|c| !c.is_empty())
            .collect::<Vec<&str>>();

        let mut connection = self._redis.lock().await;
        let _ = cmd(args[0])
            .arg(&args[1..])
            .query_async::<_, ()>(&mut *connection)
            .await;

        Ok(())
    }

    pub async fn get_job<'a, D: DeserializeOwned + Send>(
        &self,
        id: &str,
    ) -> Result<Option<Job<D, R>>, RedisError> {
        let mut connection = self._redis.lock().await;
        let job_key = crate::keys::format(
            crate::keys::TORTOISE_QUEUE_JOB,
            &[&self.namespace, &self.name, id],
        );

        let job = connection
            .json_get::<_, _, Option<String>>(&job_key, ".")
            .await?;
        if job.is_none() {
            return Ok(None);
        }

        let job = job.unwrap();
        let job = serde_json::from_str::<JobInner<D>>(&job).unwrap();
        let job = Job {
            inner: job,
            queue: Some(self),
            consumer: None,
        };

        Ok(Some(job))
    }

    pub(crate) async fn get_consumers(
        &self,
        cursor: &Option<String>,
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
        } else {
            c.arg(0);
        }

        let (cursor, consumers) = c
            .query_async::<_, (String, Vec<String>)>(&mut *connection)
            .await?;
        Ok((consumers, cursor))
    }
}

pub async fn run_cleanup_job<R: redis::aio::ConnectionLike + Send>(
    queue: &Queue<R>,
) -> Result<(), RedisError> {
    let mut cursor: Option<String> = None;
    while let (consumers, _cursor) = queue.get_consumers(&cursor).await? {
        if consumers.is_empty() {
            break;
        }

        if let Some(previous_cursor) = &cursor {
            if previous_cursor == &_cursor {
                break;
            }
        }

        cursor = Some(_cursor);
        for consumer_id in consumers {
            let online = Consumer::ping(queue, &consumer_id).await?;
            if online {
                continue;
            }

            log::info!("clean up job, dropping consumer {}", consumer_id);
            
            #[cfg(feature = "garbage_collector")]
            let garbage = channel(1);
            let consumer = Consumer {
                queue,
                initialized: true,
                identifier: consumer_id,

                #[cfg(feature = "garbage_collector")]
                garbage: garbage.0,
                #[cfg(feature = "garbage_collector")]
                garbage_recv: Mutex::new(garbage.1),
            };

            consumer.drop().await?;
        }
    }

    Ok(())
}
