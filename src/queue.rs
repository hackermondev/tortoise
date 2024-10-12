use redis::{cmd, RedisError};
use serde::{de::DeserializeOwned, Serialize};

use crate::job::Job;

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
            .arg(&job_id)
            .exec_async(connection)
            .await?;

        Ok(())
    }
}
