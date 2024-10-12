use std::sync::LazyLock;

use rand::{distributions::Alphanumeric, thread_rng, Rng};
use redis::{cmd, RedisError};
use serde::{Deserialize, Serialize};

use crate::queue::Queue;
static SCHEMA_VERSION: u64 = 1;

static SERDE_QUEUE_DEFAULT: LazyLock<Queue> = LazyLock::new(Queue::default);
fn serde_queue_default() -> &'static Queue {
    &SERDE_QUEUE_DEFAULT
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Job<'a, D> {
    #[serde(skip)]
    #[serde(default = "serde_queue_default")]
    pub(crate) queue: &'a Queue,

    pub _version: u64,
    pub id: String,
    pub data: D,
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
            queue,
            id,
            data,
        }
    }

    pub fn serialized(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self)
    }

    pub async fn save(
        &self,
        queue: &Queue,
        connection: &mut impl redis::aio::ConnectionLike,
    ) -> Result<(), RedisError> {
        let serialized = self.serialized().unwrap();
        let job_key = crate::keys::format(
            crate::keys::TORTOISE_QUEUE_JOB,
            &[&queue.namespace, &queue.name, &self.id],
        );

        cmd("SET")
            .arg(&job_key)
            .arg(&serialized)
            .exec_async(connection)
            .await?;
        Ok(())
    }
}
