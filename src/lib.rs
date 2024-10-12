pub mod consumer;
pub mod job;
pub mod queue;

pub(crate) mod keys {
    pub static TORTOISE_QUEUE_LIST: &str = "tortoise:{}:queue:{}";
    pub static TORTOISE_QUEUE_JOB: &str = "tortoise:{}:queue:{}:job:{}";
    pub static TORTOISE_QUEUE_CONSUMERS: &str = "tortoise:{}:queue:{}:progress";
    pub static TORTOISE_QUEUE_CONSUMER_PROGRESS_LIST: &str = "tortoise:{}:queue:{}:progress:{}";
    pub static TORTOISE_QUEUE_CONSUMER_CLEANUP_TRAP: &str = "tortoise:{}:queue:{}:cleanup";

    pub fn format(key: &str, data: &[&str]) -> String {
        let mut key = String::from(key);
        for var in data {
            key = key.replacen("{}", var, 1);
        }

        key
    }
}
