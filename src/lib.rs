pub mod consumer;
pub mod job;
pub mod queue;
pub(crate) mod scripts;

static TORTOISE_JOB_INDEX: &str = r#"
FT.CREATE {} ON JSON PREFIX 1 {}
SCHEMA 
    $._tortoise_version as _tortoise_version NUMERIC
    $.nonce as nonce TEXT

    $.metadata.attempts as metadata_attempts NUMERIC
    $.metadata.created_at as metadata_created_at TEXT
    $.metadata.last_attempt_at as metadata_last_attempt_at TEXT
    $.metadata.scheduled_retry_after as metadata_scheduled_retry_after NUMERIC

    $.metadata.consumer_id as metadata_consumer_id TEXT
    $.metadata.assigned as metadata_assigned TAG

    $.metadata.group_tag as metadata_group_tag TEXT
    $.metadata.completed as metadata_completed TAG
"#;

pub(crate) mod keys {
    pub static TORTOISE_QUEUE_INDEX: &str = "tortoise:{}:queue:{}";
    pub static TORTOISE_QUEUE_JOB_PREFIX: &str = "tortoise:{}:queue:{}:job:";
    pub static TORTOISE_QUEUE_JOB: &str = "tortoise:{}:queue:{}:job:{}";
    pub static TORTOISE_QUEUE_CONSUMERS: &str = "tortoise:{}:queue:{}:progress";
    pub static TORTOISE_QUEUE_CONSUMER_PROGRESS_PING: &str =
        "tortoise:{}:queue:{}:progress:{}:ping";

    pub fn format(key: &str, data: &[&str]) -> String {
        let mut key = String::from(key);
        for var in data {
            key = key.replacen("{}", var, 1);
        }

        key
    }
}

pub fn clean_tokenizer_str(data: &str) -> String {
    let mut clean = String::from(data);

    [
        ',', '.', '<', '>', '{', '}', '[', ']', '"', '\'', ':', ';', '!', '@', '#', '$', '%', '^',
        '&', '*', '(', ')', '-', '+', '=', '~',
    ]
    .map(|t| clean = clean.replace(t, &format!("\\\\{t}")));
    clean
}
