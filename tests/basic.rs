use serde_json::{json, Value};
use tortoise::{consumer::Consumer, job::Job, queue::Queue};

#[tokio::test]
async fn basic_job_assignments() {
    pretty_env_logger::init();
    let client = redis::Client::open("redis://127.0.0.1").unwrap();
    let connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("Redis is not running");

    let queue = Queue::new("queue_basic".to_string(), "test".to_string(), connection);
    queue.ensure_index().await.expect("failed to init queue");

    for _ in 0..5 {
        let data = json!({ "test": true });
        let job = Job::new(data, &queue, None);
        job.save().await.expect("Failed to save Job");
    }

    let mut consumer = Consumer::new(&queue);
    consumer
        .initalize()
        .await
        .expect("Failed to initalize consumer");

    let jobs = consumer.next_jobs::<Value>(5).await.unwrap();
    let _jobs = consumer.next_jobs::<Value>(5).await.unwrap();

    assert!(jobs.len() == 5);
    assert!(_jobs.is_empty());
}

#[tokio::test]
async fn basic_job_assignments_group() {
    let client = redis::Client::open("redis://127.0.0.1").unwrap();
    let connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("Redis is not running");

    let queue = Queue::new(
        "queue_basic_group".to_string(),
        "test".to_string(),
        connection,
    );
    queue.ensure_index().await.expect("failed to init queue");

    for i in 0..5 {
        for _ in 0..10 {
            let data = json!({ "test": true });
            let mut job = Job::new(data, &queue, None);
            job.set_group_tag(&i.to_string());
            job.save().await.expect("Failed to save Job");
        }
    }

    let mut consumer = Consumer::new(&queue);
    consumer
        .initalize()
        .await
        .expect("Failed to initalize consumer");

    for _ in 0..5 {
        let group = consumer.next_jobs_group::<Value>(100).await.unwrap();
        assert_eq!(group.len(), 10);
    }

    let empty = consumer.next_jobs_group::<Value>(100).await.unwrap();
    assert_eq!(empty.len(), 0);
}
