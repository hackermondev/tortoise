use serde_json::{json, Value};
use tortoise::{
    consumer::Consumer,
    job::Job,
    queue::{run_cleanup_job, Queue},
};

#[tokio::test]
async fn should_drop_stalled_consumer() {
    let client = redis::Client::open("redis://127.0.0.1").unwrap();
    let connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("Redis is not running");

    let queue = Queue::new(
        "queue_redundancy".to_string(),
        "test".to_string(),
        connection,
    );
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

    // Consumer should take job
    let jobs = consumer.next_jobs::<Value>(5).await.unwrap();
    assert_eq!(jobs.len(), 5);

    // No more jobs left
    {
        let jobs = consumer.next_jobs::<Value>(5).await.unwrap();
        assert_eq!(jobs.len(), 0);
    }

    // Cleanup should drop Consumer's jobs
    run_cleanup_job(&queue)
        .await
        .expect("Failed to run cleanup");

    // Jobs should be returned
    let jobs = consumer.next_jobs::<Value>(5).await.unwrap();
    assert_eq!(jobs.len(), 5);
}
