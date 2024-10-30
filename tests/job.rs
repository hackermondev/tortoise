use serde_json::{json, Value};
use tortoise::{consumer::Consumer, job::Job, queue::Queue};

#[tokio::test]
async fn basic_job() {
    let client = redis::Client::open("redis://127.0.0.1").unwrap();
    let connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("Redis is not running");

    let queue = Queue::new("queue_basic".to_string(), "test".to_string(), connection);
    let data = json!({ "test": true });

    let mut job = Job::new(data, &queue);
    job.publish().await.expect("Failed to publish Job");

    let mut consumer = Consumer::new(&queue);
    consumer
        .initalize()
        .await
        .expect("Failed to initalize consumer");

    let job = consumer
        .next_job::<Value>()
        .await
        .expect("Failed to get Consumer job");

    assert!(job.is_some());
    assert!(job.unwrap().inner.data["test"] == true);
}

#[tokio::test]
async fn updating_job() {
    let client = redis::Client::open("redis://127.0.0.1").unwrap();
    let connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("Redis is not running");

    let queue = Queue::new("queue_updating".to_string(), "test".to_string(), connection);
    let data = json!({ "counter": 1 });

    let mut job = Job::new(data, &queue);
    job.publish().await.expect("Failed to publish Job");

    let mut consumer = Consumer::new(&queue);
    consumer
        .initalize()
        .await
        .expect("Failed to initalize consumer");
    let job = consumer
        .next_job::<Value>()
        .await
        .expect("Failed to get Consumer job");

    let mut job = job.unwrap();
    let counter = job.inner.data.get_mut("counter").unwrap();
    *counter = 2.into();

    job.r#return().await.expect("Failed to return Job");

    let updated_job = consumer
        .next_job::<Value>()
        .await
        .expect("Failed to get Consumer job");

    assert!(updated_job.is_some());
    println!("{:?}", updated_job.as_ref().unwrap().metadata());
    assert!(updated_job.as_ref().unwrap().inner.data["counter"] == 2);
    assert!(updated_job.as_ref().unwrap().metadata().attempts == 2);
}

#[tokio::test]
async fn finished_job() {
    let client = redis::Client::open("redis://127.0.0.1").unwrap();
    let connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("Redis is not running");

    let queue = Queue::new("queue_finish".to_string(), "test".to_string(), connection);
    let data = json!({ "hello": "world" });

    let mut job = Job::new(data, &queue);
    job.publish().await.expect("Failed to publish Job");

    let mut consumer = Consumer::new(&queue);
    consumer
        .initalize()
        .await
        .expect("Failed to initalize consumer");
    let job = consumer
        .next_job::<Value>()
        .await
        .expect("Failed to get Consumer job");

    let mut job = job.unwrap();
    job.delete().await.expect("Failed to return Job");

    let no_job = consumer
        .next_job::<Value>()
        .await
        .expect("Failed to get Consumer job");

    assert!(no_job.is_none());
}
