extern crate env_logger;

use failure::Error;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

/// This program demonstrates consuming messages through a `Consumer`.
/// This is a convenient client that will fit most use cases.  Note
/// that messages must be marked and commited as consumed to ensure
/// only once delivery.
#[tokio::main]
async fn main() {
    env_logger::init();

    let broker = "localhost:9092".to_owned();
    let topic = "my-topic".to_owned();
    let group = "my-group".to_owned();

    if let Err(e) = consume_messages(group, topic, vec![broker]).await {
        println!("Failed consuming messages: {}", e);
    }
}

async fn consume_messages(group: String, topic: String, brokers: Vec<String>) -> Result<(), Error> {
    let mut con = Consumer::from_hosts(brokers)
        .with_topic(topic)
        .with_group(group)
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create().await?;

    loop {
        let mss = con.poll().await?;
        if mss.is_empty() {
            println!("No messages available right now.");
            return Ok(());
        }

        for ms in mss.iter() {
            for m in ms.messages() {
                println!(
                    "{}:{}@{}: {:?}",
                    ms.topic(),
                    ms.partition(),
                    m.offset,
                    m.value
                );
            }
            let _ = con.consume_messageset(ms);
        }
        con.commit_consumed().await?;
    }
}
