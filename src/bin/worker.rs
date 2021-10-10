use std::time::Duration;

use log::info;

use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;

async fn produce(brokers: &str, topic_name: &str) -> anyhow::Result<()> {
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    // This loop is non blocking: all messages will be sent one after the other, without waiting
    // for the results.
    for i in 0.. {
        // The send operation on the topic returns a future, which will be
        // completed once the result or failure from Kafka is received.
        producer
            .send(
                FutureRecord::to(topic_name)
                    .payload(&format!("Message {}", i))
                    .key(&format!("Key {}", i))
                    .headers(OwnedHeaders::new().add("header_key", "header_value")),
                Duration::from_secs(0),
            )
            .await
            .map_err(|(e, _)| e)?;

        // This will be executed when the result is received.
        info!("Delivery status for message {} received", i);
        tokio::time::sleep(Duration::from_millis(1_000)).await;
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topic = "my-topic";
    let brokers = "localhost:9092";

    produce(brokers, topic).await.unwrap();
}
