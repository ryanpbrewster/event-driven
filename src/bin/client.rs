use std::time::Duration;

use log::info;

use anyhow::anyhow;

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, DefaultConsumerContext};
use rdkafka::message::{Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let brokers = "localhost:9092";
    let group_id = "my-group";

    let context = DefaultConsumerContext;

    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");
    let consumer: StreamConsumer<_> = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&["requests"])
        .expect("Can't subscribe to specified topics");

    loop {
        let m = consumer.recv().await?;
        let payload = match m.payload_view::<str>() {
            Some(Ok(s)) => s,
            Some(Err(e)) => {
                return Err(anyhow!(
                    "Error while deserializing message payload: {:?}",
                    e
                ))
            }
            None => return Err(anyhow!("Error while deserializing message payload: None")),
        };
        info!(
            "key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
            m.key(),
            payload,
            m.topic(),
            m.partition(),
            m.offset(),
            m.timestamp()
        );
        if let Some(headers) = m.headers() {
            for i in 0..headers.count() {
                let header = headers.get(i).unwrap();
                info!("  Header {:#?}: {:?}", header.0, header.1);
            }
        }

        producer
            .send(
                FutureRecord::to("responses")
                    .payload("pong")
                    .key("my-key")
                    .headers(OwnedHeaders::new().add("header_key", "header_value")),
                Duration::from_secs(0),
            )
            .await
            .map_err(|(e, _)| e)?;
        consumer.commit_message(&m, CommitMode::Async).unwrap();
    }
}
