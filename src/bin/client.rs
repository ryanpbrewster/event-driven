use std::time::{Duration, SystemTime};

use log::info;

use anyhow::anyhow;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, DefaultConsumerContext};
use rdkafka::message::{Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;

const BROKERS: &'static str = "localhost:9092";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let admin: &AdminClient<_> = &ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let response_topic = "responses-1";
    admin
        .create_topics(
            &[NewTopic::new(response_topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .unwrap();

    let request_id = format!("{}", SystemTime::UNIX_EPOCH.elapsed().unwrap().as_millis());
    producer
        .send(
            FutureRecord::to("requests")
                .payload("ping")
                .key("my-key")
                .headers(
                    OwnedHeaders::new()
                        .add("request-id", &request_id)
                        .add("respond-to-topic", "responses-1"),
                ),
            Duration::from_secs(0),
        )
        .await
        .map_err(|(e, _)| e)?;

    wait_for_response(response_topic, &request_id).await?;
    Ok(())
}

async fn wait_for_response(response_topic: &str, request_id: &str) -> anyhow::Result<String> {
    let consumer: StreamConsumer<_> = ClientConfig::new()
        .set("group.id", "group-1")
        .set("bootstrap.servers", BROKERS)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()
        .expect("Consumer creation failed");
    consumer
        .subscribe(&[response_topic])
        .expect("Can't subscribe to specified topics");

    let mut output = None;
    while output.is_none() {
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
                let header = headers.get_as::<str>(i).unwrap();
                info!("  Header {:#?}: {:?}", header.0, header.1);
                if header.0 == "request-id" {
                    if let Ok(specified_request_id) = header.1 {
                        if specified_request_id == request_id {
                            output = Some(payload.to_owned());
                        }
                    }
                }
            }
        }
        consumer.commit_message(&m, CommitMode::Async).unwrap();
    }
    Ok(output.unwrap())
}
