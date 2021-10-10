use std::collections::HashSet;
use std::time::Duration;

use log::info;

use anyhow::anyhow;

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::{Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;

const BROKERS: &'static str = "localhost:9092";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let group_id = "my-group";

    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");
    let consumer: StreamConsumer<_> = ClientConfig::new()
        .set("group.id", group_id)
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
        .subscribe(&["requests"])
        .expect("Can't subscribe to specified topics");

    let mut registry: HashSet<String> = HashSet::new();
    loop {
        let m = consumer.recv().await?;
        let payload = match m.payload_view::<str>() {
            Some(Ok(s)) => s.to_owned(),
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
        let response_payload = if registry.insert(payload) {
            "ok"
        } else {
            "taken"
        };
        let mut response_topic = None;
        let mut request_id = None;
        if let Some(headers) = m.headers() {
            for i in 0..headers.count() {
                let header = headers.get_as::<str>(i).unwrap();
                info!("  Header {:#?}: {:?}", header.0, header.1);
                match header.0 {
                    "respond-to-topic" => {
                        if let Ok(specified_response_topic) = header.1 {
                            response_topic = Some(specified_response_topic);
                        }
                    }
                    "request-id" => {
                        if let Ok(specified_request_id) = header.1 {
                            request_id = Some(specified_request_id);
                        }
                    }
                    _ => {}
                }
            }
        }

        if let Some(response_topic) = response_topic {
            if let Some(request_id) = request_id {
                tokio::time::sleep(Duration::from_millis(200)).await;
                producer
                    .send(
                        FutureRecord::to(response_topic)
                            .payload(response_payload)
                            .key("my-key")
                            .headers(OwnedHeaders::new().add("request-id", request_id)),
                        Duration::from_secs(0),
                    )
                    .await
                    .map_err(|(e, _)| e)?;
            }
        }
        consumer.commit_message(&m, CommitMode::Async).unwrap();
    }
}
