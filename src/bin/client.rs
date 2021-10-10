use log::info;

use anyhow::anyhow;

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, DefaultConsumerContext};
use rdkafka::message::{Headers, Message};
use rdkafka::util::get_rdkafka_version;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topics = vec!["my-topic"];
    let brokers = "localhost:9092";
    let group_id = "my-group-2";

    let context = DefaultConsumerContext;

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
        .subscribe(&topics.to_vec())
        .expect("Can't subscribe to specified topics");

    let mut total = 0;
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
        total += 1;
        info!(
            "[{}] key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
            total,
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
        consumer.commit_message(&m, CommitMode::Async).unwrap();
    }
}
