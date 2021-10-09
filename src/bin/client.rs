use std::time::Duration;

use kafka::consumer::Consumer;
use kafka::error::Error as KafkaError;
use kafka::producer::{Producer, Record, RequiredAcks};

/// This program demonstrates sending single message through a
/// `Producer`.  This is a convenient higher-level client that will
/// fit most use cases.
fn main() {
    env_logger::init();

    let broker = "localhost:9092".to_owned();
    let topic = "my-topic".to_owned();

    produce_message(&topic, vec![broker.to_owned()]).unwrap();
    consume_messages(topic, vec![broker]).unwrap();
}

fn produce_message<'a, 'b>(topic: &'b str, brokers: Vec<String>) -> Result<(), KafkaError> {
    println!("About to publish a message at {:?} to: {}", brokers, topic);

    // ~ create a producer. this is a relatively costly operation, so
    // you'll do this typically once in your application and re-use
    // the instance many times.
    let mut producer = Producer::from_hosts(brokers)
        // ~ give the brokers one second time to ack the message
        .with_ack_timeout(Duration::from_secs(1))
        // ~ require only one broker to ack the message
        .with_required_acks(RequiredAcks::One)
        // ~ build the producer with the above settings
        .create()?;

    // ~ now send a single message.  this is a synchronous/blocking
    // operation.

    // ~ we're sending 'data' as a 'value'. there will be no key
    // associated with the sent message.

    // ~ we leave the partition "unspecified" - this is a negative
    // partition - which causes the producer to find out one on its
    // own using its underlying partitioner.
    producer.send(&Record::from_value(topic, "hello, kafka"))?;

    Ok(())
}

fn consume_messages(topic: String, brokers: Vec<String>) -> Result<(), KafkaError> {
    let mut con = Consumer::from_hosts(brokers).with_topic(topic).create()?;

    loop {
        println!("polling for messages to consume...");
        let mss = con.poll()?;
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
        con.commit_consumed()?;
    }
}
