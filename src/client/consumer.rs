use log::debug;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::Message;
use strum::IntoEnumIterator;
use strum_macros;
use strum_macros::{Display, EnumIter, EnumString, IntoStaticStr};

use crate::{KafkyClient, KafkyError};

#[derive(Debug)]
pub(crate) struct KafkyConsumerMessage {
    pub key: Option<String>,
    pub payload: String,
    pub partition: i32,
}

#[derive(EnumString, Display, EnumIter, PartialEq, IntoStaticStr, Debug)]
pub enum KafkyConsumerOffset {
    #[strum(serialize = "smallest")]
    Smallest,
    #[strum(serialize = "earliest")]
    Earliest,
    #[strum(serialize = "beginning")]
    Beginning,
    #[strum(serialize = "largest")]
    Largest,
    #[strum(serialize = "latest")]
    Latest,
    #[strum(serialize = "end")]
    End,
    #[strum(serialize = "error")]
    Error,
}

impl<'a> KafkyConsumerOffset {
    pub fn values_str() -> Vec<&'a str> {
        let mut offset_values: Vec<&str> = Vec::new();
        for offset in KafkyConsumerOffset::iter() {
            offset_values.push(offset.into());
        }
        offset_values
    }
}

impl KafkyClient {
    pub(crate) fn consume(
        &self,
        topics: Vec<&str>,
        consumer_group: &str,
        offset: KafkyConsumerOffset,
        message_consumer: fn(Result<KafkyConsumerMessage, KafkyError>) -> (),
    ) -> Result<(), KafkyError> {
        let mut consumer_builder = self.config_builder();
        consumer_builder
            .set("group.id", consumer_group)
            .set("enable.auto.commit", "true")
            .set("session.timeout.ms", "6000")
            .set("auto.offset.reset", offset.to_string());

        debug!("Consumer properties: {:?}", &consumer_builder);
        let consumer: BaseConsumer = consumer_builder.create()?;
        consumer.subscribe(&topics).expect("subscribe error");
        println!(
            "subscribed to {} with consumer group:{}",
            topics.join(","),
            consumer_group
        );

        for kafky_msg in consumer.iter().map(|message_result| match message_result {
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        println!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };

                let key = match m.key_view::<str>() {
                    None => None,
                    Some(Ok(s)) => Some(s.to_string()),
                    Some(Err(e)) => {
                        println!("Error while deserializing message key: {:?}", e);
                        None
                    }
                };

                Ok(KafkyConsumerMessage {
                    key,
                    payload: payload.to_string(),
                    partition: m.partition(),
                })
            }
            Err(err) => {
                return Err(err.into());
            }
        }) {
            message_consumer(kafky_msg)
        }

        Ok(())
    }
}
