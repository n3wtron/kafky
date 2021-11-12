use log::{debug, error, info};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::Message;
use serde::Serialize;
use strum::IntoEnumIterator;
use strum_macros;
use strum_macros::{Display, EnumIter, EnumString, IntoStaticStr};

use crate::{KafkyClient, KafkyError};

#[derive(Debug, Serialize)]
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

#[derive(Debug)]
pub(crate) struct KafkyConsumeProperties<'a> {
    pub topics: Vec<&'a str>,
    pub consumer_group: &'a str,
    pub offset: KafkyConsumerOffset,
    pub auto_commit: bool,
}

impl<'a> KafkyClient<'a> {
    pub(crate) fn consume<F: Fn(Result<KafkyConsumerMessage, KafkyError>) -> ()>(
        &self,
        properties: &'a KafkyConsumeProperties,
        message_consumer: F,
    ) -> Result<(), KafkyError> {
        let mut consumer_builder = self.config_builder();
        consumer_builder
            .set("group.id", properties.consumer_group)
            .set("enable.auto.commit", properties.auto_commit.to_string())
            .set("session.timeout.ms", "6000")
            .set("auto.offset.reset", properties.offset.to_string());

        debug!("Consumer properties: {:?}", &consumer_builder);
        let consumer: BaseConsumer = consumer_builder.create()?;
        consumer
            .subscribe(&properties.topics)
            .expect("subscribe error");
        info!("subscription properties {:?}", properties);

        for kafky_msg in consumer.iter().map(|message_result| match message_result {
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        error!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };

                let key = match m.key_view::<str>() {
                    None => None,
                    Some(Ok(s)) => Some(s.to_string()),
                    Some(Err(e)) => {
                        error!("Error while deserializing message key: {:?}", e);
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
