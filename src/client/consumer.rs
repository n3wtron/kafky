use log::{debug, error, info};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::FromBytes;
use rdkafka::Message;
use serde::Serialize;
use std::time::{Duration, Instant};
use strum::IntoEnumIterator;
use strum_macros;
use strum_macros::{Display, EnumIter, EnumString, IntoStaticStr};

use crate::{KafkyClient, KafkyError};

#[derive(Debug, Serialize)]
pub(crate) struct KafkyConsumerMessage<'a, K: ?Sized + FromBytes, P: ?Sized + FromBytes> {
    pub key: Option<&'a K>,
    pub payload: &'a P,
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
    pub(crate) fn consume<
        K: ?Sized + FromBytes,
        P: ?Sized + FromBytes,
        F: FnMut(Result<KafkyConsumerMessage<K, P>, KafkyError>) -> bool,
    >(
        &self,
        properties: &'a KafkyConsumeProperties,
        timeout: Option<Duration>,
        mut message_consumer: F,
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
        let start_time = Instant::now();
        // for kafky_msg in consumer.iter() {
        loop {
            if let Some(timeout) = timeout {
                let now = Instant::now();
                if now.duration_since(start_time) > timeout {
                    break;
                }
            }
            let opt_kafky_msg = consumer.poll(Duration::from_millis(100));
            if let Some(kafky_msg) = opt_kafky_msg {
                match kafky_msg {
                    Ok(m) => {
                        let opt_payload: Option<&P> = match m.payload_view::<P>() {
                            None => None,
                            Some(Ok(s)) => Some(s),
                            Some(Err(_)) => {
                                error!("Error while deserializing message payload");
                                None
                            }
                        };

                        let key: Option<&K> = match m.key_view::<K>() {
                            None => None,
                            Some(Ok(s)) => Some(s),
                            Some(Err(_)) => {
                                error!("Error while deserializing message key");
                                None
                            }
                        };
                        if let Some(payload) = opt_payload {
                            if !message_consumer(Ok(KafkyConsumerMessage {
                                key,
                                payload,
                                partition: m.partition(),
                            })) {
                                break;
                            }
                        }
                    }
                    Err(err) => {
                        error!("Error consuming messages:{}", err);
                        break;
                    }
                };
            }
        }

        Ok(())
    }
}
