use chrono::{DateTime, TimeZone, Utc};
use log::{debug, error, info};
use rdkafka::consumer::{Consumer, StreamConsumer};

use rdkafka::message::FromBytes;
use rdkafka::{Message, Timestamp};
use serde::{Serialize, Serializer};
use strum::IntoEnumIterator;
use strum_macros;
use strum_macros::{Display, EnumIter, EnumString, IntoStaticStr};
use tokio::sync::oneshot::Receiver;

use crate::{KafkyClient, KafkyError};

pub fn serialize_dt<S>(dt: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match dt {
        Some(dt) => serializer.serialize_str(&dt.to_rfc3339()),
        _ => unreachable!(),
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct KafkyConsumerMessage<'a, K: ?Sized + FromBytes, P: ?Sized + FromBytes> {
    key: Option<&'a K>,
    topic: &'a str,
    payload: &'a P,
    partition: i32,
    offset: i64,
    #[serde(
        serialize_with = "serialize_dt",
        skip_serializing_if = "Option::is_none"
    )]
    timestamp: Option<DateTime<Utc>>,
}

impl<'a, K: ?Sized + FromBytes, P: ?Sized + FromBytes> KafkyConsumerMessage<'a, K, P> {
    pub fn key(&self) -> Option<&'a K> {
        self.key
    }
    pub fn payload(&self) -> &'a P {
        self.payload
    }
    pub fn timestamp(&self) -> Option<DateTime<Utc>> {
        self.timestamp
    }
    pub fn topic(&self) -> &'a str {
        self.topic
    }
}

#[derive(EnumString, Display, EnumIter, PartialEq, IntoStaticStr, Debug)]
pub enum KafkyConsumerOffset {
    #[strum(serialize = "earliest")]
    Earliest,
    #[strum(serialize = "latest")]
    Latest,
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
    pub topics: &'a Vec<&'a str>,
    pub consumer_group: &'a str,
    pub offset: KafkyConsumerOffset,
    pub auto_commit: bool,
}

impl<'a> KafkyClient<'a> {
    pub(crate) async fn consume<
        K: ?Sized + FromBytes,
        P: ?Sized + FromBytes,
        F: FnMut(Result<KafkyConsumerMessage<K, P>, KafkyError>) -> bool,
    >(
        &self,
        properties: &'a KafkyConsumeProperties<'a>,
        stop_rx: Option<Receiver<bool>>,
        mut message_consumer: F,
    ) -> Result<(), KafkyError> {
        let mut consumer_builder = self.config_builder();
        consumer_builder
            .set("group.id", properties.consumer_group)
            .set("enable.auto.commit", properties.auto_commit.to_string())
            .set("session.timeout.ms", "6000")
            .set("auto.offset.reset", properties.offset.to_string());

        debug!("Consumer properties: {:?}", &consumer_builder);
        let consumer: StreamConsumer = consumer_builder.create()?;
        consumer
            .subscribe(properties.topics)
            .expect("subscribe error");
        info!("subscription properties {:?}", properties);

        if let Some(stop_rx) = stop_rx {
            tokio::select! {
                _ = async {
                loop{
                        if !Self::process_message(&consumer, &mut message_consumer).await {
                            break;
                        }
                } }=>{},
                _ = stop_rx =>{
                    debug!("Received close signal, stopping consumer");
                }
            }
        } else {
            loop {
                Self::process_message(&consumer, &mut message_consumer).await;
            }
        }

        Ok(())
    }

    async fn process_message<
        K: ?Sized + FromBytes,
        P: ?Sized + FromBytes,
        F: FnMut(Result<KafkyConsumerMessage<K, P>, KafkyError>) -> bool,
    >(
        consumer: &StreamConsumer,
        mut message_consumer: F,
    ) -> bool {
        let kafky_msg = consumer.recv().await;
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
                    let creation_time = match m.timestamp() {
                        Timestamp::NotAvailable => None,
                        Timestamp::CreateTime(creation_time) => {
                            Some(Utc.timestamp_millis(creation_time))
                        }
                        Timestamp::LogAppendTime(log_appended_msec) => {
                            Some(Utc.timestamp_millis(log_appended_msec))
                        }
                    };

                    message_consumer(Ok(KafkyConsumerMessage {
                        key,
                        payload,
                        topic: m.topic(),
                        partition: m.partition(),
                        offset: m.offset(),
                        timestamp: creation_time,
                    }))
                } else {
                    true
                }
            }
            Err(err) => {
                error!("Error consuming messages:{}", err);
                false
            }
        }
    }
}
