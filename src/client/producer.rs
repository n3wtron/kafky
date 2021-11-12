use std::borrow::Borrow;
use rdkafka::producer::{BaseProducer, BaseRecord, FutureProducer};
use tokio::sync::mpsc::Receiver;

use crate::{KafkyClient, KafkyError};
use crate::KafkyError::KafkaError;


impl KafkyClient {
    pub fn produce(&self, topic: &str, key: Option<String>, payload: String) -> Result<(), KafkyError> {
        let producer = self.get_producer()?;
        let mut record: BaseRecord<String, String> = BaseRecord::to(topic);
        if key.is_some() {
            record = record.key(key.as_ref().unwrap());
        }
        record = record.payload(&payload);
        producer.send(record).expect("message was not sent");
        Ok(())
    }
}
