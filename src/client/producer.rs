use log::debug;
use rdkafka::producer::{BaseRecord, Producer};
use std::time::Duration;

use crate::{KafkyClient, KafkyError};

impl<'a> KafkyClient<'a> {
    pub fn produce(
        &self,
        topic: &str,
        key: Option<String>,
        payload: String,
    ) -> Result<(), KafkyError> {
        debug!(
            "sending message to {}, key:{:?}, payload:{}",
            &topic, &key, &payload
        );
        let mut record: BaseRecord<String, String> = BaseRecord::to(topic);
        if key.is_some() {
            record = record.key(key.as_ref().unwrap());
        }
        record = record.payload(&payload);
        let producer = self.get_producer()?;
        match producer.send(record) {
            Ok(_) => {
                debug!("Message sent");
                producer.flush(Duration::from_millis(1000));
                Ok(())
            }
            Err(err) => Err(err.0.into()),
        }
    }
}
