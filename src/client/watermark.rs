use crate::{KafkyClient, KafkyError};
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::time::Duration;

impl<'a> KafkyClient<'a> {
    pub fn get_latest_offset(&self, topic: &str, partition: i32) -> Result<i64, KafkyError> {
        let client_config_builder = self.config_builder();
        let consumer: BaseConsumer = client_config_builder.create()?;
        let (_, latest_offset) =
            consumer.fetch_watermarks(topic, partition, Duration::from_secs(10))?;
        Ok(latest_offset)
    }
}
