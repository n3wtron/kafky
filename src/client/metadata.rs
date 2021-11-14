use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::util::Timeout;

use crate::{KafkyClient, KafkyError};

#[derive(Debug)]
pub(crate) struct KafkyMetadata {
    pub topics: Vec<String>,
    pub brokers: Vec<String>,
}

impl<'a> KafkyClient<'a> {
    pub fn get_metadata(&self, topic: Option<&str>) -> Result<KafkyMetadata, KafkyError> {
        let client_config_builder = self.config_builder();
        let consumer: BaseConsumer = client_config_builder.create()?;
        let metadata_response =
            consumer.fetch_metadata(topic, Timeout::from(Duration::from_millis(30000)))?;

        Ok(KafkyMetadata {
            topics: metadata_response
                .topics()
                .iter()
                .map(|t| t.name().to_string())
                .collect(),
            brokers: metadata_response
                .brokers()
                .iter()
                .map(|t| format!("{}:{}", t.host(), t.port()))
                .collect(),
        })
    }
}
