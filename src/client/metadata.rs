use std::time::Duration;

use rdkafka::consumer::Consumer;
use rdkafka::util::Timeout;

use crate::{KafkyClient, KafkyError};

#[derive(Debug)]
pub(crate) struct KafkyMetadata {
    pub topics: Vec<String>,
    pub brokers: Vec<String>,
}

impl<'a> KafkyClient<'a> {
    pub fn get_metadata(&self, topic: Option<&str>) -> Result<KafkyMetadata, KafkyError> {
        let metadata_response = self
            .get_util_consumer()?
            .fetch_metadata(topic, Timeout::from(Duration::from_millis(30000)))?;

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
