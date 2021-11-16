use std::time::Duration;

use log::{debug, error};
use rdkafka::consumer::Consumer;

use crate::{KafkyClient, KafkyError};

impl<'a> KafkyClient<'a> {
    pub fn get_latest_offset(&self, topic: &str, partition: i32) -> Result<i64, KafkyError> {
        debug!(
            "getting latest offset for topic:{}, partition:{}",
            topic, partition
        );
        let latest_offset = match self.get_util_consumer()?.fetch_watermarks(
            topic,
            partition,
            Duration::from_secs(30),
        ) {
            Ok((_, latest_offset)) => latest_offset,
            Err(e) => {
                error!(
                    "Error getting latest offset for topic:{}, partition:{}. {}",
                    topic, partition, e
                );
                0
            }
        };
        debug!(
            "latest offset for topic:{}, partition:{} = {}",
            topic, partition, &latest_offset
        );
        Ok(latest_offset)
    }
}
