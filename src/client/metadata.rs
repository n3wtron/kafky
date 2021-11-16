use std::time::Duration;

use rdkafka::consumer::Consumer;
use rdkafka::metadata::{MetadataPartition, MetadataTopic};
use rdkafka::util::Timeout;

use crate::{KafkyClient, KafkyError};
use serde::{Serialize};

#[derive(Debug,Serialize)]
pub(crate) struct KafkyPartition {
    id:i32,
    leader: i32,
    replicas: Vec<i32>,
    isrs: Vec<i32>,
}

impl KafkyPartition{
    pub fn id(&self) -> i32 {
        self.id
    }
    pub fn leader(&self) -> i32 {
        self.leader
    }
    pub fn replicas(&self) -> &Vec<i32> {
        &self.replicas
    }
    pub fn isrs(&self) -> &Vec<i32> {
        &self.isrs
    }
}

impl From<&MetadataPartition> for KafkyPartition {
    fn from(metadata_partition: &MetadataPartition) -> Self {
        KafkyPartition {
            id: metadata_partition.id(),
            leader: metadata_partition.leader(),
            replicas: Vec::from(metadata_partition.replicas()),
            isrs: Vec::from(metadata_partition.isr()),
        }
    }
}

#[derive(Debug,Serialize)]
pub(crate) struct KafkyTopic {
    name: String,
    partitions: Vec<KafkyPartition>,
}

impl KafkyTopic {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn partitions(&self) -> &Vec<KafkyPartition> {
        &self.partitions
    }
}

impl From<&MetadataTopic> for KafkyTopic {
    fn from(metadata_topic: &MetadataTopic) -> Self {
        KafkyTopic {
            name: metadata_topic.name().to_string(),
            partitions: metadata_topic
                .partitions()
                .iter()
                .map(|metadata_partition| metadata_partition.into())
                .collect(),
        }
    }
}

#[derive(Debug,Serialize)]
pub(crate) struct KafkyMetadata {
    pub topics: Vec<KafkyTopic>,
    pub brokers: Vec<String>,
}

impl KafkyMetadata {
    pub fn topic_names(&self) -> Vec<String> {
        self.topics.iter().map(|t| t.name.clone()).collect()
    }
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
                .map(|t| t.into())
                .collect(),
            brokers: metadata_response
                .brokers()
                .iter()
                .map(|t| format!("{}:{}", t.host(), t.port()))
                .collect(),
        })
    }
}
