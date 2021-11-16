use crate::{KafkyClient, KafkyError};
use rdkafka::admin::{AdminOptions, NewTopic, TopicReplication};

impl<'a> KafkyClient<'a> {
    pub async fn create_topic(
        &self,
        topic_names: &Vec<&str>,
        partitions: i32,
        replication_factor: i32,
    ) -> Result<(), KafkyError> {
        let new_topics: Vec<NewTopic> = topic_names
            .iter()
            .map(|topic_name| {
                NewTopic::new(
                    topic_name,
                    partitions,
                    TopicReplication::Fixed(replication_factor),
                )
            })
            .collect();
        self.get_admin_client()?
            .create_topics(new_topics.as_slice(), &AdminOptions::new())
            .await?;
        Ok(())
    }
}
