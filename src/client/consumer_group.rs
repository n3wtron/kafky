use crate::client::consumer::{KafkyConsumeProperties, KafkyConsumerOffset};
use crate::{KafkyClient, KafkyError};
use byteorder::{BigEndian, ReadBytesExt};
use log::debug;
use std::collections::HashMap;
use std::io::{BufRead, Cursor};
use std::str;
use std::time::Duration;

#[derive(Debug)]
enum ConsumerUpdate {
    Metadata,
    SetCommit {
        group: String,
        topic: String,
        partition: i32,
        offset: i64,
    },
    DeleteCommit {
        group: String,
        topic: String,
        partition: i32,
    },
}

fn parse_group_offset(
    key_rdr: &mut Cursor<&[u8]>,
    payload_rdr: &mut Cursor<&[u8]>,
) -> Result<ConsumerUpdate, KafkyError> {
    let group = read_str(key_rdr)?.to_owned();
    let topic = read_str(key_rdr)?.to_owned();
    let partition = key_rdr.read_i32::<BigEndian>()?;
    if !payload_rdr.get_ref().is_empty() {
        payload_rdr.read_i16::<BigEndian>()?;
        let offset = payload_rdr.read_i64::<BigEndian>()?;
        Ok(ConsumerUpdate::SetCommit {
            group,
            topic,
            partition,
            offset,
        })
    } else {
        Ok(ConsumerUpdate::DeleteCommit {
            group,
            topic,
            partition,
        })
    }
}

fn read_str<'a>(rdr: &'a mut Cursor<&[u8]>) -> Result<&'a str, KafkyError> {
    let strlen = rdr.read_i16::<BigEndian>()? as usize;
    let pos = rdr.position() as usize;
    let slice = str::from_utf8(&rdr.get_ref()[pos..(pos + strlen)])?;
    rdr.consume(strlen);
    Ok(slice)
}

#[derive(Debug)]
pub struct KafkyConsumerGroup {
    group: String,
    topics: HashMap<String, HashMap<i32, i64>>,
}

impl KafkyConsumerGroup {
    fn new(group: String, topic: String, partition: i32, offset: i64) -> KafkyConsumerGroup {
        KafkyConsumerGroup {
            group,
            topics: HashMap::from([(topic, HashMap::from([(partition, offset)]))]),
        }
    }

    fn update_topic_partition(&mut self, topic_name: String, partition: i32, offset: i64) {
        let topic_partition = self.topics.get_mut(&topic_name);
        if topic_partition.is_none() {
            let topic_partition_map = HashMap::from([(partition, offset)]);
            self.topics.insert(topic_name, topic_partition_map);
        } else {
            topic_partition.unwrap().insert(partition, offset);
        }
    }

    fn delete_topic_partition(&mut self, topic_name: &String, partition: &i32) {
        if let Some(topic_partition) = self.topics.get_mut(topic_name) {
            topic_partition.remove(partition);
        }
    }

    pub fn topics(&self) -> &HashMap<String, HashMap<i32, i64>> {
        &self.topics
    }

    pub fn group(&self) -> &str {
        &self.group
    }
}

impl<'a> KafkyClient<'a> {
    pub fn get_consumer_groups(
        &self,
        timeout_sec: u64,
    ) -> Result<HashMap<String, KafkyConsumerGroup>, KafkyError> {
        let mut result: HashMap<String, KafkyConsumerGroup> = HashMap::new();

        self.consume::<[u8], [u8], _>(
            &KafkyConsumeProperties {
                topics: &vec!["__consumer_offsets"],
                consumer_group: "kafky",
                offset: KafkyConsumerOffset::Beginning,
                auto_commit: false,
            },
            Some(Duration::from_secs(timeout_sec)),
            |consumer_offset_msg_res| {
                let msg = consumer_offset_msg_res.unwrap();
                let mut key_rdr = Cursor::new(msg.key().unwrap());
                let mut payload_rdr = Cursor::new(msg.payload());

                let key_version = key_rdr
                    .read_i16::<BigEndian>()
                    .expect("error reading key version");

                let consumer_update = match key_version {
                    0 | 1 => parse_group_offset(&mut key_rdr, &mut payload_rdr),
                    2 => Ok(ConsumerUpdate::Metadata),
                    _ => Err(KafkyError::ParseError(
                        "error parsing consumer group data".to_string(),
                    )),
                };

                debug!("consumer update: {:?}", consumer_update);
                match consumer_update {
                    Ok(update) => match update {
                        ConsumerUpdate::Metadata => true,
                        ConsumerUpdate::SetCommit {
                            group,
                            topic,
                            partition,
                            offset,
                        } => {
                            let opt_consumer_group = result.get_mut(&group);
                            if opt_consumer_group.is_none() {
                                result.insert(
                                    group.clone(),
                                    KafkyConsumerGroup::new(group, topic, partition, offset),
                                );
                            } else {
                                opt_consumer_group
                                    .unwrap()
                                    .update_topic_partition(topic, partition, offset);
                            }
                            true
                        }
                        ConsumerUpdate::DeleteCommit {
                            group,
                            topic,
                            partition,
                        } => {
                            if let Some(consumer_group) = result.get_mut(&group) {
                                consumer_group.delete_topic_partition(&topic, &partition)
                            }
                            true
                        }
                    },
                    Err(_) => false,
                }
            },
        )?;

        Ok(result)
    }
}
