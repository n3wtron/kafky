use std::io;
use std::io::Write;
use std::sync::Arc;

use clap::{App, Arg, ArgMatches, SubCommand};
use log::{debug, error};

use crate::client::kafky_client::KafkyClient;
use crate::errors::KafkyError;
use std::option::Option;

pub(crate) struct ProduceCmd{}

impl ProduceCmd {
    pub fn command<'a>() -> App<'a, 'a> {
        SubCommand::with_name("produce")
            .about("Produce messages to a topic")
            .arg(
                Arg::with_name("topic")
                    .short("t")
                    .long("topic")
                    .required(true)
                    .value_name("TOPIC_NAME"),
            )
            .arg(
                Arg::with_name("key-separator")
                    .long("key-separator")
                    .conflicts_with("json")
                    .short("k")
                    .takes_value(true),
            )
    }
    pub fn exec(
        app_matches: &ArgMatches,
        kafky_client: Arc<KafkyClient>,
    ) -> Result<(), KafkyError> {
        let topic = app_matches.value_of("topic").unwrap();
        let metadata = kafky_client.get_metadata(Some(topic))?;
        if !metadata.topics.contains(&topic.to_string()) {
            return Err(KafkyError::TopicNotFound(topic.to_string()));
        }
        let mut read_line = String::new();
        let mut payload: &str;
        let mut key: Option<String>;

        loop {
            print!("{}> ", topic);
            io::stdout().flush().unwrap();
            read_line.clear();
            match io::stdin().read_line(&mut read_line) {
                Ok(size) => {
                    if size == 0 {
                        break;
                    }
                    read_line.pop();

                    if let Some(key_separator) = app_matches.value_of("key-separator") {
                        let key_payload: Vec<&str> = read_line.split(key_separator).collect();
                        if key_payload.len() != 2 {
                            error!("key separator \"{}\" not found", key_separator);
                            continue;
                        }
                        payload = key_payload.get(1).unwrap();
                        key = key_payload.get(0).map(|s| s.to_string());
                    } else {
                        key = None;
                        payload = &read_line;
                    }

                    match kafky_client.produce(&topic, key, payload.to_string()) {
                        Ok(_) => {
                            debug!("message sent to topic {}", topic)
                        }
                        Err(error) => {
                            return Err(error);
                        }
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }
        Ok(())
    }
}
