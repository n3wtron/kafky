use std::fs::create_dir_all;
use std::io;
use std::io::Write;
use std::option::Option;

use clap::{App, Arg, ArgMatches, SubCommand};
use log::{debug, error};
use rustyline::error::ReadlineError;
use rustyline::Editor;

use crate::client::kafky_client::KafkyClient;
use crate::errors::KafkyError;
use crate::KafkyConfig;

pub(crate) struct ProduceCmd {}

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
    pub async fn exec<'a>(
        app_matches: &'a ArgMatches<'a>,
        kafky_client: &'a KafkyClient<'a>,
        config: &'a KafkyConfig<'a>,
        environment: &'a str,
    ) -> Result<(), KafkyError> {
        let topic = app_matches.value_of("topic").unwrap();
        let metadata = kafky_client.get_metadata(Some(topic))?;
        if !metadata.topic_names().contains(&topic.to_string()) {
            return Err(KafkyError::TopicNotFound(topic.to_string()));
        }

        let key_separator_opt = app_matches.value_of("key-separator");

        let mut editor = Editor::<()>::new();

        let history_folder = config.config_folder().join("history").join(environment);
        create_dir_all(&history_folder).expect("error creating history folder");
        let history_file = history_folder.join(format!(
            "{}_{}.history",
            topic,
            key_separator_opt.unwrap_or("")
        ));

        if editor.load_history(&history_file).is_err() {
            debug!("no previous history for the topic {}", topic);
        }
        let result: Result<(), KafkyError>;

        loop {
            io::stdout().flush().unwrap();
            match editor.readline(&format!("{} <- ", topic)) {
                Ok(read_line) => {
                    if let Ok((key, payload)) =
                        Self::extract_key_payload(key_separator_opt, &read_line)
                    {
                        editor.add_history_entry(&read_line);
                        match kafky_client.produce(topic, key, payload.to_string()) {
                            Ok(_) => {
                                debug!("message sent to topic {}", topic);
                            }
                            Err(error) => {
                                result = Err(error);
                                break;
                            }
                        };
                    }
                }
                Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                    result = Err(KafkyError::Exit());
                    break;
                }
                Err(e) => {
                    result = Err(e.into());
                    break;
                }
            }
        }
        editor
            .save_history(&history_file)
            .expect("unable to save history");
        if result.is_err() {
            return Err(result.err().unwrap());
        }
        Ok(())
    }

    fn extract_key_payload(
        key_separator_opt: Option<&str>,
        read_line: &str,
    ) -> Result<(Option<String>, String), KafkyError> {
        if let Some(key_separator) = key_separator_opt {
            let key_payload: Vec<&str> = read_line.split(key_separator).collect();
            if key_payload.len() != 2 {
                error!("key separator \"{}\" not found", key_separator);
                return Err(KafkyError::KeySeparatorNotFound());
            }
            Ok((
                key_payload.get(0).map(|k| k.to_string()),
                key_payload.get(1).map(|s| s.to_string()).unwrap(),
            ))
        } else {
            Ok((None, read_line.to_string()))
        }
    }
}
