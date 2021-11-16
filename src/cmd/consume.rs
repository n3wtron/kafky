use crate::client::consumer::{KafkyConsumeProperties, KafkyConsumerOffset};
use crate::client::kafky_client::KafkyClient;
use crate::errors::KafkyError;
use clap::{App, Arg, ArgGroup, ArgMatches, SubCommand};
use gethostname::gethostname;
use log::error;
use std::str::FromStr;
use std::sync::Arc;

pub (crate) struct ConsumeCmd {}

impl ConsumeCmd {
    pub fn command<'a>() -> App<'a, 'a> {
        let offset_values: Vec<&str> = KafkyConsumerOffset::values_str();
        let offset_args: Vec<Arg> = KafkyConsumerOffset::values_str()
            .iter()
            .map(|offset_name| {
                Arg::with_name(offset_name)
                    .long(offset_name)
                    .group("offset")
            })
            .collect();
        let hostname = Box::leak(Box::new(gethostname()));
        SubCommand::with_name("consume")
            .about("Consume messages from a topic")
            .arg(
                Arg::with_name("topic")
                    .short("t")
                    .multiple(true)
                    .long("topic")
                    .required(true)
                    .value_name("TOPIC_NAME"),
            )
            .arg(
                Arg::with_name("consumer-group")
                    .short("c")
                    .default_value_os(hostname)
                    .required(true)
                    .long("consumer-group")
                    .value_name("CONSUMER GROUP NAME"),
            )
            .arg(Arg::with_name("autocommit").long("autocommit").short("a"))
            .arg(
                Arg::with_name("key-separator")
                    .long("key-separator")
                    .conflicts_with("json")
                    .short("k")
                    .takes_value(true),
            )
            .arg(
                Arg::with_name("format")
                    .long("--output-format")
                    .short("o")
                    .possible_values(&["json", "text"])
                    .default_value("text"),
            )
            .arg(
                Arg::with_name("timestamp")
                    .long("timestamp")
                    .help("print timestamp message (works only with text format)"),
            )
            .args(offset_args.as_slice())
            .group(ArgGroup::with_name("offset").args(offset_values.as_slice()))
    }

    pub fn exec(
        app_matches: &ArgMatches<'_>,
        kafky_client: Arc<KafkyClient>,
    ) -> Result<(), KafkyError> {
        let format: &str = app_matches.value_of("format").unwrap().into();
        let topics: Vec<&str> = app_matches.values_of("topic").unwrap().collect();
        kafky_client.consume::<str, str, _>(
            &KafkyConsumeProperties {
                topics: &topics,
                consumer_group: app_matches.value_of("consumer-group").unwrap(),
                offset: Self::extract_offset_from_arg(app_matches)?,
                auto_commit: app_matches.is_present("autocommit"),
            },
            None,
            |msg_result| match msg_result {
                Ok(msg) => match format {
                    "json" => {
                        println!("{}", serde_json::to_string(&msg).unwrap());
                        true
                    }
                    "text" => {
                        let mut row = String::new();
                        if topics.len() > 1 {
                            row.push_str(msg.topic());
                            row.push_str(" -> ");
                        }
                        if app_matches.is_present("timestamp") {
                            row.push_str("[");
                            row.push_str(
                                &msg.timestamp()
                                    .map(|t| t.to_rfc3339())
                                    .unwrap_or(String::from("NO-TS")),
                            );
                            row.push_str("] ");
                        }
                        if app_matches.is_present("key-separator") {
                            row.push_str(msg.key().unwrap_or("null"));
                            row.push_str(app_matches.value_of("key-separator").unwrap());
                        }
                        row.push_str(msg.payload());
                        println!("{}", row);
                        true
                    }
                    _ => {
                        error!("invalid format");
                        false
                    }
                },
                Err(err) => {
                    error!("error: {:?}", err);
                    false
                }
            },
        )
    }

    fn extract_offset_from_arg(
        app_matches: &ArgMatches<'_>,
    ) -> Result<KafkyConsumerOffset, KafkyError> {
        KafkyConsumerOffset::values_str()
            .iter()
            .find(|offset_name| app_matches.is_present(offset_name))
            .or(Some(&"latest"))
            .map_or(Err(KafkyError::InvalidOffset()), |offset_name| {
                KafkyConsumerOffset::from_str(offset_name).map_err(|_| KafkyError::InvalidOffset())
            })
    }
}
