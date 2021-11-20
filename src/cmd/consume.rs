use std::str::FromStr;

use clap::{App, Arg, ArgMatches, SubCommand};
use gethostname::gethostname;
use log::error;

use crate::client::consumer::{KafkyConsumeProperties, KafkyConsumerOffset};
use crate::client::kafky_client::KafkyClient;
use crate::errors::KafkyError;

pub(crate) struct ConsumeCmd {}

impl ConsumeCmd {
    pub fn command<'a>() -> App<'a, 'a> {
        let hostname = Box::leak(Box::new(gethostname()));
        SubCommand::with_name("consume")
            .about("Consume messages from topics")
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
            .arg(
                Arg::with_name("earliest")
                    .long("earliest")
                    .help("read from the earliest offset")
                    .group("offset"),
            )
            .arg(
                Arg::with_name("latest")
                    .long("latest")
                    .help("read from the latest offset (default)")
                    .group("offset"),
            )
    }

    pub async fn exec<'a>(
        app_matches: &'a ArgMatches<'a>,
        kafky_client: &'a KafkyClient<'a>,
    ) -> Result<(), KafkyError> {
        let format: &str = app_matches.value_of("format").unwrap();
        let topics: Vec<&str> = app_matches.values_of("topic").unwrap().collect();
        kafky_client
            .consume::<str, str, _>(
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
                                row.push('[');
                                row.push_str(
                                    &msg.timestamp()
                                        .map(|t| t.to_rfc3339())
                                        .unwrap_or_else(|| String::from("NO-TS")),
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
            .await
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
