use std::io::{stdout, Write};

use clap::{App, Arg, ArgMatches, SubCommand};

use crate::{KafkyClient, KafkyError};

pub struct GetTopicCmd {}

impl GetTopicCmd {
    pub fn command<'a>() -> App<'a, 'a> {
        SubCommand::with_name("topics")
            .arg(
                Arg::with_name("format")
                    .long("output-format")
                    .short("o")
                    .takes_value(true)
                    .required(true)
                    .default_value("text")
                    .possible_values(&["table", "text", "json"]),
            )
            .about("retrieve kafka topic names")
    }

    pub(crate) fn exec(
        topic_args: &ArgMatches,
        kafky_client: &KafkyClient,
    ) -> Result<(), KafkyError> {
        let format = topic_args.value_of("format").unwrap();
        if format == "text" {
            for topic in kafky_client.get_metadata(None)?.topics {
                println!("{}", topic.name());
            }
        }
        if format == "json" {
            for topic in kafky_client.get_metadata(None)?.topics {
                println!("{}", serde_json::to_string(&topic)?);
            }
        }
        if format == "table" {
            let mut result_table = tabwriter::TabWriter::new(vec![]);
            result_table
                .write_all(b"TOPIC\tPARTITION\tLEADER\tISR\tREPLICAS\n")
                .expect("error creating table header");
            for topic in kafky_client.get_metadata(None)?.topics {
                for partition in topic.partitions() {
                    result_table
                        .write_all(
                            format!(
                                "{}\t{}\t{}\t{}\t{}\n",
                                topic.name(),
                                partition.id(),
                                partition.leader(),
                                partition
                                    .isrs()
                                    .iter()
                                    .map(|i| i.to_string())
                                    .collect::<Vec<String>>()
                                    .join(","),
                                partition
                                    .replicas()
                                    .iter()
                                    .map(|i| i.to_string())
                                    .collect::<Vec<String>>()
                                    .join(",")
                            )
                            .as_ref(),
                        )
                        .expect("error writing row");
                }
            }
            result_table.flush().expect("error flushing table");
            stdout()
                .write_all(&result_table.into_inner().unwrap())
                .expect("error printing table");
        }
        Ok(())
    }
}
