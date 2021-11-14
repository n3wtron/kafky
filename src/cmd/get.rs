use std::collections::HashMap;
use std::io::{stdout, Write};
use std::sync::{Arc, Mutex};

use clap::{App, Arg, ArgMatches, SubCommand, Values};
use log::error;

use crate::client::kafky_client::KafkyClient;
use crate::errors::KafkyError;
use crate::KafkyCmd;
use serde::Serialize;

#[derive(Serialize)]
struct ConsumerGroupRow<'a> {
    group: &'a str,
    topic: &'a str,
    partition: &'a i32,
    offset: &'a i64,
    lag: i64,
}

impl<'a> KafkyCmd<'a> {
    pub fn show_sub_command(&self) -> App<'a, 'a> {
        SubCommand::with_name("get")
            .about("Show kafka information")
            .subcommand(SubCommand::with_name("topics").about("retrieve kafka topic names"))
            .subcommand(
                SubCommand::with_name("consumer-groups")
                    .about("retrieve kafka consumer groups")
                    .arg(
                        Arg::with_name("timeout")
                            .long("timeout")
                            .takes_value(true)
                            .default_value("10")
                            .help("timeout in seconds reading __consumer_offsets topic"),
                    )
                    .arg(
                        Arg::with_name("groups")
                            .long("group")
                            .short("g")
                            .multiple(true)
                            .takes_value(true)
                            .help("filter by group names"),
                    )
                    .arg(
                        Arg::with_name("topics")
                            .long("topic")
                            .short("t")
                            .multiple(true)
                            .takes_value(true)
                            .help("filter by topic names"),
                    )
                    .arg(
                        Arg::with_name("format")
                            .long("output-format")
                            .short("o")
                            .takes_value(true)
                            .required(true)
                            .default_value("table")
                            .possible_values(&["table", "json"]),
                    ),
            )
    }
    pub fn get_exec(
        &self,
        app_matches: &ArgMatches,
        kafky_client: Arc<KafkyClient>,
    ) -> Result<(), KafkyError> {
        if app_matches.subcommand_matches("topics").is_some() {
            for topic in kafky_client.get_metadata(None)?.topics {
                println!("{}", topic)
            }
        }
        if let Some(consumer_groups_args) = app_matches.subcommand_matches("consumer-groups") {
            let timeout: u64 = consumer_groups_args
                .value_of("timeout")
                .unwrap()
                .parse()
                .unwrap();
            let group_filter: Vec<&str> = consumer_groups_args
                .values_of("groups")
                .unwrap_or(Values::default())
                .collect();
            let topic_filter: Vec<&str> = consumer_groups_args
                .values_of("topics")
                .unwrap_or(Values::default())
                .collect();
            let output_format: &str = consumer_groups_args.value_of("format").unwrap();

            let consumer_groups = kafky_client.get_consumer_groups(timeout)?;

            // (topic,partition) -> latest offset
            let latest_offset_map_mtx: Mutex<HashMap<(String, i32), i64>> =
                Mutex::new(HashMap::new());

            let rows: Vec<ConsumerGroupRow> = consumer_groups
                .iter()
                .filter(|consumer_group_tpl| {
                    group_filter.is_empty() || group_filter.contains(&consumer_group_tpl.0.as_str())
                })
                .flat_map(|consumer_group_tpl| {
                    (consumer_group_tpl.1)
                        .topics()
                        .iter()
                        .filter(|topics_tpl| {
                            topic_filter.is_empty() || group_filter.contains(&topics_tpl.0.as_str())
                        })
                        .flat_map(|topics_tpl| {
                            topics_tpl.1.iter().map(|partition_tpl| {
                                let mut cache = latest_offset_map_mtx.lock().unwrap();
                                let latest_offset = Self::get_latest_offset(
                                    &mut *cache,
                                    topics_tpl.0,
                                    partition_tpl.0,
                                    &kafky_client,
                                );
                                ConsumerGroupRow {
                                    group: consumer_group_tpl.1.group(),
                                    topic: topics_tpl.0,
                                    partition: partition_tpl.0,
                                    offset: partition_tpl.1,
                                    lag: latest_offset - partition_tpl.1,
                                }
                            })
                        })
                })
                .collect();

            match output_format {
                "json" => Self::print_json(&rows),
                "table" => Self::print_table(&rows),
                _ => {
                    error!("invalid format")
                }
            };
        }
        Ok(())
    }

    fn get_latest_offset<'b, 'c>(
        latest_offset_map: &'b mut HashMap<(String, i32), i64>,
        topic: &'c str,
        partition: &i32,
        kafky_client: &KafkyClient,
    ) -> i64 {
        *latest_offset_map
            .entry((topic.to_string(), *partition))
            .or_insert_with(|| {
                kafky_client
                    .get_latest_offset(topic, *partition)
                    .expect("error getting latest offset")
            })
    }

    fn print_json(rows: &Vec<ConsumerGroupRow>) {
        println!("{}", serde_json::to_string(rows).expect("invalid json"))
    }

    fn print_table(rows: &Vec<ConsumerGroupRow>) {
        let mut result_table = tabwriter::TabWriter::new(vec![]);
        result_table
            .write(b"GROUP\tTOPIC\tPARTITION\tOFFSET\tLAG\n")
            .expect("error creating table header");

        for row in rows {
            result_table
                .write(
                    format!(
                        "{}\t{}\t{}\t{}\t{}\n",
                        row.group, row.topic, row.partition, row.offset, row.lag
                    )
                    .as_ref(),
                )
                .expect("error writing table data");
        }
        result_table.flush().expect("error printing table");
        stdout()
            .write(&result_table.into_inner().unwrap())
            .expect("error printing table");
    }
}
