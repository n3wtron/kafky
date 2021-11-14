use std::io::{stdout, Write};
use std::sync::Arc;

use crate::client::consumer_group::KafkyConsumerGroup;
use clap::{App, Arg, ArgMatches, SubCommand, Values};
use tabwriter::TabWriter;

use crate::client::kafky_client::KafkyClient;
use crate::errors::KafkyError;
use crate::KafkyCmd;

struct ConsumerGroupRow {
    group: String,
    topic: String,
    partition: i32,
    offset: i64,
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

            let consumer_groups = kafky_client.get_consumer_groups(timeout)?;
            let mut result_table = tabwriter::TabWriter::new(vec![]);
            result_table
                .write(b"GROUP\tTOPIC\tPARTITION\tOFFSET\n")
                .expect("error creating table header");

            consumer_groups
                .iter()
                .filter(|consumer_group_tpl| {
                    group_filter.is_empty() || group_filter.contains(&consumer_group_tpl.0.as_str())
                })
                .for_each(|consumer_group_tpl| {
                    (consumer_group_tpl.1)
                        .topics()
                        .iter()
                        .filter(|topics_tpl| {
                            topic_filter.is_empty() || group_filter.contains(&topics_tpl.0.as_str())
                        })
                        .for_each(|topics_tpl| {
                            topics_tpl.1.iter().for_each(|partition_tpl| {
                                Self::write_consumer_group_table_row(
                                    &mut result_table,
                                    &consumer_group_tpl.1,
                                    topics_tpl.0,
                                    partition_tpl.0,
                                    partition_tpl.1,
                                )
                            });
                        });
                });
            result_table.flush().expect("error printing table");
            stdout()
                .write(&result_table.into_inner().unwrap())
                .expect("error printing table");
        }
        Ok(())
    }

    fn write_consumer_group_table_row(
        result_table: &mut TabWriter<Vec<u8>>,
        consumer_group: &KafkyConsumerGroup,
        topic: &String,
        partition: &i32,
        offset: &i64,
    ) {
        result_table
            .write(
                format!(
                    "{}\t{}\t{}\t{}\n",
                    consumer_group.group(),
                    topic,
                    partition,
                    offset
                )
                .as_ref(),
            )
            .expect("error writing table data");
    }
}
