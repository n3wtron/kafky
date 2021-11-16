use crate::{KafkyClient, KafkyError};
use clap::{App, Arg, ArgMatches, SubCommand};
use std::sync::Arc;

pub struct CreateCmd {}

impl CreateCmd {
    pub(super) fn command<'a>() -> App<'a, 'a> {
        SubCommand::with_name("create")
            .about("create kafka objects")
            .subcommand(
                SubCommand::with_name("topics")
                    .alias("topic")
                    .about("create a new topic")
                    .arg(
                        Arg::with_name("topic")
                            .long("topic")
                            .short("t")
                            .takes_value(true)
                            .required(true)
                            .multiple(true)
                            .help("topic name"),
                    )
                    .arg(
                        Arg::with_name("partitions")
                            .long("partitions")
                            .short("p")
                            .takes_value(true)
                            .required(true)
                            .help("number of partitions"),
                    )
                    .arg(
                        Arg::with_name("replication_factor")
                            .long("replication-factor")
                            .short("r")
                            .takes_value(true)
                            .required(true)
                            .help("replication factor"),
                    ),
            )
    }

    pub(super) async fn exec<'a>(
        arg_matches: &'a ArgMatches<'a>,
        kafky_client: Arc<KafkyClient<'a>>,
    ) -> Result<(), KafkyError> {
        if let Some(create_topic_args) = arg_matches.subcommand_matches("topics") {
            let topic_names: Vec<&str> = create_topic_args.values_of("topic").unwrap().collect();
            let partitions: i32 = create_topic_args
                .value_of("partitions")
                .unwrap()
                .parse()
                .expect("invalid partitions value");
            let replication_factor: i32 = create_topic_args
                .value_of("replication_factor")
                .unwrap()
                .parse()
                .expect("invalid replication-factor value");

            let metadata = kafky_client.get_metadata(None)?;
            let existent_topic_names: Vec<String> = metadata.topic_names();
            let topics_to_create: Vec<&str> = topic_names
                .iter()
                .filter(|new_topic| !existent_topic_names.contains(&new_topic.to_string()))
                .copied()
                .collect();
            existent_topic_names
                .iter()
                .filter(|existent_topic| topic_names.contains(&&***existent_topic))
                .for_each(|already_existent_topic| {
                    println!("topic {} already exist", already_existent_topic)
                });
            if !topics_to_create.is_empty() {
                kafky_client
                    .create_topics(&topics_to_create, partitions, replication_factor)
                    .await?;
                topics_to_create
                    .iter()
                    .for_each(|created_topic| println!("topic {} created", created_topic));
            }
        } else {
            Self::command().print_help().expect("error printing help");
        }
        Ok(())
    }
}
