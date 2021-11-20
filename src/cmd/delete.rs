use crate::{KafkyClient, KafkyError};
use clap::{App, Arg, ArgMatches, SubCommand};
use std::io::stdin;

pub struct DeleteCmd {}

impl DeleteCmd {
    pub(super) fn command<'a>() -> App<'a, 'a> {
        SubCommand::with_name("delete")
            .about("delete kafka objects")
            .subcommand(
                SubCommand::with_name("topics")
                    .alias("topic")
                    .about("delete topics")
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
                        Arg::with_name("yes")
                            .short("y")
                            .help("without confirmation"),
                    ),
            )
    }

    pub(super) async fn exec<'a>(
        arg_matches: &'a ArgMatches<'a>,
        kafky_client: &'a KafkyClient<'a>,
    ) -> Result<(), KafkyError> {
        if let Some(create_topic_args) = arg_matches.subcommand_matches("topics") {
            let topic_names: Vec<&str> = create_topic_args.values_of("topic").unwrap().collect();
            let auto_yes = create_topic_args.is_present("yes");
            if !auto_yes {
                println!(
                    "Are you sure that you want to delete the {} topics? [y/N]",
                    topic_names.join(", ")
                );
                let mut answer = String::new();
                stdin().read_line(&mut answer).expect("error reading line");
                if !answer.trim().eq_ignore_ascii_case("Y") {
                    return Ok(());
                }
            }
            let metadata = kafky_client.get_metadata(None)?;
            let existent_topic_names = metadata.topic_names();
            let topics_to_delete: Vec<&str> = topic_names
                .iter()
                .filter(|new_topic| existent_topic_names.contains(&new_topic.to_string()))
                .copied()
                .collect();
            topic_names
                .iter()
                .filter(|request_topic| !existent_topic_names.contains(&request_topic.to_string()))
                .for_each(|already_existent_topic| {
                    println!("topic {} doesn't exist", already_existent_topic)
                });
            if !topics_to_delete.is_empty() {
                kafky_client.delete_topics(&topics_to_delete).await?;
                topics_to_delete
                    .iter()
                    .for_each(|deleted_topic| println!("topic {} deleted", deleted_topic));
            }
        } else {
            Self::command().print_help().expect("error printing help");
        }
        Ok(())
    }
}
