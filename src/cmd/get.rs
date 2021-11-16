use std::sync::Arc;

use clap::{App, ArgMatches, SubCommand};

use crate::client::kafky_client::KafkyClient;
use crate::cmd::get_consumer_groups::GetConsumerGroupsCmd;
use crate::cmd::get_topic::GetTopicCmd;
use crate::errors::KafkyError;

pub(crate) struct GetCmd {}

impl GetCmd {
    pub fn command<'a>() -> App<'a, 'a> {
        SubCommand::with_name("get")
            .about("Show kafka information")
            .subcommand(GetTopicCmd::command())
            .subcommand(GetConsumerGroupsCmd::command())
    }

    pub fn exec(
        app_matches: &ArgMatches,
        kafky_client: Arc<KafkyClient>,
    ) -> Result<(), KafkyError> {
        if let Some(get_topic_args) = app_matches.subcommand_matches("topics") {
            return GetTopicCmd::exec(get_topic_args, kafky_client);
        }
        if let Some(get_consumer_groups_args) = app_matches.subcommand_matches("consumer-groups") {
            return GetConsumerGroupsCmd::exec(get_consumer_groups_args, kafky_client);
        }
        Self::command().print_help().expect("error printing get helpl");
        Ok(())
    }
}
