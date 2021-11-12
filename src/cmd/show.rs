use std::sync::Arc;

use clap::{App, ArgMatches, SubCommand};

use crate::client::kafky_client::KafkyClient;
use crate::errors::KafkyError;
use crate::KafkyCmd;

impl KafkyCmd {
    pub fn show_sub_command<'a>(&self) -> App<'a, 'a> {
        SubCommand::with_name("show")
            .about("Show kafka information")
            .subcommand(SubCommand::with_name("topics")
                .about("retrieve kafka topic names"))
    }
    pub fn show_exec(&self, app_matches: &ArgMatches, kafky_client: Arc<KafkyClient>) -> Result<(), KafkyError> {
        if app_matches.subcommand_matches("topics").is_some() {
            for topic in kafky_client.get_metadata(None)?.topics {
                println!("{}", topic)
            }
        }
        Ok(())
    }
}

