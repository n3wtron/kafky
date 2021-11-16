use std::sync::Arc;

use crate::config::KafkyConfig;
use clap::{App, Arg, ArgMatches};

use crate::cmd::config::ConfigCmd;
use crate::cmd::consume::ConsumeCmd;
use crate::cmd::get::GetCmd;
use crate::cmd::produce::ProduceCmd;
use crate::{KafkyClient, KafkyError};

pub struct RootCmd {}

impl RootCmd {
    pub fn command<'a>(config: &'a KafkyConfig) -> App<'a, 'a> {
        let environments: Vec<&str> = config
            .environments
            .iter()
            .map(|e| e.name.as_str())
            .collect();
        App::new("Kafky")
            .version("0.1")
            .author("Igor Maculan <n3wtron@gmail.com>")
            .about("Kafka terminal client")
            .arg(
                Arg::with_name("environment")
                    .long("environment")
                    .short("e")
                    .possible_values(environments.as_slice())
                    .required(true)
                    .value_name("STRING")
                    .help("environment"),
            )
            .arg(
                Arg::with_name("credential")
                    .long("credential")
                    .short("c")
                    .value_name("STRING")
                    .help("environment"),
            )
            .subcommand(ConsumeCmd::command())
            .subcommand(GetCmd::command())
            .subcommand(ProduceCmd::command())
            .subcommand(ConsumeCmd::command())
            .subcommand(ConfigCmd::command())
    }

    pub fn exec(app_matches: ArgMatches, config: &KafkyConfig) -> Result<(), KafkyError> {
        let environment = String::from(app_matches.value_of("environment").unwrap());
        let credential = Self::extract_credential(&app_matches, config, &environment)?;
        let kafky_client = Arc::new(KafkyClient::new(config, environment, credential));

        match app_matches.subcommand() {
            ("get", Some(matches)) => GetCmd::exec(matches, kafky_client.clone()),
            ("produce", Some(matches)) => ProduceCmd::exec(matches, kafky_client.clone()),
            ("consume", Some(matches)) => ConsumeCmd::exec(matches, kafky_client.clone()),
            ("config", Some(matches)) => ConfigCmd::exec(matches, config.path()),
            (_, _) => Err(KafkyError::InvalidCommand()),
        }
    }

    fn extract_credential(
        app_matches: &ArgMatches,
        config: &KafkyConfig,
        environment: &String,
    ) -> Result<String, KafkyError> {
        app_matches
            .value_of("credential")
            .map(|cred| cred.to_string())
            .or(config.get_environment(environment).and_then(|e| {
                if e.credentials.len() == 1 {
                    let first_credential = e.credentials.first().map(|c| c.name.clone()).unwrap();
                    Some(first_credential)
                } else {
                    None
                }
            }))
            .ok_or(KafkyError::NoCredentialSpecified())
    }
}
