use crate::config::KafkyConfig;
use clap::{App, Arg, ArgMatches};
use log::debug;
use tokio::signal;

use crate::cmd::config::ConfigCmd;
use crate::cmd::consume::ConsumeCmd;
use crate::cmd::create::CreateCmd;
use crate::cmd::delete::DeleteCmd;
use crate::cmd::get::GetCmd;
use crate::cmd::produce::ProduceCmd;
use crate::{KafkyClient, KafkyError};
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;

const VERSION: &str = env!("CARGO_PKG_VERSION");

pub struct RootCmd {}

impl RootCmd {
    pub fn command<'a>(config: &'a KafkyConfig) -> App<'a, 'a> {
        let environments: Vec<&str> = config
            .environments
            .iter()
            .map(|e| e.name.as_str())
            .collect();
        App::new("Kafky")
            .version(VERSION)
            .about("Kafka terminal client")
            .arg(
                Arg::with_name("environment")
                    .long("environment")
                    .short("e")
                    .possible_values(environments.as_slice())
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
            .subcommand(CreateCmd::command())
            .subcommand(DeleteCmd::command())
    }

    pub async fn exec<'a>(
        app_matches: ArgMatches<'a>,
        config: &'a KafkyConfig<'a>,
    ) -> Result<(), KafkyError> {
        let sub_command_tpl = app_matches.subcommand();
        if sub_command_tpl.1.is_none() {
            return Err(KafkyError::InvalidCommand());
        }
        debug!("sub command:{:?}", sub_command_tpl);
        if sub_command_tpl.0 == "config" {
            return ConfigCmd::exec(sub_command_tpl.1.unwrap(), config.path());
        }
        if !app_matches.is_present("environment") {
            return Err(KafkyError::EnvironmentParamNotFound());
        }
        let environment = String::from(app_matches.value_of("environment").unwrap());
        let credential = Self::extract_credential(&app_matches, config, &environment)?;

        let kafky_client = KafkyClient::new(config, environment, credential);
        let close_rx = Self::termination_receiver();

        tokio::select! {
            result = async {match sub_command_tpl {
            ("get", Some(matches)) => GetCmd::exec(matches, &kafky_client).await,
            ("produce", Some(matches)) => ProduceCmd::exec(matches, &kafky_client).await,
            ("consume", Some(matches)) => ConsumeCmd::exec(matches, &kafky_client).await,
            ("create", Some(matches)) => CreateCmd::exec(matches, &kafky_client).await,
            ("delete", Some(matches)) => DeleteCmd::exec(matches, &kafky_client).await,
            (_, _) => Err(KafkyError::InvalidCommand()),
        }} =>{result},
            _ = close_rx => {
                println!("Exiting...");
                Ok(())
            }
        }
    }

    fn termination_receiver() -> Receiver<bool> {
        let (close_tx, close_rx) = oneshot::channel();
        tokio::spawn(async move {
            signal::ctrl_c().await.expect("failed to handle ctrl+c");

            close_tx
                .send(true)
                .expect("failed to propagate exit signal");
        });
        close_rx
    }

    fn extract_credential(
        app_matches: &ArgMatches,
        config: &KafkyConfig,
        environment: &str,
    ) -> Result<String, KafkyError> {
        app_matches
            .value_of("credential")
            .map(|cred| cred.to_string())
            .or_else(|| {
                config.get_environment(environment).and_then(|e| {
                    if e.credentials.len() == 1 {
                        let first_credential =
                            e.credentials.first().map(|c| c.name.clone()).unwrap();
                        Some(first_credential)
                    } else {
                        None
                    }
                })
            })
            .ok_or_else(|| {
                KafkyError::NoCredentialSpecified(
                    config
                        .get_environment(environment)
                        .map(|e| e.get_credential_names().join(","))
                        .unwrap(),
                )
            })
    }
}
