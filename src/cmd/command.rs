use std::ffi::OsString;

use std::sync::Arc;

use crate::config::KafkyConfig;
use clap::{App, Arg, ArgMatches};
use gethostname::gethostname;

use crate::{KafkyClient, KafkyError};

pub(crate) struct KafkyCmd<'a> {
    hostname: OsString,
    config: &'a KafkyConfig<'a>,
}

impl<'a> KafkyCmd<'a> {
    pub fn new(cfg: &'a KafkyConfig) -> Result<Self, KafkyError> {
        Ok(Self {
            hostname: gethostname(),
            config: cfg,
        })
    }

    fn create_app(&'a self) -> App<'a, 'a> {
        let environments: Vec<&str> = self
            .config
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
            .subcommand(self.get_sub_command())
            .subcommand(self.produce_sub_command())
            .subcommand(self.consume_sub_command())
            .subcommand(self.consume_sub_command())
            .subcommand(self.config_sub_command())
    }
    pub fn hostname(&self) -> &OsString {
        &self.hostname
    }

    pub fn exec(&self) -> Result<(), KafkyError> {
        let app = self.create_app();
        let app_matches = &app.get_matches();

        let environment = String::from(app_matches.value_of("environment").unwrap());
        let credential = self.extract_credential(&app_matches, &environment)?;
        let kafky_client = Arc::new(KafkyClient::new(self.config, environment, credential));

        match app_matches.subcommand() {
            ("get", Some(matches)) => self.get_exec(matches, kafky_client.clone()),
            ("produce", Some(matches)) => self.produce_exec(matches, kafky_client.clone()),
            ("consume", Some(matches)) => self.consume_exec(matches, kafky_client.clone()),
            ("config", Some(matches)) => self.config_exec(matches, self.config.path()),
            (_, _) => Err(KafkyError::InvalidCommand()),
        }
    }

    pub fn print_help(&self) {
        self.create_app().print_help().expect("app help error");
    }

    fn extract_credential(
        &self,
        app_matches: &ArgMatches,
        environment: &String,
    ) -> Result<String, KafkyError> {
        app_matches
            .value_of("credential")
            .map(|cred| cred.to_string())
            .or(self.config.get_environment(environment).and_then(|e| {
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
