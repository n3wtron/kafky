use std::ffi::OsString;

use crate::config::KafkyConfig;
use clap::{App, Arg};
use gethostname::gethostname;

use crate::KafkyError;

pub(crate) struct KafkyCmd<'a> {
    hostname: OsString,
    config: &'a KafkyConfig,
}

impl<'a> KafkyCmd<'a> {
    pub fn new(cfg: &'a KafkyConfig) -> Result<Self, KafkyError> {
        Ok(Self {
            hostname: gethostname(),
            config: cfg,
        })
    }

    pub fn create_command(&'a self) -> App<'a, 'a> {
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
            .subcommand(self.show_sub_command())
            .subcommand(self.produce_sub_command())
            .subcommand(self.consume_sub_command())
    }
    pub fn hostname(&self) -> &OsString {
        &self.hostname
    }
}
