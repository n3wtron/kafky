use std::ffi::OsString;
use std::sync::Arc;
use clap::{App, Arg};
use gethostname::gethostname;
use crate::KafkyError;

pub(crate) struct KafkyCmd {
    hostname: OsString,
}

impl<'a> KafkyCmd {

    pub fn new() -> Result<Self, KafkyError> {
        Ok(Self {
            hostname: gethostname(),
        })
    }

    pub fn create_command(&'a self) -> App<'a, 'a> {
        App::new("Kafky")
            .version("0.1")
            .author("Igor Maculan <n3wtron@gmail.com>")
            .about("Kafka terminal client")
            .arg(Arg::with_name("environment")
                .long("environment")
                .short("e")
                .required(true)
                .value_name("STRING")
                .help("environment")
            )
            .arg(Arg::with_name("credential")
                .long("credential")
                .short("c")
                .value_name("STRING")
                .help("environment")
            )
            .subcommand(self.show_sub_command())
            .subcommand(self.produce_sub_command())
            .subcommand(self.consume_sub_command())
    }
    pub fn hostname(&self) -> &OsString {
        &self.hostname
    }
}
