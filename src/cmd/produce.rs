use std::io;
use std::io::Write;
use std::sync::Arc;

use clap::{App, Arg, ArgMatches, SubCommand};
use log::debug;

use crate::client::kafky_client::KafkyClient;
use crate::errors::KafkyError;
use crate::KafkyCmd;

impl KafkyCmd {
    pub fn produce_sub_command<'a>(&self) -> App<'a, 'a> {
        SubCommand::with_name("produce")
            .about("Produce messages to a topic")
            .arg(Arg::with_name("topic")
                .short("t")
                .long("topic")
                .required(true)
                .value_name("TOPIC_NAME"))
    }
    pub fn produce_exec(&self, app_matches: &ArgMatches, kafky_client: Arc<KafkyClient>) -> Result<(), KafkyError> {
        let topic = app_matches.value_of("topic").unwrap();

        let mut read_line = String::new();
        loop {
            print!("{}> ", topic);
            io::stdout().flush().unwrap();
            match io::stdin().read_line(&mut read_line) {
                Ok(size) => {
                    if size == 0 {
                        break;
                    }
                    read_line.pop();
                    match kafky_client.produce(&topic, None, read_line.clone()) {
                        Ok(_) => { debug!("message sent to topic {}",topic) }
                        Err(error) => {
                            return Err(error);
                        }
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }
        Ok(())
    }
}

