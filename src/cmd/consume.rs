use std::str::FromStr;
use std::sync::Arc;
use clap::{App, Arg, ArgGroup, ArgMatches, SubCommand};
use log::{error};
use crate::client::consumer::{KafkyConsumerOffset};
use crate::client::kafky_client::KafkyClient;
use crate::errors::KafkyError;
use crate::KafkyCmd;

impl KafkyCmd {
    pub fn consume_sub_command(&self) -> App {
        let offset_values: Vec<&str> = KafkyConsumerOffset::values_str();
        let offset_args: Vec<Arg> = KafkyConsumerOffset::values_str().iter()
            .map(|offset_name| Arg::with_name(offset_name).long(offset_name))
            .collect();

        SubCommand::with_name("consume")
            .about("Consume messages from a topic")
            .arg(Arg::with_name("topic")
                .short("t")
                .multiple(true)
                .long("topic")
                .required(true)
                .value_name("TOPIC_NAME"))
            .arg(Arg::with_name("consumerGroup")
                .short("c")
                .required(true)
                .default_value_os(self.hostname())
                .long("consumer-group")
                .value_name("CONSUMER GROUP NAME"))
            .args(offset_args.as_slice())
            .group(ArgGroup::with_name("offset")
                .required(true)
                .args(offset_values.as_slice())
            )
    }


    pub fn consume_exec(&self, app_matches: &ArgMatches<'_>, kafky_client: Arc<KafkyClient>) -> Result<(), KafkyError> {
        let topics: Vec<&str> = app_matches.values_of("topic").unwrap().collect();
        let consumer_group = app_matches.value_of("consumerGroup").unwrap();
        let offset = KafkyCmd::extract_offset_from_arg(app_matches)?;
        kafky_client.consume(topics, consumer_group, offset, |msg_result| {
            match msg_result {
                Ok(msg) => { println!("{:?}", msg) }
                Err(err) => { error!("error: {:?}",err) }
            }
        })
    }

    fn extract_offset_from_arg(app_matches: &ArgMatches<'_>) -> Result<KafkyConsumerOffset, KafkyError> {
        KafkyConsumerOffset::values_str().iter()
            .find(|offset_name| app_matches.is_present(offset_name))
            .ok_or(KafkyError::InvalidOffset())
            .and_then(|offset_name| KafkyConsumerOffset::from_str(&offset_name).map_err(|_| KafkyError::InvalidOffset()))
    }
}



