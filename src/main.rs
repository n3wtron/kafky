mod cmd;
mod errors;
mod config;
mod client;

use std::io;
use clap::{App, Arg, ArgMatches, SubCommand};
use std::string::String;
use std::sync::Arc;
use errors::KafkyError;
use client::kafky_client::KafkyClient;
use crate::config::KafkyConfig;
use tokio::sync::mpsc;
use cmd::{produce, show};
use crate::cmd::command::KafkyCmd;
use crate::cmd::{command, consume};

#[tokio::main]
async fn main() -> Result<(), errors::KafkyError> {
    env_logger::init();

    let mut home_folder = home::home_dir().map(|home| home.into_os_string()).ok_or(KafkyError::HomeFolderNotFound())?;
    home_folder.push("/.kafky.yml");
    let cfg = config::KafkyConfig::new(home_folder.to_str().unwrap())?;

    let command = KafkyCmd::new()?;
    let app = command.create_command();
    let app_matches = app.get_matches();

    let environment = String::from(app_matches.value_of("environment").unwrap());
    let credential = get_credential(&app_matches, &cfg, &environment)?;
    let kafky_client = Arc::new(KafkyClient::new(cfg, environment, credential));

    match app_matches.subcommand() {
        ("show", Some(matches)) => { command.show_exec(matches, kafky_client.clone()) }
        ("produce", Some(matches)) => { command.produce_exec(matches, kafky_client.clone()) }
        ("consume", Some(matches)) => { command.consume_exec(matches, kafky_client.clone()) }
        (_, _) => Err(KafkyError::InvalidCommand())
    }
}

fn get_credential(app_matches: &ArgMatches, cfg: &KafkyConfig, environment: &String) -> Result<String, KafkyError> {
    app_matches.value_of("credential")
        .map(|cred| cred.to_string())
        .or(cfg.get_environment(environment)
            .and_then(|e| {
                if e.credentials.len() == 1 {
                    let first_credential = e.credentials.first().map(|c| c.name.clone()).unwrap();
                    return Some(first_credential);
                }
                return None;
            })
        ).ok_or(KafkyError::NoCredentialSpecified())
}
