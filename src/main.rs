use crate::cmd::config::ConfigCmd;
use crate::cmd::root::RootCmd;
use crate::config::KafkyConfig;
use client::kafky_client::KafkyClient;
use errors::KafkyError;
use log::debug;
use std::cell::RefCell;
use std::path::Path;

mod client;
mod cmd;
mod config;
mod errors;

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::init();

    let mut config_path = home::home_dir().expect("impossible to get the home folder");
    debug!("home folder:{:?}", config_path);
    config_path.push(".kafky/config.yml");

    let cfg = load_config_or_create(&config_path).expect("configuration not found");

    let command = RefCell::new(RootCmd::command(&cfg));
    let mut usage_command = command.clone();
    match RootCmd::exec(command.into_inner().get_matches(), &cfg).await {
        Ok(_) => Ok(()),
        Err(e) => match e {
            KafkyError::InvalidCommand() => {
                usage_command
                    .get_mut()
                    .print_help()
                    .expect("cannot print help message");
                Err("".to_string())
            }
            kafky_error => Err(format!("{}", kafky_error)),
        },
    }
}

fn load_config_or_create(config_path: &Path) -> Result<KafkyConfig, KafkyError> {
    match config::KafkyConfig::load(&config_path) {
        Ok(config) => Ok(config),
        Err(e) => match e {
            KafkyError::ConfigurationNotFound(_) => {
                match KafkyConfig::create_configuration_sample(&config_path) {
                    Ok(cfg) => ConfigCmd::open_editor(&config_path).map(|_| cfg),
                    Err(e2) => Err(e2),
                }
            }
            _ => Err(e),
        },
    }
}
